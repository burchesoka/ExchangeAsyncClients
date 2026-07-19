import logging
import time
import asyncio
import json
from abc import ABC, abstractmethod

import aiohttp
from aiolimiter import AsyncLimiter

import exceptions


logger = logging.getLogger(__name__)


class BaseAsyncExchangeAPI(ABC):
    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_secret: str,
        api_key: str,
        limiters: dict[str, AsyncLimiter],
        limiters_dict: dict[str, str],
    ):
        self.session = session
        self.api_key = api_key
        self.api_secret = api_secret
        self.limiters = limiters
        self.limiters_dict = limiters_dict
        self.limits_updated = {}

    def _limiter_for(self, endpoint: str) -> AsyncLimiter:
        limit_key = self.limiters_dict.get(endpoint)
        if limit_key and limit_key in self.limiters:
            return self.limiters[limit_key]

        # Без явного правила берём самый консервативный лимитер.
        min_limit_key = str(min(int(key) for key in self.limiters.keys()))
        return self.limiters[min_limit_key]

    def update_limits(self, endpoint: str, resp_headers_limit):
        if resp_headers_limit is None:
            return

        resp_headers_limit = str(resp_headers_limit)
        if resp_headers_limit not in self.limiters:
            return
        if endpoint not in self.limiters_dict:
            return

        if self.limiters_dict[endpoint] != resp_headers_limit:
            logger.info(
                "Limits update for %s. %s --> %s",
                endpoint,
                self.limiters_dict[endpoint],
                resp_headers_limit,
            )
            self.limiters_dict[endpoint] = resp_headers_limit

    def update_limits_after_error(self, endpoint: str):
        endpoint_updated = self.limits_updated.get(endpoint)
        if endpoint_updated and time.time() - endpoint_updated < 60 * 60:
            return

        current_limit = int(self.limiters_dict[endpoint])
        all_limits = [int(k) for k in self.limiters.keys()]
        limits_lower = [lim for lim in all_limits if lim < current_limit]
        if not limits_lower:
            return

        new_limit = str(max(limits_lower))
        self.update_limits(endpoint, new_limit)
        self.limits_updated[endpoint] = time.time()

    @abstractmethod
    def _build_request_data(
        self,
        method: str,
        endpoint: str,
        params: dict | None,
        signed: bool,
    ) -> tuple[str, dict | None, dict | None, str | None]:
        """Вернуть (url, query_params, headers, body)."""

    def _extract_limit_from_headers(self, headers: "aiohttp.typedefs.LooseHeaders"):
        return None

    def _request_weight(
        self,
        endpoint: str,
        method: str,
        params: dict | None,
    ) -> int:
        return 1

    @abstractmethod
    def _is_success_response(self, response: dict, status_code: int) -> bool:
        """True, если ответ успешен для конкретной биржи."""

    @abstractmethod
    async def _handle_error_response(
        self,
        response: dict,
        status_code: int,
        url: str,
        method: str = "",
    ):
        """Бросает биржеспецифичные исключения для ошибочного ответа."""

    async def _check_response(self, response: dict, status_code: int, url: str, method: str = ""):
        if self._is_success_response(response, status_code):
            return
        await self._handle_error_response(response, status_code, url, method=method)
        raise exceptions.RequestError

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: dict | None = None,
        signed: bool = True,
        retries: int = 70,
        network_sleep_seconds: int = 3,
        timeout_sleep_seconds: int = 1,
        ratelimit_sleep_seconds: int = 30,
    ):
        last_error = None
        limiter = self._limiter_for(endpoint)
        async with limiter:
            while retries:
                url = ""
                try:
                    request_weight = int(self._request_weight(endpoint=endpoint, method=method, params=params))
                    if request_weight < 1:
                        request_weight = 1

                    # 1 токен уже захвачен через `async with limiter`.
                    # Для weight-based throttling добираем остальные токены.
                    if request_weight > 1:
                        remaining = request_weight - 1
                        max_chunk = int(getattr(limiter, "max_rate", 1)) or 1
                        logger.debug("acquire remaining=%s max_chunk=%s", remaining, max_chunk)
                        while remaining > 0:
                            chunk = min(remaining, max_chunk)
                            await limiter.acquire(chunk)
                            remaining -= chunk

                    url, query_params, headers, body = self._build_request_data(
                        method=method,
                        endpoint=endpoint,
                        params=params,
                        signed=signed,
                    )

                    async with self.session.request(
                        method=method,
                        url=url,
                        params=query_params,
                        headers=headers,
                        data=body,
                    ) as resp:
                        logger.debug('resp: %s %s %s %s %s', resp, resp.status, url, params, body)
                        logger.debug('request_weight %s %s %s %s', request_weight, method, endpoint, params)
                        logger.debug('%s level %s ', self.limiters_dict[endpoint], limiter._level)
                        resp_headers_limit = self._extract_limit_from_headers(resp.headers)
                        if isinstance(resp_headers_limit, dict):
                            resp_headers_limit.setdefault("method", method)
                            resp_headers_limit.setdefault("params", params or {})
                            resp_headers_limit.setdefault("request_weight", request_weight)
                        self.update_limits(endpoint, resp_headers_limit)
                        response_text = await resp.text()
                        response_data: dict
                        try:
                            response_data = await resp.json()
                        except json.JSONDecodeError:
                            # Некоторые биржи/CDN на 5xx возвращают HTML/plain text без JSON.
                            response_data = {
                                "code": resp.status,
                                "msg": (
                                    f"Non-JSON response (content_type={resp.content_type!r}): "
                                    f"{response_text[:500]}"
                                ),
                            }
                        await self._check_response(response_data, resp.status, url, method=method)
                        return response_data


                except aiohttp.client.ClientConnectorError as e:
                    retries -= 1
                    last_error = "ClientConnectorError"
                    logger.critical("ClientConnectorError retries=%s url=%s err=%s", retries, url, e)
                    if not retries:
                        raise exceptions.NetworkError from e
                    await asyncio.sleep(network_sleep_seconds)

                except aiohttp.client.ClientOSError as e:
                    if "Connection reset by peer" not in str(e):
                        logger.critical("Unexpected ClientOSError %s", e)
                        raise
                    retries -= 1
                    last_error = "ClientOSError"
                    logger.critical("ClientOSError retries=%s url=%s err=%s", retries, url, e)
                    if not retries:
                        raise exceptions.NetworkError from e
                    await asyncio.sleep(network_sleep_seconds)

                except asyncio.TimeoutError as e:
                    retries -= 1
                    last_error = "TimeoutError"
                    logger.critical("TimeoutError retries=%s url=%s", retries, url)
                    if not retries:
                        raise exceptions.NetworkError from e
                    await asyncio.sleep(timeout_sleep_seconds)

                except exceptions.InvalidNonce:
                    retries -= 1
                    last_error = "InvalidNonce"
                    logger.critical("InvalidNonce retries=%s url=%s", retries, url)
                    if not retries:
                        raise
                    await asyncio.sleep(network_sleep_seconds)

                except exceptions.RateLimitExceeded as e:
                    retries -= 1
                    last_error = "RateLimitExceeded"
                    logger.warning("RateLimitExceeded retries=%s url=%s", retries, url)
                    self.update_limits_after_error(endpoint)
                    if not retries:
                        raise
                    sleep_seconds = getattr(e, "retry_after_seconds", ratelimit_sleep_seconds)
                    await asyncio.sleep(sleep_seconds)

                except exceptions.ServerError:
                    retries -= 1
                    last_error = "ServerError"
                    logger.warning("ServerError retries=%s url=%s", retries, url)
                    if not retries:
                        raise
                    await asyncio.sleep(max(network_sleep_seconds, 5))

        raise Exception(f"Request failed: method={method} endpoint={endpoint} error={last_error}")
