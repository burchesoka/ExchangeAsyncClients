from aiolimiter import AsyncLimiter
import aiohttp
import hashlib
import hmac
import logging
import re
import time
from urllib.parse import urlencode

import exceptions
from base_async_exchange_api import BaseAsyncExchangeAPI


logger = logging.getLogger(__name__)


class BingxAPI(BaseAsyncExchangeAPI):
    """REST BingX (open-api.bingx.com): ответы вида {\"code\":0,\"msg\":\"\",\"data\":...}."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_secret: str,
        api_key: str,
        broker_id: str | None = None,
    ):
        self.recv_window = "5000"
        self.recv_window_shift = 0
        self.broker_id = broker_id
        self.base_url = "https://open-api.bingx.com"
        limiters = {
            "10": AsyncLimiter(10, 1),
            "20": AsyncLimiter(20, 1),
            "50": AsyncLimiter(50, 1),
            "5": AsyncLimiter(5, 1),
        }
        limiters_dict = {
            "/openApi/swap/v2/server/time": "10",
            "/openApi/swap/v2/quote/contracts": "10",
            "/openApi/swap/v2/quote/klines": "10",
            "/openApi/v1/account/apiPermissions": "5",
            "/openApi/swap/v2/user/balance": "5",
            "/openApi/swap/v2/user/positions": "5",
            "/openApi/swap/v2/user/income": "5",
            "/openApi/swap/v2/trade/order": "10",
            "/openApi/swap/v2/trade/openOrders": "10",
            "/openApi/swap/v2/trade/allOrders": "10",
            "/openApi/swap/v2/trade/allOpenOrders": "10",
            "/openApi/swap/v2/trade/leverage": "5",
            "/openApi/swap/v2/trade/marginType": "5",
            "/openApi/swap/v1/positionSide/dual": "5",
            "/openApi/swap/v2/trade/allFillOrders": "10",
            "/openApi/api/v3/capital/deposit/hisrec": "10",
        }
        super().__init__(
            session=session,
            api_secret=api_secret,
            api_key=api_key,
            limiters=limiters,
            limiters_dict=limiters_dict,
        )

    def _stringify_param(self, value) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def _sign_payload(self, params: dict) -> str:
        pairs = sorted((k, self._stringify_param(v)) for k, v in params.items() if k != "signature")
        query = "&".join(f"{k}={v}" for k, v in pairs)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _merge_signed_params(self, params: dict | None) -> dict:
        merged = dict(params or {})
        merged["timestamp"] = int(time.time() * 1000) + self.recv_window_shift
        merged["recvWindow"] = int(self.recv_window)
        return merged

    def _normalize_bingx_response(self, resp: dict | list | None):
        if not isinstance(resp, dict) or "code" not in resp:
            return resp
        code = resp.get("code")
        ok = code == 0 or code == "0"
        data = resp.get("data")
        if isinstance(data, list):
            result = {"list": data, "nextPageCursor": None}
        elif isinstance(data, dict):
            result = data
        else:
            result = data
        ret_code = 0 if ok else (int(code) if str(code).lstrip("-").isdigit() else -1)
        return {
            "retCode": ret_code,
            "retMsg": "OK" if ok else (resp.get("msg") or resp.get("message") or ""),
            "result": result,
        }

    async def update_recv_window_shift(self):
        time_now = int(time.time() * 1000)
        raw = await self.public_get_request("/openApi/swap/v2/server/time")
        if not isinstance(raw, dict):
            return
        data = raw.get("result") or raw.get("data") or raw
        if not isinstance(data, dict):
            return
        server_time = data.get("serverTime") or data.get("timestamp") or time_now
        try:
            server_time = int(server_time)
        except (TypeError, ValueError):
            server_time = time_now
        difference = time_now - server_time
        self.recv_window_shift = difference - 500
        logger.info("recv_window_shift updated: %s", self.recv_window_shift)

    async def get_request(self, endpoint: str, params: dict | None = None, retries: int = 70):
        data = await self._request(
            method="GET",
            endpoint=endpoint,
            params=params,
            signed=True,
            retries=retries,
            network_sleep_seconds=5,
            timeout_sleep_seconds=1,
            ratelimit_sleep_seconds=30,
        )
        return self._normalize_bingx_response(data)

    async def post_request(self, endpoint, body: dict, retries: int = 70):
        data = await self._request(
            method="POST",
            endpoint=endpoint,
            params=body,
            signed=True,
            retries=retries,
            network_sleep_seconds=5,
            timeout_sleep_seconds=1,
            ratelimit_sleep_seconds=30,
        )
        return self._normalize_bingx_response(data)

    async def delete_request(self, endpoint: str, params: dict | None = None, retries: int = 70):
        data = await self._request(
            method="DELETE",
            endpoint=endpoint,
            params=params,
            signed=True,
            retries=retries,
            network_sleep_seconds=5,
            timeout_sleep_seconds=1,
            ratelimit_sleep_seconds=30,
        )
        return self._normalize_bingx_response(data)

    async def public_get_request(self, endpoint: str, params: dict | None = None, retries: int = 70):
        data = await self._request(
            method="GET",
            endpoint=endpoint,
            params=params,
            signed=False,
            retries=retries,
            network_sleep_seconds=5,
            timeout_sleep_seconds=1,
            ratelimit_sleep_seconds=30,
        )
        return self._normalize_bingx_response(data)

    def _build_request_data(
        self,
        method: str,
        endpoint: str,
        params: dict | None,
        signed: bool,
    ) -> tuple[str, dict | None, dict | None, str | None]:
        if not signed:
            query_params = params or {}
            query_string = urlencode(sorted(query_params.items())) if query_params else ""
            url = f"{self.base_url}{endpoint}"
            if query_string:
                url = f"{url}?{query_string}"
            return url, None, {}, None

        merged = self._merge_signed_params(params)
        merged["signature"] = self._sign_payload(merged)
        query_string = urlencode(sorted((k, self._stringify_param(v)) for k, v in merged.items()))
        url = f"{self.base_url}{endpoint}?{query_string}"
        headers = {"X-BX-APIKEY": self.api_key}
        if self.broker_id:
            headers["X-SOURCE-KEY"] = self.broker_id
        return url, None, headers, None

    def _extract_limit_from_headers(self, headers):
        return dict(headers).get("X-RateLimit-Remaining")

    def _is_success_response(self, response: dict, status_code: int) -> bool:
        if status_code >= 400:
            return False
        if not isinstance(response, dict):
            return True
        if "code" not in response:
            return True
        code = response.get("code")
        return code == 0 or code == "0"

    async def _handle_error_response(self, response: dict, status_code: int, url: str):
        logger.debug("BingX API error url=%s response=%s", url, response)
        msg = str(response.get("msg") or response.get("message") or "")
        code = response.get("code")
        if code in (100410, "100410"):
            # BingX может временно блокировать endpoint и отдавать timestamp разблокировки.
            # Пример: "... will be unblocked after 1778064888458".
            retry_after_seconds = 30
            match = re.search(r"unblocked after\s+(\d{10,13})", msg.lower())
            if match:
                try:
                    unblock_at_ms = int(match.group(1))
                    now_ms = int(time.time() * 1000)
                    retry_after_seconds = max(1, min(300, int((unblock_at_ms - now_ms) / 1000) + 1))
                except (TypeError, ValueError):
                    retry_after_seconds = 30
            error = exceptions.RateLimitExceeded(msg or "BingX temporary frequency block")
            setattr(error, "retry_after_seconds", retry_after_seconds)
            logger.warning(
                "BingX temporary frequency block for endpoint, retry in %ss url=%s",
                retry_after_seconds,
                url,
            )
            raise error
        if code in (100001, "100001") or "signature" in msg.lower():
            await self.update_recv_window_shift()
            raise exceptions.InvalidNonce
        if code in (100202, "100202") or "balance" in msg.lower():
            raise exceptions.MarginInsufficient
        if code in (100400, "100400"):
            raise exceptions.OrderValidationError
        if code in (100503, "100503") or "too many" in msg.lower() or "rate" in msg.lower():
            raise exceptions.RateLimitExceeded
        if code in (101205, "101205") or "no position to close" in msg.lower():
            raise exceptions.ReduceImpossible
        if "not exist" in msg.lower() or "does not exist" in msg.lower():
            raise exceptions.OrderNotExist
        logger.critical("BingX API error url=%s response=%s", url, response)
        raise exceptions.RequestError


if __name__ == "__main__":
    pass
