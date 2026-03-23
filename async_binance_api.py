import hashlib
import hmac
import logging
import time
from urllib.parse import urlencode

import aiohttp
from aiolimiter import AsyncLimiter

import exceptions
from base_async_exchange_api import BaseAsyncExchangeAPI


logger = logging.getLogger(__name__)


class BinanceAPI(BaseAsyncExchangeAPI):
    def __init__(self, session: aiohttp.ClientSession, api_secret: str, api_key: str):
        self.recv_window = 15000
        self.base_url = "https://api.binance.com"
        self.futures_base_url = "https://fapi.binance.com"

        limiters = {
            "10": AsyncLimiter(10, 1),
            "20": AsyncLimiter(20, 1),
            "50": AsyncLimiter(50, 1),
            "5": AsyncLimiter(5, 1),
        }
        limiters_dict = {
            "/api/v3/time": "10",
            "/api/v3/account": "5",
            "/api/v3/openOrders": "10",
            "/api/v3/order": "10",
            "/api/v3/myTrades": "10",
            "/fapi/v1/time": "10",
            "/fapi/v2/account": "5",
            "/fapi/v2/positionRisk": "10",
            "/fapi/v1/order": "10",
            "/fapi/v1/openOrders": "10",
            "/fapi/v1/allOrders": "10",
            "/fapi/v1/leverage": "5",
            "/fapi/v1/exchangeInfo": "10",
        }
        super().__init__(
            session=session,
            api_secret=api_secret,
            api_key=api_key,
            limiters=limiters,
            limiters_dict=limiters_dict,
        )

    def _is_futures(self, endpoint: str) -> bool:
        return endpoint.startswith("/fapi/")

    def _base_for_endpoint(self, endpoint: str) -> str:
        return self.futures_base_url if self._is_futures(endpoint) else self.base_url

    def _sign_params(self, params: dict | None = None) -> dict:
        signed = dict(params or {})
        signed["timestamp"] = int(time.time() * 1000)
        signed["recvWindow"] = self.recv_window

        query_string = urlencode(signed, doseq=True)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        signed["signature"] = signature
        return signed

    def _auth_headers(self) -> dict:
        return {"X-MBX-APIKEY": self.api_key}

    async def get_request(self, endpoint: str, params: dict | None = None, retries: int = 10):
        return await self._request(
            method="GET",
            endpoint=endpoint,
            params=params,
            signed=True,
            retries=retries,
        )

    async def post_request(self, endpoint: str, body: dict | None = None, retries: int = 10):
        return await self._request(
            method="POST",
            endpoint=endpoint,
            params=body,
            signed=True,
            retries=retries,
        )

    async def delete_request(self, endpoint: str, params: dict | None = None, retries: int = 10):
        return await self._request(
            method="DELETE",
            endpoint=endpoint,
            params=params,
            signed=True,
            retries=retries,
        )

    async def public_get_request(self, endpoint: str, params: dict | None = None, retries: int = 10):
        return await self._request(
            method="GET",
            endpoint=endpoint,
            params=params,
            signed=False,
            retries=retries,
        )

    def _build_request_data(
        self,
        method: str,
        endpoint: str,
        params: dict | None = None,
        signed: bool = True,
    ) -> tuple[str, dict | None, dict | None, str | None]:
        url = f"{self._base_for_endpoint(endpoint)}{endpoint}"
        query_params = self._sign_params(params) if signed else (params or {})
        headers = self._auth_headers() if signed else None
        return url, query_params, headers, None

    def _is_success_response(self, response: dict, status_code: int) -> bool:
        if status_code < 400 and "code" not in response:
            return True
        code = response.get("code")
        if status_code < 400 and (code is None or code == 200):
            return True
        return False

    async def _handle_error_response(self, response: dict, status_code: int, url: str):
        code = response.get("code")
        msg = str(response.get("msg", ""))
        if code in (-1021,) or "timestamp for this request is outside of the recvWindow" in msg:
            raise exceptions.InvalidNonce
        if code in (-2015, -2014):
            raise exceptions.AuthenticationError
        if code in (-2019,):
            raise exceptions.MarginInsufficient
        if code in (-1015, -1003) or "too many requests" in msg.lower():
            raise exceptions.RateLimitExceeded
        if code in (-2011,):
            raise exceptions.OrderNotExist
        if code in (-2022,):
            raise exceptions.ReduceImpossible
        if code in (-2021,):
            # Часто возникает при некорректных stop/tp.
            raise exceptions.StopLossImpossible
        if code in (-1111, -1100, -1102):
            raise exceptions.OrderValidationError
        if code in (-2010,):
            raise exceptions.FailedOrder

        logger.critical("Unknown Binance API error. url=%s status=%s response=%s", url, status_code, response)
        raise exceptions.RequestError


if __name__ == "__main__":
    pass
