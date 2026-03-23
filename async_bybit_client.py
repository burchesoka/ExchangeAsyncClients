from aiolimiter import AsyncLimiter
import aiohttp
import logging
import json
import time
import hmac
import hashlib

import exceptions
from base_async_exchange_client import BaseAsyncExchangeAPI

from urllib.parse import urlencode


logger = logging.getLogger(__name__)


class BybitAPI(BaseAsyncExchangeAPI):
    def __init__(self, session: aiohttp.ClientSession, api_secret: str, api_key: str):
        self.recv_window = "15000"
        self.recv_window_shift = 0
        self.base_url = "https://api.bybit.com"
        limiters = {
            '10': AsyncLimiter(10, 1),  # 10 запросов в 1 секунду
            '20': AsyncLimiter(20, 1),
            '50': AsyncLimiter(50, 1),
            '5': AsyncLimiter(5, 1),
        }
        limiters_dict = {
            "/v5/position/closed-pnl": "50",
            "/v5/position/list": "50",
            "/v5/position/set-leverage": "10",
            "/v5/position/switch-mode": "10",
            "/v5/position/switch-isolated": "10",
            "/v5/execution/list": "50",
            "/v5/order/history": "50",
            "/v5/order/realtime": "50",
            "/v5/order/create": "20",
            "/v5/order/cancel": "20",
            "/v5/order/cancel-all": "20",
            "/v5/asset/transfer/inter-transfer": "50",
            "/v5/asset/coin/query-info": "5",
            "/v5/asset/deposit/query-internal-record": "5",
            "/v5/user/query-api": "10",
            "/v5/market/kline": "5",
            "/v5/market/instruments-info": "5",
            "/v5/market/time": "5",
            "/v5/account/info": "50",
            "/v5/account/set-margin-mode": "5",
            "/v5/account/wallet-balance": "50",
        }
        super().__init__(
            session=session,
            api_secret=api_secret,
            api_key=api_key,
            limiters=limiters,
            limiters_dict=limiters_dict,
        )


    def sign_bybit(self, params: dict) -> str:
        """Подпись для Bybit v5: query string -> HMAC SHA256, hex."""
        param_str = urlencode(sorted(params.items()))
        return hmac.new(
            self.api_secret.encode("utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def sign_post(self, json_body: json) -> dict:
        """Подпись для POST: timestamp + api_key + recv_window + jsonBodyString."""
        timestamp = str(int(time.time() * 1000) + self.recv_window_shift)
        sign_str = timestamp + self.api_key + self.recv_window + json_body
        x_bapi_sign = hmac.new(
            self.api_secret.encode("utf-8"),
            sign_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": x_bapi_sign,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "X-BAPI-SIGN-TYPE": "2",
            "Content-Type": "application/json",
        }
        return headers

    def sign_get(self, params: dict | None) -> dict:
        """Подпись для GET: timestamp + api_key + recv_window + queryString.
        queryString — только параметры запроса (category, limit, symbol и т.д.), без api_key/timestamp/recv_window.
        """
        timestamp = str(int(time.time() * 1000) + self.recv_window_shift)
        params = params or {}
        # Для подписи Bybit нужен только queryString параметров запроса (без auth-полей)
        query_string = urlencode(sorted(params.items())) if params else ""
        sign_str = timestamp + self.api_key + str(self.recv_window) + query_string
        sign = hmac.new(
            self.api_secret.encode("utf-8"),
            sign_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": sign,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "X-BAPI-SIGN-TYPE": "2",
        }
        return headers


    async def update_recv_window_shift(self):
        time_now = int(time.time() * 1000)
        server_time = await self.get_request(endpoint="/v5/market/time")
        server_time = server_time['time']
        difference = time_now - server_time
        logger.debug('difference %s', difference)
        self.recv_window_shift = difference - 2500
        logger.info("recv_window_shift updated: %s", self.recv_window_shift)

    async def get_request(self, endpoint: str, params: dict | None = None, retries: int = 70):
        return await self._request(
            method="GET",
            endpoint=endpoint,
            params=params,
            signed=True,
            retries=retries,
            network_sleep_seconds=5,
            timeout_sleep_seconds=1,
            ratelimit_sleep_seconds=30,
        )

    async def post_request(self, endpoint, body: dict, retries: int = 70):
        return await self._request(
            method="POST",
            endpoint=endpoint,
            params=body,
            signed=True,
            retries=retries,
            network_sleep_seconds=5,
            timeout_sleep_seconds=1,
            ratelimit_sleep_seconds=30,
        )

    def _build_request_data(
        self,
        method: str,
        endpoint: str,
        params: dict | None,
        signed: bool,
    ) -> tuple[str, dict | None, dict | None, str | None]:
        if method == "GET":
            query_params = params or {}
            headers = self.sign_get(query_params) if signed else None
            query_string = urlencode(sorted(query_params.items())) if query_params else ""
            url = f"{self.base_url}{endpoint}?{query_string}" if query_string else f"{self.base_url}{endpoint}"
            return url, None, headers, None

        json_body = json.dumps(params or {}, separators=(",", ":"))
        headers = self.sign_post(json_body) if signed else {"Content-Type": "application/json"}
        url = f"{self.base_url}{endpoint}"
        return url, None, headers, json_body

    def _extract_limit_from_headers(self, headers):
        return dict(headers).get("X-Bapi-Limit")

    def _is_success_response(self, response: dict, status_code: int) -> bool:
        return response.get("retCode") == 0

    async def _handle_error_response(self, response: dict, status_code: int, url: str):
        ret_code = response.get('retCode')
        ret_msg = response.get('retMsg')
        if ret_code == 10002 or 'please check your server timestamp or recv_window param' in ret_msg:
            await self.update_recv_window_shift()
            raise exceptions.InvalidNonce
        elif ret_code == 110043 or "leverage not modified" in response.get('retMsg'):
            raise exceptions.LeverageNotModified
        elif ret_code == 110025 or "Position mode is not modified" in ret_msg:
            raise exceptions.NoChange
        elif ret_code == 110007 or "CheckMarginRatio fail" in ret_msg:
            logger.warning('Insufficient available balance. Url: %s', url)
            raise exceptions.MarginInsufficient
        elif ret_code == 110094 or 'Order does not meet minimum order value' in ret_msg:
            logger.critical('Minimum limit. Url: %s \n%s', url, response)
            raise exceptions.MinimumLimitExceeded
        elif ret_code == 110017 or 'cannot fix reduce-only order qty' in ret_msg or 'orderQty will be truncated to zero' in ret_msg:
            logger.info('Reduce-only rule not satisfied. Url: %s \n%s', url, response)
            raise exceptions.ReduceImpossible
        elif ret_code == 110021:
            logger.critical('out of open interest limit. Url: %s \n%s', url, response)
            raise exceptions.OutOfOpenInterest
        elif ret_code == 110001 or 'order not exist' in ret_msg:
            raise exceptions.OrderNotExist
        elif ret_code == 10006 or 'Too many visits. Exceeded the API Rate Limit' in ret_msg:
            raise exceptions.RateLimitExceeded
        else:
            logger.critical('Unknown error %s\n%s',url, response)
            raise exceptions.RequestError


if __name__ == "__main__":
    pass
