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
        # Смещение времени (мс), добавляется к timestamp при подписи.
        # Логика аналогична BybitAPI.recv_window_shift: мы сознательно уводим timestamp чуть раньше,
        # чтобы уменьшить шанс "timestamp outside of the recvWindow".
        self.timestamp_shift = 0
        # По документации futures exchangeInfo обычно возвращает REQUEST_WEIGHT=2400/мин.
        self.request_weight_limit_1m = 2400
        self._bucket_order = ["2400", "1800", "1200", "600"]
        # Жесткий верхний бакет для тяжелых endpoint, чтобы они не "съедали" budget.
        self._endpoint_bucket_cap = {
            "/sapi/v1/asset/transfer": "600",
            "/fapi/v1/income": "1200",
            "/fapi/v1/openOrders": "1200",  # когда symbol не передан вес=40
            "/fapi/v1/klines": "1200",      # при больших limit вес до 10
        }

        limiters = {
            "600": AsyncLimiter(600, 60),
            "1200": AsyncLimiter(1200, 60),
            "1800": AsyncLimiter(1800, 60),
            "2400": AsyncLimiter(2400, 60),
        }
        limiters_dict = {
            "/fapi/v1/time": "2400",
            "/fapi/v2/account": "1800",
            "/fapi/v2/positionRisk": "1800",
            "/fapi/v1/order": "2400",
            "/fapi/v1/openOrders": "1800",
            "/fapi/v1/allOrders": "1800",
            "/fapi/v1/leverage": "2400",
            "/fapi/v1/exchangeInfo": "2400",
            "/fapi/v1/userTrades": "1800",
            "/fapi/v1/allOpenOrders": "2400",
            "/fapi/v1/openOrder": "2400",
            "/fapi/v1/positionSide/dual": "2400",
            "/fapi/v1/marginType": "2400",
            "/fapi/v1/klines": "1800",
            "/fapi/v1/income": "1200",
            "/sapi/v1/account/apiRestrictions": "2400",
            "/sapi/v1/capital/deposit/hisrec": "2400",
            "/sapi/v1/asset/transfer": "600",
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
        signed["timestamp"] = int(time.time() * 1000) + int(self.timestamp_shift)
        signed["recvWindow"] = self.recv_window

        query_string = urlencode(signed, doseq=True)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        signed["signature"] = signature
        return signed

    async def update_timestamp_shift(self, safety_ms: int = 2500):
        """
        Синхронизирует локальное время с серверным и рассчитывает timestamp_shift (мс).

        По Binance нет прямого recvWindowShift заголовка как в Bybit,
        поэтому мы корректируем timestamp при подписи запроса.
        """
        local_now_ms = int(time.time() * 1000)
        server_time_resp = await self.public_get_request("/fapi/v1/time", params=None, retries=10)
        server_time_ms = server_time_resp.get("serverTime")
        if server_time_ms is None:
            return

        difference = local_now_ms - int(server_time_ms)
        # Уводим timestamp раньше на safety_ms, чтобы точка попадала в recvWindow даже при дрейфе.
        self.timestamp_shift = difference - safety_ms
        logger.info("timestamp_shift updated: %s", self.timestamp_shift)

    def _auth_headers(self) -> dict:
        return {"X-MBX-APIKEY": self.api_key}

    def _request_weight(self, endpoint: str, method: str, params: dict | None) -> int:
        params = params or {}
        m = method.upper()

        # Weights from Binance docs (USDⓈ-M Futures + SAPI used by this client).
        if endpoint == "/fapi/v1/time":
            return 1
        if endpoint == "/fapi/v1/exchangeInfo":
            return 1
        if endpoint == "/fapi/v2/account":
            return 5
        if endpoint == "/fapi/v2/positionRisk":
            return 5
        if endpoint == "/fapi/v1/leverage":
            return 1
        if endpoint == "/fapi/v1/marginType":
            return 1
        if endpoint == "/fapi/v1/positionSide/dual":
            return 1
        if endpoint == "/fapi/v1/allOpenOrders":
            return 1
        if endpoint == "/fapi/v1/allOrders":
            return 5
        if endpoint == "/fapi/v1/userTrades":
            return 5
        if endpoint == "/fapi/v1/openOrder":
            return 1
        if endpoint == "/fapi/v1/openOrders":
            return 1 if params.get("symbol") else 40
        if endpoint == "/fapi/v1/klines":
            try:
                limit = int(params.get("limit", 500))
            except (TypeError, ValueError):
                limit = 500
            if limit < 100:
                return 1
            if limit < 500:
                return 2
            if limit <= 1000:
                return 5
            return 10
        if endpoint == "/fapi/v1/income":
            return 30
        if endpoint == "/fapi/v1/order":
            if m == "POST":
                # По документации New Order: 0 on IP rate limit.
                # Но базовый transport всегда берет минимум 1 токен для совместимости
                # с общим `async with limiter` контрактом.
                return 1
            return 1

        # SAPI endpoints used in AsyncBinanceFuturesClient.
        if endpoint == "/sapi/v1/account/apiRestrictions":
            return 1
        if endpoint == "/sapi/v1/capital/deposit/hisrec":
            return 1
        if endpoint == "/sapi/v1/asset/transfer":
            # Weight(UID) = 900. Используем как высокий вес для консервативного throttling.
            return 900

        return 1

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

    def _extract_limit_from_headers(self, headers):
        headers_map = {str(k).lower(): v for k, v in dict(headers).items()}
        used_weight_1m = headers_map.get("x-mbx-used-weight-1m")
        if used_weight_1m is None:
            return None

        try:
            used = int(used_weight_1m)
        except (TypeError, ValueError):
            return None

        return {"used_weight_1m": used}

    def update_limits(self, endpoint: str, resp_headers_limit):
        if endpoint not in self.limiters_dict:
            return

        # Совместимость со старым поведением BaseAsyncExchangeAPI (строковые бакеты).
        if isinstance(resp_headers_limit, str):
            return super().update_limits(endpoint, resp_headers_limit)

        if not isinstance(resp_headers_limit, dict):
            return

        used_weight_1m = resp_headers_limit.get("used_weight_1m")
        if used_weight_1m is None:
            return

        try:
            used_weight_1m = int(used_weight_1m)
        except (TypeError, ValueError):
            return

        max_weight_1m = max(int(self.request_weight_limit_1m), 1)
        request_weight = resp_headers_limit.get("request_weight", self._request_weight(endpoint, "GET", {}))
        try:
            request_weight = int(request_weight)
        except (TypeError, ValueError):
            request_weight = 1

        # Добавляем ограниченный буфер, чтобы не дергать бакет слишком резко.
        projected_utilization = (used_weight_1m + min(max(request_weight, 1), 20)) / max_weight_1m

        # Чем выше загрузка минутного окна, тем более консервативный бакет.
        if projected_utilization >= 0.90:
            target_bucket = "600"
        elif projected_utilization >= 0.75:
            target_bucket = "1200"
        elif projected_utilization >= 0.55:
            target_bucket = "1800"
        else:
            target_bucket = "2400"

        # Cap для тяжелых запросов по весу.
        if request_weight >= 100:
            weight_cap_bucket = "600"
        elif request_weight >= 30:
            weight_cap_bucket = "1200"
        elif request_weight >= 10:
            weight_cap_bucket = "1800"
        else:
            weight_cap_bucket = "2400"

        endpoint_cap_bucket = self._endpoint_bucket_cap.get(endpoint, "2400")
        cap_idx = max(self._bucket_order.index(endpoint_cap_bucket), self._bucket_order.index(weight_cap_bucket))
        if self._bucket_order.index(target_bucket) < cap_idx:
            target_bucket = self._bucket_order[cap_idx]

        current_bucket = self.limiters_dict.get(endpoint)
        if current_bucket != target_bucket:
            logger.info(
                "Binance limits update for %s via used-weight: %s -> %s "
                "(used=%s max=%s utilization=%.2f)",
                endpoint,
                current_bucket,
                target_bucket,
                used_weight_1m,
                max_weight_1m,
                projected_utilization,
            )
            self.limiters_dict[endpoint] = target_bucket

    async def _handle_error_response(self, response: dict, status_code: int, url: str):
        code = response.get("code")
        msg = str(response.get("msg", ""))
        if code in (-1021,) or "timestamp for this request is outside of the recvWindow" in msg:
            await self.update_timestamp_shift()
            raise exceptions.InvalidNonce
        if code in (-2015, -2014):
            raise exceptions.AuthenticationError
        if code in (-2019,):
            raise exceptions.MarginInsufficient
        if code in (-1015, -1003) or "too many requests" in msg.lower():
            raise exceptions.RateLimitExceeded
        if code in (-2011, -2013):
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
        if code in (-4164,):
            raise exceptions.MinimumLimitExceeded

        logger.critical("Unknown Binance API error. url=%s status=%s response=%s", url, status_code, response)
        raise exceptions.RequestError


if __name__ == "__main__":
    pass
