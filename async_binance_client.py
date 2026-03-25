import datetime
import logging
import time
from decimal import Decimal
from typing import Callable

import aiohttp
import pandas as pd

import exceptions

from base import (
    BaseAsyncFuturesClient,
    INTERVAL_IN_SEC,
    Exchange,
    ExecutionsData,
    InstrumentInfo,
    OrderData,
    ORDER_SPECS,
    PNLData,
    PositionData,
    PositionMode,
    WalletData,
)
from async_binance_api import BinanceAPI


logger = logging.getLogger(__name__)


class AsyncBinanceFuturesClient(BaseAsyncFuturesClient, BinanceAPI):
    def __init__(
        self,
        session: aiohttp.ClientSession,
        category: str = "linear",
        test: bool = False,
        api_key: str | None = None,
        api_secret: str | None = None,
        password: str | None = None,
    ):
        BaseAsyncFuturesClient.__init__(
            self,
            category=category,
            test=test,
            password=password,
        )
        BinanceAPI.__init__(
            self,
            session=session,
            api_key=api_key,
            api_secret=api_secret,
        )
        self.exchange = Exchange.bybit  # Сохраняем совместимость старого интерфейса.

    async def get_all_instruments_info(self) -> dict[str, InstrumentInfo]:
        response = await self.public_get_request("/fapi/v1/exchangeInfo")
        instruments: dict[str, InstrumentInfo] = {}
        for item in response.get("symbols", []):
            symbol = item.get("symbol")
            filters = {flt.get("filterType"): flt for flt in item.get("filters", [])}
            lot_size = filters.get("LOT_SIZE", {})
            price_filter = filters.get("PRICE_FILTER", {})
            if not symbol:
                continue

            instruments[symbol] = InstrumentInfo(
                symbol=symbol,
                min_order_qty=lot_size.get("minQty", "0"),
                tick_size=price_filter.get("tickSize", "0"),
            )
        return instruments

    async def get_account_info(self) -> dict:
        return await self.get_request("/fapi/v2/account")

    async def is_master_trader_account(self):
        return False

    async def get_api_key_info(self):
        return await self.get_request("/sapi/v1/account/apiRestrictions")

    async def get_user_id(self):
        account_info = await self.get_account_info()
        return account_info.get("accountAlias")

    async def get_wallet_data(self) -> WalletData:
        response = await self.get_account_info()
        wallet_balance = Decimal(str(response.get("totalWalletBalance", "0")))
        available_balance = Decimal(str(response.get("availableBalance", "0")))
        unrealized_pnl = Decimal(str(response.get("totalUnrealizedProfit", "0")))
        equity = wallet_balance + unrealized_pnl

        return WalletData(
            wallet_balance=wallet_balance,
            available_balance=available_balance,
            equity=equity,
        )

    async def switch_position_mode(
        self,
        mode: PositionMode,
        symbol: str | None = None,
        coin: str | None = None,
    ):
        params = {"dualSidePosition": "true" if mode == PositionMode.hedge else "false"}
        try:
            await self.post_request("/fapi/v1/positionSide/dual", body=params)
            return True
        except exceptions.NoChange:
            return True

    async def transfer(self, from_account: str, to_account: str, amount: float):
        transfer_map = {
            ("SPOT", "USDT_FUTURE"): "MAIN_UMFUTURE",
            ("USDT_FUTURE", "SPOT"): "UMFUTURE_MAIN",
        }
        transfer_type = transfer_map.get((from_account, to_account))
        if transfer_type is None:
            raise exceptions.TransferUnable(f"Unsupported transfer: {from_account} -> {to_account}")
        return await self.post_request(
            "/sapi/v1/asset/transfer",
            body={"type": transfer_type, "asset": "USDT", "amount": str(amount)},
        )

    async def set_leverage(
        self,
        symbol: str,
        leverage: int,
        position_mode: PositionMode = PositionMode.hedge,
        retries=20,
    ) -> bool:
        _ = position_mode
        for _i in range(retries):
            response = await self.post_request(
                "/fapi/v1/leverage",
                body={"symbol": symbol, "leverage": leverage},
            )
            if str(response.get("leverage")) == str(leverage):
                return True
        return False

    async def set_margin_mode_to_account(self, isolated: bool = False):
        _ = isolated
        return True

    async def switch_margin_mode(self, symbol: str, margin_mode: str, leverage: float) -> bool:
        _ = leverage
        mode = "ISOLATED" if margin_mode.lower() == "isolated" else "CROSSED"
        try:
            await self.post_request("/fapi/v1/marginType", body={"symbol": symbol, "marginType": mode})
            return True
        except exceptions.NoChange:
            return True

    async def get_instrument_info(self, symbol: str) -> dict:
        response = await self.public_get_request("/fapi/v1/exchangeInfo")

        symbols = response.pop("symbols", [])

        if not symbols:
            raise exceptions.NotFound(symbol)
        
        for s in symbols:
            if s.get("symbol") == symbol:
                symbol_info = s
                break
        symbol_info_filtered = {
            "symbol": symbol,
            'contract_value': '1',
        }
        for filter in symbol_info["filters"]:
            if filter["filterType"] == "LOT_SIZE":
                symbol_info_filtered["min_qty"] = filter["minQty"]
            elif filter["filterType"] == "PRICE_FILTER":
                symbol_info_filtered["tick_size"] = filter["tickSize"]
        return symbol_info_filtered

    async def get_klines_history(self, symbol: str, interval: str, candles: int) -> list:
        now = int(time.time() * 1000)
        start = now - (INTERVAL_IN_SEC[interval] * candles * 1000)
        return await self.get_klines(symbol=symbol, interval=interval, limit=candles, start=start, end=now)

    async def get_klines(self, symbol: str, interval: str, limit: int, start: int, end: int) -> list:
        response = await self.public_get_request(
            "/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
                "startTime": start,
                "endTime": end,
            },
        )
        return response

    async def get_history_data_frame(
        self,
        symbol: str,
        start_time: int | None = None,
        end_time: int | None = None,
        interval: str = "1m",
        minutes_before: int = 30,
    ) -> pd.DataFrame:
        if end_time is None:
            end_time = int(time.time() * 1000)
        if start_time is None:
            start_time = end_time - (minutes_before * 60 * 1000)

        data = await self.get_klines(
            symbol=symbol,
            interval=interval,
            limit=1000,
            start=start_time,
            end=end_time,
        )
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(
            data,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "qav",
                "trades",
                "tb_base",
                "tb_quote",
                "ignore",
            ],
        )
        return df

    async def new_order(
        self,
        symbol: str,
        side: str,
        quantity: float | str,
        order_type: str,
        position_mode: str,
        price: float | str | None = None,
        stop_price: float | str | None = None,
        take_price: float | str | None = None,
        reduce_only: bool = False,
        position_side: str | None = None,
        time_in_force: str = "GTC",
    ) -> str:
        _ = take_price
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "quantity": str(quantity),
            "type": order_type.upper().replace(" ", "_"),
            "reduceOnly": "true" if reduce_only else "false",
        }
        if price is not None:
            params["price"] = str(price)
            params["timeInForce"] = time_in_force
        if stop_price is not None:
            params["stopPrice"] = str(stop_price)
        if position_side is not None:
            params["positionSide"] = position_side.upper()

        response = await self.post_request("/fapi/v1/order", body=params)
        logger.debug('new_order response: %s', response)
        return response.get("orderId")
        if response.get("orderId") is not None:
            return response.get("orderId")
        else:
            raise Exception(response)

    async def cancel_all_orders(self):
        open_orders = await self.get_open_orders()
        symbols = sorted({order.symbol for order in open_orders})
        result = []
        for symbol in symbols:
            result.append(await self.delete_request("/fapi/v1/allOpenOrders", params={"symbol": symbol}))
        return result

    async def cancel_order(self, symbol: str, order_id: str):
        return await self.delete_request("/fapi/v1/order", params={"symbol": symbol, "orderId": order_id})

    def _order_from_binance(self, item: dict) -> OrderData:
        now_ms = int(time.time() * 1000)
        payload = {
            "orderId": str(item.get("orderId") or item.get("id") or ""),
            "symbol": item.get("symbol", ""),
            "orderType": ORDER_SPECS.get(item.get("type", "MARKET"), "Market"),
            "qty": str(item.get("origQty", item.get("qty", "0"))),
            "leavesQty": str(
                Decimal(str(item.get("origQty", "0"))) - Decimal(str(item.get("executedQty", "0")))
            ),
            "cumExecQty": str(item.get("executedQty", item.get("cumExecQty", "0"))),
            "side": item.get("side", ""),
            "price": str(item.get("price", "0")),
            "avgPrice": str(item.get("avgPrice", "0")),
            "takeProfit": str(item.get("stopPrice", "0")),
            "stopLoss": str(item.get("stopPrice", "0")),
            "orderStatus": item.get("status", "NEW"),
            "createdTime": str(item.get("time", now_ms)),
            "updatedTime": str(item.get("updateTime", now_ms)),
        }
        order = OrderData.model_validate(payload)
        order.customize()
        return order

    async def get_order_history(
        self,
        symbol: str,
        order_id: str = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[OrderData] | OrderData:
        if order_id is not None:
            response = await self.get_request("/fapi/v1/order", params={"symbol": symbol, "orderId": order_id})
            return [self._order_from_binance(response)]

        params = {"symbol": symbol, "limit": min(limit, 1000)}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        response = await self.get_request("/fapi/v1/allOrders", params=params)
        return [self._order_from_binance(item) for item in response]

    async def _check_order(self, symbol: str, order_id: str) -> OrderData:
        raise NotImplementedError

    async def get_executions(
        self,
        symbol: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[ExecutionsData]:
        params = {"symbol": symbol, "limit": min(limit, 1000)}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        response = await self.get_request("/fapi/v1/userTrades", params=params)
        results = []
        for item in response:
            payload = {
                "symbol": item.get("symbol", ""),
                "opening_position": not bool(item.get("maker", False)),
                "exec_qty": str(item.get("qty", "0")),
                "order_id": str(item.get("orderId", "")),
                "price": str(item.get("price", "0")),
                "position_side": item.get("positionSide", "BOTH"),
                "side": item.get("side", ""),
                "time": item.get("time"),
            }
            execution = ExecutionsData.model_validate(payload)
            execution.customize()
            results.append(execution)
        results.reverse()
        return results

    async def get_open_order(self, symbol: str, order_id: str, retries: int = 70) -> OrderData:
        _ = retries
        response = await self.get_request("/fapi/v1/openOrder", params={"symbol": symbol, "orderId": order_id})
        return self._order_from_binance(response)

    async def get_open_orders(
        self,
        symbol: str | None = None,
        side: str | None = None,
        sort_by_time: bool = False,
    ) -> list[OrderData]:
        params = {"symbol": symbol} if symbol else {}
        response = await self.get_request("/fapi/v1/openOrders", params=params)
        orders = [self._order_from_binance(item) for item in response]
        if side:
            orders = [o for o in orders if o.side.upper() == side.upper()]
        if sort_by_time:
            orders = sorted(orders, key=lambda x: x.updated_time)
        return orders

    def _position_from_binance(self, item: dict) -> PositionData:
        now = datetime.datetime.utcnow()
        payload = {
            "symbol": item.get("symbol", ""),
            "positionSide": item.get("positionSide", "BOTH"),
            "positionAmt": str(item.get("positionAmt", "0")),
            "avgPrice": str(item.get("entryPrice", "0")),
            "stopLoss": str(item.get("stopLossPrice", "0")),
            "takeProfit": str(item.get("takeProfitPrice", "0")),
            "liquidationPrice": str(item.get("liquidationPrice", "0")),
            "margin": str(item.get("isolatedWallet", item.get("positionInitialMargin", "0"))),
            "leverage": str(item.get("leverage", "1")),
            "createdTime": now,
            "updateTime": item.get("updateTime", int(time.time() * 1000)),
            "unrealizedProfit": str(item.get("unRealizedProfit", "0")),
        }
        position = PositionData.model_validate(payload)
        position.customize()
        return position

    async def get_all_positions(self) -> list[PositionData]:
        response = await self.get_request("/fapi/v2/positionRisk")
        results = []
        for item in response:
            if Decimal(str(item.get("positionAmt", "0"))) == Decimal("0"):
                continue
            results.append(self._position_from_binance(item))
        return results

    async def get_position(self, symbol: str, side: "str", empty_available: bool = False) -> PositionData | None:
        _ = empty_available
        positions = await self.get_all_positions()
        for position in positions:
            if position.symbol == symbol and position.side.upper() == side.upper():
                return position
        return None

    async def close_all_positions(self, symbol: str, position_data: PositionData):
        qty = abs(Decimal(position_data.size))
        if qty == Decimal("0"):
            return True
        side = "SELL" if position_data.side.upper() == "BUY" else "BUY"
        return await self.new_order(
            symbol=symbol,
            side=side,
            qty=str(qty),
            order_type="MARKET",
            reduce_only=True,
            position_side=position_data.side.upper(),
        )

    async def get_deposit_transactions(self, start_time: int = None, end_time: int = None):
        params = {}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return await self.get_request("/sapi/v1/capital/deposit/hisrec", params=params)

    async def get_closed_pnl_history(
        self,
        symbol: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[PNLData]:
        params = {"incomeType": "REALIZED_PNL", "limit": min(limit, 1000)}
        if symbol:
            params["symbol"] = symbol
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        response = await self.get_request("/fapi/v1/income", params=params)

        results = []
        for item in response:
            payload = {
                "orderId": str(item.get("tradeId", item.get("tranId", ""))),
                "symbol": item.get("symbol", ""),
                "income": str(item.get("income", "0")),
                "createdTime": int(item.get("time", 0)),
                "updatedTime": int(item.get("time", 0)),
            }
            pnl = PNLData.model_validate(payload)
            pnl.customize()
            results.append(pnl)
        return results

    async def get_closed_pnls_list(
        self,
        symbol: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
        custom_filter: Callable[[PNLData], bool] | None = None,
    ) -> list[PNLData]:
        pnls = await self.get_closed_pnl_history(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
        if custom_filter is not None:
            pnls = [item for item in pnls if custom_filter(item)]
        return pnls
