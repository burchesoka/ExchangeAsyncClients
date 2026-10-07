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
    MarginMode,
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
            broker_id: str | None = None,
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
            broker_id=broker_id,
        )

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
        if self.category == 'spot':
            return await self.get_request("/api/v3/account")
        else:
            return await self.get_request("/fapi/v2/account")

    async def is_master_trader_account(self):
        raise NotImplementedError

    async def get_api_key_info(self):
        return await self.get_request("/sapi/v1/account/apiRestrictions")

    async def get_user_id(self):
        account_info = await self.get_account_info()
        return account_info.get("accountAlias")

    async def get_wallet_data(
            self,
            logs_enabled: bool = True,
            retries: int = 25,
    ) -> WalletData:
        response = await self.get_account_info()
        wallet_balance = Decimal(str(response.get("totalWalletBalance", "0")))
        available_balance = Decimal(str(response.get("availableBalance", "0")))
        unrealized_pnl = Decimal(str(response.get("totalUnrealizedProfit", "0")))
        equity = wallet_balance + unrealized_pnl
        coins = {}
        for i in response['balances']:
            free = Decimal(i['free'])
            locked = Decimal(i['locked'])
            if free or locked:
                balance = free + locked
                coins[i['asset']] = balance.normalize()

        return WalletData(
            wallet_balance=wallet_balance,
            available_balance=available_balance,
            equity=equity,
            coins=coins
        )

    async def switch_position_mode(
            self,
            mode: PositionMode,
            symbol: str | None = None,
            coin: str | None = None,
    ):
        params = {"dualSidePosition": "true" if mode == PositionMode.hedge else "false"}
        try:
            resp = await self.post_request("/fapi/v1/positionSide/dual", body=params)
            logger.debug('switch_position_mode response: %s', resp)
            return True
        except exceptions.NoChange:
            return True

    async def transfer(self, from_account: str, to_account: str, amount: str, coin: str = None):
        transfer_map = {
            ("SPOT", "USDT_FUTURE"): "MAIN_UMFUTURE",
            ("SPOT", "FUND"): "MAIN_FUNDING",
            ("FUND", "SPOT"): "FUNDING_MAIN",
            ("USDT_FUTURE", "SPOT"): "UMFUTURE_MAIN",
            ("CONTRACT", "FUND"): "UMFUTURE_FUNDING",
            ("FUND", "CONTRACT"): "FUNDING_UMFUTURE",
        }
        transfer_type = transfer_map.get((from_account, to_account))
        if transfer_type is None:
            raise exceptions.TransferUnable(f"Unsupported transfer: {from_account} -> {to_account}")
        return await self.post_request(
            "/sapi/v1/asset/transfer",
            body={"type": transfer_type, "asset": "USDT" if not coin else coin, "amount": str(amount)},
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
                body={"symbol": symbol, "leverage": int(leverage)},
            )
            if int(response.get("leverage")) == int(leverage):
                return True
        return False

    async def set_margin_mode_to_account(self, isolated: bool = False):
        _ = isolated
        return True

    async def switch_margin_mode(self, symbol: str, margin_mode: MarginMode, leverage: float) -> bool:
        _ = leverage
        mode = "ISOLATED" if margin_mode == MarginMode.isolated else "CROSSED"
        try:
            await self.post_request("/fapi/v1/marginType", body={"symbol": symbol, "marginType": mode})
            return True
        except exceptions.NoChange:
            return True

    async def get_instrument_info(self, symbol: str) -> InstrumentInfo:
        if self.category == 'spot':
            tail = "/api/v3/exchangeInfo"
        else:
            tail = "/fapi/v1/exchangeInfo"

        response = await self.public_get_request(tail)
        symbols = response.pop("symbols", [])

        if not symbols:
            raise exceptions.NotFound(symbol)

        symbol_info = None
        for s in symbols:
            if s.get("symbol") == symbol:
                symbol_info = s
                break
        if symbol_info is None:
            raise exceptions.NotFound(symbol)

        symbol_info_filtered: dict = {
            "symbol": symbol,
            'contract_value': '1',
        }
        for filter in symbol_info["filters"]:
            if filter["filterType"] == "LOT_SIZE":
                symbol_info_filtered["min_qty"] = filter["minQty"]
            elif filter["filterType"] == "PRICE_FILTER":
                symbol_info_filtered["tick_size"] = filter["tickSize"]
        if "min_qty" not in symbol_info_filtered or "tick_size" not in symbol_info_filtered:
            raise exceptions.NotFound(symbol)
        return InstrumentInfo.model_validate(symbol_info_filtered)

    async def get_klines_history(self, symbol: str, interval: str, candles: int) -> list:
        raise NotImplementedError
        now = int(time.time() * 1000)
        start = now - (INTERVAL_IN_SEC[interval] * candles * 1000)
        return await self.get_klines(symbol=symbol, interval=interval, limit=candles, start=start, end=now)

    async def get_klines(self, symbol: str, interval: str, limit: int, start: int, end: int) -> list:
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
            "startTime": start,
            "endTime": end,
        }
        if end is None:
            params.pop('endTime')
        if self.category == 'spot':
            tail = "/api/v3/klines"
        else:
            tail = "/fapi/v1/klines"
        response = await self.public_get_request(tail, params=params)
        return response

    async def get_history_data_frame(
            self,
            symbol: str,
            interval: str,
            candles: int,
            start_time: int = None,
            max_limit: int = 1500,
    ) -> pd.DataFrame:
        return await super().get_history_data_frame(symbol, interval, candles, start_time, max_limit)

    async def new_order(
            self,
            symbol: str,
            side: str,
            quantity: float | str,
            order_type: str,
            position_mode: PositionMode,
            price: float | str | None = None,
            stop_price: float | str | None = None,
            take_price: float | str | None = None,
            reduce_only: bool = False,
            time_in_force: str = "GTC",
    ) -> str:
        _ = take_price
        side_upper = side.upper()
        position_side = None
        if reduce_only:
            position_side = 'SELL' if side_upper == "BUY" else "BUY"
        params = {
            "symbol": symbol,
            "side": side_upper,
            "quantity": str(quantity),
            "type": order_type.upper().replace(" ", "_"),
        }
        if position_mode == PositionMode.hedge:
            # Binance hedge mode требует LONG/SHORT в positionSide.
            if position_side is None:
                if reduce_only:
                    raise ValueError("position_side is required for reduce_only orders in hedge mode")
                resolved_position_side = "LONG" if side_upper == "BUY" else "SHORT"
            else:
                ps = position_side.upper()
                if ps in ("BUY", "LONG"):
                    resolved_position_side = "LONG"
                elif ps in ("SELL", "SHORT"):
                    resolved_position_side = "SHORT"
                else:
                    raise ValueError(f"Unsupported position_side for hedge mode: {position_side}")
            params["positionSide"] = resolved_position_side
            # reduceOnly в hedge mode Binance не принимает.
        else:
            params["reduceOnly"] = "true" if reduce_only else "false"
            if position_side is not None:
                ps = position_side.upper()
                if ps in ("BOTH", "LONG", "SHORT"):
                    params["positionSide"] = ps
                elif ps in ("BUY", "SELL"):
                    params["positionSide"] = "BOTH"
        if price is not None:
            params["price"] = str(price)
            params["timeInForce"] = time_in_force
        if stop_price is not None:
            params["stopPrice"] = str(stop_price)

        if self.category == 'spot':
            tail = "/api/v3/order"
            params.pop('positionSide')
        else:
            tail = "/fapi/v1/order"

        response = await self.post_request(tail, body=params)
        logger.debug('new_order response: %s', response)

        if response.get("orderId") is not None:
            return str(response.get("orderId"))
        else:
            raise Exception(response)

    async def cancel_all_orders(self):
        if self.category == 'spot':
            open_orders = await self.get_open_orders()
            tail = "/api/v3/openOrders"
        else:
            open_orders = await self.get_open_orders(coin='USDT')
            tail = "/fapi/v1/allOpenOrders"
        symbols = sorted({order.symbol for order in open_orders})
        result = []

        for symbol in symbols:
            result.append(await self.delete_request(tail, params={"symbol": symbol}))
        return result

    async def cancel_order(self, symbol: str, order_id: str):
        if self.category == 'spot':
            tail = "/api/v3/order"
        else:
            tail = "/fapi/v1/order"
        try:
            await self.delete_request(tail, params={"symbol": symbol, "orderId": order_id})
            return True
        except exceptions.OrderNotExist:
            try:
                await self._check_order(symbol, order_id)
            except exceptions.OrderNotFound:
                pass

            logger.error('Order does not exist symbol: %s | id: %s', symbol, order_id)
            return True

    def _order_from_binance(self, item: dict) -> OrderData:
        now_ms = int(time.time() * 1000)
        qty = Decimal(item.get("origQty", item.get("qty", "0")))
        side = item.get("side", "")
        status = item.get("status")
        price = str(item.get("price", "0"))
        avg_price = item.get("avgPrice", "0")
        cum_exec_qty = item.get("executedQty", item.get("cumExecQty"))
        leaves_qty = Decimal(str(item.get("origQty", "0"))) - Decimal(str(item.get("executedQty", "0")))

        if self.category == 'spot':
            qty = qty - Decimal(item.get('commission', '0'))
            if not side:
                if item.get('isBuyer'):
                    side = 'BUY'
                else:
                    side = 'SELL'
            if status is None:
                status = 'FILLED'
                leaves_qty = '0'
            if avg_price == '0' and status == 'FILLED':
                avg_price = price
            if cum_exec_qty is None:
                cum_exec_qty = qty

        payload = {
            "orderId": str(item.get("orderId") or item.get("id") or ""),
            "symbol": item.get("symbol", ""),
            "orderType": ORDER_SPECS.get(item.get("type", "LIMIT"), "Limit"),
            "qty": str(qty.normalize()),
            "leavesQty": str(leaves_qty),
            "cumExecQty": cum_exec_qty,
            "side": side,
            "price": price,
            "avgPrice": avg_price,
            "takeProfit": str(item.get("stopPrice", "0")),
            "stopLoss": str(item.get("stopPrice", "0")),
            "orderStatus": status,
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
            start_time: int = None,
            end_time: int = None,
            retries: int = 120
    ) -> list[OrderData] | OrderData:
        if order_id is not None:
            if self.category == 'spot':
                ex = await self.get_executions(symbol=symbol, order_id=order_id)
                logger.debug(ex)
                if ex:
                    orders = []
                    for i in range(len(ex)):
                        orders.append(self._order_from_binance(ex[i]))
                    if len(orders) > 1:
                        logger.info(orders)
                        logger.info(ex)
                        raise Exception('Several executions for one order')
                    return orders[0]

                tail = "/api/v3/order"
            else:
                tail = "/fapi/v1/order"
            response = await self.get_request(tail, params={"symbol": symbol, "orderId": order_id})
            order = self._order_from_binance(response)
            if order.order_status.upper() == 'NEW':
                raise exceptions.OrderNotFound
            return order

        params = {"symbol": symbol, "limit": 500}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        response = await self.get_request("/fapi/v1/allOrders", params=params)
        return [self._order_from_binance(item) for item in response]

    async def _check_order(self, symbol: str, order_id: str) -> OrderData:
        logger.debug('check_order if it filled or not')

        order_data = await self.get_order_history(
            symbol=symbol,
            order_id=order_id
        )

        if not order_data:
            logger.critical('Order not found id: %s', order_id)
            raise Exception
        order_status = order_data.order_status

        if order_status.upper() == 'FILLED':
            logger.warning('Order was filled!!! id: %s', order_id)
            raise exceptions.AlreadyFilledOrder
        elif 'CANCELLED' in order_status.upper():
            logger.info('Order was cancelled, id: %s | status: %s', order_id, order_status)
            return
        elif 'partial' in order_status.lower():
            logger.warning('Order was partially filled!!! id: %s %s', order_id, order_status)
            raise exceptions.PartiallyFilledOrder
        else:
            logger.info('Order was not filled, id: %s | status: %s', order_id, order_status)
            return

    async def get_executions(
            self,
            symbol: str,
            start_time: int | None = None,
            end_time: int | None = None,
            limit: int = 1000,
            order_id: str = None,
    ) -> list[ExecutionsData] | dict:
        params = {"symbol": symbol, "limit": min(limit, 1000)}
        if start_time is not None:
            start_time = int(start_time * 1000)
            params["startTime"] = start_time
        if end_time is not None:
            end_time = int(end_time * 1000)
            params["endTime"] = end_time
        if order_id:
            params["orderId"] = order_id

        if self.category == 'spot':
            tail = "/api/v3/myTrades"
        else:
            tail = "/fapi/v1/userTrades"

        response = await self.get_request(tail, params=params)
        logger.debug('get_executions response: %s', response)
        if order_id:
            return response

        results = []
        for item in response:
            position_side = item.get("positionSide", "BOTH")
            side = item.get("side", "")

            if position_side == "LONG" and side == "BUY":
                opening_position = True
            elif position_side == "SHORT" and side == "SELL":
                opening_position = True
            else:
                opening_position = False
            if self.category == 'spot':
                position_side = "LONG"
                opening_position = item['isBuyer']
                side = 'BUY' if opening_position else 'SELL'

            payload = {
                "symbol": item.get("symbol", ""),
                "opening_position": opening_position,
                "exec_qty": str(item.get("qty", "0")) if not self.category == 'spot' else str(
                    (Decimal(item.get("qty", "0")) - Decimal(item.get("commission", "0"))).normalize()),
                "order_id": str(item.get("orderId", "")),
                "price": str(item.get("price", "0")),
                "position_side": position_side,
                "side": side,
                "time": item.get("time"),
            }
            execution = ExecutionsData.model_validate(payload)
            execution.customize()
            results.append(execution)
        results.reverse()
        return results

    async def get_open_order(self, symbol: str, order_id: str, retries: int = 70) -> OrderData:
        if self.category == 'spot':
            tail = "/api/v3/order"
        else:
            tail = "/fapi/v1/openOrder"

        try:
            response = await self.get_request(tail, params={"symbol": symbol, "orderId": order_id})
            order = self._order_from_binance(response)
            if 'FILLED' == order.order_status.upper():
                raise exceptions.AlreadyFilledOrder
            elif 'CANCEL' in order.order_status.upper():
                logger.debug('Cancelled order in history %s', order)
                raise exceptions.CancelledOrder
            return order
        except exceptions.OrderNotExist:
            try:
                order = await self.get_order_history(symbol, order_id, retries=retries)
            except (exceptions.OrderNotFound, exceptions.OrderNotExist):
                logger.critical('Order for %s not found: %s', symbol, order_id)
                raise exceptions.OrderNotFound

            if 'FILLED' in order.order_status.upper():
                raise exceptions.AlreadyFilledOrder
            elif 'CANCEL' in order.order_status.upper():
                logger.debug('Cancelled order in history %s', order)
                raise exceptions.CancelledOrder
            elif 'NEW' in order.order_status.upper():
                logger.critical('New order in history??? %s', order)
                raise exceptions.CancelledOrder
            else:
                logger.critical('Unexpected order status: %s', order.order_status)
                raise Exception(f'WTF {order.order_status=}')

    async def get_open_orders(
            self,
            symbol: str | None = None,
            coin: str | None = None,
            retries: int = 70
    ) -> list[OrderData]:
        params = {"symbol": symbol} if symbol else {}

        if self.category == 'spot':
            tail = "/api/v3/openOrders"

        else:
            tail = "/fapi/v1/openOrders"

        response = await self.get_request(tail, params=params)
        logger.debug(response)
        orders = [self._order_from_binance(item) for item in response]
        # if side:
        #     orders = [o for o in orders if o.side.upper() == side.upper()]
        # if sort_by_time:
        #     orders = sorted(orders, key=lambda x: x.updated_time)
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
        if self.category == 'spot':
            wallet_data = await self.get_wallet_data()

            positions = []
            for coin, size in wallet_data.coins.items():
                positions.append(PositionData(
                    symbol=coin,
                    size=size,
                    side='BUY',
                    avg_price='0',
                    stop_price='0',
                    take_price='0',
                    liq_price='0',
                    position_margin='0',
                    leverage='1',
                    created_time='17896314986062',
                    updated_time='17896314986062',
                    unrealised_pnl='0',
                ))
            return positions

        response = await self.get_request("/fapi/v2/positionRisk")
        results = []
        for item in response:
            if Decimal(str(item.get("positionAmt", "0"))) == Decimal("0"):
                continue
            results.append(self._position_from_binance(item))
        return results

    async def get_position(self, symbol: str, side: str, empty_available: bool = False) -> PositionData | None:
        if self.category == 'spot':
            if symbol != 'BTC':
                symbol = symbol.replace('BTC', '')
            wallet_data = await self.get_wallet_data()

            for coin, size in wallet_data.coins.items():
                if symbol == coin:
                    return PositionData(
                        symbol=coin,
                        size=size,
                        side='BUY',
                        avg_price='0',
                        stop_price='0',
                        take_price='0',
                        liq_price='0',
                        position_margin='0',
                        leverage='1',
                        created_time='17896314986062',
                        updated_time='17896314986062',
                        unrealised_pnl='0',
                    )
            return None

        side_ = 'LONG' if side.upper() == 'BUY' else 'SHORT'
        response = await self.get_request("/fapi/v2/positionRisk", params={"symbol": symbol})

        for item in response:
            logger.debug('get_position item: %s', item)
            if item.get("symbol") == symbol and item.get("positionSide") == side_:
                if Decimal(item.get("positionAmt")) or (not Decimal(item.get("positionAmt")) and empty_available):
                    return self._position_from_binance(item)
                else:
                    return None
            elif item.get("symbol") == symbol and item.get("positionSide") == "BOTH":
                logger.debug('get_position item BOTH: %s', item)
                if not Decimal(item.get("positionAmt")) and empty_available:
                    return self._position_from_binance(item)
                else:
                    item["positionSide"] = "LONG" if Decimal(item.get("positionAmt")) > 0 else "SHORT"
                    if item["positionSide"] == side.upper():
                        return self._position_from_binance(item)
                    else:
                        continue

        return None

    async def close_all_positions(self, symbol: str, position_data: PositionData):
        raise NotImplementedError
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
        results.reverse()
        return results

    async def get_closed_pnls_list(
            self,
            start_time: int = None,
            end_time: int = None,
            symbol: str = None,
            order_id: str = None,
    ) -> list[PNLData]:
        pnls = await self.get_closed_pnl_history(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
        )
        # if order_id is not None:
        #     pnls = [item for item in pnls if custom_filter(item)]
        return pnls
