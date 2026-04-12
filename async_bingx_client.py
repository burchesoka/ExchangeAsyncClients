import asyncio
import datetime
import logging
import time
import uuid

from typing import Callable
from decimal import Decimal

import aiohttp
import pandas as pd

import exceptions

from base import (
    BaseAsyncFuturesClient,
    INTERVAL_IN_SEC,
    MarginMode,
    PositionData,
    OrderData,
    InstrumentInfo,
    PNLData,
    WalletData,
    ExecutionsData,
    PositionMode,
    Exchange,
)
from async_bingx_api import BingxAPI


logger = logging.getLogger(__name__)

INTERVAL_FOR_BINGX = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "12h": "12h",
    "1d": "1d",
    "1w": "1w",
    "1M": "1M",
}


def _to_bingx_symbol(symbol: str) -> str:
    s = symbol.strip().upper().replace("-", "")
    if "-" in symbol:
        return symbol.strip().upper()
    if s.endswith("USDT"):
        return f"{s[:-4]}-USDT"
    return symbol.strip().upper()


def _bingx_position_to_model(pos: dict) -> PositionData:
    ps = (pos.get("positionSide") or "LONG").upper()
    side = "BUY" if ps == "LONG" else "SELL"
    ts = pos.get("updateTime") or int(time.time() * 1000)
    try:
        ts_int = int(ts)
    except (TypeError, ValueError):
        ts_int = int(time.time() * 1000)
    ct = datetime.datetime.utcfromtimestamp(ts_int / 1000.0)
    row = {
        "symbol": (pos.get("symbol") or "").replace("-", ""),
        "side": side,
        "positionAmt": pos.get("positionAmt") or "0",
        "avgPrice": pos.get("avgPrice") or pos.get("entryPrice") or "0",
        "stopLoss": pos.get("stopLoss") or "0",
        "takeProfit": pos.get("takeProfit") or "0",
        "liquidationPrice": pos.get("liquidationPrice") or "0",
        "margin": pos.get("isolatedMargin") or pos.get("margin") or "0",
        "leverage": str(pos.get("leverage", "1")),
        "createdTime": ct,
        "updateTime": ts_int,
        "unrealizedProfit": pos.get("unrealizedProfit") or pos.get("unrealisedPnl") or "0",
    }
    pd = PositionData.model_validate(row)
    pd.customize()
    return pd


def _bingx_order_to_model(order: dict) -> OrderData:
    """Поля ответа BingX swap -> OrderData."""
    orig = order.get("origQty") or order.get("qty") or "0"
    exe = order.get("executedQty") or order.get("cumExecQty") or "0"
    try:
        leaves = str(Decimal(str(orig)) - Decimal(str(exe)))
    except Exception:
        leaves = orig
    row = {
        "orderId": str(order.get("orderId", "")),
        "symbol": order.get("symbol", ""),
        "orderType": order.get("type") or order.get("orderType") or "MARKET",
        "qty": str(orig),
        "leavesQty": leaves,
        "cumExecQty": str(exe),
        "side": order.get("side", ""),
        "price": str(order.get("price") or "0"),
        "avgPrice": str(order.get("avgPrice") or order.get("avgPx") or "0"),
        "takeProfit": str(order.get("takeProfit") or order.get("profit") or "0"),
        "stopLoss": str(order.get("stopLoss") or order.get("stopPrice") or "0"),
        "orderStatus": order.get("status") or order.get("orderStatus") or "NEW",
        "createdTime": str(order.get("time") or order.get("createTime") or "0"),
        "updatedTime": str(order.get("updateTime") or order.get("time") or "0"),
    }
    od = OrderData.model_validate(row)
    od.customize()
    return od


class AsyncBingxFuturesClient(BaseAsyncFuturesClient, BingxAPI):
    def __init__(
            self,
            session: aiohttp.ClientSession,
            category: str = 'linear',
            test: bool = False,
            api_key: str = None,
            api_secret: str = None,
            password: str = None,
            broker_id: str | None = None,
    ):
        BaseAsyncFuturesClient.__init__(
            self,
            category=category,
            test=test,
            password=password,
        )
        BingxAPI.__init__(
            self,
            session=session,
            api_secret=api_secret,
            api_key=api_key,
            broker_id=broker_id,
        )

    async def get_all_instruments_info(self):
        result = await self.public_get_request("/openApi/swap/v2/quote/contracts")
        if result.get("retMsg") != "OK":
            logger.critical("quote/contracts error %s", result)
            raise Exception(result)
        raw = result.get("result") or {}
        if isinstance(raw, dict) and "contracts" in raw:
            res = raw["contracts"]
        elif isinstance(raw, dict) and "list" in raw:
            res = raw["list"]
        elif isinstance(raw, list):
            res = raw
        else:
            res = []

        instruments = {}
        for i in res:
            sym = i.get("symbol") or i.get("currency")
            if not sym:
                continue
            min_q = i.get("minQty") or i.get("tradeMinQty") or i.get("size") or "0"
            tick = i.get("tickSize") or i.get("priceTick") or "0"
            instruments[sym.replace("-", "")] = InstrumentInfo(
                symbol=sym.replace("-", ""),
                min_order_qty=min_q,
                tick_size=tick,
            )
        return instruments

    async def is_master_trader_account(self):
        return False

    async def get_api_key_info(self):
        bal = await self.get_request("/openApi/swap/v2/user/balance", params={})
        if bal.get("retMsg") == "OK" and bal.get("result") is not None:
            return bal.get("result")
        logger.critical("get_api_key_info error %s", bal)
        raise Exception(bal)

    async def get_user_id(self):
        info = await self.get_api_key_info()
        if isinstance(info, dict):
            b = info.get("balance") if "balance" in info else info
            if isinstance(b, dict):
                return str(b.get("userId") or b.get("uid") or "")
        return ""

    async def get_wallet_data(
            self,
            logs_enabled: bool = True,
            retries: int = 25,
    ) -> WalletData:
        while True:
            response = await self.get_request("/openApi/swap/v2/user/balance", params={})
            if response.get("retMsg") == "OK":
                try:
                    result = response.get("result") or {}
                    if isinstance(result, dict) and "balance" in result:
                        b = result["balance"]
                    elif isinstance(result, list) and result:
                        b = result[0]
                    else:
                        b = result
                    if not isinstance(b, dict):
                        raise ValueError("unexpected balance payload")
                    row = {
                        "walletBalance": b.get("balance") or b.get("walletBalance") or "0",
                        "totalAvailableBalance": b.get("availableMargin")
                        or b.get("availableBalance")
                        or b.get("available")
                        or "0",
                        "equity": b.get("equity") or b.get("totalEquity") or b.get("balance") or "0",
                    }
                    return WalletData.model_validate(row)
                except Exception as e:
                    if logs_enabled:
                        logger.critical(
                            "Can't get BingX wallet data | Error: %s | %s | %s",
                            type(e),
                            e,
                            response,
                        )
                    if retries > 0:
                        retries -= 1
                        await asyncio.sleep(1)
                        continue
                    raise e
            if retries > 0:
                retries -= 1
                await asyncio.sleep(1)
                continue
            raise exceptions.EmptyWallet(response)

    async def get_account_info(self):
        return await self.get_request("/openApi/swap/v2/user/balance", params={})

    async def transfer(self, from_account: str, to_account: str, amount: float):
        """
        Переводы между кошельками BingX — отдельный wallet API; в swap-клиенте не реализовано.
        """
        logger.info("transfer %s USDT from %s to %s (not implemented for BingX)", amount, from_account, to_account)
        raise exceptions.TransferUnable("BingX transfer: use wallet/openApi endpoints separately")

    async def switch_position_mode(
        self,
        mode: PositionMode,
        symbol: str = None,
        coin: str = None
    ):
        _ = symbol, coin
        params = {"dualSidePosition": "true" if mode == PositionMode.hedge else "false"}
        try:
            response = await self.post_request("/openApi/swap/v2/trade/positionSide/dual", body=params)
        except exceptions.NoChange:
            logger.info("Position mode is %s already", mode)
            return True

        if response.get("retMsg") == "OK" or response.get("retCode") == 0:
            logger.info("Position mode %s switched successful", mode)
            return True
        logger.error("response: %s", response)
        raise Exception(f"Unexpected error. switch_position_mode {response=}")

    async def set_leverage(
            self,
            symbol: str,
            leverage: int,
            position_mode: PositionMode = PositionMode.hedge,
            retries=20
    ) -> bool:
        _ = position_mode, retries
        params = {
            "symbol": _to_bingx_symbol(symbol),
            "leverage": leverage,
        }
        retries_local = 5
        while retries_local:
            try:
                response = await self.post_request("/openApi/swap/v2/trade/leverage", body=params)
            except (exceptions.LeverageNotModified, exceptions.NoChange):
                logger.info("Leverage is %s already", leverage)
                return True

            if response.get("retMsg") == "OK" or response.get("retCode") == 0:
                logger.info("Set leverage %s for %s successful | resp: %s", leverage, symbol, response)
                return True
            logger.error("response: %s", response)
            retries_local -= 1
            await asyncio.sleep(1)
        return False

    async def set_margin_mode_to_account(self, isolated: bool = False):
        logger.info("set_margin_mode_to_account BingX: no global account endpoint, skipped | isolated=%s", isolated)

    async def switch_margin_mode(self, symbol: str, margin_mode: MarginMode, leverage: float) -> bool:
        """
        Режим маржи по символу (CROSSED / ISOLATED).
        """
        params = {
            "symbol": _to_bingx_symbol(symbol),
            "marginType": "ISOLATED" if margin_mode == MarginMode.isolated else "CROSSED",
        }
        _ = leverage
        try:
            response = await self.post_request("/openApi/swap/v2/trade/marginType", body=params)
        except exceptions.NoChange:
            return True
        return response.get("retMsg") == "OK" or response.get("retCode") == 0


    async def get_instrument_info(self, symbol: str) -> InstrumentInfo:
        logger.debug("get_instrument_info: %s", symbol)
        all_inst = await self.get_all_instruments_info()
        key = symbol.replace("-", "").upper()
        if key not in all_inst:
            raise exceptions.NotFound(symbol)
        return all_inst[key]

    async def get_klines_history(self, symbol: str, interval: str, candles: int) -> list:
        time_now = time.time()
        start_time = int((time_now - INTERVAL_IN_SEC[interval] * candles) * 1000)
        end_time = int(time_now * 1000)
        max_limit = 1000
        candles_left = candles

        if candles > max_limit:
            klines_list = []
            pages = candles // max_limit
            if candles % max_limit:
                pages += 1

            for i in range(pages):
                limit = max_limit if (pages > 1 or candles_left == max_limit) else candles_left
                start = int((time_now - INTERVAL_IN_SEC[interval] * (i + 1) * max_limit) * 1000)
                end = int((time_now - INTERVAL_IN_SEC[interval] * i * max_limit) * 1000)

                # if pages > 1:
                # else:
                #     end = int(time_now * 1000)

                candles_left -= max_limit
                pages -= 1
                try:
                    new_klines_list = await self.get_klines(
                        symbol=symbol,
                        interval=interval,
                        limit=limit,
                        start=start,
                        end=end
                    )
                    if not new_klines_list:
                        break
                    logger.debug('%s  %s', datetime.datetime.fromtimestamp(int(new_klines_list[-1][0]) / 1000), new_klines_list)

                    klines_list += new_klines_list
                except Exception as e:
                    logger.exception(e)
                    break

        else:
            klines_list = await self.get_klines(
                symbol=symbol,
                interval=interval,
                limit=candles,
                start=start_time,
                end=end_time,
            )

        return klines_list

    async def get_klines(self, symbol: str, interval: str, limit: int, start: int, end: int) -> list:
        params = {
            "symbol": _to_bingx_symbol(symbol),
            "interval": INTERVAL_FOR_BINGX.get(interval, interval),
            "limit": limit,
        }
        if start is not None:
            params["startTime"] = start
        if end is not None:
            params["endTime"] = end
        klines = await self.public_get_request("/openApi/swap/v2/quote/klines", params=params)
        raw = klines.get("result")
        if isinstance(raw, dict) and "list" in raw:
            return raw.get("list") or []
        if isinstance(raw, list):
            return raw
        return []

    async def get_history_data_frame(
            self,
            symbol: str,
            interval: str,
            candles: int,
            start_time: int = None,
            max_limit: int = 1000,
    ) -> pd.DataFrame:
        return await super().get_history_data_frame(symbol, interval, candles, start_time, max_limit)

    async def new_order(
            self,
            symbol: str,
            quantity: str,
            order_type: str,
            side: str,
            price: float | None = None,
            stop_price: float | None = None,
            take_price: float | None = None,
            reduce_only: bool = False,
            position_mode: PositionMode = PositionMode.hedge,
    ) -> str | None:
        logger.info(
            'new_order | symbol: %s | quantity: %s | order_type: %s | side: %s | '
            'price: %s stop_price: %s | reduce_only: %s | position_mode: %s',
            symbol,
            quantity,
            order_type,
            side,
            price,
            stop_price,
            reduce_only,
            position_mode
        )

        params = {
            "symbol": _to_bingx_symbol(symbol),
            "side": side.upper(),
            "type": order_type.upper().replace(" ", "_"),
            "quantity": str(quantity),
            "reduceOnly": "true" if reduce_only else "false",
        }
        if price is not None and str(price) not in ("None", "none"):
            params["price"] = str(price)
        if position_mode == PositionMode.hedge:
            params["positionSide"] = "LONG" if side.upper() == "BUY" else "SHORT"

        match order_type:
            case "STOP" | "STOP_MARKET":
                if stop_price is not None:
                    params["stopPrice"] = str(stop_price)
            case "TAKE_PROFIT" | "TAKE_PROFIT_MARKET":
                if stop_price is not None:
                    params["stopPrice"] = str(stop_price)
            case "LIMIT":
                if stop_price:
                    params["stopLoss"] = str(stop_price)
                if take_price:
                    params["takeProfit"] = str(take_price)

        logger.debug("Order params: %s", params)

        order = await self.post_request("/openApi/swap/v2/trade/order", body=params)

        if order.get("retMsg") != "OK" and order.get("retCode") != 0:
            logger.critical("Order fail: %s", order)
            raise Exception(order)

        res = order.get("result") or {}
        oid = res.get("orderId") if isinstance(res, dict) else None
        if oid is None and isinstance(res, dict):
            oid = (res.get("order") or {}).get("orderId")
        logger.info("new order created. id: %s", oid)
        return str(oid) if oid is not None else None

    async def cancel_all_orders(self):
        logger.info("cancel_all_orders (BingX: по символам открытых ордеров)")
        open_resp = await self.get_request("/openApi/swap/v2/trade/openOrders", params={})
        raw = open_resp.get("result") or {}
        lst = raw.get("list") if isinstance(raw, dict) else raw
        if not isinstance(lst, list):
            lst = []
        symbols = {str(o.get("symbol")) for o in lst if o.get("symbol")}
        for sym in symbols:
            await self.post_request(
                "/openApi/swap/v2/trade/allOpenOrders",
                body={"symbol": sym},
            )

    async def cancel_order(self, symbol: str, order_id: str):
        logger.info("_cancel_order %s id: %s", symbol, order_id)
        params = {
            "symbol": _to_bingx_symbol(symbol),
            "orderId": order_id,
        }

        try:
            canceled_order = await self.delete_request("/openApi/swap/v2/trade/order", params=params)
            logger.debug("_cancel_order: %s", canceled_order)
            return True
        except exceptions.OrderNotExist:
            try:
                await self._check_order(symbol, order_id)
            except exceptions.OrderNotFound:
                pass

            logger.error('Order does not exist symbol: %s | id: %s', symbol, order_id)
            return True

    async def get_order_history(
            self,
            symbol: str,
            order_id: str = None,
            start_time: int = None,
            end_time: int = None,
            retries: int = 120
    ) -> list[OrderData] | OrderData:
        tries_total = retries
        logger.debug('get_order_history %s | %s | %s | %s | %s', symbol, order_id, start_time, end_time, retries)
        if start_time and end_time:
            period = (end_time - start_time)
            if period >= 604800000:
                results = []
                while period > 0:
                    period -= 604000000
                    if period < 0:
                        end = end_time
                    else:
                        end = start_time + 604000000
                    results += await self.get_order_history(
                        symbol=symbol,
                        start_time=start_time,
                        end_time=end,
                        order_id=order_id,
                        retries=retries
                    )
                    start_time = end + 1

                return results

        order_info = {}
        orders = []
        cursor = None
        page = 1
        max_pages = 20
        while not order_info:
            params = {}
            if symbol:
                params["symbol"] = _to_bingx_symbol(symbol)

            if order_id:
                params["orderId"] = order_id
            else:
                params["limit"] = 50
                if cursor:
                    params["cursor"] = cursor

            if start_time and end_time:
                params["startTime"] = start_time
                params["endTime"] = end_time

            if order_id:
                order_info = await self.get_request("/openApi/swap/v2/trade/order", params=params)
            else:
                order_info = await self.get_request("/openApi/swap/v2/trade/allOrders", params=params)

            res = order_info.get("result") or {}
            if order_id and isinstance(res, dict) and res.get("orderId"):
                chunk = [res]
            else:
                chunk = res.get("list") if isinstance(res, dict) else None
                if chunk is None and isinstance(res, list):
                    chunk = res
            chunk = chunk or []
            orders += chunk

            next_cur = res.get("nextPageCursor") if isinstance(res, dict) else None
            if not order_id and next_cur:
                cursor = next_cur
                if page < max_pages:
                    order_info = {}
                    page += 1
                    continue

            if not orders and order_id:
                if retries > 1:
                    logger.warning('Order for %s not found id: %s Retries %s', symbol, order_id, retries)

                    retries -= 1
                    current_try = tries_total - retries
                    if current_try < 10:
                        logger.debug('2 %s', current_try)
                        await asyncio.sleep(2)
                    elif 10 <= current_try < 20:
                        logger.debug('10 %s', current_try)
                        await asyncio.sleep(10)
                    else:
                        logger.debug('20 %s', current_try)
                        await asyncio.sleep(20)

                    order_info = {}
                    continue
                else:
                    logger.critical('Order for %s not found id: %s', symbol, order_id)
                    raise exceptions.OrderNotFound

            if not order_id:
                orders_list = []
                for order in orders:
                    orders_list.append(_bingx_order_to_model(order))
                return orders_list

            order = orders[0]
            logger.debug(order)
            order_data = _bingx_order_to_model(order)
            return order_data

    async def _check_order(self, symbol: str, order_id: str):
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
            start_time: datetime.datetime = None,
            end_time = None,
            retries: int = 70,
    ) -> list[ExecutionsData]:
        _ = retries
        next_page_cursor = ""
        executions = []
        limit = 100
        params: dict = {
            "symbol": _to_bingx_symbol(symbol),
            "limit": limit,
        }
        if start_time is not None:
            if isinstance(start_time, datetime.datetime):
                params["startTime"] = int(start_time.timestamp() * 1000)
            else:
                params["startTime"] = int(start_time * 1000) if float(start_time) < 1e12 else int(start_time)
        if end_time is not None:
            if isinstance(end_time, datetime.datetime):
                params["endTime"] = int(end_time.timestamp() * 1000)
            else:
                params["endTime"] = int(end_time * 1000) if float(end_time) < 1e12 else int(end_time)

        while True:
            if next_page_cursor:
                params["cursor"] = next_page_cursor
            response = await self.get_request("/openApi/swap/v2/trade/myTrades", params=params)

            if response.get("retMsg") != "OK" and response.get("retCode") != 0:
                logger.critical(response)
                raise Exception("get_executions ERROR")

            res = response.get("result") or {}
            chunk = res.get("list") if isinstance(res, dict) else None
            if chunk is None and isinstance(res, list):
                chunk = res
            chunk = chunk or []
            executions += chunk

            next_cur = res.get("nextPageCursor") if isinstance(res, dict) else None
            if next_cur and len(chunk) == limit:
                next_page_cursor = next_cur
            else:
                break

        executions_data = []
        for ex in executions:
            qty = ex.get("qty") or ex.get("executedQty") or "0"
            opening_position = Decimal(str(qty)) >= 0
            ps = (ex.get("positionSide") or "LONG").upper()
            position_side = "BUY" if ps == "LONG" else "SELL"
            data = ExecutionsData(
                symbol=(ex.get("symbol") or "").replace("-", ""),
                opening_position=opening_position,
                side=ex.get("side", ""),
                position_side=position_side,
                exec_qty=qty,
                order_id=str(ex.get("orderId", "")),
                price=ex.get("price") or ex.get("execPrice") or "0",
                time=ex.get("time") or ex.get("fillTime"),
            )
            data.customize()
            executions_data.append(data)
        return executions_data

    async def get_open_order(self, symbol: str, order_id: str, retries: int = 70) -> OrderData:
        logger.debug('get_open_order for %s, order_id: %s', symbol, order_id)

        tries = 3
        retries = int(retries / tries)
        while tries:
            orders = await self.get_open_orders(symbol=symbol, retries=retries)

            for order in orders:
                if order_id == order.order_id:
                    return order

            try:
                order = await self.get_order_history(symbol, order_id, retries=retries)
            except (exceptions.OrderNotFound, exceptions.OrderNotExist):
                logger.critical('Order for %s not found: %s', symbol, order_id)
                if tries:
                    tries -= 1
                    await asyncio.sleep(2)
                    continue
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
            symbol: str = None,
            coin: str = None,
            retries: int = 70
    ) -> list[OrderData]:
        _ = coin, retries
        logger.debug("get_open_orders for %s", symbol)

        params = {}
        if symbol:
            params["symbol"] = _to_bingx_symbol(symbol)

        order_info = await self.get_request("/openApi/swap/v2/trade/openOrders", params=params)
        logger.debug("order_info: %s", order_info)

        if order_info.get("retMsg") != "OK" and order_info.get("retCode") != 0:
            logger.critical(order_info)
            return []

        raw = order_info.get("result") or {}
        orders_raw_list = raw.get("list") if isinstance(raw, dict) else order_info.get("result")
        if orders_raw_list is None and isinstance(raw, list):
            orders_raw_list = raw
        orders_raw_list = orders_raw_list or []
        logger.debug("got_open_orders: %s", orders_raw_list)

        if not orders_raw_list:
            logger.debug("orders not found")
            return []

        return [_bingx_order_to_model(order) for order in orders_raw_list]

    async def get_all_positions(self) -> list[PositionData]:
        position_response = await self.get_request("/openApi/swap/v2/user/positions", params={})
        raw = position_response.get("result") or {}
        lst = raw.get("list") if isinstance(raw, dict) else None
        if lst is None and isinstance(raw, list):
            lst = raw
        lst = lst or []

        positions = []
        for position in lst:
            amt = Decimal(str(position.get("positionAmt") or "0"))
            if amt == 0:
                continue
            positions.append(_bingx_position_to_model(position))
        return positions

    async def get_position(self, symbol: str, side: str, empty_available: bool = False) -> PositionData | None:
        side_u = side.upper()
        _ = empty_available
        position_response = await self.get_request(
            "/openApi/swap/v2/user/positions",
            params={"symbol": _to_bingx_symbol(symbol)},
        )
        raw = position_response.get("result") or {}
        positions_list = raw.get("list") if isinstance(raw, dict) else None
        if positions_list is None and isinstance(raw, list):
            positions_list = raw
        positions_list = positions_list or []

        want_ps = "LONG" if side_u == "BUY" else "SHORT"
        position = None
        zero_position = None
        for pos in positions_list:
            ps = (pos.get("positionSide") or "").upper()
            if ps == want_ps:
                position = pos
                break
            if Decimal(str(pos.get("positionAmt") or "0")) == 0:
                zero_position = pos

        if position is None:
            if zero_position is None:
                return None
            position = dict(zero_position)
            position["positionSide"] = want_ps

        if Decimal(str(position.get("positionAmt") or "0")) == 0 and not empty_available:
            return None

        return _bingx_position_to_model(position)

    async def close_all_positions(self, symbol: str, position_data: PositionData):
        logger.warning('close_all_positions')

        close_positions_order_id = {}
        retries = 10
        while retries and not close_positions_order_id:
            try:
                close_positions_order_id = await self.new_order(
                    symbol=symbol,
                    order_type='MARKET',
                    side='SELL' if position_data.side.upper() == 'BUY' else 'BUY',
                    quantity=float(position_data.size),
                    reduce_only=True,
                )

            except Exception as e:
                logger.critical('Unexpected error: % s | %s', type(e), e)
                retries -= 1
                await asyncio.sleep(0.7)

        # return self.get_order_history(symbol=symbol, order_id=close_positions_order_id)

        logger.debug('close_all_positions: %s', close_positions_order_id)

    async def get_deposit_transactions(self, start_time: int = None, end_time: int = None):
        params = {}

        if start_time:
            params["startTime"] = str(start_time)
            params["endTime"] = str(end_time)

        response = None
        retries = 5
        cursor = None
        transfers_list = []
        while retries and not response:
            if cursor:
                params['cursor'] = cursor

            try:
                response = await self.get_request(
                    "/openApi/wallet/v1/capital/deposit/hisrec",
                    params=params,
                )

            except Exception as e:
                logger.critical("Unexpected error: %s | %s", type(e), e)
                logger.exception(e)
                retries -= 1
                if not retries:
                    raise e
                await asyncio.sleep(0.5)
                continue

            if response:
                logger.debug("response: %s", response)

                res = response.get("result") or {}
                rows = res.get("rows") or res.get("data") or res.get("list") or []
                if isinstance(rows, dict):
                    rows = rows.get("rows") or []
                for transfer in rows:
                    transfers_list.append(transfer)
                next_cur = res.get("nextPageCursor") if isinstance(res, dict) else None
                if next_cur:
                    cursor = next_cur
                    response = None
                    continue
                logger.debug("trafers_list: %s", transfers_list)
                return transfers_list
            raise Exception("empty deposit response")

    async def get_closed_pnl_history(
            self,
            start_time: int = None,
            end_time: int = None,
            symbol: str = None,
    ) -> list[PNLData]:

        if start_time and end_time:
            period = (end_time - start_time)
            seven_days = 604800000
            if period >= seven_days:
                results = []
                while period > 0:
                    period -= 604000000
                    if period < 0:
                        end = end_time
                    else:
                        end = start_time + 604000000
                    results += await self.get_closed_pnl_history(start_time=start_time, end_time=end, symbol=symbol)
                    start_time = end + 1

                return results

        params: dict = {
            "incomeType": "REALIZED_PNL",
            "limit": 100,
        }

        if symbol:
            params["symbol"] = _to_bingx_symbol(symbol)

        if start_time and end_time:
            params["startTime"] = start_time
            params["endTime"] = end_time

        logger.debug("get_closed_pnl start: %s | end: %s", start_time, end_time)

        response = None
        retries = 15
        cursor = None
        pnls_list = []
        while retries and not response:
            if cursor:
                params["cursor"] = cursor

            response = await self.get_request("/openApi/swap/v2/user/income", params=params)

            res = response.get("result") or {}
            lst = res.get("list") if isinstance(res, dict) else None
            if lst is None and isinstance(res, list):
                lst = res
            lst = lst or []

            for r in lst:
                row = {
                    "orderId": str(r.get("orderId") or r.get("infoId") or r.get("tradeId", "")),
                    "symbol": (r.get("symbol") or "").replace("-", ""),
                    "income": r.get("income") or r.get("amount") or "0",
                    "createdTime": int(r.get("time") or 0),
                    "updatedTime": int(r.get("time") or 0),
                }
                pnls_list.append(PNLData.model_validate(row))
            next_cur = res.get("nextPageCursor") if isinstance(res, dict) else None
            if next_cur:
                cursor = next_cur
                response = None
                continue
            return pnls_list

    async def get_closed_pnls_list(
            self,
            start_time: int = None,
            end_time: int = None,
            symbol: str = None,
            order_id: str = None
    ) -> list[PNLData]:
        logger.debug(
            'get_closed_pnls_list | symbol: %s | start_time: %s | end_time: %s | order_id: %s',
            symbol,
            start_time,
            end_time,
            order_id
        )
        retries = 70
        while retries > 0:
            closed_pnls = await self.get_closed_pnl_history(
                start_time=start_time,
                end_time=end_time,
                symbol=symbol,
            )
            if order_id is not None:
                closed_odred_id_in_history = False
                for closed_pnl in closed_pnls:
                    if closed_pnl.order_id == order_id:
                        closed_odred_id_in_history = True
                        break

                if not closed_odred_id_in_history:
                    logger.info('closed_order_id_in_history not found! retries: %s', retries)
                    retries -= 7
                    if retries > 0:
                        time.sleep(1)
                        continue
                    else:
                        # raise exceptions.ClosedPnlNotFound
                        logger.critical(
                            'ClosedPnl by order id Not Found! order_id: %s | closed_pnls: %s',
                            order_id,
                            closed_pnls,
                        )
                        return closed_pnls

            return closed_pnls
