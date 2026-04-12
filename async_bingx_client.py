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
    INTERVAL_FOR_BYBIT,
    INTERVAL_FROM_BYBIT_TO_MY,
    ORDER_SPECS,
    Exchange,
)
from async_bingx_api import BingxAPI


logger = logging.getLogger(__name__)


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
            api_key=api_key,
            api_secret=api_secret,
            broker_id=broker_id,
        )

    async def get_all_instruments_info(self):
        res = []
        params = {'category': self.category}
        while True:
            result = await self.get_request("/v5/market/instruments-info", params=params)
            res += (result['result']['list'])

            if result['result'].get('nextPageCursor'):
                params['cursor'] = result['result']['nextPageCursor']
            else:
                break

        instruments = {}
        for i in res:
            instruments[i['symbol']] = InstrumentInfo(
                symbol=i['symbol'],
                min_order_qty=i['lotSizeFilter']['minOrderQty'],
                tick_size=i['priceFilter']['tickSize']
            )
        return instruments

    async def is_master_trader_account(self):
        acc_info = await self.get_account_info()

        if acc_info.get('isMasterTrader'):
            return True
        else:
            return False

    async def get_api_key_info(self):
        api_info = await self.get_request("/v5/user/query-api")

        if api_info.get('result'):
            logger.debug(api_info)
            return api_info.get('result')
        else:
            logger.critical('get_api_key_info error %s', api_info)
            raise Exception(api_info)

    async def get_user_id(self):
        api_info = await self.get_api_key_info()
        return api_info['userID']

    async def get_wallet_data(
            self,
            logs_enabled: bool = True,
            retries: int = 25,
    ) -> WalletData:
        while True:
            params = {"accountType": "UNIFIED"}

            response = await self.get_request('/v5/account/wallet-balance', params=params)

            if response.get('info'):
                response = response.get('info')

            if response.get('retMsg') == 'OK':
                try:
                    wallet_balance_data_in_usdt = None
                    wallet_balance_data = response.get('result').get('list')[0]
                    for coin_data in wallet_balance_data['coin']:
                        if coin_data.get('coin').upper() == 'USDT':
                            wallet_balance_data_in_usdt = coin_data

                    if wallet_balance_data_in_usdt is None:
                        if logs_enabled:
                            logger.warning(
                                'wallet_balance_data_in_usdt is NONE | response: %s | retries: %s',
                                response,
                                retries
                            )
                        if retries > 0:
                            retries -= 1
                            await asyncio.sleep(1)

                            continue
                        else:
                            raise exceptions.EmptyWallet(response)

                    wallet_balance_data |= wallet_balance_data_in_usdt

                    if not wallet_balance_data['totalAvailableBalance']:
                        wallet_balance_data['totalAvailableBalance'] = '0'
                    # if not wallet_balance_data_in_usdt['availableToWithdraw']:
                    #     wallet_balance_data_in_usdt['availableToWithdraw'] = Decimal(wallet_balance_data_in_usdt['walletBalance']) - \
                    #                                                          Decimal(wallet_balance_data_in_usdt['totalPositionIM']) + \
                    #                                                          Decimal(wallet_balance_data_in_usdt['unrealisedPnl'])

                    return WalletData.model_validate(wallet_balance_data)
                except Exception as e:
                    if logs_enabled:
                        logger.critical("Wrong API settings? Or zero balance? Can't get wallet data | Error: %s | %s", type(e), e)
                    raise e

    async def get_account_info(self):
        acc_info = await self.get_request("/v5/account/info")
        if acc_info is not None and acc_info.get('result'):
            return acc_info.get('result')
        else:
            raise Exception(f'Unexpected response {acc_info=}')

    async def transfer(self, from_account: str, to_account: str, amount: float):
        """
        from_account, to_account: CONTRACT, FUND or UNIFIED
        """
        logger.info('transfer %s USDT from %s to %s', amount, from_account, to_account)

        transfer_id = str(uuid.uuid4())
        params = {
            "transferId": transfer_id,
            "coin": "USDT",
            "amount": str(amount),
            "fromAccountType": from_account,
            "toAccountType": to_account
        }
        response = await self.post_request("/v5/asset/transfer/inter-transfer", body=params)

        if response.get('retCode') == 0:
            logger.info('Transfer successful %s', response)
            return response
        else:
            logger.warning('Transfer unable %s', response)
            raise exceptions.TransferUnable

    async def switch_position_mode(
        self,
        mode: PositionMode,
        symbol: str = None,
        coin: str = None
    ):
        if not any([coin, symbol]):
            logger.critical('coin and symbol cannot be both empty')
            raise ValueError

        params = {
            "category": self.category,
            "mode": "0" if mode == PositionMode.one_way else "3"
        }

        if symbol:
            params['symbol'] = symbol
        else:
            params['coin'] = coin

        try:
            response = await self.post_request("/v5/position/switch-mode", body=params)
        except exceptions.NoChange:
            logger.info('Position mode is %s already', mode)
            return True

        if response.get('retCode') == 0:
            logger.info('Position mode %s switched successful', mode)
            return True

        elif response.get('retCode') == 110025 or 'Position mode is not modified' in response.get('retMsg'):
            logger.info('Position mode is %s already', mode)
            return True
        else:
            logger.error('response: %s', response)
            raise Exception(f'Unexpected error. switch_position_mode {response=}')

    async def set_leverage(
            self,
            symbol: str,
            leverage: int,
            position_mode: PositionMode = PositionMode.hedge,
            retries=20
    ) -> bool:
        params = {
            'category': self.category,
            'symbol': symbol,
            'buyLeverage': str(leverage),
            'sellLeverage': str(leverage),
        }
        retries_local = 5
        while retries_local:
            try:
                response = await self.post_request("/v5/position/set-leverage", body=params)
            except (exceptions.LeverageNotModified, exceptions.NoChange):
                logger.info('Leverage is %s already', leverage)
                return True

            ret_code = response.get('retCode')
            if ret_code == 0:
                logger.info('Set leverage %s for %s successful | resp: %s', leverage, symbol, response)
                return True
            else:
                logger.error('response: %s', response)
                return False
                retries_local -= 1
                await asyncio.sleep(1)

    async def set_margin_mode_to_account(self, isolated: bool = False):
        params = {
            "setMarginMode": "REGULAR_MARGIN",
        }
        if isolated:
            params['setMarginMode'] = 'ISOLATED_MARGIN'

        resp = await self.post_request("/v5/account/set-margin-mode", body=params)
        logger.info(resp)

    async def switch_margin_mode(self, symbol: str, margin_mode: MarginMode, leverage: float) -> bool:
        """
        Unified account is forbidden to switch margin mode
        """
        return True

        params = {
            "category": "linear",
            "symbol": symbol,
            "tradeMode": "0" if margin_mode == MarginMode.cross else "1",
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage),
        }

        response = await self.post_request("/v5/position/switch-isolated", body=params)

        if response.get('retMsg') == 'OK':
            logger.info('Set leverage %s and mode %s successful', leverage, margin_mode)
            return True
        else:
            logger.error('response error: %s', response)
            return False


    async def get_instrument_info(self, symbol: str) -> InstrumentInfo:
        logger.debug('get_instrument_info: gets exchange info for symbol: %s', symbol)

        params = {
            "category": self.category,
            "symbol": symbol
        }
        instrument_info = await self.get_request("/v5/market/instruments-info", params=params)

        if instrument_info.get('retMsg') != 'OK':
            logger.critical('Unexpected error. instrument_info: %s', instrument_info)
            raise Exception

        price_info = instrument_info.get('result').get('list')[0]['priceFilter']
        symbol = instrument_info.get('result').get('list')[0]['symbol']
        lot_size_info = instrument_info.get('result').get('list')[0]['lotSizeFilter']
        return InstrumentInfo(
            symbol=symbol,
            min_order_qty=lot_size_info['minOrderQty'],
            tick_size=price_info['tickSize'],
            contract_value='1',
        )

    async def get_klines_history(self, symbol: str, interval: str, candles: int) -> list:
        raise NotImplementedError
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
            "category": self.category,
            "symbol": symbol,
            "interval": INTERVAL_FOR_BYBIT[interval],
            "limit": limit,
            "start": start,
            "end": end,
        }
        if end is None:
            params.pop('end')
        klines = await self.get_request("/v5/market/kline", params=params)
        klines_list = klines.get('result').get('list')
        return klines_list

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

        if reduce_only:
            position_idx = 2 if side == 'BUY' else 1
        else:
            position_idx = 1 if side == 'BUY' else 2

        params = {
            "category": self.category,
            "symbol": symbol,
            "orderType": ORDER_SPECS[order_type],
            "side": ORDER_SPECS[side],
            "qty": str(quantity),
            "price": str(price),
            "reduceOnly": reduce_only,
        }
        if position_mode == PositionMode.hedge:
            params["positionIdx"] = position_idx,

        match order_type:
            case 'STOP' | 'STOP_MARKET':
                params['triggerPrice'] = str(stop_price)
                params['triggerDirection'] = 1 if side == 'BUY' else 2

            case 'TAKE_PROFIT' | 'TAKE_PROFIT_MARKET':
                params['triggerPrice'] = str(stop_price)
                params['triggerDirection'] = 1 if side == 'SELL' else 2
            case 'LIMIT':
                if stop_price:
                    params['stopLoss'] = str(stop_price)
                    # params['triggerDirection'] = 1 if side == 'BUY' else 2
                if take_price:
                    params['takeProfit'] = str(take_price)

        logger.debug('Order params: %s', params)

        order = await self.post_request("/v5/order/create", body=params)

        if order.get('retMsg') != 'OK':
            logger.critical('Order fail: %s', order)
            raise Exception(order)

        logger.info('new order created. id: %s', order['result']['orderId'])
        return order['result']['orderId']

    async def cancel_all_orders(self):
        logger.info('cancel_all_orders')
        params = {
            "category": self.category,
            "settleCoin": 'USDT'
        }
        canceled_orders = await self.post_request("/v5/order/cancel-all", body=params)
        logger.info('canceled_orders: %s', canceled_orders)

    async def cancel_order(self, symbol: str, order_id: str):
        logger.info('_cancel_order %s id: %s', symbol, order_id)
        params = {
            "category": self.category,
            "symbol": symbol,
            "orderId": order_id
        }

        try:
            canceled_order = await self.post_request("/v5/order/cancel", body=params)
            logger.debug('_cancel_order: %s', canceled_order)
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
            params = {
                "category": self.category,
            }
            if symbol:
                params['symbol'] = symbol

            if order_id:
                params['orderId'] = order_id
            else:
                params['limit'] = '50'
                if cursor:
                    params['cursor'] = cursor

            if start_time and end_time:
                params["startTime"] = start_time
                params["endTime"] = end_time

            order_info = await self.get_request('/v5/order/history', params=params)

            orders += order_info.get('result').get('list')

            if not order_id and order_info['result']['nextPageCursor']:
                cursor = order_info['result']['nextPageCursor']
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
                    if not order['takeProfit']:
                        order['takeProfit'] = '0'
                    if not order['stopLoss']:
                        order['stopLoss'] = '0'
                    if not order['avgPrice']:
                        order['avgPrice'] = order['price']

                    order_data = OrderData.model_validate(order)
                    order_data.customize()
                    orders_list.append(order_data)

                return orders_list

            order = orders[0]
            logger.debug(order)
            if not order['takeProfit']:
                order['takeProfit'] = '0'
            if not order['stopLoss']:
                order['stopLoss'] = '0'
            if not order['avgPrice']:
                order['avgPrice'] = order['price']

            order_data = OrderData.model_validate(order)
            order_data.customize()
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
        next_page_cursor = ''
        executions = []
        limit = 100
        params = {
            'symbol': symbol,
            'category': self.category,
            'limit': limit,
            # 'startTime': start_time,
            # 'endTime': int(datetime.datetime.now().timestamp() * 1000)
        }
        while True:
            if start_time:
                start_time = int(start_time * 1000)
                end_time = int(end_time * 1000)
                params['startTime'] = start_time
                params['endTime'] = end_time
            if next_page_cursor:
                params['cursor'] = next_page_cursor
                params['startTime'] = start_time
                params['endTime'] = end_time
                params.pop('startTime')
                params.pop('endTime')
            response = await self.get_request('/v5/execution/list', params=params)
            # logger.debug(response)

            if response['retCode'] == 0:
                if response['result']:
                    executions += response['result']['list']
            else:
                logger.critical(response)
                raise Exception('get_executions ERROR')

            if response['result']['nextPageCursor'] and len(response['result']['list']) == limit:
                next_page_cursor = response['result']['nextPageCursor']
            else:
                break

        executions_data = []
        for ex in executions:
            if ex['execType'].upper() != 'TRADE':
                continue

            opening_position = True
            if Decimal(ex['closedSize']) > 0:
                opening_position = False
            if opening_position:
                position_side = ex['side'].upper()
            else:
                position_side = 'SELL' if ex['side'].upper() == 'BUY' else 'BUY'
            data = ExecutionsData(
                symbol=ex['symbol'],
                opening_position=opening_position,
                side=ex['side'],
                position_side=position_side,
                exec_qty=ex['execQty'],
                order_id=ex['orderId'],
                price=ex['execPrice'],
                time=ex.get('execTime'),
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
        logger.debug('get_open_orders for %s', symbol)

        params = {
            "category": self.category,
        }

        if symbol:
            params['symbol'] = symbol.upper()
        elif coin:
            params['settleCoin'] = coin.upper()
        else:
            logger.critical('get_open_orders did not got symbol/coin')
            raise AttributeError('get_open_orders did not got symbol/coin')

        order_info = await self.get_request('/v5/order/realtime', params=params)
        logger.debug('order_info: %s', order_info)

        if order_info.get('retMsg') != 'OK':
            logger.critical(order_info)
            return []

        orders_raw_list = order_info.get('result').get('list')
        logger.debug('got_open_orders: %s', orders_raw_list)

        if not orders_raw_list:
            logger.debug('orders not found')
            return []

        orders = []
        for order in orders_raw_list:
            if not order['takeProfit']:
                order['takeProfit'] = '0'
            if not order['avgPrice']:
                order['avgPrice'] = order['price']
            if not order['stopLoss']:
                order['stopLoss'] = '0'

            order_data = OrderData.model_validate(order)
            order_data.customize()
            orders.append(order_data)
        return orders

    async def get_all_positions(self) -> list[PositionData]:
        params = {
            "category": self.category,
            "settleCoin": 'USDT',
        }
        position_response = await self.get_request("/v5/position/list", params=params)

        positions = []
        for position in position_response['result']['list']:
            if position['liqPrice'] == '':
                position['liqPrice'] = '0.000000000001'

            if position['unrealisedPnl'] == '':
                position['unrealisedPnl'] = '0'
            if position['stopLoss'] == '':
                position['stopLoss'] = '0'
            if position['takeProfit'] == '':
                position['takeProfit'] = '0'
            positions.append(PositionData.model_validate(position))
        return positions

    async def get_position(self, symbol: str, side: str, empty_available: bool = False) -> PositionData | None:
        side = side.upper()
        retries = 100
        params = {
            "category": self.category,
            "symbol": symbol
        }
        position_response = await self.get_request("/v5/position/list", params=params)

        # logger.debug('get_positions for %s response: %s', symbol, position_response)
        positions_list = position_response['result']['list']

        if not positions_list:
            return None

        position = None
        zero_position = None
        for pos in positions_list:
            if pos.get('side').upper() == side:
                position = pos
                break
            elif (empty_available and (pos.get('positionIdx') == 2 and side == 'SELL') or (pos.get('positionIdx') == 1 and side == 'BUY')):
                """ 'positionIdx': 2 is for SELL position 'positionIdx': 1 is for BUY position"""
                zero_position = pos

        if position is None:
            if zero_position is None:
                return None
            else:
                position = zero_position
                position['side'] = side

        position['side'] = position['side'].upper()

        if position['liqPrice'] == '':
            position['liqPrice'] = '0.000000000001'
        if empty_available and position['avgPrice'] == '':
            position['avgPrice'] = '0.000000000001'
        if empty_available and position['createdTime'] == '':
            position['createdTime'] = datetime.datetime.now()
            position['updatedTime'] = datetime.datetime.now()

        if position['avgPrice'] == '' and not empty_available:
            return None

        if position['unrealisedPnl'] == '':
            position['unrealisedPnl'] = '0'
        if position['stopLoss'] == '':
            position['stopLoss'] = '0'
        if position['takeProfit'] == '':
            position['takeProfit'] = '0'

        position_data = PositionData.model_validate(position)
        logger.debug('position data: %s', position_data)

        return position_data

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
                response = await self.get_request("/v5/asset/deposit/query-internal-record", params=params)

            except Exception as e:
                logger.critical('Unexpected error: % s | %s', type(e), e)
                logger.exception(e)
                raise e

                retries -= 1
                if not retries:
                    raise e
                await asyncio.sleep(0.5)

            if response:
                logger.debug('response: %s', response)

                for transfer in response['result']['rows']:
                    transfers_list.append(transfer)
                if response['result']['nextPageCursor']:
                    cursor = response['result']['nextPageCursor']
                    response = None
                    continue
                else:
                    logger.debug('trafers_list: %s', transfers_list)
                    return transfers_list
            else:
                raise Exception

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

        params = {
            "category": self.category,
            "limit": 100
        }

        if symbol:
            params['symbol'] = symbol

        if start_time and end_time:
            params["startTime"] = str(start_time)
            params["endTime"] = str(end_time)

        logger.debug('get_closed_pnl start: %s | end: %s', start_time, end_time)

        response = None
        retries = 15
        cursor = None
        pnls_list = []
        while retries and not response:
            if cursor:
                params['cursor'] = cursor

            response = await self.get_request("/v5/position/closed-pnl", params=params)

            if response['result']:
                for pnl in response['result']['list']:
                    pnls_list.append(PNLData.model_validate(pnl))
                if response['result']['nextPageCursor']:
                    cursor = response['result']['nextPageCursor']
                    response = None
                    continue
                else:
                    return pnls_list
            else:
                raise Exception(f'response error: {response}')

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
