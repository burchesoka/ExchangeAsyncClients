import asyncio
import time
import datetime
from decimal import ROUND_DOWN, Decimal
import logging
import os

from dotenv import load_dotenv

import aiohttp

from async_binance_websocket import AsyncBinanceWebsocket, test_binance_websocket
from base import MarginMode, PositionData
import exceptions
from clients import AsyncBybitFuturesClient, AsyncBinanceFuturesClient, AsyncBingxFuturesClient
from clients.base import INTERVAL_IN_SEC, PositionMode, OrderData


load_dotenv()


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s:%(lineno)s - %(levelname)s - %(message)s',
    )


setup_logging()
logger = logging.getLogger(__name__)


async def diagnose_binance_ws():
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_SECRET')
    ws = AsyncBinanceWebsocket(api_key=api_key, api_secret=api_secret)

    # 1) Public stream diagnosis (does not depend on API key)
    ws.create_klines_queues(['BTCUSDT'])
    public_task = asyncio.create_task(
        ws.public_ws(['btcusdt@kline_1m'])
    )
    try:
        kline = await asyncio.wait_for(ws.klines_queues['BTCUSDT'].get(), timeout=20)
        logger.info('PUBLIC OK: got kline BTCUSDT: %s', kline)
    except asyncio.TimeoutError:
        logger.error('PUBLIC FAIL: no kline event in 20s')
    finally:
        public_task.cancel()
        await asyncio.gather(public_task, return_exceptions=True)

    # 2) Private stream diagnosis (depends on key permissions)
    if not api_key:
        logger.error('PRIVATE FAIL: BINANCE_API_KEY is empty')
        return
    headers = {"X-MBX-APIKEY": api_key}
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            listen_key = await ws._create_listen_key(session)
            logger.info('PRIVATE OK: listenKey created: %s...', listen_key[:10])
            await ws._close_listen_key(session, listen_key)
        except Exception as e:
            logger.error('PRIVATE FAIL: cannot create listenKey: %s', e)


async def test_executions(client: AsyncBybitFuturesClient | AsyncBinanceFuturesClient, symbol: str, position: PositionData):
    pos_size = position.size if position else 0
    x = await client.get_executions(symbol=symbol)

    print('Start pos:', pos_size)

    prev_time = None
    for i in x:

        if prev_time is not None:
            if i.time > prev_time:
                raise Exception('Time is not sorted')
        prev_time = i.time

        if i.opening_position:
            pos_size -= i.exec_qty
        else:
            pos_size += i.exec_qty

        print(i)
        # print(i.symbol, datetime.datetime.fromtimestamp(i.time / 1000), i.side, i.exec_qty, 'o:', i.opening_position,
        #       'pr:', i.price, 'pos:', pos_size)

        # if pos_size == 0:
        #     print(x)
        #     break


async def test_dataframe(
        client: AsyncBybitFuturesClient | AsyncBinanceFuturesClient,
        symbol: str,
        interval: str,
        candles: int,
        max_limit: int = 15,
        start_time: int | None = None):
    dataframe = await client.get_history_data_frame(symbol=symbol, interval=interval, candles=candles,
                                                    start_time=start_time, max_limit=max_limit)

    for i in range(len(dataframe)):
        print(datetime.datetime.fromtimestamp(dataframe.open_time[i] / 1000), dataframe.close[i])
        if i != 0:
            if dataframe.open_time[i] - dataframe.open_time[i - 1] != INTERVAL_IN_SEC[interval] * 1000:
                raise Exception('Time interval is not correct')


async def update_position_orders(
        futures_client: AsyncBybitFuturesClient | AsyncBinanceFuturesClient,
        position_data,
        active_orders: list,
        symbol: str,
):
    pos_size = position_data.size
    orders = {}
    closing_orders = {}

    opening_side = position_data.side.upper()
    for active_order in active_orders:
        if active_order.side.upper() == opening_side:
            orders[active_order.order_id] = active_order

    orders_ids = []
    orders_ids_before_closing_order = []
    closing_orders_ids = []

    start_time = datetime.datetime.now().timestamp() - datetime.timedelta(hours=167).total_seconds()
    end_time = datetime.datetime.now().timestamp()
    prev_position = Decimal('0')
    prev_position_closed_once = False
    prev_position_detected = False
    for i in range(25):
        executions = await futures_client.get_executions(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time
        )
        prev_ex_time = None
        for ex in executions:
            if prev_ex_time is not None:
                print('ex.time > prev_ex_time ', ex.time > prev_ex_time)
            prev_ex_time = ex.time
            logger.debug('%s %s %s', pos_size, prev_position, ex)
            if ex.opening_position:
                pos_size -= ex.exec_qty
                orders_ids.append(ex.order_id)
                if closing_orders_ids:
                    orders_ids_before_closing_order.append(ex.order_id)
                if prev_position_detected:
                    prev_position -= ex.exec_qty
                    if prev_position == 0 and not prev_position_closed_once:
                        prev_position_closed_once = True
                    elif prev_position == 0 and prev_position_closed_once:
                        logger.info('CheckAgainNeeded lost orders?')
                        await asyncio.sleep(60)
                        raise Exception('CheckAgainNeeded')
            else:
                pos_size += ex.exec_qty
                closing_orders_ids.append(ex.order_id)
                if len(set(closing_orders_ids)) > 1:
                    # await _cancel_active_orders(active_orders=active_orders, except_closing_order=True)
                    logger.info(
                        'CheckAgainNeeded closing_orders_qty > 1 lost orders? \nexecutions: %s',
                        # self,
                        executions
                    )
                    await asyncio.sleep(30)
                    raise Exception('CheckAgainNeeded')

                prev_position = ex.exec_qty
                prev_position_detected = True

            if pos_size == 0:
                break
        if pos_size == 0:
            break
        else:
            end_time = start_time
            start_time = start_time - datetime.timedelta(hours=167).total_seconds()

    if pos_size != 0:
        await asyncio.sleep(1)
        logger.info('CheckAgainNeeded ',)
        raise Exception('CheckAgainNeeded')

    orders_ids = list(set(orders_ids))
    orders_ids_before_closing_order = list(set(orders_ids_before_closing_order))
    closing_orders_ids = list(set(closing_orders_ids))

    for order_id in orders_ids:
        try:
            orders[order_id] = await futures_client.get_order_history(
                symbol=symbol,
                order_id=order_id,
                retries=2
            )
        except exceptions.OrderNotFound:
            try:
                orders[order_id] = await futures_client.get_open_order(
                    symbol=symbol,
                    order_id=order_id
                )
                logger.info('Order partially filled? %s', orders[order_id])
            except (exceptions.AlreadyFilledOrder, exceptions.CancelledOrder):
                logger.info('CheckAgainNeeded')
                raise exceptions.CheckAgainNeeded

    logger.debug('ORDERS UPDATED \n%s\n%s', orders, closing_orders_ids)


async def main(bingx: bool = False, bybit: bool = False, binance: bool = False):
    async with aiohttp.ClientSession() as session:
        ''' Bullet '''
        api_key = os.getenv('BYBIT_API_KEY')
        api_secret = os.getenv('BYBIT_SECRET')

        bybit_client = AsyncBybitFuturesClient(
            session=session,
            api_key=api_key,
            api_secret=api_secret,
            category="linear",
            test=False,
        )

        """ BINANCE """
        api_key = os.getenv('BINANCE_API_KEY')
        api_secret = os.getenv('BINANCE_SECRET')

        binance_client = AsyncBinanceFuturesClient(
            session=session,
            api_key=api_key,
            api_secret=api_secret,
            # category="linear",
            test=False,
            # broker_id=settings.bybit_broker_id,
        )

        """ BINGX """
        api_key = os.getenv('BINGX_API_KEY')
        api_secret = os.getenv('BINGX_API_SECRET')

        bingx_client = AsyncBingxFuturesClient(
            session=session,
            api_key=api_key,
            api_secret=api_secret,
            # category="linear",
            test=False,
            # broker_id=settings.bybit_broker_id,
        )

        if bingx:
            x = input('Continue with bingx? Y/N ')
            if x.lower() == 'y':
                loops = []
                for i in range(1):
                    loops.append(asyncio.create_task(test_all(bingx_client, position_mode=PositionMode.hedge)))
                await asyncio.gather(*loops)
        
        if bybit:
            x = input('Continue with bybit? Y/N ')
            if x.lower() == 'y':
                loops = []
                for i in range(1):
                    loops.append(asyncio.create_task(test_all(bybit_client, position_mode=PositionMode.hedge)))
                await asyncio.gather(*loops)
        
        if binance:
            x = input('Continue with binance? Y/N ')
            if x.lower() == 'y':
                loops = []
                loops.append(asyncio.create_task(test_all(binance_client, position_mode=PositionMode.hedge)))
                await asyncio.gather(*loops)


async def main_test(client: AsyncBybitFuturesClient | AsyncBinanceFuturesClient):
    wallet = await client.get_wallet_data()
    print('wallet ', wallet)
    return
    x = await client.get_open_orders(symbol='ETHUSDT')
    print('get_open_orders ', x)
    order_id = 'a3764a2b-6315-46d0-919a-121f365e230a'
    try:
        x = await client.get_open_order(symbol='ETHUSDT', order_id=order_id)
        print('get_open_order ', x)
    except exceptions.CancelledOrder:
        print('CancelledOrder')
    try:
        x = await client.get_order_history(symbol='ETHUSDT', order_id=order_id)
        print('get_order_history ', x)
    except Exception as e:
        print('get_order_history error ', e)
    return
    start_time = int((time.time() - (60 * 60 * 180)) * 1000)
    print(start_time)
    await test_dataframe(client, 'ETHUSDT', '1h', 50)
    return
    # order_id = await client.new_order(
    #         symbol='XRPUSDT',
    #         quantity='7',
    #         order_type='MARKET',
    #         side='SELL',
    #         reduce_only=False,
    #         position_mode='hedge'
    #     )
    # print(order_id)
    # pos = await client.get_position(symbol='MUSDT', side='BUY')
    # print(pos)
    # await update_position_orders(client, pos, [], 'MUSDT')
    x = await client.switch_position_mode_for_one_symbol(symbol='ETHUSDT', mode='one_way')
    print('switch_position_mode_for_one_symbol ', x)
    # x = await client.switch_position_mode_for_one_symbol(symbol='ETHUSDT', mode='hedge')
    # print('switch_position_mode_for_one_symbol ', x)
    return
    # wallet = await client.get_wallet_data()
    x = await client.get_account_info()
    print('get_account_info ', x)
    x = await client.is_master_trader_account()
    print('is_master_trader_account ', x)
    x = await client.get_api_key_info()
    print('get_api_key_info ', x)
    x = await client.get_user_id()
    print('get_user_id ', x)
    x = await client.get_instrument_info(symbol='ETHUSDT')
    print('get_instrument_info ', x)
    return
    wallet = await client.get_wallet_data()
    print(wallet)
    # wallet = await client.get_all_positions()
    # print(wallet)

    # order_id = await client.new_order(
    #         symbol='ETHUSDT',
    #         price='1500',
    #         quantity='0.04',
    #         order_type='LIMIT',
    #         side='BUY',
    #         reduce_only=False,
    #         position_mode='hedge'
    #     )
    # print(order_id)
    x = await client.get_open_orders(symbol='ETHUSDT')
    print('get_open_order ', x)
    x = await client.get_position(symbol='ETHUSDT', side='BOTH')
    print('get_position ', x)
    x = await client.get_position(symbol='ETHUSDT', side='SELL')
    print('get_position ', x)
    x = await client.get_instrument_info(symbol='ETHUSDT')
    print('get_instrument_info ', x)
    x = await client.get_account_info()
    print('get_account_info ', x)
    return
    # x = await client.get_open_orders()
    # print('get_open_orders ', x)
    # x = await client.get_executions(symbol='ETHUSDT')
    # print('get_executions ', x)
    # x = await client.get_history_data_frame(symbol='ETHUSDT', interval='1h', candles=1200)
    start_time = int((time.time() - (60 * 60 * 180)) * 1000)
    print(start_time)

    # await test_dataframe(client, 'ETHUSDT', '1h', 20, start_time)

    return
    # x = await client.get_position(symbol='XRPUSDT', side='SELL')
    # print(x)
    # x = await client.get_position(symbol='XRPUSDT', side='BOTH')
    # print(x)
    return
    await test_executions(client, 'DOGEUSDT')
    await test_executions(client, 'XRPUSDT')
    return
    wallet = await client.get_wallet_data()
    print(wallet)
    wallet = await client.get_wallet_data()
    print(wallet)

    order_id = await client.new_order(
        symbol='ETHUSDT',
        price='1800',
        quantity='0.03',
        order_type='LIMIT',
        side='BUY',
        reduce_only=False,
        position_mode='hedge'
    )

    """ test get_open_orders """
    x = await client.get_open_orders()
    print('get_open_orders ', x)
    x = await client.get_open_orders(coin='USDT')
    print('client get_open_orders ', x)
    x = await client.cancel_all_orders()
    print('cancel_all_orders ', x)
    await client.cancel_all_orders()
    return

    order_id = await client.new_order(
        symbol='XRPUSDT',
        price='1',
        quantity='1',
        order_type='LIMIT',
        side='BUY',
        reduce_only=False,
        position_mode='hedge'
    )
    print(order_id)
    try:
        x = await client.get_open_order(symbol='XRPUSDT', order_id=order_id)
        print('get_open_order ', x)
    except Exception as e:
        print('get_open_order error ', e)

    x = await client.check_order(symbol='XRPUSDT', order_id=order_id)
    print('check_order ', x)


async def test_market_order(
    client: AsyncBybitFuturesClient | AsyncBinanceFuturesClient,
    position_mode: PositionMode
    ):
    if position_mode == PositionMode.hedge:
        pass
    else:
        raise Exception('Position mode is not hedge')
    
    symbol = 'XRPUSDT'
    quantity = '1'

    x = await client.switch_position_mode(symbol=symbol, mode=PositionMode.hedge)
    print('switch_position_mode ', x)

    y = input('Make BUY MARKET orders for %s with quantity %s? Continue? Y/N ' % (symbol, quantity))
    if y.lower() == 'y':

        try:
            long_order_id = await client.new_order(
                symbol=symbol,
                quantity=quantity,
                order_type='MARKET',
                side='BUY',
                reduce_only=False,
                position_mode=position_mode
            )
            print('long_order_id ', long_order_id)
        except Exception as e:
            print('new_order error ', e)
            raise e

        x = await client.get_order_history(symbol=symbol, order_id=long_order_id)
        print('get_order_history ', x)
        position_data = await client.get_position(
            symbol=symbol,
            side='BUY',
        )

        print('position_data ', position_data)
        if position_data.size != Decimal(quantity):
            raise Exception('Position size is not correct')

    '''  SHORT '''
    instrument_info = await client.get_instrument_info(symbol=symbol)
    print('instrument_info ', instrument_info)
    quantity_short = str(Decimal(quantity) * Decimal('2.2').quantize(Decimal(instrument_info.min_order_qty), rounding=ROUND_DOWN))
    print('new quantity for reverse order', quantity_short)


    y = input('Make SELL MARKET orders for %s with quantity %s? Continue? Y/N ' % (symbol, quantity_short))
    if y.lower() != 'y':
        return

    short_order_id = await client.new_order(
        symbol=symbol,
        quantity=quantity_short,
        order_type='MARKET',
        side='SELL',
        reduce_only=False,
        position_mode=position_mode
    )
    print('short_order_id ', short_order_id)
    x = await client.get_order_history(symbol=symbol, order_id=short_order_id)
    print('get_order_history ', x)
    position_data = await client.get_position(
        symbol=symbol,
        side='SELL',
    )
    print('position_data ', position_data)
    if position_data.size < Decimal('0'):
        raise Exception('Position size is negative')
    if position_data.size != Decimal(quantity_short):
        raise Exception('Position size is not correct')

    y = input('Close positions? Continue? Y/N ')
    if y.lower() != 'y':
        return

    '''  CLOSE '''
    order_id = await client.new_order(
        symbol=symbol,
        quantity=quantity_short,
        order_type='MARKET',
        side='BUY',
        reduce_only=True,
        position_mode=position_mode
    )
    x = await client.get_order_history(symbol=symbol, order_id=order_id)
    print('get_order_history ', x)

    position_data = await client.get_position(
        symbol=symbol,
        side='SELL',
        empty_available=True
    )
    print('position_data SHORT', position_data)
    if position_data and position_data.size != Decimal('0'):
        raise Exception('Position size is not 0')
    
    order_id = await client.new_order(
        symbol=symbol,
        quantity=quantity,
        order_type='MARKET',
        side='SELL',
        reduce_only=True,
        position_mode=position_mode
    )
    x = await client.get_order_history(symbol=symbol, order_id=order_id)
    print('get_order_history ', x)

    position_data = await client.get_position(
        symbol=symbol,
        side='BUY',
        empty_available=True
    )
    print('position_data LONG', position_data)
    if position_data and position_data.size != Decimal('0'):
        raise Exception('Position size is not 0')

def check_order_data(order_data: OrderData, quantity: str, price: str, order_status: str, side: str):
    if order_data.order_status != order_status:
        raise Exception('Order status is not %s' % (order_status))
    if order_data.qty != Decimal(quantity):
        raise Exception('Order quantity is not %s' % (quantity))
    if order_data.price != Decimal(price):
        raise Exception('Order price is not %s' % (price))
    if order_data.side != side:
        raise Exception(f'Order side is not {side}, is {order_data.side}')

async def test_limit_order(
    client: AsyncBybitFuturesClient | AsyncBinanceFuturesClient | AsyncBingxFuturesClient,
    symbol: str,
    position_mode: PositionMode,
):
    x = await client.cancel_all_orders()
    print('cancel_cancel_all_orders', x)
    
    ''' Make LIMIT order '''
    symbol = 'XRPUSDT'
    price = '1.01'
    quantity = '1'

    y = input('Make limit order for %s at %s position_mode: %s? Continue? Y/N ' % (symbol, price, position_mode))
    if y.lower() == 'y':
        x = await client.switch_position_mode(symbol=symbol, mode=position_mode)
        print('switch_position_mode ', x)
        order_id = await client.new_order(
            symbol=symbol,
            price=price,
            quantity=quantity,
            order_type='LIMIT',
            side='BUY',
            reduce_only=False,
            position_mode=position_mode
        )
        print('order_id ', order_id)
        x = await client.get_open_order(symbol=symbol, order_id=order_id)
        print('get_open_order ', x)
        check_order_data(x, quantity, price, 'NEW', 'BUY')

        x = await client.get_open_orders(symbol=symbol)
        try:
            x = await client.get_order_history(symbol=symbol, order_id=order_id, retries=1)
            print('get_order_history new?', x)
            raise Exception('NEW Order found in get_order_history')
        except exceptions.OrderNotFound:
            print('OrderNotFound')
        x = await client.cancel_order(symbol=symbol, order_id=order_id)
        print('cancel_order ', x)

        try:
            x = await client.get_open_order(symbol='ETHUSDT', order_id=order_id)
            raise Exception('Order not cancelled')
        except (exceptions.CancelledOrder, exceptions.OrderNotExist):
            print('CancelledOrder')

        x = await client.get_order_history(symbol=symbol, order_id=order_id)
        print('get_order_history ', x)
        check_order_data(x, quantity, price, 'CANCELLED', 'BUY')

        order_ids = []
        for i in range(2):
            order_ids.append(await client.new_order(
                symbol=symbol,
                price=price,
                quantity=quantity,
                order_type='LIMIT',
                side='BUY',
                reduce_only=False,
                position_mode=PositionMode.hedge
            ))
        print('order_ids ', order_ids)

        x = await client.get_open_orders(symbol=symbol)
        if len(x) != 2:
            raise Exception('get_open_orders length is not 2')

        x = await client.get_open_orders(coin='USDT')
        print('get_all_orders coin USDT ', x)
        print('get_all_orders len coin USDT ', len(x))
        x = await client.cancel_all_orders()
        print('cancel_cancel_all_orders', x)

async def test_instrument_info(client: AsyncBybitFuturesClient | AsyncBinanceFuturesClient | AsyncBingxFuturesClient):
    def check_instrument_info(instrument_info):
        if instrument_info.tick_size > Decimal('1'):
            print('instrument_info ', instrument_info)
            raise Exception('Tick size is not valid')
        if instrument_info.symbol == 'YFIUSDT':
            print('instrument_info ', instrument_info)
            if instrument_info.tick_size != Decimal('1'):
                raise Exception('Tick size is not 1')
        elif instrument_info.symbol == 'BTCUSDT':
            print('instrument_info ', instrument_info)
            if instrument_info.tick_size != Decimal('0.1'):
                raise Exception('Tick size is not 0.1')
    instrument_info = await client.get_all_instruments_info()
    for i in instrument_info.values():
        check_instrument_info(i)

    instrument_info = await client.get_instrument_info(symbol='YFIUSDT')
    print('instrument_info ', instrument_info)
    check_instrument_info(instrument_info)

    instrument_info = await client.get_instrument_info(symbol='ETHUSDT')
    print('instrument_info ', instrument_info)
    check_instrument_info(instrument_info)

    instrument_info = await client.get_instrument_info(symbol='BTCUSDT')
    print('instrument_info ', instrument_info)
    check_instrument_info(instrument_info)

async def test_all(client: AsyncBybitFuturesClient | AsyncBinanceFuturesClient | AsyncBingxFuturesClient,
                   position_mode: PositionMode = PositionMode.hedge):
    wallet = await client.get_wallet_data()
    print('wallet ', wallet)

    symbol = 'XRPUSDT'
    # symbol = 'MUSDT'
    leverage = 10

    await test_instrument_info(client)
    
    pos = await client.get_position(symbol=symbol, side='BUY')
    print(pos)
    if pos is not None:
        raise Exception('Position is not None for BUY')
    pos = await client.get_position(symbol=symbol, side='SELL')
    print(pos)
    if pos is not None:
        raise Exception('Position is not None for SELL')



    # await test_market_order(client=client, position_mode=position_mode)
    # return
    # await test_limit_order(client, symbol, position_mode)
    # return
    await test_executions(client, symbol, pos)

    y = input('Switch_margin_mode and set_leverage for %s? Y/N ' % (symbol))
    if y.lower() == 'y':
        x = await client.switch_margin_mode(symbol=symbol, margin_mode=MarginMode.cross, leverage=leverage)
        print('switch_margin_mode ', x)
        x = await client.switch_position_mode(symbol=symbol, mode=position_mode)
        print('switch_position_mode ', x)
        x = await client.set_leverage(symbol=symbol, leverage=10)
        print('set_leverage ', x)

        position_data_start = await client.get_position(
            symbol=symbol,
            side='BUY',
            empty_available=True
        )
        if position_data_start.leverage != str(leverage):
            raise Exception('Leverage is not correct')


    return

    # instrument_info = await client.get_instrument_info(symbol=symbol)
    # print('instrument_info ', instrument_info)

    # api_key_info = await client.get_api_key_info()
    # print('get_api_key_info ', api_key_info)
    # return

    acc_info = await client.get_account_info()
    acc_info.pop('positions')
    acc_info.pop('assets')
    print('acc_info ', acc_info)
    return

    x = await client.transfer(from_account='CONTRACT', to_account='FUND', amount=Decimal('1'))
    print('transfer_funds ', x)

    x = await client.transfer(from_account='FUND', to_account='CONTRACT', amount=Decimal('1'))
    print('transfer_funds ', x)

    return

    closed_pnls_list = await client.get_closed_pnls_list(
        start_time=int(datetime.datetime.strptime('2026-03-24 00:00:00', '%Y-%m-%d %H:%M:%S').timestamp() * 1000),
        end_time=int(time.time() * 1000))
    print('closed_pnls_list %s:' % (symbol))
    prev_time = None
    for i in closed_pnls_list:
        if prev_time is not None:
            print('i.created_time > prev_time ', i.created_time > prev_time)
            if i.created_time > prev_time:
                raise Exception('created_time is not sorted')
        prev_time = i.created_time
        print(i)
        print(datetime.datetime.fromtimestamp(i.created_time / 1000))

    return

    closed_pnls_list = await client.get_closed_pnls_list(symbol='DOGEUSDT')
    print('closed_pnls_list DOGEUSDT:')
    prev_time = None
    for i in closed_pnls_list:
        if prev_time is not None:
            print('i.created_time > prev_time ', i.created_time > prev_time)
        prev_time = i.created_time
        print(i)
        print(datetime.datetime.fromtimestamp(i.created_time / 1000))

    position_data_start_buy = await client.get_position(
        symbol=symbol,
        side='BUY',
        empty_available=True
    )
    print('position_data_start BUY', position_data_start_buy)
    if position_data_start_buy.size != Decimal('0'):
        raise Exception('Position size is not 0')
    position_data_start_sell = await client.get_position(
        symbol=symbol,
        side='SELL',
        empty_available=True
    )
    print('position_data_start SELL', position_data_start_sell)
    if position_data_start_sell.size != Decimal('0'):
        raise Exception('Position size is not 0')

    y = input('test_dataframe? Y/N ')
    if y.lower() == 'y':
        start_time = int((time.time() - (60 * 60 * 180)) * 1000)
        print(start_time)
        await test_dataframe(client, 'ETHUSDT', '1m', 50, max_limit=10, start_time=start_time)

        await test_dataframe(client, 'ETHUSDT', '1h', 50, max_limit=10, )


    


if __name__ == "__main__":
    ''' pip install python-dotenv '''
    asyncio.run(main(bingx=True, bybit=False, binance=False))
    # test_bybit_websocket(bybit_api_key='', bybit_secret='')
    # test_binance_websocket(binance_api_key=os.getenv('BINANCE_API_KEY'), binance_secret=os.getenv('BINANCE_SECRET'))