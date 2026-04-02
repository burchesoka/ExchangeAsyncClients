import asyncio
import copy
import json
import logging
import time
import hmac
import websockets.asyncio.client


logger = logging.getLogger(__name__)


WSS_NAME = "Unified V5"
PRIVATE_WSS = "wss://{SUBDOMAIN}.{DOMAIN}.{TLD}/v5/private"
PUBLIC_WSS = "wss://{SUBDOMAIN}.{DOMAIN}.com/v5/public/{CHANNEL_TYPE}"
AVAILABLE_CHANNEL_TYPES = [
    "inverse",
    "linear",
    "spot",
    "option",
    "misc/status",
    "private",
]

TLD_MAIN = "com"


class AsyncBybitWebsocket:
    def __init__(self, api_key: str = None, api_secret: str = None):
        self.api_key = api_key
        self.api_secret = api_secret

        self.lock = asyncio.Lock()

        self.klines_queues = {}
        self.orders_queue = asyncio.Queue()
        self.wallet_queue = asyncio.Queue()

        self.orders_items = []

        self.orders_filtered_queues = {}

    async def sign_in_ws(self, ws):
        # Generate expires.
        expires = int((time.time() + 1) * 1000)

        # Generate signature.
        signature = str(hmac.new(
            bytes(self.api_secret, "utf-8"),
            bytes(f"GET/realtime{expires}", "utf-8"), digestmod="sha256"
        ).hexdigest())

        # Authenticate with API.
        await ws.send(
            json.dumps({
                "op": "auth",
                "args": [self.api_key, expires, signature]
            })
        )

    async def subscribe_private(self, ws, orders: bool, wallet: bool):
        if not any([orders, wallet]):
            raise ValueError
        topics = []
        if orders:
            topics.append("order") # order.linear ???
        if wallet:
            topics.append("wallet")
        subs = dict(
            op='subscribe',
            args=topics
        )
        await ws.send(json.dumps(subs))


    async def subscribe_public(self, ws, klines_topics: list[str]):
        subs = dict(
            op='subscribe',
            args=klines_topics
        )
        await ws.send(json.dumps(subs))


    async def private_ws(self, orders: bool, wallet: bool):
        url = PRIVATE_WSS.format(SUBDOMAIN='stream', DOMAIN='bybit', TLD=TLD_MAIN)

        async for ws in websockets.asyncio.client.connect(
                url,
                ping_interval=45,
                # ping_interval=None,
        ):
            if not any([orders, wallet]):
                raise ValueError

            try:
                logger.info('Private WS Connected')
                await self.sign_in_ws(ws)


                async for msg in ws:
                    try:
                        msg = json.loads(msg)
                        logger.info("Private WS msg: %s", msg)
                        topic = msg.get('topic')
                        op = msg.get('op')
                        if op and op == 'auth':
                            logger.info('Logged in: %s', msg['success'])
                            if msg['success']:
                                await self.subscribe_private(ws, orders=orders, wallet=wallet)
                            else:
                                logger.critical('Log in ERROR')
                                raise Exception('Log in ERROR')

                        if topic:
                            if topic == 'order':
                                await self.orders_queue.put(msg['data'])
                            elif topic == 'wallet':
                                await self.wallet_queue.put(msg)

                        error = msg.get('ret_msg')
                        if error:
                            logger.error('!!! %s', error)
                        data = msg.get('data')
                    except Exception as e:
                        logger.critical('Unknown error %s', e.args)
                        raise e

            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError) as e:
                logger.info('Reconnect private WS (%s)', e)
                continue
            except asyncio.exceptions.CancelledError as e:
                logger.info('Connection disconnected by keyboard interrupt')
                break
            except Exception as e:
                logger.exception(e)
                break

        logger.info('Private WS Disconnected')


    async def public_ws(self, klines_topics: list[str]):
        url = PUBLIC_WSS.format(SUBDOMAIN='stream', DOMAIN='bybit', TLD=TLD_MAIN, CHANNEL_TYPE='linear')
        async for ws in websockets.asyncio.client.connect(
                url,
                ping_interval=45,
                # ping_interval=None,
        ):
            try:
                logger.info('Public WS Connected')
                await self.subscribe_public(ws, klines_topics=klines_topics)

                async for msg in ws:
                    try:
                        msg = json.loads(msg)
                        logger.debug(msg)

                        error = msg.get('ret_msg')
                        if error:
                            logger.error('!!! %s', error)
                        data = msg.get('data')
                        if msg.get('topic') and 'kline' in msg.get('topic'):
                            symbol = msg['topic'].replace('kline.', '')
                            symbol = symbol[symbol.find('.') + 1:]
                            await self.klines_queues[symbol].put(data)
                    except Exception as e:
                        logger.critical('Unknown error %s', e.args)
                        raise e

            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError):
                continue
            except asyncio.exceptions.CancelledError as e:
                logger.info('Connection disconnected by keyboard interrupt')
                break
            except Exception as e:
                logger.exception(e)
                break

        logger.info('Public WS Disconnected')

    async def orders_getter_loop(self):
        while True:
            items_copy = copy.deepcopy(await self.orders_queue.get())
            async with self.lock:
                if str(items_copy) not in self.orders_items:
                    self.orders_items.append(str(items_copy))
                    orders_by_symbol = {}
                    logger.debug(items_copy)
                    for order in items_copy:
                        symbol = order['symbol']
                        if symbol in orders_by_symbol.keys():
                            orders_by_symbol[symbol].append(order)
                        else:
                            orders_by_symbol[symbol] = [order]
                    for k, v in orders_by_symbol.items():
                        try:
                            await self.orders_filtered_queues[k].put(v)
                        except KeyError:
                            logger.critical(
                                'Order for Symbol (%s) Not In Config came to websocket: \n%s',
                                k,
                                v,
                            )

                    if len(self.orders_items) > 5:
                        self.orders_items = self.orders_items[-5:]

    def create_orders_queues(self, symbols_list: list[str]):
        for symbol in symbols_list:
            self.orders_filtered_queues[symbol] = asyncio.Queue()

    def create_klines_queues(self, symbols_list: list[str]):
        for symbol in symbols_list:
            self.klines_queues[symbol] = asyncio.Queue()

    async def run_all_ws(
            self,
            orders: bool = False,
            wallet: bool = False,
            klines_topics: list[str] = None,
            triple: bool = False
    ):
        logger.info('run_all_ws websockets ver: %s', websockets.__version__)
        loops = [
            self.orders_getter_loop(),
        ]
        if orders or wallet:
            loops.append(self.private_ws(orders, wallet))
            if triple:
                loops.append(self.private_ws(orders, wallet))
                loops.append(self.private_ws(orders, wallet))
        if klines_topics:
            loops.append(self.public_ws(klines_topics))

        await asyncio.gather(*loops)

    async def get_klines_test(self):
        while True:
            klines = await self.klines_queues['BTCUSDT'].get()
            print(f'!!!!!!!!!---- {klines}')

    async def get_orders_test(self):
        while True:
            klines = await self.orders_filtered_queues['HYPEUSDT'].get()
            print(f'@@@@@@@---- {klines}')


def test_websocket(bybit_api_key: str, bybit_secret: str):
    ws = BybitWebsocket(api_key=bybit_api_key, api_secret=bybit_secret)
    ws.create_orders_queues(['HYPEUSDT'])
    ws.create_klines_queues(['BTCUSDT', 'DOGEUSDT'])

    asyncio.run(ws.run_all_ws(
        orders=True,
        wallet=False,
        klines_topics=["kline.60.BTCUSDT", "kline.1.DOGEUSDT"],
        triple=True
    ))
