import asyncio
import copy
import hashlib
import hmac
import json
import logging
import time

import websockets.asyncio.client


logger = logging.getLogger(__name__)


WSS_NAME = "Binance Futures"
PRIVATE_WSS = "wss://ws-fapi.binance.com/ws-fapi/v1"
PUBLIC_WSS = "wss://fstream.binance.com/ws"


class AsyncBinanceWebsocket:
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
        timestamp = int(time.time() * 1000)
        payload = {
            "apiKey": self.api_key,
            "timestamp": timestamp,
        }
        query = "&".join(f"{k}={payload[k]}" for k in sorted(payload))
        signature = hmac.new(
            bytes(self.api_secret, "utf-8"),
            bytes(query, "utf-8"),
            digestmod=hashlib.sha256,
        ).hexdigest()
        payload["signature"] = signature
        await ws.send(
            json.dumps({
                "id": str(timestamp),
                "method": "session.logon",
                "params": payload,
            })
        )

    async def subscribe_private(self, ws, orders: bool, wallet: bool):
        if not any([orders, wallet]):
            raise ValueError

        topics = []
        if orders:
            topics.append("ORDER_TRADE_UPDATE")
        if wallet:
            topics.append("ACCOUNT_UPDATE")

        await ws.send(
            json.dumps(
                {
                    "id": str(int(time.time() * 1000)),
                    "method": "userDataStream.subscribe",
                    "params": {"topics": topics},
                }
            )
        )

    async def subscribe_public(self, ws, klines_topics: list[str]):
        await ws.send(
            json.dumps(
                {
                    "method": "SUBSCRIBE",
                    "params": klines_topics,
                    "id": int(time.time() * 1000),
                }
            )
        )

    async def private_ws(self, orders: bool, wallet: bool):
        async for ws in websockets.asyncio.client.connect(
            PRIVATE_WSS,
            ping_interval=45,
        ):
            if not any([orders, wallet]):
                raise ValueError

            try:
                logger.info("Private WS Connected")
                await self.sign_in_ws(ws)

                async for raw_msg in ws:
                    msg = json.loads(raw_msg)
                    logger.info("Private WS msg: %s", msg)

                    if msg.get("status") == 200 and msg.get("result", {}).get("authorizedSince"):
                        await self.subscribe_private(ws, orders=orders, wallet=wallet)
                        continue

                    event_type = msg.get("e") or msg.get("event", {}).get("e")
                    if event_type == "ORDER_TRADE_UPDATE" and orders:
                        payload = msg.get("o") or msg.get("event", {}).get("o") or msg
                        await self.orders_queue.put([payload] if isinstance(payload, dict) else payload)
                    elif event_type == "ACCOUNT_UPDATE" and wallet:
                        await self.wallet_queue.put(msg)

            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError) as e:
                logger.info("Reconnect private WS (%s)", e)
                continue
            except asyncio.exceptions.CancelledError:
                logger.info("Connection disconnected by keyboard interrupt")
                break
            except Exception as e:
                logger.exception(e)
                break

        logger.info("Private WS Disconnected")

    async def public_ws(self, klines_topics: list[str]):
        async for ws in websockets.asyncio.client.connect(
            PUBLIC_WSS,
            ping_interval=45,
        ):
            try:
                logger.info("Public WS Connected")
                await self.subscribe_public(ws, klines_topics=klines_topics)

                async for raw_msg in ws:
                    msg = json.loads(raw_msg)
                    logger.debug(msg)

                    stream = msg.get("stream", "")
                    data = msg.get("data")
                    if not stream or data is None:
                        continue

                    if "@kline_" in stream:
                        symbol = stream.split("@", 1)[0].upper()
                        await self.klines_queues[symbol].put(data)

            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError):
                continue
            except asyncio.exceptions.CancelledError:
                logger.info("Connection disconnected by keyboard interrupt")
                break
            except Exception as e:
                logger.exception(e)
                break

        logger.info("Public WS Disconnected")

    async def orders_getter_loop(self):
        while True:
            items_copy = copy.deepcopy(await self.orders_queue.get())
            async with self.lock:
                if str(items_copy) not in self.orders_items:
                    self.orders_items.append(str(items_copy))
                    orders_by_symbol = {}
                    logger.debug(items_copy)
                    for order in items_copy:
                        symbol = order.get("s") or order.get("symbol")
                        if not symbol:
                            continue
                        if symbol in orders_by_symbol:
                            orders_by_symbol[symbol].append(order)
                        else:
                            orders_by_symbol[symbol] = [order]

                    for k, v in orders_by_symbol.items():
                        try:
                            await self.orders_filtered_queues[k].put(v)
                        except KeyError:
                            logger.critical(
                                "Order for Symbol (%s) Not In Config came to websocket: \n%s",
                                k,
                                v,
                            )

                    if len(self.orders_items) > 5:
                        self.orders_items = self.orders_items[-5:]

    def create_orders_queues(self, symbols_list: list[str]):
        for symbol in symbols_list:
            self.orders_filtered_queues[symbol.upper()] = asyncio.Queue()

    def create_klines_queues(self, symbols_list: list[str]):
        for symbol in symbols_list:
            self.klines_queues[symbol.upper()] = asyncio.Queue()

    async def run_all_ws(
        self,
        orders: bool = False,
        wallet: bool = False,
        klines_topics: list[str] = None,
        triple: bool = False,
    ):
        logger.info("run_all_ws websockets ver: %s", websockets.__version__)
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


def test_binance_websocket(binance_api_key: str, binance_secret: str):
    ws = AsyncBinanceWebsocket(api_key=binance_api_key, api_secret=binance_secret)
    ws.create_orders_queues(['HYPEUSDT'])
    ws.create_klines_queues(['BTCUSDT', 'DOGEUSDT'])

    asyncio.run(ws.run_all_ws(
        orders=True,
        wallet=False,
        klines_topics=["kline.60.BTCUSDT", "kline.1.DOGEUSDT"],
        triple=True
    ))