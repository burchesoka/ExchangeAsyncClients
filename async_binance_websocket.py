import asyncio
import copy
import json
import logging
import time

import aiohttp
import websockets.asyncio.client


logger = logging.getLogger(__name__)


WSS_NAME = "Binance Futures"
PRIVATE_WSS = "wss://fstream.binance.com/ws/{listen_key}"
PUBLIC_WSS = "wss://fstream.binance.com/ws"
FAPI_BASE_URL = "https://fapi.binance.com"


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

    @staticmethod
    def _normalize_kline(symbol: str, raw) -> dict:
        # Binance kline payload обычно в raw["k"]
        k = raw.get("k", raw) if isinstance(raw, dict) else {}
        if not isinstance(k, dict):
            return {}
        return {
            "symbol": symbol.upper(),
            "interval": str(k.get("i", "")),
            "start": int(k.get("t", 0) or 0),
            "end": int(k.get("T", 0) or 0),
            "open": str(k.get("o", "0")),
            "high": str(k.get("h", "0")),
            "low": str(k.get("l", "0")),
            "close": str(k.get("c", "0")),
            "volume": str(k.get("v", "0")),
            "turnover": str(k.get("q", "0")),
            "confirm": bool(k.get("x", False)),
            "timestamp": int(raw.get("E", 0) or 0) if isinstance(raw, dict) else 0,
        }

    async def _create_listen_key(self, session: aiohttp.ClientSession) -> str:
        headers = {"X-MBX-APIKEY": self.api_key}
        async with session.post(f"{FAPI_BASE_URL}/fapi/v1/listenKey", headers=headers) as resp:
            data = await resp.json()
            listen_key = data.get("listenKey")
            if not listen_key:
                raise RuntimeError(f"Can't create listenKey: {data}")
            return listen_key

    async def _keepalive_listen_key(
        self,
        session: aiohttp.ClientSession,
        listen_key: str,
        stop_event: asyncio.Event,
    ):
        headers = {"X-MBX-APIKEY": self.api_key}
        while not stop_event.is_set():
            try:
                await asyncio.sleep(30 * 60)
                if stop_event.is_set():
                    return
                async with session.put(
                    f"{FAPI_BASE_URL}/fapi/v1/listenKey",
                    headers=headers,
                    params={"listenKey": listen_key},
                ) as resp:
                    if resp.status >= 400:
                        data = await resp.text()
                        logger.warning("listenKey keepalive failed: status=%s body=%s", resp.status, data)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("listenKey keepalive error: %s", e)

    async def _close_listen_key(self, session: aiohttp.ClientSession, listen_key: str):
        headers = {"X-MBX-APIKEY": self.api_key}
        try:
            async with session.delete(
                f"{FAPI_BASE_URL}/fapi/v1/listenKey",
                headers=headers,
                params={"listenKey": listen_key},
            ):
                return
        except Exception as e:
            logger.debug("listenKey close failed: %s", e)

    async def subscribe_public(self, ws, klines_topics: list[str]):
        normalized_topics = []
        for topic in klines_topics:
            t = topic.strip()
            if "@kline_" in t:
                normalized_topics.append(t.lower())

        await ws.send(
            json.dumps(
                {
                    "method": "SUBSCRIBE",
                    "params": normalized_topics,
                    "id": int(time.time() * 1000),
                }
            )
        )

    async def private_ws(self, orders: bool, wallet: bool):
        if not any([orders, wallet]):
            raise ValueError

        headers = {"X-MBX-APIKEY": self.api_key}
        async with aiohttp.ClientSession(headers=headers) as session:
            while True:
                keepalive_task = None
                stop_event = asyncio.Event()
                listen_key = None
                try:
                    listen_key = await self._create_listen_key(session)
                    url = PRIVATE_WSS.format(listen_key=listen_key)
                    logger.info("Private WS listenKey created")

                    keepalive_task = asyncio.create_task(
                        self._keepalive_listen_key(session, listen_key, stop_event)
                    )

                    async for ws in websockets.asyncio.client.connect(
                        url,
                        ping_interval=45,
                    ):
                        try:
                            logger.info("Private WS Connected")
                            async for raw_msg in ws:
                                msg = json.loads(raw_msg)
                                logger.info("Private WS msg: %s", msg)

                                event_type = msg.get("e")
                                if event_type == "ORDER_TRADE_UPDATE" and orders:
                                    payload = msg.get("o") or msg
                                    await self.orders_queue.put([payload] if isinstance(payload, dict) else payload)
                                elif event_type == "ACCOUNT_UPDATE" and wallet:
                                    await self.wallet_queue.put(msg)
                                elif event_type == "listenKeyExpired":
                                    logger.warning("listenKey expired, reconnecting")
                                    break

                        except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError) as e:
                            logger.info("Reconnect private WS (%s)", e)
                            continue
                        except asyncio.exceptions.CancelledError:
                            logger.info("Connection disconnected by keyboard interrupt")
                            raise
                        except Exception as e:
                            logger.exception(e)
                            break

                    # break async for connect loop and recreate listen key
                    stop_event.set()
                    if keepalive_task:
                        keepalive_task.cancel()
                        await asyncio.gather(keepalive_task, return_exceptions=True)
                    if listen_key:
                        await self._close_listen_key(session, listen_key)
                    continue

                except asyncio.CancelledError:
                    stop_event.set()
                    if keepalive_task:
                        keepalive_task.cancel()
                        await asyncio.gather(keepalive_task, return_exceptions=True)
                    if listen_key:
                        await self._close_listen_key(session, listen_key)
                    logger.info("Private WS Disconnected")
                    break
                except Exception as e:
                    stop_event.set()
                    if keepalive_task:
                        keepalive_task.cancel()
                        await asyncio.gather(keepalive_task, return_exceptions=True)
                    if listen_key:
                        await self._close_listen_key(session, listen_key)
                    logger.exception(e)
                    await asyncio.sleep(1)
                    continue

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
                    # logger.debug(msg)

                    stream = msg.get("e", "") or msg.get("stream", "")
                    if not stream:
                        continue

                    if "kline" in stream:
                        data = msg.get("k")
                        symbol = data.get("s").upper()
                        normalized = self._normalize_kline(symbol=symbol, raw=data)
                        if normalized:
                            await self.klines_queues[symbol].put(normalized)

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


        


        loops.append(self.get_klines_test())
        loops.append(self.get_orders_test())



        
        await asyncio.gather(*loops)
    
    async def get_klines_test(self):
        while True:
            klines = await self.klines_queues['BTCUSDT'].get()
            logger.info(f'!!!!!!!!!---- {klines}')

    async def get_orders_test(self):
        while True:
            klines = await self.orders_filtered_queues['HYPEUSDT'].get()
            logger.info(f'@@@@@@@---- {klines}')


def test_binance_websocket(binance_api_key: str, binance_secret: str):
    ws = AsyncBinanceWebsocket(api_key=binance_api_key, api_secret=binance_secret)
    ws.create_orders_queues(['HYPEUSDT'])
    ws.create_klines_queues(['BTCUSDT', 'DOGEUSDT'])

    asyncio.run(ws.run_all_ws(
        orders=True,
        wallet=False,
        klines_topics=["BTCUSDT@kline_1h", "DOGEUSDT@kline_1m"],
        triple=True
    ))