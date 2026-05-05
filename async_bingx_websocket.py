import asyncio
import copy
import gzip
import hashlib
import hmac
import json
import logging
import time
from urllib.parse import urlencode

import aiohttp
import websockets.asyncio.client

from base import OrderData


logger = logging.getLogger(__name__)


WSS_NAME = "BingX Swap Market"
REST_BASE = "https://open-api.bingx.com"
MARKET_WSS = "wss://open-api-swap.bingx.com/swap-market"
ACCOUNT_WSS = "wss://open-api-swap.bingx.com/swap-market"

LISTEN_KEY_CREATE = "/openApi/user/auth/userDataStream"

# Интервалы как в REST swap v2 (совпадают с async_bingx_client.INTERVAL_FOR_BINGX)
INTERVAL_FOR_BINGX_WS = {
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


def _stringify_param(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _sign_payload(api_secret: str, params: dict) -> str:
    pairs = sorted((k, _stringify_param(v)) for k, v in params.items() if k != "signature")
    query = "&".join(f"{k}={v}" for k, v in pairs)
    return hmac.new(
        api_secret.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _signed_request_url(endpoint: str, api_secret: str, api_key: str, params: dict | None) -> tuple[str, dict]:
    merged = dict(params or {})
    merged["timestamp"] = int(time.time() * 1000)
    merged["recvWindow"] = 5000
    merged["signature"] = _sign_payload(api_secret, merged)
    qs = urlencode(sorted((k, _stringify_param(v)) for k, v in merged.items()))
    url = f"{REST_BASE}{endpoint}?{qs}"
    headers = {"X-BX-APIKEY": api_key}
    return url, headers


def _maybe_decompress(raw: bytes | str) -> bytes:
    if isinstance(raw, str):
        return raw.encode("utf-8")
    if len(raw) >= 2 and raw[0] == 0x1F and raw[1] == 0x8B:
        return gzip.decompress(raw)
    return raw


def _parse_listen_key_response(data: dict) -> str:
    """Поддерживает оба формата:
    1) {"code":0,"data":{"listenKey":"..."}}
    2) {"listenKey":"..."}
    """
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected listenKey response: {data!r}")
    direct_key = data.get("listenKey")
    if direct_key:
        return str(direct_key)
    if data.get("code") not in (0, "0"):
        raise RuntimeError(f"listenKey error: {data}")
    inner = data.get("data") or {}
    if isinstance(inner, dict):
        key = inner.get("listenKey")
        if key:
            return str(key)
    raise RuntimeError(f"No listenKey in response: {data}")


def _as_bool(value, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "y"):
            return True
        if normalized in ("0", "false", "no", "n", ""):
            return False
    return default


class AsyncBingxWebsocket:
    """
    WebSocket BingX USDT-M swap (swap-market), аналог по роли AsyncBybitWebsocket:
    - публичные kline: подписка {\"id\",\"reqType\":\"sub\",\"dataType\":\"BTC-USDT@kline_1m\"}
    - приват: listenKey в query + события e=ORDER_TRADE_UPDATE / ACCOUNT_UPDATE (как в bingx-php SDK)
    """

    def __init__(self, api_key: str | None = None, api_secret: str | None = None):
        self.api_key = api_key
        self.api_secret = api_secret

        self.lock = asyncio.Lock()

        self.klines_queues: dict[str, asyncio.Queue] = {}
        self.orders_queue: asyncio.Queue = asyncio.Queue()
        self.wallet_queue: asyncio.Queue = asyncio.Queue()

        self.orders_items: list[str] = []
        self.orders_filtered_queues: dict[str, asyncio.Queue] = {}
        self._last_kline_by_topic: dict[tuple[str, str], dict] = {}

    @staticmethod
    def _normalize_order_payload(order: dict) -> dict:
        """ORDER_TRADE_UPDATE['o'] — поля в стиле Binance (i,s,S,...) + fallback под REST BingX."""
        if not isinstance(order, dict):
            return {}
        if "i" not in order and order.get("orderId") is not None:
            payload = dict(order)
        else:
            now_ms = int(time.time() * 1000)
            sym = str(order.get("s", "")).replace("-", "")
            payload = {
                "orderId": str(order.get("i", order.get("orderId", ""))),
                "symbol": sym,
                "orderType": str(order.get("o", order.get("type", "MARKET"))),
                "qty": str(order.get("q", order.get("origQty", "0"))),
                "cumExecQty": str(order.get("z", order.get("executedQty", "0"))),
                "side": str(order.get("S", order.get("side", ""))),
                "price": str(order.get("p", order.get("price", "0"))),
                "avgPrice": str(order.get("ap", order.get("avgPrice", "0"))),
                "takeProfit": str(order.get("tp", order.get("takeProfit", "0")) or "0"),
                "stopLoss": str(order.get("sl", order.get("stopLoss", order.get("sp", "0"))) or "0"),
                "orderStatus": str(order.get("X", order.get("status", "NEW"))),
                "createdTime": str(order.get("T", order.get("time", now_ms))),
                "updatedTime": str(order.get("t", order.get("updateTime", now_ms))),
                "leavesQty": str(order.get("l", order.get("leavesQty", "0"))),
            }
        for k in ("takeProfit", "stopLoss", "avgPrice", "price"):
            if payload.get(k) in ("", None):
                payload[k] = "0"
        return payload

    @staticmethod
    def _looks_like_order_payload(payload: dict) -> bool:
        if not isinstance(payload, dict):
            return False
        # На BingX/совместимых event'ах ордера обычно содержат хотя бы id + symbol.
        has_id = payload.get("i") is not None or payload.get("orderId") is not None
        has_symbol = payload.get("s") is not None or payload.get("symbol") is not None
        return has_id and has_symbol

    @staticmethod
    def _normalize_kline(symbol_no_dash: str, raw, interval_hint: str = "") -> dict:
        # BingX может присылать kline как dict, как {"k": {...}} или как list[dict].
        if isinstance(raw, list):
            if not raw:
                return {}
            raw = raw[0]
        if not isinstance(raw, dict):
            return {}
        item = raw.get("k") if isinstance(raw.get("k"), dict) else raw
        if not isinstance(item, dict):
            return {}
        interval = interval_hint or str(item.get("i", item.get("interval", "")))
        start = int(item.get("t", item.get("T", item.get("start", 0))) or 0)
        end = int(item.get("T", item.get("end", 0)) or 0)
        return {
            "symbol": symbol_no_dash.upper(),
            "interval": interval,
            "start": start,
            "end": end,
            "open": str(item.get("o", item.get("open", "0"))),
            "high": str(item.get("h", item.get("high", "0"))),
            "low": str(item.get("l", item.get("low", "0"))),
            "close": str(item.get("c", item.get("close", "0"))),
            "volume": str(item.get("v", item.get("volume", "0"))),
            "turnover": str(item.get("q", item.get("turnover", "0"))),
            "confirm": _as_bool(
                item.get(
                    "x",
                    item.get(
                        "confirm",
                        item.get("X", item.get("isClosed", item.get("closed", False))),
                    ),
                ),
                default=False,
            ),
            "timestamp": int(raw.get("E", item.get("T", 0)) or 0),
        }

    async def _listen_key_generate(self, session: aiohttp.ClientSession) -> str:
        url, headers = _signed_request_url(LISTEN_KEY_CREATE, self.api_secret, self.api_key, {})
        async with session.post(url, headers=headers) as resp:
            data = await resp.json()
        return _parse_listen_key_response(data)

    async def _listen_key_extend(self, session: aiohttp.ClientSession, listen_key: str) -> None:
        url, headers = _signed_request_url(
            LISTEN_KEY_CREATE, self.api_secret, self.api_key, {"listenKey": listen_key}
        )
        async with session.put(url, headers=headers) as resp:
            if resp.status >= 400:
                text = await resp.text()
                logger.warning("listenKey extend failed: status=%s body=%s", resp.status, text)

    async def _listen_key_delete(self, session: aiohttp.ClientSession, listen_key: str) -> None:
        url, headers = _signed_request_url(
            LISTEN_KEY_CREATE, self.api_secret, self.api_key, {"listenKey": listen_key}
        )
        try:
            async with session.delete(url, headers=headers) as resp:
                if resp.status >= 400:
                    text = await resp.text()
                    logger.debug("listenKey delete: status=%s body=%s", resp.status, text)
        except Exception as e:
            logger.debug("listenKey delete error: %s", e)

    async def _listen_key_extend_loop(
        self,
        session: aiohttp.ClientSession,
        listen_key: str,
        stop: asyncio.Event,
    ):
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=30 * 60)
                return
            except asyncio.TimeoutError:
                pass
            if stop.is_set():
                return
            try:
                await self._listen_key_extend(session, listen_key)
            except Exception as e:
                logger.warning("listenKey extend: %s", e)

    @staticmethod
    async def _ws_send_json(ws, payload: dict):
        await ws.send(json.dumps(payload))

    async def subscribe_public(self, ws, klines_topics: list[str]):
        normalized: list[tuple[str, str]] = []
        for topic in klines_topics:
            t = topic.strip()
            if "@kline_" in t:
                sym_raw, interval = t.split("@kline_", 1)
                bx_interval = INTERVAL_FOR_BINGX_WS.get(interval, interval)
                sym_bx = _to_bingx_symbol(sym_raw)
                normalized.append((sym_bx, bx_interval))
                continue
            if "@" in t and "@kline" in t:
                parts = t.split("@")
                sym_bx = _to_bingx_symbol(parts[0])
                for p in parts[1:]:
                    if p.startswith("kline_"):
                        interval = p.replace("kline_", "")
                        bx_interval = INTERVAL_FOR_BINGX_WS.get(interval, interval)
                        normalized.append((sym_bx, bx_interval))
                continue
            sym_bx = _to_bingx_symbol(t)
            normalized.append((sym_bx, INTERVAL_FOR_BINGX_WS.get("1m", "1m")))

        for i, (sym_bx, bx_interval) in enumerate(normalized):
            data_type = f"{sym_bx}@kline_{bx_interval}"
            req_id = f"bingx_sub_{int(time.time() * 1000)}_{i}"
            await self._ws_send_json(ws, {"id": req_id, "reqType": "sub", "dataType": data_type})

    async def private_ws(self, orders: bool, wallet: bool):
        if not any([orders, wallet]):
            raise ValueError("orders and wallet cannot both be False")
        if not self.api_key or not self.api_secret:
            raise ValueError("BingX private WebSocket requires api_key and api_secret")

        try:
            while True:
                stop_extend = asyncio.Event()
                try:
                    async with aiohttp.ClientSession() as session:
                        listen_key = await self._listen_key_generate(session)
                        extend_task = asyncio.create_task(
                            self._listen_key_extend_loop(session, listen_key, stop_extend)
                        )
                        url = f"{ACCOUNT_WSS}?listenKey={listen_key}"
                        try:
                            async for ws in websockets.asyncio.client.connect(
                                url,
                                ping_interval=45,
                            ):
                                try:
                                    logger.info("BingX private WS connected")
                                    # На части аккаунтов одних только listenKey событий недостаточно —
                                    # явно подписываемся на приватные каналы.
                                    subs: list[str] = []
                                    if orders:
                                        subs.extend(
                                            [
                                                "ORDER_TRADE_UPDATE",
                                                "user.order",
                                                "user.order.update",
                                                "ORDER",
                                            ]
                                        )
                                    if wallet:
                                        subs.extend(
                                            [
                                                "ACCOUNT_UPDATE",
                                                "user.account",
                                                "user.balance",
                                            ]
                                        )
                                    for i, data_type in enumerate(dict.fromkeys(subs)):
                                        payload = {
                                            "id": f"bingx_private_sub_{int(time.time() * 1000)}_{i}",
                                            "reqType": "sub",
                                            "dataType": data_type,
                                        }
                                        try:
                                            await self._ws_send_json(ws, payload)
                                            logger.debug("BingX private subscribe sent: %s", payload)
                                        except Exception as e:
                                            logger.debug(
                                                "BingX private subscribe failed for %s: %s",
                                                data_type,
                                                e,
                                            )

                                    async for raw in ws:
                                        try:
                                            if isinstance(raw, bytes):
                                                raw = _maybe_decompress(raw)
                                                text = raw.decode("utf-8", errors="replace")
                                            else:
                                                text = str(raw)

                                            logger.debug("BingX private WS msg: %s", text)
                                            # Heartbeat приватного BingX сокета приходит как plain-text "Ping".
                                            if text.strip().lower() == "ping":
                                                await ws.send("Pong")
                                                continue
                                            msg = json.loads(text)
                                        except json.JSONDecodeError:
                                            logger.debug("skip non-json frame %s", raw)
                                            continue

                                        if "ping" in msg:
                                            await self._ws_send_json(ws, {"pong": msg["ping"]})
                                            continue

                                        ev = msg.get("e")
                                        if ev == "listenKeyExpired":
                                            logger.warning(
                                                "BingX listenKey expired, reconnecting private WS with new listenKey"
                                            )
                                            break
                                        if ev == "SNAPSHOT":
                                            # Снимки аккаунта очень шумные; они не являются update ордера.
                                            continue

                                        if orders:
                                            order_candidates: list[dict] = []
                                            if ev == "ORDER_TRADE_UPDATE":
                                                o = msg.get("o")
                                                if isinstance(o, dict):
                                                    order_candidates.append(o)
                                            if isinstance(msg.get("o"), list):
                                                order_candidates.extend(i for i in msg["o"] if isinstance(i, dict))

                                            # Fallback: payload может приходить как {"dataType":"...ORDER...", "data":{...}}
                                            data_type = str(msg.get("dataType", ""))
                                            data = msg.get("data")
                                            if isinstance(data, dict) and ("ORDER" in data_type.upper() or self._looks_like_order_payload(data)):
                                                order_candidates.append(data)
                                            elif isinstance(data, list):
                                                for item in data:
                                                    if isinstance(item, dict) and self._looks_like_order_payload(item):
                                                        order_candidates.append(item)

                                            # Ещё один fallback: само корневое сообщение может быть ордером.
                                            if self._looks_like_order_payload(msg):
                                                order_candidates.append(msg)

                                            if order_candidates:
                                                await self.orders_queue.put(order_candidates)
                                                logger.info("BingX orders queued: %s", len(order_candidates))
                                        elif wallet and ev == "ACCOUNT_UPDATE":
                                            if msg.get("a") is not None:
                                                await self.wallet_queue.put(msg)
                                except (
                                    websockets.exceptions.ConnectionClosed,
                                    websockets.exceptions.ConnectionClosedError,
                                ) as e:
                                    logger.info("Reconnect private BingX WS (%s)", e)
                                    break
                                except asyncio.CancelledError:
                                    raise
                                except Exception as e:
                                    logger.exception(e)
                                    break
                        finally:
                            stop_extend.set()
                            extend_task.cancel()
                            try:
                                await extend_task
                            except asyncio.CancelledError:
                                pass
                            # Важно: не удаляем listenKey на каждом reconnect.
                            # BingX может вернуть тот же listenKey при быстром пересоздании,
                            # и delete из старого цикла "убивает" новый private stream.
                            # Ключ будет обновлен/пересоздан самим циклом при необходимости.
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.exception(e)
                    await asyncio.sleep(2)
                    continue
        except asyncio.CancelledError:
            logger.info("BingX private WS cancelled")
            raise

        logger.info("BingX private WS disconnected")

    async def public_ws(self, klines_topics: list[str]):
        async for ws in websockets.asyncio.client.connect(
            MARKET_WSS,
            ping_interval=45,
        ):
            try:
                logger.info("BingX public WS connected")
                await self.subscribe_public(ws, klines_topics=klines_topics)

                async for raw in ws:
                    try:
                        if isinstance(raw, bytes):
                            raw = _maybe_decompress(raw)
                            text = raw.decode("utf-8", errors="replace")
                        else:
                            text = str(raw)
                        msg = json.loads(text)
                    except json.JSONDecodeError:
                        continue

                    if "ping" in msg:
                        await self._ws_send_json(ws, {"pong": msg["ping"]})
                        continue

                    data_type = msg.get("dataType")
                    data = msg.get("data")
                    # logger.debug("BingX public WS msg: %s", msg)
                    if not data_type or not isinstance(data_type, str) or "@kline_" not in data_type:
                        continue
                    sym_bx = data_type.split("@")[0]
                    interval = data_type.split("@kline_", 1)[-1] if "@kline_" in data_type else ""
                    symbol = sym_bx.replace("-", "")
                    normalized = self._normalize_kline(
                        symbol_no_dash=symbol,
                        raw=data,
                        interval_hint=interval,
                    )
                    if not normalized:
                        continue
                    q = self.klines_queues.get(symbol.upper())
                    if q is not None:
                        topic_key = (normalized["symbol"], normalized["interval"])
                        prev = self._last_kline_by_topic.get(topic_key)
                        if prev and prev.get("start") != normalized.get("start"):
                            prev_closed = dict(prev)
                            prev_closed["confirm"] = True
                            await q.put(prev_closed)
                        self._last_kline_by_topic[topic_key] = dict(normalized)
                        await q.put(normalized)
                        logger.info(
                            "kline queued %s %s close=%s confirm=%s",
                            normalized.get("symbol"),
                            normalized.get("interval"),
                            normalized.get("close"),
                            normalized.get("confirm"),
                        )
                    else:
                        logger.debug("no klines queue for %s", symbol)

            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError):
                continue
            except asyncio.CancelledError:
                logger.info("BingX public WS cancelled")
                break
            except Exception as e:
                logger.exception(e)
                break

        logger.info("BingX public WS disconnected")

    async def orders_getter_loop(self):
        while True:
            items_copy = copy.deepcopy(await self.orders_queue.get())
            async with self.lock:
                if str(items_copy) not in self.orders_items:
                    self.orders_items.append(str(items_copy))
                    orders_by_symbol: dict[str, list] = {}
                    logger.debug(items_copy)
                    for order in items_copy:
                        try:
                            normalized = self._normalize_order_payload(order)
                            order_data = OrderData.model_validate(normalized)
                            order_data.customize()
                            logger.info(
                                "BingX order parsed id=%s symbol=%s status=%s side=%s",
                                order_data.order_id,
                                order_data.symbol,
                                order_data.order_status,
                                order_data.side,
                            )
                        except Exception as e:
                            logger.error("Failed to parse BingX order message: %s | order=%s", e, order)
                            raise e

                        symbol = order_data.symbol
                        if symbol in orders_by_symbol:
                            orders_by_symbol[symbol].append(order_data)
                        else:
                            orders_by_symbol[symbol] = [order_data]
                    for k, v in orders_by_symbol.items():
                        try:
                            await self.orders_filtered_queues[k].put(v)
                        except KeyError:
                            # Не теряем ордера по символам, которые не были заранее добавлены в конфиг.
                            self.orders_filtered_queues[k] = asyncio.Queue()
                            await self.orders_filtered_queues[k].put(v)
                            logger.warning(
                                "Order queue for %s was missing; created dynamically and enqueued update",
                                k,
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
        klines_topics: list[str] | None = None,
        triple: bool = False,
        test: bool = False,
    ):
        logger.info("run_all_ws BingX websockets ver: %s", websockets.__version__)
        loops: list = [self.orders_getter_loop()]
        if orders or wallet:
            loops.append(self.private_ws(orders, wallet))
            if triple:
                logger.warning(
                    "BingX private WS triple mode disabled to avoid listenKeyExpired races"
                )
        if klines_topics:
            loops.append(self.public_ws(klines_topics))

        if test:
            loops.append(self.get_klines_test('BTCUSDT'))
            loops.append(self.get_klines_test('DOGEUSDT'))
            for symbol in self.orders_filtered_queues:
                loops.append(self.get_orders_test(symbol))

        await asyncio.gather(*loops)

    async def get_klines_test(self, symbol: str):
        while True:
            klines = await self.klines_queues[symbol].get()
            if klines['symbol'] != symbol:
                raise ValueError(f"Symbol mismatch: {klines['symbol']} != {symbol}")
            if klines['confirm']:
                raise Exception(f"Kline confirmed: {klines}")
            print(f"!!!!!!!!!---- {klines}")

    async def get_orders_test(self, symbol: str):
        while True:
            orders = await self.orders_filtered_queues[symbol].get()
            print(f"@@@@@@@ {symbol} ---- {orders}")


def test_bingx_websocket(bingx_api_key: str, bingx_secret: str):
    ws = AsyncBingxWebsocket(api_key=bingx_api_key, api_secret=bingx_secret)
    ws.create_orders_queues(["XRPUSDT", "DOGEUSDT"])
    ws.create_klines_queues(["BTCUSDT", "DOGEUSDT"])

    asyncio.run(
        ws.run_all_ws(
            orders=True,
            wallet=False,
            # klines_topics=["BTCUSDT@kline_1h", "DOGEUSDT@kline_1m"],
            triple=True,
            test=True,
        )
    )
