# Async Futures Clients

Python modules for working with futures APIs of Bybit, BingX and Binance.

## Installation from GitHub

```bash
pip install git+https://github.com/burchesoka/ExchangeAsyncClients.git
```

## Logging

By default the library does not configure Python logging. Call `setup_clients_logging()` once at startup:

```python
import logging

from clients import setup_clients_logging

setup_clients_logging(level=logging.INFO)  # use logging.DEBUG for verbose output
```

If your application already configures logging, `setup_clients_logging()` only adjusts library logger levels without changing your handlers or format.

## Usage

```python
import aiohttp
import asyncio

from clients.async_bybit_client import AsyncBybitFuturesClient


async def main():
    async with aiohttp.ClientSession() as session:
        client = AsyncBybitFuturesClient(
            session=session,
            api_key="YOUR_KEY",
            api_secret="YOUR_SECRET",
            category="linear",
            test=False,
            brocker_id="Your_brocker_id",
        )
        wallet = await client.get_wallet_data()
        print(wallet)


if __name__ == "__main__":
    asyncio.run(main())
```

## Imports

Recommended import style:

```python
from clients.async_bybit_client import AsyncBybitFuturesClient
from clients.async_binance_client import AsyncBinanceFuturesClient
from clients.base import BaseAsyncFuturesClient, PositionMode
```

Backward-compatible (legacy) imports still work:

```python
from async_bybit_client import AsyncBybitFuturesClient
from async_binance_client import AsyncBinanceFuturesClient
```
