# Async Futures Clients

Python modules for working with futures APIs of Bybit and Binance.

## Installation from GitHub

```bash
pip install git+https://github.com/burchesoka/ExchangeAsyncClients.git
```

## Usage

```python
import aiohttp
import asyncio

from async_bybit_client import AsyncBybitFuturesClient


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
