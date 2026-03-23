import asyncio
import datetime
import logging
import time

import pandas as pd

from typing import Callable

from pydantic import BaseModel, Field, AliasChoices
from abc import ABC, abstractmethod
from decimal import Decimal
from enum import Enum

import exceptions


logger = logging.getLogger(__name__)

INTERVAL_IN_SEC = {
    '1m': 60,
    '3m': 180,
    '5m': 300,
    '15m': 15 * 60,
    '30m': 30 * 60,
    '1h': 3600,
    '2h': 120 * 60,
    '4h': 240 * 60,
    '6h': 360 * 60,
    '12h': 720 * 60,
    '1d': 24 * 60 * 60,
    '1w': 7 * 24 * 60 * 60,
    '1M': 31 * 24 * 60 * 60,  # ????????
}

INTERVAL_FOR_BYBIT = {
    '1m': '1',
    '3m': '3',
    '5m': '5',
    '15m': '15',
    '30m': '30',
    '1h': '60',
    '2h': '120',
    '4h': '240',
    '6h': '360',
    '12h': '720',
    '1d': 'D',
    '1w': 'W',
    '1M': 'M',
}

INTERVAL_FROM_BYBIT_TO_MY = {
    '1': '1m',
    '3': '3m',
    '5': '5m',
    '15': '15m',
    '30': '30m',
    '60': '1h',
    '120': '2h',
    '240': '4h',
    '360': '6h',
    '720': '12h',
    'D': '1d',
    'W': '1w',
    'M': '1M',
}

ORDER_SPECS = {
    'LIMIT': 'Limit',
    'MARKET': 'Market',
    'STOP_MARKET': 'Market',
    'STOP': 'Limit',
    'TAKE_PROFIT': 'Limit',
    'TAKE_PROFIT_MARKET': 'Market',
    'BUY': 'Buy',
    'SELL': 'Sell',
}


class PositionMode(str, Enum):
    one_way = 'one_way'
    hedge = 'hedge'


class Exchange(str, Enum):
    bybit = 'bybit'
    okx = 'okx'
    bingx = 'bingx'


class BaseAsyncFuturesClient(ABC):
    def __init__(
        self,
        category: str = "linear",
        test: bool = False,
        password: str | None = None,
    ):
        self.category = category
        self.test = test
        self.password = password

    async def switch_position_mode_for_one_symbol(self, mode: PositionMode, symbol: str):
        return await self.switch_position_mode(mode=mode, symbol=symbol)

    async def switch_position_mode_for_all_symbols(self, mode: PositionMode, coin: str):
        return await self.switch_position_mode(mode=mode, coin=coin)

    @abstractmethod
    async def switch_position_mode(
        self,
        mode: PositionMode,
        symbol: str | None = None,
        coin: str | None = None,
    ):
        raise NotImplementedError


class InstrumentInfo(BaseModel):
    symbol: str
    min_order_qty: Decimal = Field(validation_alias=AliasChoices('minOrderQty', 'min_order_qty'))
    tick_size: Decimal = Field(validation_alias=AliasChoices('tickSize', 'tick_size'))


class WalletData(BaseModel):
    wallet_balance: Decimal = Field(validation_alias=AliasChoices('walletBalance', 'balance', 'wallet_balance'))
    available_balance: Decimal = Field(
        validation_alias=AliasChoices('totalAvailableBalance', 'availableMargin', 'available_balance')
    )
    equity: Decimal


class ExecutionsData(BaseModel):
    symbol: str
    opening_position: bool
    exec_qty: Decimal
    order_id: str
    price: Decimal
    position_side: str
    side: str

    def customize(self):
        self.symbol = self.symbol.replace('-', '')
        self.side = self.side.upper()
        self.position_side = self.position_side.upper()

        if 'LONG' in self.side.upper():
            self.side = 'BUY'
        elif 'SHORT' in self.side.upper():
            self.side = 'SELL'

        if 'LONG' in self.position_side.upper():
            self.position_side = 'BUY'
        elif 'SHORT' in self.position_side.upper():
            self.position_side = 'SELL'


class PositionData(BaseModel):
    symbol: str
    side: str = Field(validation_alias=AliasChoices("side", 'positionSide'))
    size: Decimal = Field(validation_alias=AliasChoices("size", 'positionAmt'))
    avg_price: Decimal = Field(alias='avgPrice')
    stop_price: Decimal = Field(validation_alias=AliasChoices('stopLoss'))
    take_price: Decimal = Field(validation_alias=AliasChoices('takeProfit'))
    liq_price: Decimal = Field(validation_alias=AliasChoices("liqPrice", "liquidationPrice"))
    position_margin: str = Field(validation_alias=AliasChoices("positionBalance", "margin"))
    leverage: str
    created_time: datetime.datetime = Field(alias='createdTime')
    updated_time: datetime.datetime = Field(validation_alias=AliasChoices('updatedTime', 'updateTime'))
    unrealised_pnl: Decimal = Field(validation_alias=AliasChoices('unrealisedPnl', 'unrealizedProfit'))

    def customize(self):
        self.symbol = self.symbol.replace('-', '')
        if 'LONG' in self.side.upper():
            self.side = 'BUY'
        elif 'SHORT' in self.side.upper():
            self.side = 'SELL'


class OrderData(BaseModel):
    order_id: str = Field(validation_alias=AliasChoices("orderId", "i"))
    symbol: str = Field(validation_alias=AliasChoices("symbol", "s"))
    order_type: str | None = Field(validation_alias=AliasChoices("orderType", 'type', 'o'))
    qty: Decimal = Field(validation_alias=AliasChoices("qty", 'origQty', 'q'))
    leaves_qty: Decimal = Field(alias='leavesQty')
    cum_exec_qty: Decimal = Field(validation_alias=AliasChoices('cumExecQty', 'executedQty', 'z'))
    side: str = Field(validation_alias=AliasChoices("side", "S"))
    price: Decimal = Field(validation_alias=AliasChoices("price", "p"))
    avg_price: Decimal = Field(validation_alias=AliasChoices("avgPrice", "ap"))
    take_price: Decimal = Field(validation_alias=AliasChoices("takeProfit", "sp"))  # BINGX???
    stop_price: Decimal = Field(validation_alias=AliasChoices("stopLoss", "sp"))
    order_status: str = Field(validation_alias=AliasChoices("orderStatus", 'status', 'X'))

    # fee: Decimal = Field(alias="cumExecFee")

    time: str = Field(validation_alias=AliasChoices("createdTime", "time", 'T'))
    updated_time: str = Field(validation_alias=AliasChoices("updatedTime", "updateTime", 'T'))

    def customize(self):
        self.symbol = self.symbol.replace('-', '')

        self.order_status = self.order_status.upper()

        if self.order_status == 'PENDING':
            self.order_status = 'NEW'

        if 'CANCELLED' in self.order_status:
            if self.qty > self.cum_exec_qty > Decimal('0'):
                self.order_status += '_PARTIALLY_FILLED'

        if self.leaves_qty != (self.qty - self.cum_exec_qty):
            self.leaves_qty = self.qty - self.cum_exec_qty


class PNLData(BaseModel):
    order_id: str = Field(validation_alias=AliasChoices("orderId", "tradeId"))
    symbol: str

    # qty: Decimal = Field(validation_alias=AliasChoices("qty", ))
    # closed_qty: Decimal | None = Field(alias="closedSize")
    # side: str
    # order_type: str | None = Field(alias="orderType")

    # avg_entry_price: Decimal = Field(alias='avgEntryPrice')
    # avg_exit_price: Decimal = Field(alias='avgExitPrice')
    closed_pnl: Decimal = Field(validation_alias=AliasChoices('closedPnl', 'income'))

    created_time: int = Field(alias='createdTime')
    updated_time: int = Field(alias='updatedTime')

    def customize(self):
        self.symbol = self.symbol.replace('-', '')


class SavePnlsAndGetFeeResponse(BaseModel):
    fee: Decimal
    closed_pnl: Decimal
    auto_withdraw: bool
