class AlreadyFilledOrder(Exception):
    """Raised when order was filled before we got notification"""


class PartiallyFilledOrder(Exception):
    """Raised when order was partially filled before we got notification"""


class CancelledOrder(Exception):
    """ CancelledOrder """


class MaximumOrdersExceeded(Exception):
    """ Bybit: 30073 already had 50 working open orders """


class AlreadyClosedPosition(Exception):
    """Raised when position was already closed"""


class ClosedPosition(Exception):
    """Raised when position was closed"""


class StopLossImpossible(Exception):
    """ Got -2021 error code: stop order would trigger immediately """


class TakeProfitImpossible(Exception):
    """ Got -2021 error code: stop order would trigger immediately """


class LimitOrderImpossible(Exception):
    """ estimated will trigger liq """


class MarginInsufficient(Exception):
    """ Got -2019 error code: Margin insufficient """


class OutOfOpenInterest(Exception):
    """ Got -110021 error code (bybit): out of open interest limit: open interest limited on MAGICUSDT """


class MinimumLimitExceeded(Exception):
    """ The number of contracts exceeds minimum limit allowed """


class MaximumLimitExceeded(Exception):
    """ The number of contracts exceeds maximum limit allowed """


class MinimalValueFiveUSDT(Exception):
    """ Got -4164 error code: Order's notional must be no smaller than 5.0 (unless you choose reduce only) """


class WrongConfig(Exception):
    """ Unable to parse config """


class WrongEnv(Exception):
    """ Unable to parse environment variables """


class TransferUnable(Exception):
    """ "retCode":12010 transfer err (already used id or not enough coins) """


class CannotTransfer(Exception):
    """ "retCode":131206,"retMsg":"cannot be transfer" - Probably Unified acc """


class CheckAgainNeeded(Exception):
    """ Need to check everything again """


class CheckEverythingNeeded(Exception):
    """ Need to check everything """


class ReduceImpossible(Exception):
    """Reduce-only rule not satisfied"""


class NotSupportedForCopyTrade(Exception):
    """ symbol is not supported for copytrade leader """


class DisabledOnClose(Exception):
    """ Strategy disabled on close position """


class NotCorrectMessage(Exception):
    """Некорректное сообщение в бот, которое не удалось распарсить"""


class EmptyWallet(Exception):
    """ Wallet is empty """


class BotIsDisabled(Exception):
    """ Bot is disabled """


class BotIsPaused(Exception):
    """ Bot is disabled """


class UrgentlyClosedPosition(Exception):
    """ Urgently Closed Position """


class OrderValidationError(Exception):
    """ OrderValidationError """


class OrderNotFound(Exception):
    """ OrderNotFound error """


class NewOrderNotFound(Exception):
    """ New created OrderNotFound error """


class FilledOrdersNotFound(Exception):
    """ Position ordersNot Found error """


class Looped(Exception):
    """ Check everything method looped """


class RequestError(Exception):
    """Ошибка запроса в API """


class UnprocessableEntity(Exception):
    """ 422 Unprocessable Entity error """


class FailedDependency(Exception):
    """ 424 Failed Dependency error """


class ServerError(Exception):
    """ Server error exception"""


class NotFound(Exception):
    """ Not found on server error exception"""


class AuthenticationError(Exception):
    """ AuthenticationError """


class UnifiedAccount(Exception):
    """ Clients account is unified """


class MasterTraderAccount(Exception):
    """ Clients account is master trader """


class ReadonlyApiKeys(Exception):
    """ Readonly Api Keys """


class InsufficientBalance(Exception):
    """ Balance less than required """


class MaxBalanceExceeded(Exception):
    """ Balance more than required """


class NotThirdPartyApp(Exception):
    """" API key Not connected to ThirdPartyApp """


class NoNeededPermissions(Exception):
    """Not all needed permissions checked in API keys"""


class UnexpectedError(Exception):
    """ UnexpectedError """


class AuthenticationError(Exception):
    """ AuthenticationError """


class AuthErrorExpiredKeys(Exception):
    """ Your api key has expired error """


class NoChange(Exception):
    """ NoChange response """


class BotIsActive(Exception):
    """ NoChange response """


class TradingBotStopped(Exception):
    """ Trading bot was stopped by sigterm """


class KillNow(Exception):
    """ kill process """


class WebsocketLostConnection(Exception):
    """ WebsocketLostConnection """


class MaximumStrategiesReached(Exception):
    """ Maximum Strategies Reached """


class SetLeverageFail(Exception):
    """ SetLeverageFail """


class SetMarginFail(Exception):
    """ SetLeverageFail """


class UnifiedAccount(Exception):
    """ accountType only support UNIFIED error"""


class NotUnifiedAccount(Exception):
    """ not supported for not UNIFIED accounts """


class LeverageNotModified(Exception):
    """ LeverageNotModified error """


class OrderNotExist(Exception):
    """ OrderNotExist or can not close """


class EmptyResponse(Exception):
    """ Empty response """


class NetworkError(Exception):
    """ NetworkError """


class FailedOrder(Exception):
    """ Failed to submit order """


class SecondBotConflict(Exception):
    """ SecondBotConflict """

class InvalidNonce(Exception):
    """ invalid request, please check your server timestamp or recv_window param """


class RateLimitExceeded(Exception):
    """ Too many visits. Exceeded the API Rate Limit """
