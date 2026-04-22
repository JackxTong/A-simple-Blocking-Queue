from enum import Enum

class DataMatchingConfiguration:
    NOTIONAL_PRECISION = 1e-4
    PRICE_PRECISION = 0.005 # Assuming standard precision, adjust to your original config
    BACKWARD_MINS = 7
    FORWARD_MINS = 1

class EndReason(Enum):
    COUNTERPARTY_TRADED_WITH_BARCLAYS = "COUNTERPARTY_TRADED_WITH_BARCLAYS"
    COUNTERPARTY_TRADED_AWAY = "COUNTERPARTY_TRADED_AWAY"
    COUNTERPARTY_REJECTED = "COUNTERPARTY_REJECTED"
    CLIENT_REJECTED = "CLIENT_REJECTED"

class SdrColumns(Enum):
    executionTimestamp = "sdrExecutionTimestamp"
    sdrSize = "sdrSize"
    sdrLegPrice = "sdrLegPrice"
    PackageTransactionPrice = "PackageTransactionPrice"
    ExpirationDate = "drvExpiryDate" # Mapped to match Mifid for downstream year parsing

class MifidColumns(Enum):
    tradingDateTime = "tradingDateTime"
    notionalAmount = "Notional Amount"
    drvExpiryDate = "Drv Expiry Date"
    price = "Price"
    drvExpiryYear = "Drv Expiry Year"
    source = "source"

class RfqColumns(Enum):
    date = "tradeDate"
    time = "time"
    requestId = "requestId"
    numLegs = "numLegs"
    regulatoryScope = "regulatoryScope"
    legInstrumentMaturityDate = "legInstrumentMaturityDate"
    legPricingConvention = "legPricingConvention"
    legSize = "legSize"
    legDv01 = "legDv01"
    legQuotePrice = "legQuotePrice"
    parentQuotePrice = "parentQuotePrice"
    endReason = "endReason"
    sourceTimestamp = "sourceTimestamp"
    datetime_parsed = "datetime"
    venue = "venue"

class MatchAttributes(Enum):
    MATCH = "Match"
    MATCHED_PRICE = "MatchedPrice"
