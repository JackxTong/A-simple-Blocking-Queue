from enum import Enum

class MifidMatchingConfiguration:
    NOTIONAL_TOLERANCE = 1e-4

class MifidColumns(Enum):
    tradingDateTime = "tradingDateTime"
    notionalAmount = "Notional Amount"
    drvExpiryDate = "Drv Expiry Date"
    price = "Price"
    drvExpiryYear = "Drv Expiry Year"
    priceCurrency = "priceCurrency"
    source = "source"
    instrumentFullName = "instrumentFullName"
    cfiGroupName = "cfiGroupName"
    drvUnderlyingIndexName = "drvUnderlyingIndexName"

class RfqColumns(Enum):
    dateTime = "soruceTimeStamp"
    datetime_parsed = "datetime"
    date = "date"
    rfqId = "requestId"
    sizeK = "Size (k)"
    legInstrumentMaturityDate = "legInstrumentMaturityDate"
    regulatoryScope = "regulatoryScope"
    venue = "venue"
    numLegs = "numLegs"
    endReason = "endReason"
    legIndex = "legIndex"

class ColumnUtils(Enum):
    modelVersion = "modelVersion"
    runDateTime = "runDateTime"
    MATCH = "Match"
    MATCH_CONFIDENCE = "MatchConfidence"

class EndReason(Enum):
    COUNTERPARTY_TRADED_WITH_BARCLAYS = "COUNTERPARTY_TRADED_WITH_BARCLAYS"
    COUNTERPARTY_TRADED_AWAY = "COUNTERPARTY_TRADED_AWAY"
    CLIENT_REJECTED = "CLIENT_REJECTED"
    
class MatchAttributes(Enum):
    MATCH = "Match"
    MATCH_CONFIDENCE = "MatchConfidence"