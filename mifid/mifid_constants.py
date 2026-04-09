from enum import Enum

class MifidMatchingConfiguration:
    NOTIONAL_TOLERANCE = 1e-4

class MifidColumns(Enum):
    tradingDateTime = "Trading Date Time"
    notionalAmount = "Notional Amount"
    drvExpiryDate = "Drv Expiry Date"
    price = "Price"
    drvExpiryYear = "Drv Expiry Year"

class RfqColumns(Enum):
    dateTime = "Date Time"
    datetime_parsed = "datetime"
    date = "date"
    rfqId = "rfqId"
    sizeK = "Size (k)"
    legInstrumentMaturityDate = "legInstrumentMaturityDate"
    
class MatchAttributes(Enum):
    MATCH = "Match"
    MATCH_CONFIDENCE = "MatchConfidence"