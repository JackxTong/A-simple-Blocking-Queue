from enum import Enum

class MifidMatchingConfiguration:
    NOTIONAL_TOLERANCE = 1e-4

class MifidColumns(Enum):
    tradingDateTime = "tradingDateTime"
    notionalAmount = "Notional Amount"
    drvExpiryDate = "Drv Expiry Date"
    price = "Price"
    drvExpiryYear = "Drv Expiry Year"

class RfqColumns(Enum):
    dateTime = "soruceTimeStamp"
    datetime_parsed = "datetime"
    date = "date"
    rfqId = "requestId"
    sizeK = "Size (k)"
    legInstrumentMaturityDate = "legInstrumentMaturityDate"
    regulatoryScope = "regulatoryScope"
    venue = "venue"
    
class MatchAttributes(Enum):
    MATCH = "Match"
    MATCH_CONFIDENCE = "MatchConfidence"