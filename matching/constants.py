from enum import Enum

class MatchingConfig:
    NOTIONAL_TOLERANCE = 1e-4
    BACKWARD_MINS = 5
    FORWARD_MINS = 5

class EndReason(Enum):
    COUNTERPARTY_TRADED_WITH_BARCLAYS = "COUNTERPARTY_TRADED_WITH_BARCLAYS"
    COUNTERPARTY_TRADED_AWAY = "COUNTERPARTY_TRADED_AWAY"
    COUNTERPARTY_REJECTED = "COUNTERPARTY_REJECTED"

class RfqCols:
    rfq_id = "requestId"
    trade_date = "tradeDate"
    trade_time = "tradeTime"
    currency = "currency"
    pricing_convention = "legPricingConvention"
    regulatory_scope = "regulatoryScope"
    size = "legSize"
    maturity = "legInstrumentMaturityDate"
    venue = "venue"
    num_legs = "numLegs"

class PublicCols:
    time = "publicTime"
    size = "publicSize"
    maturity_year = "publicMaturityYear"
    price = "publicPrice"
    source = "publicSource"

class OutputCols:
    MATCH = "Match"
    MATCHED_PRICE = "price"