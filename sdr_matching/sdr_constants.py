from enum import Enum
import numpy as np
import pandas as pd

class DataMatchingConfiguration:
    MODEL_VERSION = '20240531'
    REALTIME_PUBLICATION_FREQ = 'ST'

    TIME_WINDOW_SECONDS_BEFORE_FIRST_QUOTE = 5
    TIME_WINDOW_SECONDS_AFTER_LAST_QUOTE = 10

    NOTIONAL_RULE_VALID = True
    PRICE_RULE_VALID = False

    MAX_RATE_DIFF = 0.35

class RfqDataProcessingConfiguration:
    # Thresholds for the notional rounding logic
    # This can be interpreted as:
    # - if notional < 1e3, round to nearest 5
    # - if notional is within [1e3, 1e4), round to nearest 100
    # - if notional is within [1e4, 1e5), round to nearest 1000
    # and so on
    NOTIONAL_ROUNDING_THRESHOLDS = [1e3, 1e4, 1e5, 1e6, 1e8, 5e8, 1e9, 11e11]
    NOTIONAL_ROUND_TO_NEAREST_DICT = {0: 5, 1: 100, 2: 1e3, 3: 1e4, 4: 1e6, 5: 1e7, 6: 5e7, 7: 1e9, 8: 5e10}

    # Tenors (in days) for defining the capping size logic
    # This can be interpreted as:
    # - if tenor <= 46 days, cap the notional to 13000*1e6 USD
    # - if tenor is within (47, 107], cap the notional to 4100*1e6 USD
    # - if tenor is within (108, 198], cap the notional to 1600*1e6 USD
    # and so on
    # As of 13/02/2024. This might change in the future.
    TENOR_BASED_NOTIONAL_CAPPING_THRESHOLD = [46, 107, 198, 381, 746, 1842, 3668, 19973]

    NOTIONAL_CAPPING_SIZE_DICT = {
        0: 13000 * 1e6,
        1: 4100 * 1e6,
        2: 1600 * 1e6,
        3: 2100 * 1e6,
        4: 1100 * 1e6,
        5: 550 * 1e6,
        6: 410 * 1e6,
        7: 270 * 1e6,
        8: 340 * 1e6,
    }

DV01_BINS_DF = pd.DataFrame(
    {
        "dv01_upper_bound": [1e4, 2e4, 3e4, 4e4, 5e4, 7e4, 1e5, 1e9],
        "dv01_bucket": [0, 1, 2, 3, 4, 5, 6, 7],
        "dv01_group_name": ["0-10k", "10k-20k", "20k-30k", "30k-40k", "40k-50k", "50k-70k", "70k-100k", ">100k"]
    }
)

# column names from SDR table
class SdrColumns(Enum):
    sym = "sym"
    sdrParentPrice = "sdrParentPrice"
    sdrLegPrice = "sdrLegPrice"
    sdrParentPriceWithEcnFee = "sdrParentPriceWithEcnFee"
    sdrLegPriceWithEcnFee = "sdrLegPriceWithEcnFee"
    executionTimestamp = "sdrExecutionTimestamp"
    PaymentFrequency1 = "PaymentFrequency1"
    ResetFrequency1 = "ResetFrequency1"
    PaymentFrequency2 = "PaymentFrequency2"
    ResetFrequency2 = "ResetFrequency2"
    DisseminationId = "sdrDisseminationId"
    ExecutionVenue = "ExecutionVenue"
    PriceNotation = "PriceNotation"
    sdrSize = "sdrSize"
    sdrPriceUnit = "sdrPriceUnit"
    AdditionalPriceNotation = "AdditionalPriceNotation"
    PackageTransactionPrice = "PackageTransactionPrice"
    PackageTransactionPriceNotation = "PackageTransactionPriceNotation"
    OtherPaymentAmount = "OtherPaymentAmount"

# column names created for temporary usage in code
class ColumnUtils(Enum):
    cappedLegSize = "cappedLegSize"
    notional_threshold_bucket = "notional_threshold_bucket"
    notional_capping_bucket = "notional_capping_bucket"
    roundedLegSize = "roundedLegSize"
    tenor_days = "tenor_days"
    firstQuoteTime = "firstQuoteTime"
    lastQuoteTime = "lastQuoteTime"
    matchConfidence = "matchConfidence"
    modelVersion = "modelVersion"
    runDateTime = "runDateTime"

# column names from RFQ table
class RfqColumns(Enum):
    date = "tradeDate"
    time = "time"
    requestId = "requestId"
    numLegs = "numLegs"
    requestType = "requestType"
    regulatoryScope = "regulatoryScope"
    negotiationState = "negotiationState"
    clientName = "clientName"
    legIndex = "legIndex"
    legInstrumentName = "legInstrumentName"
    legInstrumentType = "legInstrumentType"
    legInstrumentMaturityDate = "legInstrumentMaturityDate"
    legSwapEffectiveDate = "legSwapEffectiveDate"
    legSwapFloatingLegPayFrequency = "leg_SwapFloatingLegPayFrequency" # not in kdb, python populated
    legSwapFixedLegPayFrequency = "leg_SwapFixedLegPayFrequency" # not in kdb, python populated
    legSwapFloatingLegFrequency = "legSwapFloatingLegFrequency"
    # for LIBOR legSwapFloatingLegResetFrequency==legSwapFloatingLegFrequency always
    # for SOFR legSwapFloatingLegResetFrequency==INVALID always this column is redundant
    legSwapFloatingLegResetFrequency = "legSwapFloatingLegResetFrequency"
    legSwapFloatingLegIndex = "legSwapFloatingLegIndex"
    legSwapFixedLegCouponFrequency = "legSwapFixedLegCouponFrequency" # kdb columns to generate fixed leg payment frequency
    legPricingConvention = "legPricingConvention"
    legSize = "legSize"
    legDv01 = "legDv01"
    legQuotePrice = "legQuotePrice"
    legQuoteMidPrice = "legQuoteMidPrice"
    legDealerSide = "legDealerSide"
    endReason = "endReason"
    sourceTimestamp = "sourceTimestamp"
    parentQuotePrice = "parentQuotePrice"
    parentQuoteMidPrice = "parentQuoteMidPrice"
    parentDealerSide = "parentDealerSide"
    endQuoteRank = "endQuoteRank"
    endQuoteTiedStatus = "endQuoteTiedStatus"
    enquiryType = "enquiryType"
    legSwapFixedLegRate = "legSwapFixedLegRate"

class MatchAttributes(Enum):
    PERFECT_MATCH = "PERFECT_MATCH"
    SIMPLE_MATCH = "SIMPLE_MATCH"
    RFQ_COUNT = "RFQ_COUNT"
    MATCH = "Match"
    TIE_BREAK_LOGIC = "TieBreakLogic"

class EndReason(Enum):
    COUNTERPARTY_TRADED_WITH_BARCLAYS = "COUNTERPARTY_TRADED_WITH_BARCLAYS"
    COUNTERPARTY_TRADED_AWAY = "COUNTERPARTY_TRADED_AWAY"
    COUNTERPARTY_REJECTED = "COUNTERPARTY_REJECTED"
    COUNTERPARTY_TIMEOUT = "COUNTERPARTY_TIMEOUT"
    BARCLAYS_TIMEOUT = "BARCLAYS_TIMEOUT"
    BARCLAYS_REJECTED = "BARCLAYS_REJECTED"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    ERROR = "ERROR"

    @classmethod
    def _missing_(cls, value) -> "EndReason":
        return EndReason.ERROR

class EndQuoteRank(Enum):
    BEST = "BEST"
    COVER = "COVER"
    WIDER_THAN_COVER = "WIDER_THAN_COVER"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    ERROR = "ERROR"

    @classmethod
    def _missing_(cls, value) -> "EndQuoteRank":
        return EndQuoteRank.ERROR

class EndQuoteTiedStatus(Enum):
    TIED = "TIED"
    NOT_TIED = "NOT_TIED"
    UNKNOWN = "UNKNOWN"
    NOT_APPLICABLE = "NOT_APPLICABLE"