import dsp_kx as kx
import logging
import numpy as np
import pandas as pd
from datetime import datetime

from swaps_analytics.core.constants import AssetClass, EnvType
from swaps_analytics.core.currencies import CurrencyEnum
from swaps_analytics.core.databases.connection_utils import ConnectionDetailsBuilder
from swaps_analytics.core.utils.enum_utils import currency_to_region
from swaps_analytics.irs.sdr_matching.sdr_constants import RfqColumns


class ExtractSwapSdrDataFromKdb:
    def __init__(self, start_date: datetime.date, end_date: datetime.date, currency: CurrencyEnum, env: EnvType):
        self.start_date = start_date
        self.end_date = end_date
        self.gateway_connection = ConnectionDetailsBuilder(AssetClass.RATES).set_env_type(env).build()
        self.currency = currency
        self.region = currency_to_region.get(currency)
        self.CONSTRAINT_PREFIX = {
            CurrencyEnum.EUR: "EUR*",
            CurrencyEnum.GBP: "GBP*",
            CurrencyEnum.USD: "USD-SOFR*",
        }
        self.LEG_CONSTRAINT_PREFIX = {
            CurrencyEnum.EUR: "*InterestRateSwap/EUR*",
            CurrencyEnum.GBP: "*InterestRateSwap/GBP*",
            CurrencyEnum.USD: "*SOFR*",
        }

    def _get_rfq_query(self) -> str:
        table_function_name = ".broadwaymetrics." + self.region + ".RFQ;"
        constraint = f'.constr.FIELDLIKE[`legInstrumentType; "{self.LEG_CONSTRAINT_PREFIX[self.currency]}"] ,'

        query = (
            """
            {[rates;startDate;endDate]
                data:
                    rates(?;
                    """
            + table_function_name
            + """
                    .constr.WITHIN[`date; (startDate; endDate)],
                    .constr.FIELDLIKE[`market; "*IRS*"],
                    """
            + constraint
            + """
                    .constr.FBY[=;`revisionNumber;max;`requestId];
                    .grp.FIELD[`requestId`legIndex];
                    .fstat.FIELDLASTAS[`date; `tradeDate],
                    .fstat.FIELDLAST[`numLegs`requestType`enquiryType`legInstrumentType`regulatoryScope`legInstrumentName`legInstrumentMaturityDate],
                    .fstat.FIELDLAST[`legSwapEffectiveDate`legPricingConvention`parentQuotePrice`parentQuoteMidPrice`parentQuoteMidPriceSource`legQuotePrice`legQuoteMidPrice],
                    .fstat.FIELDLAST[`legDealerSide`endReason`parentDealerSide`legSwapFloatingLegFrequency`legSwapFloatingLegResetFrequency],
                    .fstat.FIELDLAST[`legSwapFloatingLegIndex`legSwapFixedLegCouponFrequency`endQuoteRank`endQuoteTiedStatus`legSize`legDv01`legSwapFixedLegRate],
                    .fstat.FIELDLASTAS[("p"$; `sourceTimestamp); `sourceTimestamp],
                    .fstat.FIELDFIRSTAS[("p"$; `eventTime); `firstQuoteTime]);
                data: `requestId xkey select from data where numLegs = (count;legIndex) fby requestId;

                lastQuoteTimes:
                    rates(?;
                    """
            + table_function_name
            + """
                    .constr.WITHIN[`date; (startDate; endDate)],
                    .constr.FIELDLIKE[`market; "*IRS*"],
                    """
            + constraint
            + """
                    .constr.NOTFIELD[`negotiationState; `REJECTED`PENDING_DONE];
                    .grp.FIELD[`requestId];
                    .fstat.FIELDLAST[`date],
                    .fstat.FIELDLASTAS[("p"$; `eventTime); `lastQuoteTime]);

                lastQuoteTimes: select requestId, lastQuoteTime from lastQuoteTimes;
                data lj `requestId xkey lastQuoteTimes
            }"""
        )
        return query

    def get_rfq_data_from_kdb(self) -> pd.DataFrame:
        query = self._get_rfq_query()
        df = kx.q(query, self.gateway_connection, self.start_date, self.end_date).pd().reset_index()
        
        for col in [RfqColumns.requestId.value, RfqColumns.legInstrumentName.value]:
            df[col] = df[col].str.decode("utf-8")

        df_price_trace = self.get_rfq_price_trace_from_kdb(rfqIds=df[RfqColumns.requestId.value])
        df = pd.merge(df, df_price_trace, on=[RfqColumns.requestId.value])
        
        df.loc[df["legPricingConvention"] == "RateQuoted", "parentMidFromPriceTrace"] = (
            df.loc[df["legPricingConvention"] == "RateQuoted", "parentMidFromPriceTrace"] / 100
        )
        df["parentMidFromPriceTrace"] = abs(df["parentMidFromPriceTrace"]) * np.sign(df["parentQuotePrice"])
        
        logging.info(
            f"There are {df.shape[0]} trades (legs) in RFQ between {self.start_date}-{self.end_date}. "
            f"num SEF={df.loc[df['regulatoryScope'] == 'SEF'].shape[0]}, num MTF={df.loc[df['regulatoryScope'] == 'MTF'].shape[0]} trades."
        )
        return df

    def _get_sdr_query(self):
        sym_constraint = f'.constr.FIELDLIKEANY[`sym; ("InterestRate:IRSwap:OIS"; "{self.CONSTRAINT_PREFIX[self.currency]}")] ,'
        query_sdr = (
            """{[rates;startDate;endDate]
                startDateTime: "p"$startDate;
                endDateTime: "p"$(endDate + 1);
                data:
                    rates(?;
                        `.sdr.pisc.1.SDR;
                        .constr.WITHIN[`date; (startDate; endDate + 20)],
                        .constr.WITHIN[`ExecutionTimestamp; (startDateTime; endDateTime)],
                        """
            + sym_constraint
            + """
                        .constr.FIELD[`NotionalCurrencyLeg1; `"""
            + self.currency.value
            + """],
                        .constr.FIELD[`NotionalCurrencyLeg2; `"""
            + self.currency.value
            + """],
                        .constr.FIELD[`ActionType; `NEW`CORRECT`REVI],
                        .constr.FIELD[`EventType; `Trade];
                        0b;
                        .istat.FIELD[`sym`FixedRatePaymentFrequencyPeriodLeg1`FixedRatePaymentFrequencyPeriodLeg2],
                        .istat.FIELD[`FixedRatePaymentFrequencyPeriodMultiplierLeg1`FixedRatePaymentFrequencyPeriodMultiplierLeg2],
                        .istat.FIELD[`FloatingRatePaymentFrequencyPeriodLeg1`FloatingRatePaymentFrequencyPeriodLeg2],
                        .istat.FIELD[`FloatingRatePaymentFrequencyPeriodMultiplierLeg1`FloatingRatePaymentFrequencyPeriodMultiplierLeg2],
                        .istat.FIELD[`FloatingRateResetFrequencyPeriodLeg1`FloatingRateResetFrequencyPeriodLeg2],
                        .istat.FIELD[`FloatingRateResetFrequencyPeriodMultiplierLeg1`FloatingRateResetFrequencyPeriodMultiplierLeg2],
                        .istat.FIELD[`PackageIndicator`PackageTransactionPrice`PackageTransactionPriceNotation`PackageTransactionSpread`PackageTransactionSpreadCurrency],
                        .istat.FIELDAS[("d"$; `ExecutionTimestamp); `tradeDate],
                        .istat.FIELDAS[`FixedRateLeg1; `PriceNotation],
                        .istat.FIELDAS[`FixedRateLeg2; `AdditionalPriceNotation],
                        .istat.FIELDAS[`NotionalAmountLeg1; `sdrSize],
                        .istat.FIELDAS[("f"$; `OtherPaymentAmount); `OtherPaymentAmount],
                        .istat.FIELDAS[`DisseminationIdentifier; `sdrDisseminationId],
                        .istat.FIELDAS[`ExecutionTimestamp; `sdrExecutionTimestamp],
                        .istat.FIELDAS[`MandatoryClearingIndicator; `ExecutionVenue],
                        .istat.FIELDAS[`ExpirationDate; `legInstrumentMaturityDate],
                        .istat.FIELDAS[`EffectiveDate; `legSwapEffectiveDate]);
                
                data: select from data where ((not null PriceNotation) or (not null AdditionalPriceNotation) or (not null PackageTransactionPrice) or (not null OtherPaymentAmount));
                data: update numLegs: count i by sdrExecutionTimestamp from data
            }"""
        )
        return query_sdr

    def _get_rfq_price_trace_query(self) -> str:
        price_trace_table_name = ".swapstrading." + self.region + ".SMMPriceTrace;"
        query = (
            """
            {[rates;startDate;endDate;rfqs]
                data:
                    rates(?;
                    """
            + price_trace_table_name
            + """
                    .constr.WITHIN[`date; (startDate; endDate)],
                    .constr.NOT[.constr.FIELDLIKE[`requestId; "*BARC_RATES_IOB*"]],
                    .constr.FIELD[`requestId; string rfqs],
                    .constr.FIELDNOTNULL[`price],
                    .constr.FIELDLIKE[`calculatorName; "SwapMidPricer*"],
                    .constr.FBY[=;`revisionNumber;max;`requestId];
                    .grp.FIELD[`requestId];
                    .fstat.FIELDLASTAS[`side; `parentDealerSide],
                    .fstat.FIELDLAST[`price])
            }"""
        )
        return query

    def get_sdr_data_from_kdb(self) -> pd.DataFrame:
        query = self._get_sdr_query()
        df = kx.q(query, self.gateway_connection, self.start_date, self.end_date).pd()
        df["legSwapFixedLegRate"] = df["PriceNotation"] # Placeholder mapping for consistency
        logging.info(f"There are {df.shape[0]} SDR trades executed between {df['sdrExecutionTimestamp'].min()} - {df['sdrExecutionTimestamp'].max()}.")
        return df

    def get_rfq_price_trace_from_kdb(self, rfqIds) -> pd.DataFrame:
        query = self._get_rfq_price_trace_query()
        df = kx.q(query, self.gateway_connection, self.start_date, self.end_date, list(set(rfqIds))).pd().reset_index()
        df["parentMidFromPriceTrace"] = df["price"]
        df[RfqColumns.requestId.value] = df[RfqColumns.requestId.value].str.decode("utf-8")
        return df[[RfqColumns.requestId.value, "parentMidFromPriceTrace"]]