import logging
import numpy as np
import pandas as pd
from typing import Tuple

from swaps_analytics.irs.sdr_matching.sdr_constants import (
    RfqDataProcessingConfiguration as rfqdpc,
    EndReason,
    RfqColumns,
    SdrColumns,
    ColumnUtils,
)

def round_to_nearest_integer(value: float) -> int:
    return int(value + 0.5)

class ProcessRfqAndSdrData:
    def __init__(self, rfq_data: pd.DataFrame, sdr_data: pd.DataFrame, pricing_convention: str, traded_end_state_only: bool, num_max_legs: int):
        self.rfq_data, self.sdr_data = self.filter_data_based_on_config(rfq_data, sdr_data, pricing_convention, traded_end_state_only, num_max_legs)
        self.sdr_data = self.extract_sdr_logic_from_raw_data(self.sdr_data)
        self.sdr_data = self.sdr_data_processing_and_leg_frequency(self.sdr_data)
        self.rfq_data = self.rfq_data_processing_and_leg_frequency(self.rfq_data)
        self.rfq_data = self.compute_rfq_parent_dv01(self.rfq_data)
        logging.info(f"All RFQ #: {len(self.rfq_data)}")

    def filter_data_based_on_config(self, df_rfq: pd.DataFrame, df_sdr: pd.DataFrame, pricing_convention: str, traded_end_state_only: bool, num_max_legs: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if pricing_convention != "all":
            df_rfq = df_rfq[df_rfq[RfqColumns.legPricingConvention.value] == pricing_convention]
            logging.info(f"Filter in pricing convention={pricing_convention} #: {len(df_rfq)}")

        df_rfq = df_rfq[df_rfq[RfqColumns.numLegs.value] <= num_max_legs]
        df_sdr = df_sdr[df_sdr["numLegs"] <= num_max_legs] # using raw string for sdr column

        if traded_end_state_only:
            df_rfq = df_rfq.loc[df_rfq[RfqColumns.endReason.value].isin([EndReason.COUNTERPARTY_TRADED_AWAY.value, EndReason.COUNTERPARTY_TRADED_WITH_BARCLAYS.value])]
            logging.info(f"Filter in TradedRFQs #: {len(df_rfq)}")
        return df_rfq, df_sdr

    @staticmethod
    def extract_sdr_logic_from_raw_data(df_sdr: pd.DataFrame) -> pd.DataFrame:
        """Handles logic previously done in KDB query."""
        df_sdr["sdrLegPrice"] = np.where(pd.notna(df_sdr["PriceNotation"]), 100 * df_sdr["PriceNotation"], np.nan)
        df_sdr["sdrLegPrice"] = np.where(pd.notna(df_sdr["AdditionalPriceNotation"]), 100 * df_sdr["AdditionalPriceNotation"], df_sdr["sdrLegPrice"])
        df_sdr["PackageTransactionPrice"] = np.where(pd.isna(df_sdr["PackageTransactionPrice"]), df_sdr["OtherPaymentAmount"], df_sdr["PackageTransactionPrice"])
        
        # Build frequencies from Multiplier + Period strings
        for leg in [1, 2]:
            for freq_type, col_prefix in [("ResetFrequency", "FloatingRateResetFrequencyPeriod"), 
                                          ("FixedPaymentFrequency", "FixedRatePaymentFrequencyPeriod"), 
                                          ("FloatingPaymentFrequency", "FloatingRatePaymentFrequencyPeriod")]:
                multiplier_col = f"{col_prefix}MultiplierLeg{leg}"
                period_col = f"{col_prefix}Leg{leg}"
                df_sdr[f"{freq_type}{leg}"] = df_sdr[multiplier_col].fillna("").astype(str).str.replace(".0", "", regex=False) + df_sdr[period_col].fillna("")
                df_sdr[f"{freq_type}{leg}"] = df_sdr[f"{freq_type}{leg}"].replace("nan", "")
        
        cols_to_drop = [c for c in df_sdr.columns if "MultiplierLeg" in c or c.endswith("Leg1") or c.endswith("Leg2") and c not in ["sdrSize", "tradeDate"]]
        return df_sdr.drop(columns=cols_to_drop, errors="ignore")

    @staticmethod
    def sdr_data_processing_and_leg_frequency(df_sdr: pd.DataFrame) -> pd.DataFrame:
        logging.info("Processing SDR data")
        df_sdr.is_copy = False
        
        freq_map = {"YEAR": "Y", "MNTH": "M", "WEEK": "W", "EXPI": "T", "DAIL": "D"}
        columns_to_format = ["ResetFrequency1", "ResetFrequency2", "FixedPaymentFrequency1", "FixedPaymentFrequency2", "FloatingPaymentFrequency1", "FloatingPaymentFrequency2"]

        for col in columns_to_format:
            # Replace period names with abbreviations
            for k, v in freq_map.items():
                df_sdr[col] = df_sdr[col].str.replace(k, v)

        df_sdr[RfqColumns.leg_SwapFixedLegPayFrequency.value] = np.where(df_sdr["FixedPaymentFrequency1"] == "", df_sdr["FixedPaymentFrequency2"], df_sdr["FixedPaymentFrequency1"])
        df_sdr[RfqColumns.leg_SwapFloatingLegPayFrequency.value] = np.where(df_sdr["FloatingPaymentFrequency1"] == "", df_sdr["FloatingPaymentFrequency2"], df_sdr["FloatingPaymentFrequency1"])
        
        df_sdr["PaymentFrequency1"] = df_sdr[RfqColumns.leg_SwapFixedLegPayFrequency.value]
        df_sdr["PaymentFrequency2"] = df_sdr[RfqColumns.leg_SwapFloatingLegPayFrequency.value]
        return df_sdr

    @staticmethod
    def rfq_data_processing_and_leg_frequency(df_rfq: pd.DataFrame) -> pd.DataFrame:
        logging.info("Processing RFQ data")
        df_rfq[ColumnUtils.notional_threshold_bucket.value] = np.digitize(x=df_rfq[RfqColumns.legSize.value], bins=rfqdpc.NOTIONAL_ROUNDING_THRESHOLDS, right=False)

        for ind in df_rfq.index:
            round_to_nearest = rfqdpc.NOTIONAL_ROUND_TO_NEAREST_DICT[df_rfq.loc[ind, ColumnUtils.notional_threshold_bucket.value]]
            notional_with_respect_to_rounding_size = df_rfq.loc[ind, RfqColumns.legSize.value] / round_to_nearest
            df_rfq.loc[ind, ColumnUtils.roundedLegSize.value] = int(round_to_nearest * round_to_nearest_integer(notional_with_respect_to_rounding_size))

        df_rfq[ColumnUtils.tenor_days.value] = (df_rfq[RfqColumns.legInstrumentMaturityDate.value] - df_rfq[RfqColumns.legSwapEffectiveDate.value]).dt.days
        df_rfq[ColumnUtils.notional_capping_bucket.value] = np.digitize(x=df_rfq[ColumnUtils.tenor_days.value], bins=rfqdpc.TENOR_BASED_NOTIONAL_CAPPING_THRESHOLD, right=True)

        for ind in df_rfq.index:
            notional_cap_size_bucket = df_rfq.loc[ind, ColumnUtils.notional_capping_bucket.value]
            notional_cap_size = rfqdpc.NOTIONAL_CAPPING_SIZE_DICT.get(notional_cap_size_bucket, np.inf)
            df_rfq.loc[ind, ColumnUtils.cappedLegSize.value] = min(df_rfq.loc[ind, ColumnUtils.roundedLegSize.value], notional_cap_size)

        df_rfq["dv01_bucket"] = np.digitize(df_rfq[RfqColumns.legDv01.value], rfqdpc.DV01_BINS_DF["dv01_upper_bound"].tolist())

        euribor_mapping = {"ANNUAL": "1Y", "SEMI_ANNUAL": "6M", "QUARTERLY": "3M", "ONE_TERM": "1T"}
        df_rfq[RfqColumns.leg_SwapFixedLegPayFrequency.value] = df_rfq[RfqColumns.legSwapFixedLegCouponFrequency.value].map(lambda x: euribor_mapping.get(x, ""))

        is_euribor = df_rfq[RfqColumns.legInstrumentType.value].isin(['InterestRateSwap/EUR/EUREURIBOR', 'InterestRateSwap/EUR/EUREURIBOR/IMM'])
        df_rfq.loc[is_euribor, RfqColumns.leg_SwapFloatingLegPayFrequency.value] = df_rfq.loc[is_euribor, RfqColumns.legSwapFloatingLegFrequency.value].map(euribor_mapping).fillna("")
        df_rfq.loc[~is_euribor, RfqColumns.leg_SwapFloatingLegPayFrequency.value] = df_rfq.loc[~is_euribor, RfqColumns.legSwapFloatingLegFrequency.value].map({"ANNUAL": "1Y"}).fillna("1T")

        df_rfq[ColumnUtils.lastQuoteTime.value] = np.where(pd.isnull(df_rfq[ColumnUtils.lastQuoteTime.value]), df_rfq[RfqColumns.sourceTimestamp.value], df_rfq[ColumnUtils.lastQuoteTime.value])

        bad_price_filter = (df_rfq[RfqColumns.parentQuoteMidPrice.value] == 0) | (df_rfq["parentQuoteMidPriceSource"].isin(["", "Disabled"]))
        df_rfq.loc[bad_price_filter, RfqColumns.parentQuoteMidPrice.value] = df_rfq.loc[bad_price_filter, "parentMidFromPriceTrace"]

        npv_filter = (df_rfq[RfqColumns.legPricingConvention.value] == "NPV") & ((abs(df_rfq[RfqColumns.legQuoteMidPrice.value] / df_rfq[RfqColumns.legQuotePrice.value]) > 8) | (abs(df_rfq[RfqColumns.legQuoteMidPrice.value] / df_rfq[RfqColumns.legQuotePrice.value]) < 0.6))
        df_rfq.loc[npv_filter, RfqColumns.parentQuoteMidPrice.value] = df_rfq.loc[npv_filter, "parentMidFromPriceTrace"]
        return df_rfq

    @staticmethod
    def compute_rfq_parent_dv01(df: pd.DataFrame) -> pd.DataFrame:
        df = df.set_index(RfqColumns.requestId.value)
        df["parentDv01"] = df[RfqColumns.legDv01.value]

        switch_indices = (df[RfqColumns.numLegs.value] == 2) & (df[RfqColumns.legPricingConvention.value] == "RateQuoted")
        if not df.loc[switch_indices].empty:
            df.loc[switch_indices, "parentDv01"] = df.loc[switch_indices].groupby(RfqColumns.requestId.value).apply(lambda x: x[RfqColumns.legDv01.value].max())

        butterfly_indices = (df[RfqColumns.numLegs.value] == 3) & (df[RfqColumns.legPricingConvention.value] == "RateQuoted")
        if not df.loc[butterfly_indices].empty:
            df.loc[butterfly_indices, "parentDv01"] = df.loc[butterfly_indices].groupby(RfqColumns.requestId.value).apply(lambda x: (x.loc[x[RfqColumns.legIndex.value] == 1, RfqColumns.legDv01.value].values[0]))

        npv_indices = df[RfqColumns.legPricingConvention.value] == "NPV"
        df.loc[npv_indices, "parentDv01"] = df.loc[npv_indices].groupby(RfqColumns.requestId.value).apply(lambda x: x[RfqColumns.legDv01.value].sum())

        return df.reset_index()