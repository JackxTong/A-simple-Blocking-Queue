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
    """
    Function used to round a number to its nearest integer
    :param value:     input which is to be rounded
    :return:          rounded value of the input
    """
    return int(value + 0.5)

class ProcessRfqAndSdrData:
    def __init__(
        self,
        rfq_data: pd.DataFrame,
        sdr_data: pd.DataFrame,
        pricing_convention: str,
        traded_end_state_only: bool,
        num_max_legs: int,
    ):
        """
        This class is used to process the RFQ and SDR data which has been extracted from KDB
        :param rfq_data:             data extracted from RFQ table
        :param sdr_data:             data extracted from SDR table
        :param pricing_convention:   desired pricing convention of RFQ
        :param traded_end_state_only: whether to filter for only (traded & traded_away)
        :param num_max_legs:         maximum number of legs to be considered
        """

        self.rfq_data = rfq_data
        self.sdr_data = sdr_data
        self.pricing_convention = pricing_convention
        self.traded_end_state_only = traded_end_state_only
        self.num_max_legs = num_max_legs

        # Filter data based on config parameters
        self.rfq_data, self.sdr_data = self.filter_data_based_on_config(self.rfq_data, self.sdr_data)

        # Process the two dataFrames
        self.sdr_data = self.sdr_data_processing_and_leg_frequency(self.sdr_data)
        self.rfq_data = self.rfq_data_processing_and_leg_frequency(self.rfq_data)
        self.rfq_data = self.compute_rfq_parent_dv01(self.rfq_data)
        logging.info(f"All RFQ #: {len(self.rfq_data)}")

    def filter_data_based_on_config(
        self, df_rfq: pd.DataFrame, df_sdr: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Filter data based on desired pricing convention, maximum number of legs and end state of RFQ
        :param df_rfq:    DataFrame containing RFQ table
        :param df_sdr:    DataFrame containing SDR table
        :return:          filtered RFQ and SDR tables
        """

        # Filter data based on price convention
        if self.pricing_convention != "all":
            df_rfq = df_rfq[df_rfq[RfqColumns.legPricingConvention.value] == self.pricing_convention]
            logging.info(f"Filter in pricing convention={self.pricing_convention} #: {len(df_rfq)}")

        # Filter data based on number of legs
        df_rfq = df_rfq[df_rfq[RfqColumns.numLegs.value] <= self.num_max_legs]
        df_sdr = df_sdr[df_sdr[RfqColumns.numLegs.value] <= self.num_max_legs]

        # Filter data based on end state of RFQ
        if self.traded_end_state_only:
            df_rfq = df_rfq.loc[
                df_rfq[RfqColumns.endReason.value].isin(
                    [EndReason.COUNTERPARTY_TRADED_AWAY.value, EndReason.COUNTERPARTY_TRADED_WITH_BARCLAYS.value]
                )
            ]
            logging.info(f"Filter in TradedRFQs #: {len(df_rfq)}")

        return df_rfq, df_sdr

    @staticmethod
    def sdr_data_processing_and_leg_frequency(df_sdr: pd.DataFrame) -> pd.DataFrame:
        """
        Data Extracted from SDR table is processed prior to application of matching rules
        :param df_sdr:      DataFrame containing SDR table
        :return df_sdr:     Processed version of input dataFrame
        """

        logging.info("Processing SDR data")
        # Derive fixed and floating leg frequency
        # Either of ResetFrequency1 or ResetFrequency2 is mentioned as null for each entry in SDR table. Entry
        # corresponding to a null reset frequency is the fixed leg index and vice versa
        df_sdr.is_copy = False

        def format_output(x):
            res = []
            for e in x:
                if type(e) != str:
                    e_str = e.astype(str)
                else:
                    e_str = e

                if e_str == "YEAR":
                    e_str = "Y"
                elif e_str == "MNTH":
                    e_str = "M"
                elif e_str == "WEEK":
                    e_str = "W"
                elif e_str == "EXPI":
                    e_str = "T"
                elif e_str == "DAIL":
                    e_str = "D"
                res.append(e_str)
            return "".join(res)

        columns_to_format = [
            "ResetFrequency1",
            "ResetFrequency2",
            "FixedPaymentFrequency1",
            "FixedPaymentFrequency2",
            "FloatingPaymentFrequency1",
            "FloatingPaymentFrequency2",
        ]

        for col in columns_to_format:
            df_sdr[col] = df_sdr[col].apply(format_output)
        # TODO: check if the logic is correct
        df_sdr[RfqColumns.leg_SwapFixedLegPayFrequency.value] = np.where(
            df_sdr["FixedPaymentFrequency1"] == "", df_sdr["FixedPaymentFrequency2"], df_sdr["FixedPaymentFrequency1"]
        )

        df_sdr[RfqColumns.leg_SwapFloatingLegPayFrequency.value] = np.where(
            df_sdr["FloatingPaymentFrequency1"] == "",
            df_sdr["FloatingPaymentFrequency2"],
            df_sdr["FloatingPaymentFrequency1"],
        )

        df_sdr["PaymentFrequency1"] = df_sdr[RfqColumns.leg_SwapFixedLegPayFrequency.value]
        df_sdr["PaymentFrequency2"] = df_sdr[RfqColumns.leg_SwapFloatingLegPayFrequency.value]

        return df_sdr

    @staticmethod
    def rfq_data_processing_and_leg_frequency(df_rfq: pd.DataFrame) -> pd.DataFrame:
        """
        Data Extracted from RFQ table is processed prior to application of matching rules
        :param df_rfq:      DataFrame containing SDR table
        :return df_rfq:     Processed version of input dataFrame
        """

        logging.info("Processing RFQ data")

        # Leg notionals are rounded based on their range
        df_rfq[ColumnUtils.notional_threshold_bucket.value] = np.digitize(
            x=df_rfq[RfqColumns.legSize.value], bins=rfqdpc.NOTIONAL_ROUNDING_THRESHOLDS, right=False
        )

        for ind in df_rfq.index:
            round_to_nearest = rfqdpc.NOTIONAL_ROUND_TO_NEAREST_DICT[
                df_rfq.loc[ind, ColumnUtils.notional_threshold_bucket.value]
            ]
            notional_with_respect_to_rounding_size = df_rfq.loc[ind, RfqColumns.legSize.value] / round_to_nearest
            df_rfq.loc[ind, ColumnUtils.roundedLegSize.value] = int(
                round_to_nearest * round_to_nearest_integer(notional_with_respect_to_rounding_size)
            )

        # Notional values are capped based on the cap sizes, which depend on the leg tenor
        df_rfq[ColumnUtils.tenor_days.value] = (
            df_rfq[RfqColumns.legInstrumentMaturityDate.value] - df_rfq[RfqColumns.legSwapEffectiveDate.value]
        ).dt.days
        df_rfq[ColumnUtils.notional_capping_bucket.value] = np.digitize(
            x=df_rfq[ColumnUtils.tenor_days.value], bins=rfqdpc.TENOR_BASED_NOTIONAL_CAPPING_THRESHOLD, right=True
        )

        for ind in df_rfq.index:
            notional_cap_size_bucket = df_rfq.loc[ind, ColumnUtils.notional_capping_bucket.value]
            notional_cap_size = rfqdpc.NOTIONAL_CAPPING_SIZE_DICT[notional_cap_size_bucket]
            if (
                notional_cap_size_bucket < len(rfqdpc.TENOR_BASED_NOTIONAL_CAPPING_THRESHOLD)
                and df_rfq.loc[ind, ColumnUtils.roundedLegSize.value] > notional_cap_size
            ):
                df_rfq.loc[ind, ColumnUtils.cappedLegSize.value] = notional_cap_size
            else:
                df_rfq.loc[ind, ColumnUtils.cappedLegSize.value] = df_rfq.loc[ind, ColumnUtils.roundedLegSize.value]

        # Dv01 based bucket - used for post-processing of results
        df_rfq["dv01_bucket"] = np.digitize(
            df_rfq[RfqColumns.legDv01.value], rfqdpc.DV01_BINS_DF["dv01_upper_bound"].tolist()
        )

        euribor_mapping = {
            "ANNUAL": "1Y",
            "SEMI_ANNUAL": "6M",
            "QUARTERLY": "3M",
            "ONE_TERM": "1T"
        }
        ois_mapping = {"ANNUAL": "1Y"}

        # ANNUAL accounts for >=99% 1Y+ SOFR swaps fixed
        df_rfq[RfqColumns.leg_SwapFixedLegPayFrequency.value] = df_rfq[
            RfqColumns.legSwapFixedLegCouponFrequency.value
        ].map(lambda x: euribor_mapping.get(x, ""))

        # ANNUAL accounts for >=99% 1Y+ SOFR swaps floating
        # for libor, annual=1Y, semi=6M, quarter=3M; for OIS, annual=1Y, everything else is 1T

        # Define euribor types
        euribor_types = ['InterestRateSwap/EUR/EUREURIBOR', 'InterestRateSwap/EUR/EUREURIBOR/IMM']
        is_euribor = df_rfq[RfqColumns.legInstrumentType.value].isin(euribor_types)

        # Only apply this mapping for Euribor
        df_rfq.loc[is_euribor, RfqColumns.leg_SwapFloatingLegPayFrequency.value] = (
            df_rfq.loc[is_euribor, RfqColumns.legSwapFloatingLegFrequency.value]
            .map(euribor_mapping)
            .fillna("")
        )

        df_rfq.loc[~is_euribor, RfqColumns.leg_SwapFloatingLegPayFrequency.value] = (
            df_rfq.loc[~is_euribor, RfqColumns.legSwapFloatingLegFrequency.value]
            .map(ois_mapping)
            .fillna("1T")
        )

        # set lastQuoteTime to sourceTimestamp when lastQuoteTime is missing; this occurs when the end reason is REJECTED or NOT_APPLICABLE
        df_rfq[ColumnUtils.lastQuoteTime.value] = np.where(
            pd.isnull(df_rfq[ColumnUtils.lastQuoteTime.value]),
            df_rfq[RfqColumns.sourceTimestamp.value],
            df_rfq[ColumnUtils.lastQuoteTime.value],
        )

        # set parentQuoteMidPrice to parentMidFromPriceTrace when parentQuoteMidPrice == 0
        bad_price_filter = (df_rfq[RfqColumns.parentQuoteMidPrice.value] == 0) | (
            df_rfq["parentQuoteMidPriceSource"].isin(["", "Disabled"])
        )
        df_rfq.loc[bad_price_filter, RfqColumns.parentQuoteMidPrice.value] = df_rfq.loc[
            bad_price_filter, "parentMidFromPriceTrace"
        ]

        # for NPV trades, sometimes parent/leg QuoteMidPrice are wrong by a factor of 10 (MAC trades), set to parentMidFromPriceTrace
        npv_filter = (df_rfq[RfqColumns.legPricingConvention.value] == "NPV") & (
            (abs(df_rfq[RfqColumns.legQuoteMidPrice.value] / df_rfq[RfqColumns.legQuotePrice.value]) > 8) \
            | (abs(df_rfq[RfqColumns.legQuoteMidPrice.value] / df_rfq[RfqColumns.legQuotePrice.value]) < 0.6)
        )
        df_rfq.loc[npv_filter, RfqColumns.parentQuoteMidPrice.value] = df_rfq.loc[npv_filter, "parentMidFromPriceTrace"]
        return df_rfq

    @staticmethod
    def compute_rfq_parent_dv01(df: pd.DataFrame) -> pd.DataFrame:
        df = df.set_index(RfqColumns.requestId.value)
        df["parentDv01"] = df[RfqColumns.legDv01.value]

        # calculation of parent_dv01 for rate-quoted switches
        switch_indices = (df[RfqColumns.numLegs.value] == 2) & (
            df[RfqColumns.legPricingConvention.value] == "RateQuoted"
        )
        df_2_leg_rate_quoted = df.loc[switch_indices]

        if not df_2_leg_rate_quoted.empty:
            df.loc[switch_indices, "parentDv01"] = df_2_leg_rate_quoted.groupby(RfqColumns.requestId.value).apply(
                lambda x: x[RfqColumns.legDv01.value].max()
            )

        # calculation of parent dv01 for rate-quoted flies
        butterfly_indices = (df[RfqColumns.numLegs.value] == 3) & (
            df[RfqColumns.legPricingConvention.value] == "RateQuoted"
        )
        df_3_leg_rate_quoted = df.loc[butterfly_indices]

        if df_3_leg_rate_quoted.shape[0] > 0:
            df.loc[butterfly_indices, "parentDv01"] = df_3_leg_rate_quoted.groupby(RfqColumns.requestId.value).apply(
                lambda x: (x.loc[x[RfqColumns.legIndex.value] == 1, RfqColumns.legDv01.value].values[0])
            )

        # calculation of parent dv01 for NPV trades
        npv_indices = df[RfqColumns.legPricingConvention.value] == "NPV"
        df_npvs = df.loc[npv_indices]
        df.loc[npv_indices, "parentDv01"] = df_npvs.groupby(RfqColumns.requestId.value).apply(
            lambda x: x[RfqColumns.legDv01.value].sum()
        )

        return df.reset_index()