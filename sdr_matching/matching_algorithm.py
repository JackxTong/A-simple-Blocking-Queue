import logging
import numpy as np
import pandas as pd
from tqdm import tqdm
from typing import List, Tuple
from swaps_analytics.core.currencies import CurrencyEnum

# Internal imports based on your repository structure
from swaps_analytics.irs.sdr_matching.matching_algorithm_utils import (
    filter_dataframe_based_on_notional,
    filter_dataframe_based_on_price,
    filter_dataframe_based_on_feature_values,
    rfq_data_has_identical_legs,
    sdr_data_has_identical_legs,
    get_identical_leg_indices_of_rfq,
    tie_breaking_logic,
    derive_parent_price_from_leg_prices_for_rate_quoted_swaps,
    derive_parent_dealer_side,
    apply_mac_and_npv_changes,
)
from swaps_analytics.irs.sdr_matching.sdr_constants import (
    DataMatchingConfiguration as dmconfig,
    EndReason,
    EndQuoteRank,
    RfqColumns,
    SdrColumns,
    ColumnUtils,
    MatchAttributes,
)

class SdrDataMatchingAlgorithm:
    def __init__(
        self,
        currency: CurrencyEnum,
        rfq_data: pd.DataFrame,
        sdr_data: pd.DataFrame,
        num_max_legs: int,
        end_reason_list: List[EndReason],
        regulatory_scope: str,
        time_window_after_last_quote: int = dmconfig.TIME_WINDOW_SECONDS_AFTER_LAST_QUOTE,
        time_window_before_first_quote: int = dmconfig.TIME_WINDOW_SECONDS_BEFORE_FIRST_QUOTE,
        price_rule_valid: bool = dmconfig.PRICE_RULE_VALID,
        notional_rule_valid: bool = dmconfig.NOTIONAL_RULE_VALID,
    ):
        """
        This class is used to match data between RFQ and SDR table
        """
        self.rfq_data = rfq_data.sort_values(
            by=[RfqColumns.numLegs.value, RfqColumns.endReason.value], ascending=[False, False]
        ).reset_index(drop=True)
        
        self.sdr_data = sdr_data
        self.num_max_legs = num_max_legs
        self.end_reason_list = end_reason_list
        self.regulatory_scope = regulatory_scope
        self.time_window_after_last_quote = time_window_after_last_quote
        self.time_window_before_first_quote = time_window_before_first_quote
        self.price_rule_valid = price_rule_valid
        self.notional_rule_valid = notional_rule_valid

        self.features_to_match = [
            RfqColumns.date.value,
            RfqColumns.numLegs.value,
            RfqColumns.legInstrumentMaturityDate.value,
            RfqColumns.legSwapEffectiveDate.value,
            RfqColumns.leg_SwapFixedLegPayFrequency.value,
            RfqColumns.leg_SwapFloatingLegPayFrequency.value,
        ]

        self.identical_leg_feature_columns = [
            RfqColumns.requestId.value,
            RfqColumns.date.value,
            RfqColumns.numLegs.value,
            RfqColumns.legInstrumentName.value,
            RfqColumns.legInstrumentMaturityDate.value,
            RfqColumns.legSwapEffectiveDate.value,
            RfqColumns.leg_SwapFloatingLegPayFrequency.value,
            RfqColumns.leg_SwapFixedLegPayFrequency.value,
            RfqColumns.legPricingConvention.value,
        ]

        if self.notional_rule_valid:
            self.identical_leg_feature_columns.append(RfqColumns.legSize.value)

        # Counters and Trackers
        self.correct_leg_matches = 0
        self.correct_leg_match_indices = []
        self.uniqueness_leg_match = 0
        self.uniqueness_leg_match_indices = []
        
        self.all_correct_rfq_matches = 0
        self.all_failed_rfq_matches = 0
        self.partially_correct_rfq_matches = 0
        self.partially_failed_rfq_matches = 0

        logging.info("Running the matching algorithm between RFQ and SDR tables")
        self.match_output = self.match_rfq(df_rfq=self.rfq_data, df_sdr=self.sdr_data)

        logging.info("Computing parent level quotes")
        # Logic for parent level quotes and filtering
        self.match_output_including_parent_level_data = self.get_parent_level_quotes(df=self.match_output)

        self.match_output_including_parent_level_data = self.unmatch_trades_if_price_diff_too_large(
            df=self.match_output_including_parent_level_data
        )
        
        self.match_output_including_parent_level_data = self.unmatch_trades_if_sdr_parent_price_is_nan(
            df=self.match_output_including_parent_level_data
        )

        self.match_output_including_parent_level_data = self.add_match_confidence(
            df=self.match_output_including_parent_level_data
        )

        if self.num_max_legs > 1:
            logging.info("Derive parent dealer side")
            self.match_output_including_parent_level_data = derive_parent_dealer_side(
                df=self.match_output_including_parent_level_data,
                leg_dealer_side_column=RfqColumns.legDealerSide.value,
                parent_dealer_side_column=RfqColumns.parentDealerSide.value,
            )

    @staticmethod
    def unmatch_trades_if_price_diff_too_large(df: pd.DataFrame):
        df["abs_price_diff_bps"] = abs(df['parentMidFromPriceTrace'] - df[SdrColumns.sdrParentPrice.value])
        
        # NPV specific filter
        npv_filter = df[RfqColumns.legPricingConvention.value] == "NPV"
        df.loc[npv_filter, "abs_price_diff_bps"] = (
            df.loc[npv_filter, "abs_price_diff_bps"] / df.loc[npv_filter, "parentDv01"]
        )

        large_diff_filter = (df[MatchAttributes.MATCH.value] == True) & (
            df["abs_price_diff_bps"] > dmconfig.MAX_RATE_DIFF
        )
        df.loc[large_diff_filter, MatchAttributes.MATCH.value] = False
        return df

    @staticmethod
    def unmatch_trades_if_sdr_parent_price_is_nan(df: pd.DataFrame):
        df.loc[pd.isnull(df[SdrColumns.sdrParentPrice.value]), MatchAttributes.MATCH.value] = False
        return df

    @staticmethod
    def add_match_confidence(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[
            (df[MatchAttributes.MATCH.value] == True) & (df[MatchAttributes.TIE_BREAK_LOGIC.value] == False),
            ColumnUtils.matchConfidence.value,
        ] = "perfect_match"
        
        df.loc[
            (df[MatchAttributes.MATCH.value] == True) & (df[MatchAttributes.TIE_BREAK_LOGIC.value] == True),
            ColumnUtils.matchConfidence.value,
        ] = "simple_match"
        
        df.loc[df[MatchAttributes.MATCH.value] == False, ColumnUtils.matchConfidence.value] = "no_match"
        return df

    def add_sdr_price_unit(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[df[RfqColumns.legPricingConvention.value] == "RateQuoted", SdrColumns.sdrPriceUnit.value] = "bps"
        df.loc[df[RfqColumns.legPricingConvention.value] == "NPV", SdrColumns.sdrPriceUnit.value] = self.currency.value
        return df
    
    @staticmethod
    def convert_empty_sdr_price_string_to_nan(df: pd.DataFrame) -> pd.DataFrame:
        price_columns = [
            SdrColumns.PackageTransactionPrice.value,
            SdrColumns.OtherPaymentAmount.value,
            SdrColumns.sdrLegPrice.value,
        ]
        for col in price_columns:
            df.loc[df[col] == "", col] = np.nan
            df[col] = df[col].astype(float)
        return df

    def replace_sdr_npv_price_and_convert_sign(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        The NPV leg price is in OtherPaymentAmount in SDR table, we need to replace sdrLegPrice (rate) with OtherPaymentAmount.

        PackageTransactionPrice (where we find match for multi-legged NPV trades) can be positive or negative.
        The sign of PackageTransactionPrice corresponds to client's direction.

        OtherPaymentAmount (where we find match for 1-legged NPV trades) is reported as a positive number.
        We filter entries by abs(price) in filter_dataframe_based_on_price function and adjust the sign here.
        """
        npv_filter = df[RfqColumns.legPricingConvention.value] == "NPV"
        # Logic to adjust signs based on client direction and package transaction price
        df.loc[npv_filter, SdrColumns.OtherPaymentAmount.value] = np.sign(
            df.loc[npv_filter, RfqColumns.legQuotePrice.value]
        ) * abs(df.loc[npv_filter, SdrColumns.OtherPaymentAmount.value])
        
        df.loc[npv_filter, SdrColumns.sdrLegPrice.value] = df.loc[npv_filter, SdrColumns.OtherPaymentAmount.value]
        
        df.loc[npv_filter, SdrColumns.PackageTransactionPrice.value] = np.sign(
            df.loc[npv_filter, RfqColumns.parentQuotePrice.value]
        ) * abs(df.loc[npv_filter, SdrColumns.PackageTransactionPrice.value])
        return df

    def filter_sdr_table_based_on_rfq_leg(
        self,
        df_rfq: pd.DataFrame,
        df_sdr: pd.DataFrame,
        request_id: str,
        leg_index: int,
        end_reason: EndReason,
        features_to_match: List[str],
    ) -> pd.DataFrame:
        df_rfq_req = df_rfq.loc[df_rfq[RfqColumns.requestId.value] == request_id]
        df_rfq_leg = df_rfq_req.loc[df_rfq_req[RfqColumns.legIndex.value] == leg_index]

        # Extract values for filtering
        rfq_leg_data = df_rfq_leg.loc[
            df_rfq_leg.index[0],
            [
                ColumnUtils.firstQuoteTime.value,
                ColumnUtils.lastQuoteTime.value,
                RfqColumns.legPricingConvention.value,
                RfqColumns.legQuotePrice.value,
                RfqColumns.parentQuotePrice.value,
                ColumnUtils.cappedLegSize.value,
                RfqColumns.legDv01.value,
            ]
        ]

        # 1. Filter by Instrument Features
        df_sdr_filtered = filter_dataframe_based_on_feature_values(
            data_to_be_filtered=df_sdr, reference_data=df_rfq_leg, features_to_match=features_to_match
        )

        # 2. Filter by Time Window
        time1 = rfq_leg_data[ColumnUtils.firstQuoteTime.value] - pd.Timedelta(self.time_window_before_first_quote, "s")
        time2 = rfq_leg_data[ColumnUtils.lastQuoteTime.value] + pd.Timedelta(self.time_window_after_last_quote, "s")
        
        df_sdr_filtered = df_sdr_filtered.loc[
            df_sdr_filtered[SdrColumns.executionTimestamp.value].between(time1, time2)
        ]

        # 3. Filter by quoted Price
        if self.price_rule_valid and end_reason == EndReason.COUNTERPARTY_TRADED_WITH_BARCLAYS:
            pricing_convention = rfq_leg_data[RfqColumns.legPricingConvention.value]
            if pricing_convention != "NPV":
                price_column = SdrColumns.sdrLegPrice.value
                quoted_price = rfq_leg_data[RfqColumns.legQuotePrice.value]
            else:
                price_column = SdrColumns.PackageTransactionPrice.value
                quoted_price = rfq_leg_data[RfqColumns.parentQuotePrice.value]

            df_sdr_filtered = filter_dataframe_based_on_price(
                data_to_be_filtered=df_sdr_filtered,
                price_column=price_column,
                reference_price=quoted_price,
            )
        elif self.price_rule_valid and end_reason != EndReason.COUNTERPARTY_TRADED_WITH_BARCLAYS:
            if pricing_convention == "NPV":
                df_sdr_filtered = df_sdr_filtered.loc[
                    ~pd.isnull(df_sdr_filtered[SdrColumns.PackageTransactionPrice.value])
                ]

        # 4. Filter by Notional
        if (not df_sdr_filtered.empty) and self.notional_rule_valid and (rfq_leg_data[RfqColumns.legPricingConvention.value] != "NPV"):
            df_sdr_filtered = filter_dataframe_based_on_notional(
                data_to_be_filtered=df_sdr_filtered,
                notional_column=SdrColumns.sdrSize.value,
                reference_notional=rfq_leg_data[ColumnUtils.cappedLegSize.value]
            )

        return df_sdr_filtered

    def match_rfq(self, df_sdr: pd.DataFrame, df_rfq: pd.DataFrame) -> pd.DataFrame:
        # Prepare output columns
        sdr_data_columns = list(df_sdr.columns)
        sdr_data_columns[sdr_data_columns.index(RfqColumns.date.value)] = "date_sdr"
        sdr_data_columns[sdr_data_columns.index(RfqColumns.sym.value)] = "sym_sdr"
        sdr_data_columns[sdr_data_columns.index(RfqColumns.numLegs.value)] = "numLegs_sdr"
        # Handle renames if necessary
        output_columns = list(df_rfq.columns) + [MatchAttributes.TIE_BREAK_LOGIC.value] + sdr_data_columns + [MatchAttributes.MATCH.value]
        
        match_output = pd.DataFrame(columns=output_columns)

        for end_reason in self.end_reason_list:
            df_rfq_source = df_rfq.loc[df_rfq[RfqColumns.endReason.value] == end_reason.value]

            # for client_reject, we only care about RFMs
            if end_reason == EndReason.COUNTERPARTY_REJECTED:
                df_rfq_source = df_rfq_source.loc[df_rfq_source[RfqColumns.requestType.value] == "RFM"]

            if self.regulatory_scope != "all":
                df_rfq_source = df_rfq_source.loc[df_rfq_source[RfqColumns.regulatoryScope.value] == self.regulatory_scope]

            df_rfq_source = df_rfq_source.reset_index(drop=True)

            if len(df_rfq_source) == 0:
                continue

            # run matching logic for each Request ID
            for request_id in tqdm(
                list(df_rfq_source[RfqColumns.requestId.value].unique()),
                desc=f"Matching RFQs with end reason {end_reason.value}",
                leave=False,
            ):
                num_legs = df_rfq_source.loc[
                    df_rfq_source[RfqColumns.requestId.value] == request_id, RfqColumns.numLegs.value].values[0]
                number_legs_correct = 0
                pricing_convention = df_rfq_source.loc[
                    df_rfq_source[RfqColumns.requestId.value] == request_id, RfqColumns.legPricingConvention.value].values[0]
                
                is_mac = True if "MAC" in df_rfq_source.loc[
                    df_rfq_source[RfqColumns.requestId.value] == request_id, 
                    RfqColumns.legInstrumentName.value
                ].values[0] else False

                # run matching algo for each leg
                if num_legs > 1:
                    features_to_match = self.features_to_match.copy()
                    if pricing_convention == "NPV":
                        apply_mac_and_npv_changes(
                            features_to_match=features_to_match,
                            is_mac=is_mac,
                        )
                else:
                    features_to_match = [x for x in self.features_to_match if x != RfqColumns.numLegs.value]
                    if pricing_convention == "NPV":
                        apply_mac_and_npv_changes(
                            features_to_match=features_to_match,
                            is_mac=is_mac,
                        )

                for leg_index in range(num_legs):
                    # for a particular request_id and leg_index, filter the SDR table based on values in RFQ table
                    df_sdr_filtered = self.filter_sdr_table_based_on_rfq_leg(
                        df_rfq=df_rfq_source,
                        df_sdr=df_sdr,
                        request_id=request_id,
                        leg_index=leg_index,
                        end_reason=end_reason,
                        features_to_match=features_to_match,
                    )

                    # classify the match as correct/incorrect based on length ofg the filtered SDR table
                    df_sdr_filtered, rfq_leg_data, identical_legs = self.classify_sdr_entry_based_on_matching(
                        df_rfq=df_rfq, df_sdr_filtered=df_sdr_filtered, request_id=request_id, leg_index=leg_index
                    )

                    # if we get a correct match, check if there is one-to-one mapping between the two entries. Having filtered a single entry of SDR table fort a particular RFQ leg; that particular SDR entry should only map to that particular RFQ leg
                    if len(df_sdr_filtered) == 1:
                        # for a particular SDR entry, filter RFQ table
                        df_rfq_subset = self.filter_sdr_table_based_on_rfq_leg(
                            df_rfq=df_rfq_source,
                            df_sdr_entry=df_sdr_filtered,
                            end_reason=end_reason,
                            pricing_convention=pricing_convention,
                            features_to_match=features_to_match,
                        )
                        # classify the match as correct/incorrect based on length of the filtered RFQ table
                        self.classify_rfq_entry_based_on_matching(
                            df_rfq_subset=df_rfq_subset,
                            request_id=request_id,
                            leg_index=leg_index,
                            df_sdr_entry=df_sdr_filtered,
                            identical_leg_indices=identical_legs,
                        )

                    # if we have a one-to-one mapping, leg is correctly matched. 
                    if (
                        (len(self.correct_leg_match_indices) > 0)
                        and (len(self.uniqueness_leg_match_indices) > 0)
                        and ((request_id, leg_index) == self.correct_leg_match_indices[-1])
                        and ((request_id, leg_index) == self.uniqueness_leg_match_indices[-1])
                    ):
                        number_legs_correct += 1

                        if end_reason == EndReason.COUNTERPARTY_TRADED_WITH_BARCLAYS:
                            df_sdr = df_sdr.loc[
                                df_sdr[SdrColumns.DisseminationId.value] != df_sdr_filtered[SdrColumns.DisseminationId.value].values[0]
                            ]
                        match_output.loc[len(match_output), :] = rfq_leg_data + list(df_sdr_filtered.values[0]) + [True]
                    else:
                        match_output.loc[len(match_output), :] = rfq_leg_data + [""] * len(sdr_data_columns) + [False]

                # if all legs are correctly/uniquely matched, RFQ is correctly matched
                if number_legs_correct == num_legs:
                    self.all_correct_rfq_matches += 1
                else:
                    self.partially_failed_rfq_matches += 1
                
                if number_legs_correct > 0:
                    self.partially_correct_rfq_matches += 1
                else:
                    self.all_failed_rfq_matches += 1

                
        all_leg_match_pct = np.round(
            (self.all_correct_rfq_matches * 100) / (self.all_correct_rfq_matches + self.partially_failed_rfq_matches), 2
        )
        at_least_one_leg_match_pct = np.round(
            (self.partially_correct_rfq_matches * 100) / (self.partially_correct_rfq_matches + self.all_failed_rfq_matches), 2
        )

        logging.info(f"All-leg match count: {self.all_correct_rfq_matches}, at least one leg match count: {self.partially_correct_rfq_matches}")
        logging.info(f"All-leg match percentage: {all_leg_match_pct}%, at least one leg match percentage: {at_least_one_leg_match_pct}%")

        match_output = self.convert_empty_sdr_price_string_to_nan(match_output)
        match_output = self.add_sdr_price_unit(match_output)
        match_output = self.replace_sdr_npv_price_and_convert_sign(match_output)

        return match_output
    
    def get_parent_level_quotes(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Get parent level quotes for the RFQ
        :param df: DataFrame with leg level matches between RFQ and SDR
        :return: DataFrame containing both leg level and parent level quotes for RFQ and SDR
        '''

        df[SdrColumns.sdrParentPrice.value] = np.nan
        npv_filter = df[RfqColumns.legPricingConvention.value] == "NPV"
        if npv_filter.sum() > 0:
            df.loc[npv_filter, SdrColumns.sdrParentPrice.value] = df.loc[npv_filter, SdrColumns.PackageTransactionPrice.value]

        one_legged_rate_quoted_filter = (df[RfqColumns.numLegs.value] == 1) & (
            df[RfqColumns.legPricingConvention.value] == "RateQuoted"
        )
        df.loc[one_legged_rate_quoted_filter, SdrColumns.sdrParentPrice.value] = df.loc[
            one_legged_rate_quoted_filter, SdrColumns.sdrLegPrice.value
        ]
        if self.num_max_legs > 1:
            # calculate parent quote price for data from RFQ table for rate quoted swaps
            df = derive_parent_price_from_leg_prices_for_rate_quoted_swaps(
                df=df,
                leg_price_colume = RfqColumns.legQuotePrice.value,
                parent_price_column = RfqColumns.parentQuotePrice.value,
            )
            # calculate parent price for data from SDR table for rate quoted swaps
            df = derive_parent_price_from_leg_prices_for_rate_quoted_swaps(
                df=df,
                leg_price_colume = SdrColumns.sdrLegPrice.value,
                parent_price_column = SdrColumns.sdrParentPrice.value,
            )

        # convert rate quoted prices to bps
        df.loc[
            df[RfqColumns.legPricingConvention.value] == "RateQuoted", 
            [
                SdrColumns.sdrLegPrice.value,
                RfqColumns.legQuotePrice.value,
                RfqColumns.parentQuotePrice.value,
                RfqColumns.parentQuoteMidPrice.value,
                SdrColumns.sdrParentPrice.value,
                'parentMidFromPriceTrace',
            ],
        ] *= 100

        # create columns priceWithEcnFee and legSdrPriceWithEcnFee
        df[SdrColumns.priceWithEcnFee.value] = df[SdrColumns.sdrParentPrice.value]
        df[SdrColumns.sdrParentPriceWithEcnFee.value] = df[SdrColumns.sdrParentPrice.value]

        return df
    
    def remove_ecn_fee_from_rfqs(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove the TradeWeb fee (0.02 - 0.03 bps) from the SDR matched quote for the following cases:
            - traded with Barclays
            - traded away, but end quote rank is best
        This is to ensure that the trade price matches our historic quote price and there is no error in series 
        """
        df["price_difference"] = df[SdrColumns.sdrParentPrice.value] - df[RfqColumns.parentQuotePrice.value]
        df.loc[
            (
                (df[RfqColumns.EndQuoteRank.value] == EndQuoteRank.BEST.value) 
                & (df[RfqColumns.numLegs.value] <= self.num_max_legs)
                & (df[RfqColumns.legPricingConvention.value] == "RateQuoted")
                & (df["price_difference"] < (dmconfig.PRICE_PRECISION * 100))
            ),
            (SdrColumns.sdrParentPrice.value, SdrColumns.sdrLegPrice.value),
        ] = df.loc[
            (
                (df[RfqColumns.EndQuoteRank.value] == EndQuoteRank.BEST.value) 
                & (df[RfqColumns.numLegs.value] <= self.num_max_legs)
                & (df[RfqColumns.legPricingConvention.value] == "RateQuoted")
                & (df["price_difference"] < (dmconfig.PRICE_PRECISION * 100))
            ),
            (RfqColumns.parentQuotePrice.value, RfqColumns.legQuotePrice.value),
        ].values

