import pandas as pd
import numpy as np
from tqdm import tqdm
from public_trade_matching.constants import EndReason, RfqColumns, SdrColumns, MifidColumns, MatchAttributes
from public_trade_matching.matching_utils import (
    filter_by_time_window, filter_dataframe_based_on_notional, 
    filter_dataframe_based_on_price, filter_mifid_by_maturity_year, tie_breaking_closest_time
)

class UnifiedMatchingAlgorithm:
    def __init__(self, rfq_data: pd.DataFrame, mifid_data: pd.DataFrame, sdr_data: pd.DataFrame):
        self.rfq_data = self._preprocess_rfq(rfq_data)
        self.mifid_data = mifid_data
        self.sdr_data = sdr_data

    def _preprocess_rfq(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df[RfqColumns.datetime_parsed.value] = pd.to_datetime(df[RfqColumns.sourceTimestamp.value])
        
        # Enforce Single-Leg and Valid End Reasons
        valid_reasons = [e.value for e in EndReason]
        df = df[(df[RfqColumns.endReason.value].isin(valid_reasons)) & (df[RfqColumns.numLegs.value] == 1)]
        
        # Parse Venue for MiFID checks
        df[RfqColumns.venue.value] = np.where(df[RfqColumns.requestId.value].str.startswith('TW', na=False), 'TRADEWEB', np.nan)
        df[RfqColumns.venue.value] = np.where(df[RfqColumns.requestId.value].str.startswith('BBG', na=False), 'BLOOMBERG', df[RfqColumns.venue.value])
        
        if df[RfqColumns.legSize.value].dtype == 'O':
            df[RfqColumns.legSize.value] = df[RfqColumns.legSize.value].str.replace(',', '').astype(float)
            
        return df

    def execute_matching(self) -> pd.DataFrame:
        matched_results = []
        
        for trade_date in self.rfq_data[RfqColumns.date.value].unique():
            df_rfq_date = self.rfq_data[self.rfq_data[RfqColumns.date.value] == trade_date]
            
            mifid_base = self.mifid_data[pd.to_datetime(self.mifid_data[MifidColumns.tradingDateTime.value]).dt.date == trade_date]
            sdr_base = self.sdr_data[pd.to_datetime(self.sdr_data[SdrColumns.executionTimestamp.value]).dt.date == trade_date]

            for _, rfq_row in tqdm(df_rfq_date.iterrows(), total=len(df_rfq_date), desc=f"Matching {trade_date}"):
                
                rfq_dict = rfq_row.to_dict()
                rfq_dict[MatchAttributes.MATCH.value] = False
                rfq_dict[MatchAttributes.MATCHED_PRICE.value] = np.nan
                
                rfq_time = rfq_row[RfqColumns.datetime_parsed.value]
                rfq_size = rfq_row[RfqColumns.legSize.value]
                reg_scope = rfq_row[RfqColumns.regulatoryScope.value]

                # ==========================================
                # PATH A: MiFID / MTF Matching
                # ==========================================
                if reg_scope == "MTF":
                    filtered = filter_by_time_window(mifid_base, MifidColumns.tradingDateTime.value, rfq_time)
                    if filtered.empty: matched_results.append(rfq_dict); continue

                    filtered = filtered[filtered[MifidColumns.source.value] == rfq_row[RfqColumns.venue.value]]
                    if filtered.empty: matched_results.append(rfq_dict); continue

                    filtered = filter_dataframe_based_on_notional(filtered, MifidColumns.notionalAmount.value, rfq_size)
                    if filtered.empty: matched_results.append(rfq_dict); continue

                    filtered = filter_mifid_by_maturity_year(filtered, MifidColumns.drvExpiryDate.value, rfq_row[RfqColumns.legInstrumentMaturityDate.value])
                    if filtered.empty: matched_results.append(rfq_dict); continue

                    if len(filtered) > 1:
                        filtered = tie_breaking_closest_time(filtered, MifidColumns.tradingDateTime.value, rfq_time)

                    rfq_dict[MatchAttributes.MATCH.value] = True
                    rfq_dict[MatchAttributes.MATCHED_PRICE.value] = filtered.iloc[0][MifidColumns.price.value]

                # ==========================================
                # PATH B: SDR / SEF Matching
                # ==========================================
                elif reg_scope == "SEF":
                    filtered = filter_by_time_window(sdr_base, SdrColumns.executionTimestamp.value, rfq_time)
                    if filtered.empty: matched_results.append(rfq_dict); continue

                    filtered = filter_dataframe_based_on_notional(filtered, SdrColumns.sdrSize.value, rfq_size)
                    if filtered.empty: matched_results.append(rfq_dict); continue

                    pricing_conv = rfq_row[RfqColumns.legPricingConvention.value]
                    if pricing_conv == "RateQuoted":
                        filtered = filter_dataframe_based_on_price(
                            df=filtered, 
                            price_column=SdrColumns.sdrLegPrice.value, 
                            reference_price=rfq_row[RfqColumns.legQuotePrice.value]
                        )
                    else: # NPV
                        filtered = filter_dataframe_based_on_price(
                            df=filtered, 
                            price_column=SdrColumns.PackageTransactionPrice.value, 
                            reference_price=rfq_row[RfqColumns.parentQuotePrice.value],
                            reference_dv01=rfq_row[RfqColumns.legDv01.value]
                        )
                    if filtered.empty: matched_results.append(rfq_dict); continue

                    if len(filtered) > 1:
                        filtered = tie_breaking_closest_time(filtered, SdrColumns.executionTimestamp.value, rfq_time)

                    rfq_dict[MatchAttributes.MATCH.value] = True
                    # Resolve final price based on convention
                    if pricing_conv == "RateQuoted":
                        rfq_dict[MatchAttributes.MATCHED_PRICE.value] = filtered.iloc[0][SdrColumns.sdrLegPrice.value]
                    else:
                        rfq_dict[MatchAttributes.MATCHED_PRICE.value] = filtered.iloc[0][SdrColumns.PackageTransactionPrice.value]

                # Append either the unmatched dict or the successfully matched dict
                matched_results.append(rfq_dict)

        return pd.DataFrame(matched_results)
