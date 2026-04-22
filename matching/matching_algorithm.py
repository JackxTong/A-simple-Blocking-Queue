import pandas as pd
import numpy as np
from tqdm import tqdm
from public_trade_matching.constants import MatchingConfig, EndReason, RfqCols, PublicCols, OutputCols
from public_trade_matching.matching_utils import (
    filter_by_time_window, filter_by_exact_notional, filter_by_maturity_year, tie_breaking_closest_time
)

class UnifiedMatchingAlgorithm:
    def __init__(self, rfq_data: pd.DataFrame, mifid_data: pd.DataFrame, sdr_data: pd.DataFrame):
        self.rfq_data = self._preprocess_rfq(rfq_data)
        self.mifid_data = mifid_data
        self.sdr_data = sdr_data
        self.final_columns = [
            RfqCols.rfq_id, RfqCols.trade_date, RfqCols.trade_time, RfqCols.currency, 
            RfqCols.pricing_convention, RfqCols.regulatory_scope
        ]

    def _preprocess_rfq(self, df: pd.DataFrame) -> pd.DataFrame:
        # Filter to end reasons we care about AND strictly Single-Leg
        valid_reasons = [e.value for e in EndReason]
        df = df[(df['endReason'].isin(valid_reasons)) & (df[RfqCols.num_legs] == 1)].copy()
        
        df[RfqCols.trade_time] = pd.to_datetime(df[RfqCols.trade_time])
        df[RfqCols.venue] = np.where(df[RfqCols.rfq_id].str.startswith('TW', na=False), 'TRADEWEB', np.nan)
        df[RfqCols.venue] = np.where(df[RfqCols.rfq_id].str.startswith('BBG', na=False), 'BLOOMBERG', df[RfqCols.venue])
        
        if df[RfqCols.size].dtype == 'O':
            df[RfqCols.size] = df[RfqCols.size].str.replace(',', '').astype(float)
        return df

    def _get_sdr_price_for_rfq(self, rfq_pricing_convention: str, sdr_row: pd.Series) -> float:
        """Resolves the SDR Parent Price based on quoting convention for single-leg trades."""
        if rfq_pricing_convention == "RateQuoted":
            return sdr_row['sdrLegPrice']
        return sdr_row['pkgPrice']

    def execute_matching(self) -> pd.DataFrame:
        results = []
        
        for trade_date in self.rfq_data[RfqCols.trade_date].unique():
            df_rfq_date = self.rfq_data[self.rfq_data[RfqCols.trade_date] == trade_date]
            mifid_base = self.mifid_data[self.mifid_data[PublicCols.time].dt.date == trade_date]
            sdr_base = self.sdr_data[self.sdr_data[PublicCols.time].dt.date == trade_date]

            for _, rfq in tqdm(df_rfq_date.iterrows(), total=len(df_rfq_date), desc=f"Matching {trade_date}"):
                base_dict = rfq[self.final_columns].to_dict()
                base_dict[OutputCols.MATCH] = False
                base_dict[OutputCols.MATCHED_PRICE] = np.nan

                # Determine routing based on Regulatory Scope
                if rfq[RfqCols.regulatory_scope] == "MTF":
                    public_df = mifid_base
                    use_venue_filter = True
                elif rfq[RfqCols.regulatory_scope] == "SEF":
                    public_df = sdr_base
                    use_venue_filter = False
                else:
                    results.append(base_dict)
                    continue

                # 1. Time Window Filter
                filtered = filter_by_time_window(public_df, PublicCols.time, rfq[RfqCols.trade_time], MatchingConfig.BACKWARD_MINS, MatchingConfig.FORWARD_MINS)
                if filtered.empty: results.append(base_dict); continue

                # 2. Venue Filter (MiFID MTF only)
                if use_venue_filter:
                    filtered = filtered[filtered[PublicCols.source] == rfq[RfqCols.venue]]
                    if filtered.empty: results.append(base_dict); continue

                # 3. Exact Notional Filter
                filtered = filter_by_exact_notional(filtered, PublicCols.size, rfq[RfqCols.size], MatchingConfig.NOTIONAL_TOLERANCE)
                if filtered.empty: results.append(base_dict); continue

                # 4. Maturity Year Filter
                filtered = filter_by_maturity_year(filtered, PublicCols.maturity_year, rfq[RfqCols.maturity])
                if filtered.empty: results.append(base_dict); continue

                # 5. Tie Breaker
                if len(filtered) > 1:
                    filtered = tie_breaking_closest_time(filtered, PublicCols.time, rfq[RfqCols.trade_time])

                # Match Successful
                matched_row = filtered.iloc[0]
                base_dict[OutputCols.MATCH] = True
                
                # Resolve Price
                if rfq[RfqCols.regulatory_scope] == "MTF":
                    base_dict[OutputCols.MATCHED_PRICE] = matched_row[PublicCols.price]
                else:
                    base_dict[OutputCols.MATCHED_PRICE] = self._get_sdr_price_for_rfq(rfq[RfqCols.pricing_convention], matched_row)

                results.append(base_dict)

        return pd.DataFrame(results)