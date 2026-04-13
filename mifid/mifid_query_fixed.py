import logging
import pandas as pd
from tqdm import tqdm
import numpy as np

from swaps_analytics.irs.mifid_matching.mifid_constants import (
    MifidMatchingConfiguration as config,
    MifidColumns,
    RfqColumns,
    MatchAttributes
)
from swaps_analytics.irs.mifid_matching.mifid_matching_utils import (
    filter_mifid_by_time_window,
    filter_mifid_by_exact_notional,
    filter_mifid_by_maturity_year,
    tie_breaking_closest_time
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MifidDataMatchingAlgorithm:
    def __init__(
        self,
        rfq_data: pd.DataFrame,
        mifid_data: pd.DataFrame,
        backward_timedelta: int = 5,
        forward_timedelta: int = 5,
        regulatory_scope: str = "SEF",
        debug_mode: bool = False
    ):
        self.backward_timedelta = backward_timedelta
        self.forward_timedelta = forward_timedelta
        self.regulatory_scope = regulatory_scope
        self.mifid_cols_to_concat = ['price', 'instrumentFullName', 'Drv Expiry Year', 'notionalAmount']
        
        if debug_mode:
            logging.getLogger().setLevel(logging.DEBUG)

        logging.info("Initializing and pre-processing RFQ and MiFID DataFrames")
        self.rfq_data = self._preprocess_rfq_data(rfq_data)
        self.mifid_data = self._preprocess_mifid_data(mifid_data)

        self.match_rates_by_date = []

        logging.info("Running the matching algorithm between RFQ and MiFID tables")
        self.match_output = self.match_mifid_trades()

    def _preprocess_rfq_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df[RfqColumns.datetime_parsed.value] = pd.to_datetime(df['sourceTimestamp'])
        df[RfqColumns.date.value] = df[RfqColumns.datetime_parsed.value].dt.date
        if df['legSize'].dtype == 'O':
            df['legSize'] = df['legSize'].str.replace(',', '').astype(float)
        return df

    def _preprocess_mifid_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df[MifidColumns.tradingDateTime.value] = pd.to_datetime(df[MifidColumns.tradingDateTime.value])
        if df[MifidColumns.notionalAmount.value].dtype == 'O':
            df[MifidColumns.notionalAmount.value] = df[MifidColumns.notionalAmount.value].str.replace(',', '').astype(float)
        return df

    def match_mifid_trades(self) -> pd.DataFrame:
        matched_results = []
        if self.regulatory_scope != "all":
            self.rfq_data = self.rfq_data.loc[
                self.rfq_data[RfqColumns.regulatoryScope.value] == self.regulatory_scope
            ]

        # Base unmatched template including new confidence and tie-break tracking
        unmatched_defaults = pd.DataFrame({
            'mifid_price': [np.nan],
            'instrumentFullName': [np.nan],
            'Drv Expiry Year': [np.nan],
            'notionalAmount': [np.nan],
            MatchAttributes.MATCH.value: [False],
            'tie_break_used': [False],
            'matchConfidence': ["no_match"]
        })

        unique_dates = self.rfq_data[RfqColumns.date.value].unique()

        for trade_date in unique_dates:
            df_rfq_date = self.rfq_data[self.rfq_data[RfqColumns.date.value] == trade_date]
            matches_for_date = 0

            # Pre-filter MiFID data for the date
            mifid_date_base = self.mifid_data[self.mifid_data[MifidColumns.tradingDateTime.value].dt.date == trade_date]

            for request_id in tqdm(
                df_rfq_date['requestId'].unique(),
                desc=f"Matching MiFID Trades for {trade_date}",
                leave=False
            ):
                # Isolate ALL legs for this specific request ID
                rfq_legs = df_rfq_date[df_rfq_date['requestId'] == request_id]
                request_id_results = []
                
                logging.debug(f"\n--- Matching RFQ ID: {request_id} ({len(rfq_legs)} legs) ---")

                # Iterate through every leg of the current request ID
                for idx, rfq_row in rfq_legs.iterrows():
                    rfq_row_df = pd.DataFrame([rfq_row]).reset_index(drop=True)

                    rfq_time = rfq_row[RfqColumns.datetime_parsed.value]
                    rfq_size = rfq_row['legSize']
                    rfq_maturity = rfq_row[RfqColumns.legInstrumentMaturityDate.value]

                    def handle_unmatched_state(filter_name):
                        logging.debug(f"Leg {idx} failed at {filter_name} filter.")
                        combined_match = pd.concat([rfq_row_df, unmatched_defaults], axis=1)
                        request_id_results.append(combined_match)

                    # 1. Filter by Time Window
                    mifid_filtered = filter_mifid_by_time_window(
                        df_mifid=mifid_date_base, time_col=MifidColumns.tradingDateTime.value,
                        ref_time=rfq_time, backward_mins=self.backward_timedelta, forward_mins=self.forward_timedelta
                    )
                    if mifid_filtered.empty:
                        handle_unmatched_state("Time Window")
                        continue

                    # 2. Filter by Exact Notional (Must pass regardless of row count)
                    mifid_filtered = filter_mifid_by_exact_notional(
                        df_mifid=mifid_filtered, notional_col=MifidColumns.notionalAmount.value,
                        ref_notional=rfq_size, tolerance=config.NOTIONAL_TOLERANCE
                    )
                    if mifid_filtered.empty:
                        handle_unmatched_state("Exact Notional")
                        continue

                    # 3. Filter by Maturity Year (Must pass regardless of row count)
                    mifid_filtered = filter_mifid_by_maturity_year(
                        df_mifid=mifid_filtered, expiry_col=MifidColumns.drvExpiryYear.value,
                        ref_maturity_date=rfq_maturity
                    )
                    if mifid_filtered.empty:
                        handle_unmatched_state("Maturity Year")
                        continue

                    # 4. Tie-Breaking Logic
                    tie_break_triggered = len(mifid_filtered) > 1
                    if tie_break_triggered:
                        logging.debug(f"-> Tie-breaker needed for {len(mifid_filtered)} rows.")
                        mifid_filtered = tie_breaking_closest_time(
                            df_mifid=mifid_filtered, time_col=MifidColumns.tradingDateTime.value, ref_time=rfq_time
                        )

                    # Append Successful Match for this leg
                    mifid_subset = mifid_filtered[self.mifid_cols_to_concat].reset_index(drop=True)
                    mifid_subset = mifid_subset.rename(columns={'price': 'mifid_price'})
                    
                    combined_match = pd.concat([rfq_row_df, mifid_subset], axis=1)
                    combined_match[MatchAttributes.MATCH.value] = True
                    combined_match['tie_break_used'] = tie_break_triggered
                    
                    # Apply Confidence Logic immediately based on tie break status
                    combined_match['matchConfidence'] = "simple_match" if tie_break_triggered else "perfect_match"
                    
                    matches_for_date += 1
                    request_id_results.append(combined_match)

                # --- Post-Processing for the full Request ID (All Legs) ---
                req_df = pd.concat(request_id_results, ignore_index=True)
                
                # Check if ALL legs for this Request ID successfully matched
                all_legs_matched = req_df[MatchAttributes.MATCH.value].all()
                req_df['allLegsMatch'] = all_legs_matched

                matched_results.append(req_df)

            # Record tracking stats
            date_match_rate = matches_for_date / len(df_rfq_date) if len(df_rfq_date) > 0 else 0
            self.match_rates_by_date.append({'Date': trade_date, 'MatchRate': date_match_rate})
            logging.info(f"Match rate for {trade_date}: {date_match_rate:.2%}")

        if matched_results:
            return pd.concat(matched_results, ignore_index=True)
        else:
            return pd.DataFrame()