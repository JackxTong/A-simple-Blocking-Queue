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

# Configure logging at the module level
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
        """
        Class to match internal RFQ data against public MiFID tape data.
        """
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

        # FIX 1: Create a concrete 1-row DataFrame to prevent NaN casting on False
        unmatched_defaults = pd.DataFrame({
            'mifid_price': [np.nan],
            'instrumentFullName': [np.nan],
            'Drv Expiry Year': [np.nan],
            'notionalAmount': [np.nan],
            MatchAttributes.MATCH.value: [False]
        })

        unique_dates = self.rfq_data[RfqColumns.date.value].unique()

        for trade_date in unique_dates:
            df_rfq_date = self.rfq_data[self.rfq_data[RfqColumns.date.value] == trade_date]
            matches_for_date = 0

            # Pre-filter MiFID data
            mifid_date_base = self.mifid_data[self.mifid_data[MifidColumns.tradingDateTime.value].dt.date == trade_date]

            for request_id in tqdm(
                df_rfq_date['requestId'].unique(),
                desc=f"Matching MiFID Trades for {trade_date}",
                leave=False
            ):
                rfq_row = df_rfq_date[df_rfq_date['requestId'] == request_id].iloc[0]
                
                # FIX 2: Instantiate rfq_row_df at the TOP of the loop so failures capture the correct row
                rfq_row_df = pd.DataFrame([rfq_row]).reset_index(drop=True)

                rfq_time = rfq_row[RfqColumns.datetime_parsed.value]
                rfq_size = rfq_row['legSize']
                rfq_maturity = rfq_row[RfqColumns.legInstrumentMaturityDate.value]

                logging.debug(f"\n--- Matching RFQ ID: {request_id} ---")
                
                # Helper function to streamline the unmatched logic
                def handle_unmatched_state(filter_name):
                    logging.debug(f"Failed at {filter_name} filter. Appending unmatched row for ID {request_id}.")
                    combined_match = pd.concat([rfq_row_df, unmatched_defaults], axis=1)
                    matched_results.append(combined_match)

                initial_len = len(mifid_date_base)

                # 1. Filter by Time Window
                mifid_filtered = filter_mifid_by_time_window(
                    df_mifid=mifid_date_base,
                    time_col=MifidColumns.tradingDateTime.value,
                    ref_time=rfq_time,
                    backward_mins=self.backward_timedelta,
                    forward_mins=self.forward_timedelta
                )

                current_len = len(mifid_filtered)
                logging.debug(f"1. Time Window Filter   | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")

                if mifid_filtered.empty:
                    handle_unmatched_state("Time Window")
                    continue

                initial_len = current_len

                # 4. Filter by Exact Notional
                mifid_filtered = filter_mifid_by_exact_notional(
                    df_mifid=mifid_filtered,
                    notional_col=MifidColumns.notionalAmount.value,
                    ref_notional=rfq_size,
                    tolerance=config.NOTIONAL_TOLERANCE
                )

                current_len = len(mifid_filtered)
                logging.debug(f"4. Exact Notional Filter| Filtered out: {initial_len - current_len:<5} | Left: {current_len}")

                if mifid_filtered.empty:
                    handle_unmatched_state("Exact Notional")
                    continue

                initial_len = current_len

                # 5. Filter by Maturity Year
                if len(mifid_filtered) > 1:
                    mifid_filtered = filter_mifid_by_maturity_year(
                        df_mifid=mifid_filtered,
                        expiry_col=MifidColumns.drvExpiryYear.value,
                        ref_maturity_date=rfq_maturity
                    )
                    current_len = len(mifid_filtered)
                    logging.debug(f"5. Maturity Year Filter | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")

                if mifid_filtered.empty:
                    handle_unmatched_state("Maturity Year")
                    continue

                # 6. Tie-Breaking Logic
                if len(mifid_filtered) > 1:
                    logging.debug(f"-> Tie-breaker needed for {len(mifid_filtered)} rows. Selecting closest time.")
                    mifid_filtered = tie_breaking_closest_time(
                        df_mifid=mifid_filtered,
                        time_col=MifidColumns.tradingDateTime.value,
                        ref_time=rfq_time
                    )

                # Successful Match Processing
                mifid_subset = mifid_filtered[self.mifid_cols_to_concat].reset_index(drop=True)
                mifid_subset = mifid_subset.rename(columns={'price': 'mifid_price'})
                
                combined_match = pd.concat([rfq_row_df, mifid_subset], axis=1)
                combined_match[MatchAttributes.MATCH.value] = True
                matches_for_date += 1

                logging.debug(f"Successfully matched ID {request_id}. Appending shape {combined_match.shape}.")
                matched_results.append(combined_match)

            # Record tracking stats
            date_match_rate = matches_for_date / len(df_rfq_date) if len(df_rfq_date) > 0 else 0
            self.match_rates_by_date.append({'Date': trade_date, 'MatchRate': date_match_rate})
            logging.info(f"Matching rate for {trade_date}: {date_match_rate:.2%}")

        if matched_results:
            return pd.concat(matched_results, ignore_index=True)
        else:
            return pd.DataFrame()