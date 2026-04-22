import logging
import pandas as pd
from tqdm import tqdm
import numpy as np

from swaps_analytics.irs.mifid_matching.mifid_constants import (
    MifidDataMatchingConfiguration as config,
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
from swaps_analytics.irs.mifid_matching.mifid_constants import EndReason

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MifidDataMatchingAlgorithm:
    def __init__(
        self,
        rfq_data: pd.DataFrame,
        mifid_data: pd.DataFrame,
        backward_timedelta: int = 5,
        forward_timedelta: int = 5,
        regulatory_scope: str = "MTF",
        debug_mode: bool = False,
        end_reason_list=[
            EndReason.COUNTERPARTY_TRADED_WITH_BARCLAYS.value,
            EndReason.COUNTERPARTY_TRADED_AWAY.value,
            EndReason.COUNTERPARTY_REJECTED.value,
        ],
    ):
        """
        Class to match internal RFQ data against public MiFID tape data.
        """
        self.backward_timedelta = backward_timedelta
        self.forward_timedelta = forward_timedelta
        self.regulatory_scope = regulatory_scope
        self.self_mifid_cols_to_concat = [
            MifidColumns.price.value, 
            MifidColumns.instrumentFullName.value, 
            MifidColumns.drvExpiryYear.value, 
            MifidColumns.notionalAmount.value
        ]
        self.debug_mode = debug_mode
        self.end_reason_list = end_reason_list

        if debug_mode:
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logging.getLogger().setLevel(logging.CRITICAL + 1)
            logging.disable(logging.CRITICAL)

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
        
        if df[RfqColumns.legSize.value].dtype == 'O':
            df[RfqColumns.legSize.value] = df[RfqColumns.legSize.value].str.replace(',', '').astype(float)
            
        df_mtf = df[
            (df['regulatoryScope'] == 'MTF')
            & (df['endReason']).isin(self.end_reason_list)
        ].copy()
        
        logging.info(f"There were {df.shape[0]} RFQ legs, of which {df_mtf.shape[0]} trades are MTF and in end_reason_list.")
        
        df_mtf[RfqColumns.venue.value] = np.where(df_mtf[RfqColumns.requestId.value].str.startswith('TW', na=False), 'TRADEWEB', np.nan)
        df_mtf[RfqColumns.venue.value] = np.where(df_mtf[RfqColumns.requestId.value].str.startswith('BBG', na=False), 'BLOOMBERG', df_mtf[RfqColumns.venue.value])
        
        return df_mtf

    def _preprocess_mifid_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df[MifidColumns.tradingDateTime.value] = pd.to_datetime(df[MifidColumns.tradingDateTime.value])
        
        if df[MifidColumns.notionalAmount.value].dtype == 'O':
            df[MifidColumns.notionalAmount.value] = df[MifidColumns.notionalAmount.value].str.replace(',', '').astype(float)
            
        df["Drv Expiry Year"] = pd.to_datetime(df[MifidColumns.drvExpiryDate.value]).dt.year
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
            MifidColumns.instrumentFullName.value: [np.nan],
            MifidColumns.drvExpiryYear.value: [np.nan],
            MifidColumns.notionalAmount.value: [np.nan],
            MatchAttributes.MATCH.value: [False]
        })

        unique_dates = self.rfq_data[RfqColumns.date.value].unique()

        for trade_date in unique_dates:
            df_rfq_date = self.rfq_data[self.rfq_data[RfqColumns.date.value] == trade_date]
            matches_for_date = 0
            
            mifid_date_base = self.mifid_data[self.mifid_data[MifidColumns.tradingDateTime.value].dt.date == trade_date]
            
            for request_id in tqdm(
                df_rfq_date[RfqColumns.requestId.value].unique(),
                desc=f"Matching MiFID Trades for {trade_date}",
                leave=False
            ):
                rfq_row = df_rfq_date[df_rfq_date[RfqColumns.requestId.value] == request_id].iloc[0]
                
                # FIX 2: Instantiate rfq_row_df at the TOP of the loop
                rfq_row_df = pd.DataFrame([rfq_row]).reset_index(drop=True)
                
                rfq_time = rfq_row[RfqColumns.datetime_parsed.value]
                rfq_size = rfq_row[RfqColumns.legSize.value]
                rfq_maturity = rfq_row[RfqColumns.legInstrumentMaturityDate.value]
                rfq_venue = rfq_row[RfqColumns.venue.value]
                
                logging.debug(f"\n--- Matching RFQ ID: {request_id} ---")
                
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
                if self.debug_mode:
                    print(f"1. Time Window Filter | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")
                
                if mifid_filtered.empty:
                    handle_unmatched_state("Time Window")
                    continue

                initial_len = current_len

                # 2. Filter by Venue (Source)
                mifid_filtered = mifid_filtered[mifid_filtered[MifidColumns.source.value] == rfq_venue]
                current_len = len(mifid_filtered)
                if self.debug_mode:
                    print(f"2. Venue Filter       | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")

                if mifid_filtered.empty:
                    handle_unmatched_state("Venue Filter")
                    continue

                initial_len = current_len

                # 3. Filter by Exact Notional
                mifid_filtered = filter_mifid_by_exact_notional(
                    df_mifid=mifid_filtered,
                    notional_col=MifidColumns.notionalAmount.value,
                    ref_notional=rfq_size,
                    tolerance=config.NOTIONAL_TOLERANCE
                )
                current_len = len(mifid_filtered)
                if self.debug_mode:
                    print(f"3. Exact Notional Filter| Filtered out: {initial_len - current_len:<5} | Left: {current_len}")

                if mifid_filtered.empty:
                    handle_unmatched_state("Exact Notional")
                    continue

                initial_len = current_len

                # 4. Filter by Maturity Year
                if len(mifid_filtered) > 1:
                    mifid_filtered = filter_mifid_by_maturity_year(
                        df_mifid=mifid_filtered,
                        expiry_col=MifidColumns.drvExpiryYear.value,
                        ref_maturity_date=rfq_maturity
                    )
                
                current_len = len(mifid_filtered)
                if self.debug_mode:
                    print(f"4. Maturity Year Filter | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")

                if len(mifid_filtered) == 1:
                    if pd.notna(mifid_filtered[MifidColumns.drvExpiryYear.value].iloc[0]):
                        mifid_expiry_year = mifid_filtered[MifidColumns.drvExpiryYear.value].iloc[0]
                        rfq_maturity_year = pd.to_datetime(rfq_maturity).year
                        
                        if mifid_expiry_year != rfq_maturity_year:
                            if self.debug_mode:
                                print(f"unmatching becuz of different expiry {mifid_expiry_year} vs {rfq_maturity_year}")
                            handle_unmatched_state("Maturity Year")
                            continue
                elif mifid_filtered.empty:
                    handle_unmatched_state("Maturity Year")
                    continue

                # 5. Tie-Breaking Logic
                if len(mifid_filtered) > 1:
                    if self.debug_mode:
                        print(f"-> Tie-breaker needed for {len(mifid_filtered)} rows. Selecting closest time.")
                    mifid_filtered = tie_breaking_closest_time(
                        df_mifid=mifid_filtered,
                        time_col=MifidColumns.tradingDateTime.value,
                        ref_time=rfq_time
                    )

                # Successful Match Processing
                mifid_subset = mifid_filtered[self.self_mifid_cols_to_concat].reset_index(drop=True)
                mifid_subset = mifid_subset.rename(columns={MifidColumns.price.value: 'mifid_price'})
                
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