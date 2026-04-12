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

class MifidDataMatchingAlgorithm:
    def __init__(
        self,
        rfq_data: pd.DataFrame,
        mifid_data: pd.DataFrame,
        backward_timedelta: int = 5,
        forward_timedelta: int = 5,
        regulatory_scope: str = "SEF",
    ):
        """
        Class to match internal RFQ data against public MiFID tape data.
        """
        self.backward_timedelta = backward_timedelta
        self.forward_timedelta = forward_timedelta
        self.regulatory_scope = regulatory_scope
        self.mifid_cols_to_concat = ['price', 'instrumentFullName', 'Drv Expiry Year', 'notionalAmount']

        logging.info("Initializing and pre-processing RFQ and MiFID DataFrames")
        self.rfq_data = self._preprocess_rfq_data(rfq_data)
        self.mifid_data = self._preprocess_mifid_data(mifid_data)

        self.match_rates_by_date = []

        logging.info("Running the matching algorithm between RFQ and MiFID tables")
        self.match_output = self.match_mifid_trades()

    def _preprocess_rfq_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df[RfqColumns.datetime_parsed.value] = pd.to_datetime(
            df['sourceTimestamp']
        )
        df[RfqColumns.date.value] = df[RfqColumns.datetime_parsed.value].dt.date
        # Clean size (k) to float
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

        mifid_results = pd.DataFrame()
        mifid_results['mifid_price'] = np.nan
        mifid_results['instrumentFullName'] = np.nan
        mifid_results['Drv Expiry Year'] = np.nan
        mifid_results['notionalAmount'] = np.nan
        mifid_results[MatchAttributes.MATCH.value] = False

        unique_dates = self.rfq_data[RfqColumns.date.value].unique()

        for trade_date in unique_dates:
            df_rfq_date = self.rfq_data[self.rfq_data[RfqColumns.date.value] == trade_date]
            matches_for_date = 0

            # Pre-filter MiFID data for the current date to make prints and filtering cleaner/faster
            mifid_date_base = self.mifid_data[self.mifid_data[MifidColumns.tradingDateTime.value].dt.date == trade_date]

            for request_id in tqdm(
                df_rfq_date['requestId'].unique(),
                desc=f"Matching MiFID Trades for {trade_date}",
                leave=False
            ):

                rfq_row = df_rfq_date[df_rfq_date['requestId'] == request_id].iloc[0]
                rfq_time = rfq_row[RfqColumns.datetime_parsed.value]
                rfq_size = rfq_row['legSize']
                rfq_maturity = rfq_row[RfqColumns.legInstrumentMaturityDate.value]

                rfq_venue = rfq_row['venue']
                # rfq_currency = rfq_row[RfqColumns.legCurrency.value]

                print(f"\n--- Matching RFQ ID: {request_id} ---")
                initial_len = len(mifid_date_base)

                mifid_filtered = pd.DataFrame()

                # 1. Filter by Time Window
                mifid_filtered = filter_mifid_by_time_window(
                    df_mifid=mifid_date_base,
                    time_col=MifidColumns.tradingDateTime.value,
                    ref_time=rfq_time,
                    backward_mins=self.backward_timedelta,
                    forward_mins=self.forward_timedelta
                )

                ### optional printing ################################
                current_len = len(mifid_filtered)

                print(f"1. Time Window Filter | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")

                if mifid_filtered.empty:
                    combined_match = pd.concat([rfq_row_df, mifid_results], axis=1)
                    matched_results.append(combined_match)
                    print(f'joining unmatched of shape {combined_match.shape} for id {request_id}')

                    continue

                initial_len = current_len

                ####################################################

                # # 2. Filter by Venue (Source)
                # mifid_filtered = mifid_filtered[mifid_filtered[MifidColumns.source.value] == rfq_venue]
                # current_len = len(mifid_filtered)
                # print(f"2. Venue Filter       | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")

                if mifid_filtered.empty:
                    combined_match = pd.concat([rfq_row_df, mifid_results], axis=1)
                    matched_results.append(combined_match)
                    print(f'joining unmatched of shape {combined_match.shape} for id {request_id}')

                    continue

                # initial_len = current_len

                # # 3. Filter by Currency
                # mifid_filtered = mifid_filtered[mifid_filtered[MifidColumns.currency.value] == rfq_currency]
                # current_len = len(mifid_filtered)
                # print(f"3. Currency Filter    | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")

                if mifid_filtered.empty:
                    combined_match = pd.concat([rfq_row_df, mifid_results], axis=1)
                    matched_results.append(combined_match)
                    print(f'joining unmatched of shape {combined_match.shape} for id {request_id}')

                    continue

                # initial_len = current_len


                # 4. Filter by Exact Notional
                mifid_filtered = filter_mifid_by_exact_notional(
                    df_mifid=mifid_filtered,
                    notional_col=MifidColumns.notionalAmount.value,
                    ref_notional=rfq_size,
                    tolerance=config.NOTIONAL_TOLERANCE
                )

                current_len = len(mifid_filtered)

                print(f"4. Exact Notional Filter| Filtered out: {initial_len - current_len:<5} | Left: {current_len}")

                if mifid_filtered.empty:
                    combined_match = pd.concat([rfq_row_df, mifid_results], axis=1)
                    matched_results.append(combined_match)
                    print(f'joining unmatched of shape {combined_match.shape} for id {request_id}')

                    continue

                initial_len = current_len

                # 5. Filter by Maturity Year (may contains NaN)
                if len(mifid_filtered) > 1:
                    mifid_filtered = filter_mifid_by_maturity_year(
                        df_mifid=mifid_filtered,
                        expiry_col=MifidColumns.drvExpiryYear.value,
                        ref_maturity_date=rfq_maturity
                    )
                    current_len = len(mifid_filtered)

                print(f"5. Maturity Year Filter | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")

                if mifid_filtered.empty:
                    combined_match = pd.concat([rfq_row_df, mifid_results], axis=1)
                    print(f'joining unmatched of shape {combined_match.shape} for id {request_id}')

                    matched_results.append(combined_match)
                    continue


                # 6. Tie-Breaking Logic (Closest Time)
                if len(mifid_filtered) > 1:
                    # print(f"-> Tie-breaker needed for {len(mifid_filtered)} rows. Selecting closest time.")
                    mifid_filtered = tie_breaking_closest_time(
                        df_mifid=mifid_filtered,
                        time_col=MifidColumns.tradingDateTime.value,
                        ref_time=rfq_time
                    )

                # Append result regardless of match status
                rfq_row_df = pd.DataFrame([rfq_row]).reset_index(drop=True)

                # Extract only specific columns from mifid
                mifid_subset = mifid_filtered[self.mifid_cols_to_concat].reset_index(drop=True)
                # Rename price column
                mifid_subset = mifid_subset.rename(columns={'price': 'mifid_price'})
                combined_match = pd.concat([rfq_row_df, mifid_subset], axis=1)
                combined_match[MatchAttributes.MATCH.value] = True
                matches_for_date += 1

                print(f'joining combined_match of shape {combined_match.shape} for id {request_id}')
                matched_results.append(combined_match)

            # Record tracking stats
            
            # print(f'complete {trade_date}, matched_results len {len(matched_results)}')
            date_match_rate = matches_for_date / len(df_rfq_date) if len(df_rfq_date) > 0 else 0
            self.match_rates_by_date.append({'Date': trade_date, 'MatchRate': date_match_rate})
            # print(f"MATCHING RATE FOR DATE {trade_date}: {date_match_rate:.2%}")

        if matched_results:
            return pd.concat(matched_results, ignore_index=True)
        else:
            return pd.DataFrame()