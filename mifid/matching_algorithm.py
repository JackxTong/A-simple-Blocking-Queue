import logging
import pandas as pd
import numpy as np
from tqdm import tqdm

from swaps_analytics.irs.mifid_matching.mifid_constants import (
    MifidMatchingConfiguration as config,
    MifidColumns,
    RfqColumns,
    MatchAttributes
)

class MifidDataMatchingAlgorithm:
    def __init__(
        self,
        rfq_data: pd.DataFrame,
        mifid_data: pd.DataFrame,
        backward_timedelta: int = 5,
        forward_timedelta: int = 5,
    ):
        """
        Class to match internal RFQ data against public MiFID tape data.
        """
        self.backward_timedelta = backward_timedelta
        self.forward_timedelta = forward_timedelta
        
        logging.info("Initializing and pre-processing RFQ and MiFID DataFrames")
        self.rfq_data = self._preprocess_rfq_data(rfq_data)
        self.mifid_data = self._preprocess_mifid_data(mifid_data)
        
        self.match_rates_by_date = []
        
        logging.info("Running the matching algorithm between RFQ and MiFID tables")
        self.match_output = self.match_mifid_trades()

    def _preprocess_rfq_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df[RfqColumns.datetime_parsed.value] = pd.to_datetime(
            df[RfqColumns.dateTime.value], format="%d/%m/%Y %H:%M:%S"
        )
        df[RfqColumns.date.value] = df[RfqColumns.datetime_parsed.value].dt.date
        # Clean size (k) to float
        if df[RfqColumns.sizeK.value].dtype == 'O':
            df[RfqColumns.sizeK.value] = df[RfqColumns.sizeK.value].str.replace(',', '').astype(float)
        return df

    def _preprocess_mifid_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df[MifidColumns.tradingDateTime.value] = pd.to_datetime(df[MifidColumns.tradingDateTime.value])
        if df[MifidColumns.notionalAmount.value].dtype == 'O':
            df[MifidColumns.notionalAmount.value] = df[MifidColumns.notionalAmount.value].str.replace(',', '').astype(float)
        return df

    @staticmethod
    def filter_mifid_by_time_window(df_mifid: pd.DataFrame, time_col: str, ref_time: pd.Timestamp, backward_mins: int, forward_mins: int) -> pd.DataFrame:
        start_time = ref_time - pd.Timedelta(minutes=backward_mins)
        end_time = ref_time + pd.Timedelta(minutes=forward_mins)
        return df_mifid[(df_mifid[time_col] >= start_time) & (df_mifid[time_col] <= end_time)]

    @staticmethod
    def filter_mifid_by_exact_notional(df_mifid: pd.DataFrame, notional_col: str, ref_notional: float, tolerance: float) -> pd.DataFrame:
        return df_mifid[
            (df_mifid[notional_col] >= ref_notional - tolerance) & 
            (df_mifid[notional_col] <= ref_notional + tolerance)
        ]

    @staticmethod
    def filter_mifid_by_maturity_year(df_mifid: pd.DataFrame, expiry_col: str, ref_maturity_date: pd.Timestamp) -> pd.DataFrame:
        # Drop NaNs first to safely check the year
        df_filtered = df_mifid.dropna(subset=[expiry_col]).copy()
        if df_filtered.empty:
            return df_filtered
        
        ref_year = pd.to_datetime(ref_maturity_date).year
        df_filtered['Drv Expiry Year'] = pd.to_datetime(df_filtered[expiry_col]).dt.year
        return df_filtered[df_filtered['Drv Expiry Year'] == ref_year]

    @staticmethod
    def tie_breaking_closest_time(df_mifid: pd.DataFrame, time_col: str, ref_time: pd.Timestamp) -> pd.DataFrame:
        df_mifid = df_mifid.copy()
        time_diffs = abs((df_mifid[time_col] - ref_time).dt.total_seconds())
        min_diff = time_diffs.min()
        return df_mifid[time_diffs == min_diff].head(1)

    def match_mifid_trades(self) -> pd.DataFrame:
        matched_results = []
        
        unique_dates = self.rfq_data[RfqColumns.date.value].unique()
        
        for trade_date in unique_dates:
            df_rfq_date = self.rfq_data[self.rfq_data[RfqColumns.date.value] == trade_date]
            matches_for_date = 0
            
            # Pre-filter MiFID data for the current date to make prints and filtering cleaner/faster
            mifid_date_base = self.mifid_data[self.mifid_data[MifidColumns.tradingDateTime.value].dt.date == trade_date]
            
            for request_id in tqdm(
                df_rfq_date[RfqColumns.rfqId.value].unique(),
                desc=f"Matching MiFID Trades for {trade_date}",
                leave=False
            ):
                rfq_row = df_rfq_date[df_rfq_date[RfqColumns.rfqId.value] == request_id].iloc[0]
                rfq_time = rfq_row[RfqColumns.datetime_parsed.value]
                rfq_size = rfq_row[RfqColumns.sizeK.value]
                rfq_maturity = rfq_row[RfqColumns.legInstrumentMaturityDate.value]
                rfq_venue = rfq_row[RfqColumns.venue.value]
                rfq_currency = rfq_row[RfqColumns.legCurrency.value]
                
                print(f"\n--- Matching RFQ ID: {request_id} ---")
                initial_len = len(mifid_date_base)
                
                # 1. Filter by Time Window
                mifid_filtered = self.filter_mifid_by_time_window(
                    df_mifid=mifid_date_base,
                    time_col=MifidColumns.tradingDateTime.value,
                    ref_time=rfq_time,
                    backward_mins=self.backward_timedelta,
                    forward_mins=self.forward_timedelta
                )
                current_len = len(mifid_filtered)
                print(f"1. Time Window Filter  | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")
                if mifid_filtered.empty:
                    continue
                initial_len = current_len
                
                # 2. Filter by Venue (Source)
                mifid_filtered = mifid_filtered[mifid_filtered[MifidColumns.source.value] == rfq_venue]
                current_len = len(mifid_filtered)
                print(f"2. Venue Filter        | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")
                if mifid_filtered.empty:
                    continue
                initial_len = current_len

                # 3. Filter by Currency
                mifid_filtered = mifid_filtered[mifid_filtered[MifidColumns.currency.value] == rfq_currency]
                current_len = len(mifid_filtered)
                print(f"3. Currency Filter     | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")
                if mifid_filtered.empty:
                    continue
                initial_len = current_len
                    
                # 4. Filter by Exact Notional
                mifid_filtered = self.filter_mifid_by_exact_notional(
                    df_mifid=mifid_filtered,
                    notional_col=MifidColumns.notionalAmount.value,
                    ref_notional=rfq_size,
                    tolerance=config.NOTIONAL_TOLERANCE
                )
                current_len = len(mifid_filtered)
                print(f"4. Exact Notional Filter| Filtered out: {initial_len - current_len:<5} | Left: {current_len}")
                if mifid_filtered.empty:
                    continue
                initial_len = current_len
                    
                # 5. Filter by Maturity Year (handles NaN by dropping them internally)
                mifid_filtered = self.filter_mifid_by_maturity_year(
                    df_mifid=mifid_filtered,
                    expiry_col=MifidColumns.drvExpiryDate.value,
                    ref_maturity_date=rfq_maturity
                )
                current_len = len(mifid_filtered)
                print(f"5. Maturity Year Filter | Filtered out: {initial_len - current_len:<5} | Left: {current_len}")
                if mifid_filtered.empty:
                    continue

                # 6. Tie-Breaking Logic (Closest Time)
                if len(mifid_filtered) > 1:
                    print(f"-> Tie-breaker needed for {len(mifid_filtered)} rows. Selecting closest time.")
                    mifid_filtered = self.tie_breaking_closest_time(
                        df_mifid=mifid_filtered,
                        time_col=MifidColumns.tradingDateTime.value,
                        ref_time=rfq_time
                    )
                
                # Append Matched Row
                mifid_filtered = mifid_filtered.reset_index(drop=True)
                rfq_row_df = pd.DataFrame([rfq_row]).reset_index(drop=True)
                
                combined_match = pd.concat([rfq_row_df, mifid_filtered], axis=1)
                combined_match[MatchAttributes.MATCH.value] = True
                
                matched_results.append(combined_match)
                matches_for_date += 1
            
            # Record tracking stats
            date_match_rate = matches_for_date / len(df_rfq_date) if len(df_rfq_date) > 0 else 0
            self.match_rates_by_date.append({'Date': trade_date, 'MatchRate': date_match_rate})
            logging.info(f"MATCHING RATE FOR DATE {trade_date}: {date_match_rate:.2%}")

        if matched_results:
            return pd.concat(matched_results, ignore_index=True)
        else:
            return pd.DataFrame()
