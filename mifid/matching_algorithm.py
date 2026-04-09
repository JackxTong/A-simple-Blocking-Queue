import logging
import pandas as pd
from tqdm import tqdm

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

    def match_mifid_trades(self) -> pd.DataFrame:
        matched_results = []
        
        unique_dates = self.rfq_data[RfqColumns.date.value].unique()
        
        for trade_date in unique_dates:
            df_rfq_date = self.rfq_data[self.rfq_data[RfqColumns.date.value] == trade_date]
            matches_for_date = 0
            
            for request_id in tqdm(
                df_rfq_date[RfqColumns.rfqId.value].unique(),
                desc=f"Matching MiFID Trades for {trade_date}",
                leave=False
            ):
                rfq_row = df_rfq_date[df_rfq_date[RfqColumns.rfqId.value] == request_id].iloc[0]
                rfq_time = rfq_row[RfqColumns.datetime_parsed.value]
                rfq_size = rfq_row[RfqColumns.sizeK.value]
                rfq_maturity = rfq_row[RfqColumns.legInstrumentMaturityDate.value]
                
                # 1. Filter by Time Window
                mifid_filtered = filter_mifid_by_time_window(
                    df_mifid=self.mifid_data,
                    time_col=MifidColumns.tradingDateTime.value,
                    ref_time=rfq_time,
                    backward_mins=self.backward_timedelta,
                    forward_mins=self.forward_timedelta
                )
                
                if mifid_filtered.empty:
                    continue
                    
                # 2. Filter by Exact Notional
                mifid_filtered = filter_mifid_by_exact_notional(
                    df_mifid=mifid_filtered,
                    notional_col=MifidColumns.notionalAmount.value,
                    ref_notional=rfq_size,
                    tolerance=config.NOTIONAL_TOLERANCE
                )
                
                if mifid_filtered.empty:
                    continue
                    
                # 3. Filter by Maturity Year
                mifid_filtered = filter_mifid_by_maturity_year(
                    df_mifid=mifid_filtered,
                    expiry_col=MifidColumns.drvExpiryDate.value,
                    ref_maturity_date=rfq_maturity
                )
                
                if mifid_filtered.empty:
                    continue

                # 4. Tie-Breaking Logic (Closest Time)
                if len(mifid_filtered) > 1:
                    mifid_filtered = tie_breaking_closest_time(
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