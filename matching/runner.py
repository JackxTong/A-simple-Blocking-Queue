import logging
from datetime import datetime
import pandas as pd
import time

from swaps_analytics.core.constants import EnvType
from swaps_analytics.core.currencies import CurrencyEnum
from public_trade_matching.kdb_extractor import ExtractPublicTradeData
from public_trade_matching.matching_algorithm import UnifiedMatchingAlgorithm

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class UnifiedMatchingRunner:
    def __init__(self, run_date: datetime.date, env: EnvType, currency: CurrencyEnum):
        self.run_date = run_date
        self.start_date = (run_date - pd.offsets.BusinessDay(5)).date()
        self.env = env
        self.currency = currency

    def run(self) -> pd.DataFrame:
        start = time.time()
        logging.info("Extracting data from KDB...")
        extractor = ExtractPublicTradeData(self.start_date, self.run_date, self.currency, self.env)
        
        rfq_data = extractor.get_rfq_data()
        if rfq_data.empty:
            logging.warning("No RFQ data found.")
            return pd.DataFrame()

        mifid_data = extractor.get_mifid_data()
        sdr_data = extractor.get_sdr_data()

        logging.info("Running unified matching algorithm...")
        matcher = UnifiedMatchingAlgorithm(rfq_data, mifid_data, sdr_data)
        final_output = matcher.execute_matching()

        end = time.time()
        logging.info(f"Matching Complete in {(end - start) / 60:.2f} minutes.")
        
        if not final_output.empty:
            match_rate = final_output[final_output['Match'] == True].shape[0] / len(final_output)
            logging.info(f"Total Single-Leg RFQs Processed: {len(final_output)}")
            logging.info(f"Overall Match Rate: {match_rate:.2%}")
        
        return final_output

if __name__ == "__main__":
    runner = UnifiedMatchingRunner(
        run_date=pd.Timestamp("2026-04-22").date(), 
        env=EnvType.UAT_DR_NYK, 
        currency=CurrencyEnum.USD
    )
    result_df = runner.run()
    # publisher logic here...