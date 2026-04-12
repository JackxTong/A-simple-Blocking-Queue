import logging
import os
import warnings
from pathlib import Path

import pandas as pd

from swaps_analytics.core.constants import EnvType
from swaps_analytics.core.currencies import CurrencyEnum
from swaps_analytics.irs.sdr_matching.matching_algorithm import SdrDataMatchingAlgorithm
from swaps_analytics.irs.sdr_matching.publish_sdr_output import (
    publish_sdr_output_rats,
    filter_out_unmatched_trades,
    prepare_data,
)
from swaps_analytics.irs.sdr_matching.sdr_constants import RfqColumns, SdrColumns
from swaps_analytics.irs.sdr_matching.sdr_match_statistics import SdrDataMatchingStatistics
from swaps_analytics.irs.sdr_matching.sdr_matching_runner import SdrMatchingRunner

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")

def publish_and_save_matched_results(sdr_matched_output: SdrDataMatchingAlgorithm, rats_port: int, csv_file_path: Path):
    s = SdrDataMatchingStatistics(
        start_date=start_date,
        end_date=end_date,
        df_matched_output=sdr_matched_output.match_output_including_parent_level_data,
    )
    logging.info(f"\n {s.match_percentages}")
    logging.info(f"\n {s.dv01_based_results}")

    # published matched only parent and leg level data
    leg_and_parent_matched_tickets = sdr_matched_output.match_output_including_parent_level_data
    matched_df = filter_out_unmatched_trades(leg_and_parent_matched_tickets.copy())
    df_to_publish = prepare_data(matched_df)
    df_to_publish = df_to_publish.sort_values(by=[RfqColumns.date.value, SdrColumns.executionTimestamp.value, "sym"])

    publish_sdr_output_rats(df_to_publish, rats_port)
    if csv_file_path is not None:
        df_to_publish.to_csv(csv_file_path)

if __name__ == "__main__":
    # WARNINGS:
    # 1. the new column from rfq table legSwapFixedLegRate is only populated from 22.04.2024. sdr matching doesn't work before
    # 2. the merge between rfq and smm price trace is only on requestId
    # 3. don't forget to update MODEL_VERSION variable whenever the code is changed
    currency = CurrencyEnum.USD
    start_date = pd.Timestamp("2026-03-15").date()
    end_date = pd.Timestamp("2026-03-19").date()
    csv_path = Path(r"d:\sdr_match_output")

    sdr_runner = SdrMatchingRunner(currency=currency, run_date=end_date, start_date=None, env=EnvType.UAT_DR_NYK)
    csv_file = f"sdr_matched_trade_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"

    if not csv_path.exists():
        os.mkdir(csv_path)

    sdr_matched_output = sdr_runner.get_matched_rfq_sdr_data()

    # Publish to RATS
    # SdrMatchedSwapTradesLegLevelData will contain identical data as in prod table sdrMatchedTrade
    publish_and_save_matched_results(sdr_matched_output, rats_port=54275, csv_file_path=csv_path / csv_file)

    # Save raw results (matched and unmatched) locally to be used in postprocess_results.py
    df = sdr_matched_output.match_output_including_parent_level_data
    df.to_csv(csv_path / "matched_sdr_raw.csv")