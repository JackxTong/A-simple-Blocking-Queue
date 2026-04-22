import logging
import time
from datetime import datetime
from typing import List
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from swaps_analytics.core.constants import EnvType
from swaps_analytics.irs.mifid_matching.mifid_query_single_leg_final import MifidDataMatchingAlgorithm
from swaps_analytics.irs.mifid_matching.extract_swap_mifid_kdb_data import ExtractSwapMifidDataFromKdb
from swaps_analytics.core.utils.enum_utils import currency_to_region
from swaps_analytics.core.currencies import CurrencyEnum
from swaps_analytics.irs.mifid_matching.mifid_constants import EndReason

class MifidMatchingRunner:
    """
    Publish matched MiFID/RFQ trades executed on the run_date.
    """

    def __init__(
        self,
        run_date: datetime.date,
        env: EnvType,
        currency: CurrencyEnum,
        start_date: datetime.date = None,
    ):
        self.currency = currency
        self.run_date = run_date
        self.env = env
        self.backward_timedelta = 7
        self.forward_timedelta = 1
        
        if start_date is None:
            self.start_date = (run_date - pd.offsets.BusinessDay(5)).date()
        else:
            self.start_date = start_date

        self.region = currency_to_region.get(currency)

    def get_matched_rfq_mifid_data(
        self,
        request_ids_to_exclude: List[str] = [],
    ) -> MifidDataMatchingAlgorithm:
        start = time.time()
        swap_mifid_data_getter = ExtractSwapMifidDataFromKdb(
            start_date=self.start_date, end_date=self.run_date, currency=self.currency, env=self.env
        )
        
        rfq_data = swap_mifid_data_getter.get_rfq_data_from_kdb()
        if len(request_ids_to_exclude) > 0:
            rfq_data = rfq_data.loc[~rfq_data["requestId"].isin(request_ids_to_exclude)].reset_index(drop=True)
            logging.info(f"{len(request_ids_to_exclude)} RFQs are excluded from matching.")

        if rfq_data.empty:
            raise SystemExit("No new RFQ trades since last run!")
        else:
            self.rfq_data = rfq_data
        
        self.mifid_data = swap_mifid_data_getter.get_mifid_data_from_kdb()
        end = time.time()
        logging.info(
            f"Time taken to extract RFQ and mifid between {self.start_date} and {self.run_date} from KDB: {np.round((end-start)/60, 2)} minutes."
        )

    def match(self):
        start = time.time()
        mifid_matching_algo = MifidDataMatchingAlgorithm(
            rfq_data=self.rfq_data,
            mifid_data=self.mifid_data,
            backward_timedelta=self.backward_timedelta,
            forward_timedelta=self.forward_timedelta,
        )
        
        mifid_matched_output = mifid_matching_algo.match_output
        self.match_rates_by_date = mifid_matching_algo.match_rates_by_date
        end = time.time()
        logging.info(
            f"Time taken to match RFQ/mifid trades between {self.start_date} and {self.run_date}: {np.round((end-start)/60, 2)} minutes."
        )
        
        return mifid_matched_output

    def publish(self):
        start = time.time()
        logging.info(f"Starting MiFID matching process for {self.run_date}")
        # already_matched_rfq_ids = self.get_existing_matched_request_ids("sym").unique()

        # self.get_matched_rfq_mifid_data(request_ids_to_exclude=already_matched_rfq_ids)
        self.matched_df = self.match()

        # publisher = PublishSdrOutputToKdb(
        #     currency=self.currency,
        #     matched_df=self.matched_df,
        #     env_type=self.env,
        #     publish_only_matched_trades=True,
        # )
        # publisher.publish_data()

        end = time.time()
        logging.info(f"Published MiFID/RFQ matched output. Time taken: {(end - start) / 60:.2f} minutes.")