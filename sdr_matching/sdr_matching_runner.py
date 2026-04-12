import logging
import time
from datetime import datetime, timedelta
from typing import List

import dsp_kx as kx
import numpy as np
import pandas as pd

from swaps_analytics.core.constants import AssetClass, EnvType
from swaps_analytics.core.currencies import CurrencyEnum
from swaps_analytics.core.databases.connection_utils import ConnectionDetailsBuilder
from swaps_analytics.core.utils.enum_utils import currency_to_region
from swaps_analytics.irs.sdr_matching.extract_swap_sdr_kdb_data import ExtractSwapSdrDataFromKdb
from swaps_analytics.irs.sdr_matching.matching_algorithm import SdrDataMatchingAlgorithm
from swaps_analytics.irs.sdr_matching.publish_sdr_output import PublishSdrOutputToKdb
from swaps_analytics.irs.sdr_matching.sdr_constants import EndReason
from swaps_analytics.irs.sdr_matching.sdr_data_processing import ProcessRfqAndSdrData

class SdrMatchingRunner:
    """
    Publish matched SDR/RFQ trades executed on run_date and previous business date (5 days).
    This is an attempt to capture as many matches as possible because there can be a delay in SDR data.
    """

    def __init__(
        self,
        currency: CurrencyEnum,
        run_date: datetime.date,
        env: EnvType,
        start_date: datetime.date = None,
        pricing_convention: str = "all",
        regulatory_scope: str = "SEF",
        traded_end_state_only: bool = False,
    ):
        """
        :param pricing_convention:      "RateQuoted", "NPV" or "all".
        :param regulatory_scope:        "SEF", "MTF", "all" currently we support only "SEF" as we might need to look in Propellant Mifid for "MTF" tra
        :param request_ids_to_exclude:  For real-time publication, exclude these request ids because they have already been matched earlier in the day
        :param traded_end_state_only:   whether to filter for only (traded & traded_away)
        """

        self.currency = currency
        self.run_date = run_date
        # it's still possible that self.start_date is a non trading day but it's ok to use as an interval start
        if start_date is None:
            self.start_date = (run_date - pd.offsets.BusinessDay(15)).date()
        else:
            self.start_date = start_date

        self.env = env
        self.region = currency_to_region.get(currency)
        self.num_max_legs = 500
        self.traded_end_state_only = traded_end_state_only
        if pricing_convention not in ["RateQuoted", "NPV", "all"]:
            raise ValueError(f"unsupported {pricing_convention}")
        self.pricing_convention = pricing_convention
        self.regulatory_scope = regulatory_scope

    def get_existing_matched_request_ids(self):
        query = (
            """
            {[rates;startDate;endDate]
                data:
                    rates(?;
                        `.swapstrading."""
            + self.region
            + """.matchedRfqSdrTrades;
                        .constr.WITHIN[`date; (startDate; endDate)];
                        // startDate is the trading day before endDate (runDate).
                        .grp.FIELD[`sym`matchConfidence];
                        .istat.FIELDAS[(count; `tradeDate); `counts])
            }"""
        )
        gateway_connection = ConnectionDetailsBuilder(AssetClass.RATES).set_env_type(self.env).build()
        if self.currency == CurrencyEnum.USD:
            # to query existing matched request we need to use self.run_date + 1
            # because matchedRfqSdrTrades is UTC timestamped whereas run_date is NYK local time
            df = kx.q(query, gateway_connection, self.start_date, self.run_date + timedelta(days=1))
        else:
            df = kx.q(query, gateway_connection, self.start_date, self.run_date)

        df = df.pd().reset_index().drop(columns=["counts"])
        return df

    def get_matched_rfq_sdr_data(
        self,
        request_ids_to_exclude: List[str] = [],
    ) -> SdrDataMatchingAlgorithm:
        start = time.time()
        swap_sdr_data_getter = ExtractSwapSdrDataFromKdb(
            start_date=self.start_date, end_date=self.run_date, currency=self.currency, env=self.env
        )
        rfq_data = swap_sdr_data_getter.get_rfq_data_from_kdb()
        if len(request_ids_to_exclude) > 0:
            rfq_data = rfq_data.loc[~rfq_data["requestId"].isin(request_ids_to_exclude)].reset_index(drop=True)
            logging.info(f"requestId={request_ids_to_exclude} are excluded from matching.")

        if rfq_data.empty:
            raise SystemExit(f"No new RFQ trades since last run!")

        sdr_data = swap_sdr_data_getter.get_sdr_data_from_kdb()

        processed_data = ProcessRfqAndSdrData(
            rfq_data=rfq_data,
            sdr_data=sdr_data,
            pricing_convention=self.pricing_convention,
            num_max_legs=self.num_max_legs,
            traded_end_state_only=self.traded_end_state_only,
        )

        sdr_matched_output = SdrDataMatchingAlgorithm(
            currency=self.currency,
            rfq_data=processed_data.rfq_data,
            sdr_data=processed_data.sdr_data,
            num_max_legs=self.num_max_legs,
            end_reason_list=[
                EndReason.COUNTERPARTY_TRADED_WITH_BARCLAYS,
                EndReason.COUNTERPARTY_TRADED_AWAY,
                EndReason.COUNTERPARTY_REJECTED,
            ],
            regulatory_scope=self.regulatory_scope,
        )
        end = time.time()
        logging.info(
            f"Time taken to match RFQ/SDR trades between {self.start_date} and {self.run_date}: {np.round((end-start)/60,2)} minutes."
        )

        return sdr_matched_output

    def publish(self):
        start = time.time()
        already_matched_rfq_ids = self.get_existing_matched_request_ids()["sym"].unique()
        matched_output = self.get_matched_rfq_sdr_data(
            request_ids_to_exclude=already_matched_rfq_ids,
        )

        self.matched_df = matched_output.match_output_including_parent_level_data
        publisher = PublishSdrOutputToKdb(
            currency=self.currency,
            matched_df=self.matched_df,
            env_type=self.env,
            publish_only_matched_trades=True,
        )
        publisher.publish_data()
        end = time.time()
        logging.info(f"Published SDR/RFQ matched output. Time taken: {(end-start)/60} minutes.")