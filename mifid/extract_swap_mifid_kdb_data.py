import dsp_kx as kx
import logging
import numpy as np
import pandas as pd
from datetime import datetime

from swaps_analytics.core.constants import AssetClass, EnvType
from swaps_analytics.core.currencies import CurrencyEnum
from swaps_analytics.core.databases.connection_utils import ConnectionDetailsBuilder
from swaps_analytics.core.utils.enum_utils import currency_to_region
from swaps_analytics.irs.mifid_matching.mifid_constants import RfqColumns
from swaps_analytics.irs.mifid_matching.mifid_constants import *

class ExtractSwapMifidDataFromKdb:
    def __init__(self, start_date: datetime.date, end_date: datetime.date, currency: CurrencyEnum, env: EnvType):
        """
        This class is used to fetch desired data (RFQ and mifid) from KDB for given dates
        :param start_date:    start date of the period for which data is to be fetched
        :param end_date:      end date of the period for which data is to be fetched
        """
        self.start_date = start_date
        self.end_date = end_date
        self.gateway_connection = ConnectionDetailsBuilder(AssetClass.RATES).set_env_type(env).build()
        self.currency = currency
        self.region = currency_to_region.get(currency)
        
        self.CONSTRAINT_PREFIX = {
            CurrencyEnum.EUR: "EUR*",
            CurrencyEnum.GBP: "GBP*",
            CurrencyEnum.USD: "USD-SOFR*",
        }

        self.LEG_CONSTRAINT_PREFIX = {
            CurrencyEnum.EUR: "*InterestRateSwap/EUR*",
            CurrencyEnum.GBP: "*InterestRateSwap/GBP*",
            CurrencyEnum.USD: "*SOFR*",
        }

    def _get_rfq_query(self) -> str:
        table_function_name = ".broadwaymetrics." + self.region + ".RFQ"
        constraint = f'.constr.FIELDLIKE[`legInstrumentType; "{self.LEG_CONSTRAINT_PREFIX[self.currency]}"]'
        
        query = (
            """
            {[rates;startDate;endDate]
                data:
                    rates[?;
            """
            + table_function_name
            + """
                        .constr.WITHIN[`date; (startDate; endDate)],
                        .constr.FIELDLIKE[`market; "*IRS*"],
            """
            + constraint
            + """
                        .constr.FBY[=;`revisionNumber;max;`requestId];
                        .grp.FIELD[`requestId`legIndex];
                        .fstat.FIELDLASTAS[`date; `tradeDate],
                        .fstat.FIELDLAST
                        [`numLegs`requestType`enquiryType`legInstrumentType`regulatoryScope`legInstrumentMaturityDate`endReason`legSize],
                        .fstat.FIELDLASTAS[("p$"; `sourceTimestamp); `sourceTimestamp]);
                data: `requestId xkey select from data where numLegs = (count;legIndex) fby requestId
            }"""
        )
        return query

    def get_rfq_data_from_kdb(self) -> pd.DataFrame:
        query = self._get_rfq_query()
        df = kx.q(
            query,
            self.gateway_connection,
            self.start_date,
            self.end_date,
        )
        df = df.pd().reset_index()
        for col in [RfqColumns.requestId.value]:
            df[col] = df[col].str.decode("utf-8")
            
        num_sef = df.loc[df["regulatoryScope"] == "SEF"].shape[0]
        num_mtf = df.loc[df["regulatoryScope"] == "MTF"].shape[0]
        
        logging.info(
            f"There are {df.shape[0]} trades (legs) in RFQ between {self.start_date}-{self.end_date}. num SEF={num_sef}, num MTF={num_mtf}"
        )
        return df

    def _get_mifid_query(self):
        query_mifid = ("""
            {[rates;startDate;endDate]
                startDateTime: "p"$startDate;
                endDateTime: "p"$(endDate + 1);
                data:
                    rates[?;
                        `.marketinsights.slough.propellantTrades;
                        .constr.WITHIN[`date; (startDate; endDate + 30)],
                        .constr.WITHIN[`postTradeDateTime; (startDateTime; endDateTime)],
                        .constr.FIELDLIKE[`cfiCode; "SR*"];
                        0b;
                        .istat.FIELD
                        [`date`tradingDateTime`postTradeDateTime`notionalAmount`drvExpiryDate`price`notionalCurrency`source`instrumentFullN
                        ame`drvUnderlyingIndexName`cfiGroupName`cfiCode]
                    );
                data: select from data where (not null price)
            }""")
        return query_mifid

    def get_mifid_data_from_kdb(self) -> pd.DataFrame:
        query = self._get_mifid_query()
        df = kx.q(
            query,
            self.gateway_connection,
            self.start_date,
            self.end_date,
        ).pd()
        
        for col in [MifidColumns.instrumentFullName.value]:
            df[col] = df[col].str.decode("utf-8")
            
        df = df[df['notionalCurrency'] == self.currency.value]
        cleaned_df = df[df['drvExpiryDate'].notna()]
        
        logging.info(
            f"There are {df.shape[0]} trades before cleaning. There are {cleaned_df.shape[0]} rows after removing NaN in drv expiry dates"
        )
        return cleaned_df