import logging

import numpy as np
import pandas as pd
from swaps_analytics.core.currencies import CurrencyEnum

from swaps_analytics.core.constants import AssetClass, EnvType
from swaps_analytics.core.publishing.columns import KdbColumn
from swaps_analytics.core.publishing.constants import (
    SupportedTripletForPublishing,
    get_supported_kdb_connection_for_publishing,
)
from swaps_analytics.core.publishing.kdb_triplet_data_publisher import KdbTripletDataPublisher
from swaps_analytics.core.publishing.rats_data_publisher import RatsDataPublisher
from swaps_analytics.core.regression_model.common.kdb_publisher_helper import KdbTableName
from swaps_analytics.irs.sdr_matching.sdr_constants import (
    DataMatchingConfiguration as dmconfig,
    RfqColumns,
    SdrColumns,
    ColumnUtils,
    MatchAttributes,
)

class SdrDataMatchingKdbColumn(KdbColumn):
    """
    This class defines the schema of the output which is to be published to RATS box
    """
    time = RfqColumns.time.value, "t"
    sym = SdrColumns.sym.value, "s"
    legIndex = RfqColumns.legIndex.value, "i"
    numLegs = RfqColumns.numLegs.value, "i"
    legQuotePrice = RfqColumns.legQuotePrice.value, "f"
    parentQuotePrice = RfqColumns.parentQuotePrice.value, "f"
    tradeDate = RfqColumns.date.value, "d"
    sdrExecutionTimestamp = SdrColumns.executionTimestamp.value, "p"
    DisseminationId = SdrColumns.disseminationId.value, "C"
    sdrSize = SdrColumns.sdrSize.value, "f"
    sdrLegPrice = SdrColumns.sdrLegPrice.value, "f"
    sdrParentPrice = SdrColumns.sdrParentPrice.value, "f"
    sdrPriceUnit = SdrColumns.sdrPriceUnit.value, "s"
    matchConfidence = ColumnUtils.matchConfidence.value, "s"
    modelVersion = ColumnUtils.modelVersion.value, "s"
    runDateTime = ColumnUtils.runDateTime.value, "p"

    def __init__(self, kdb_col: str, kdb_type_char: str):
        super().__init__(kdb_col=kdb_col, kdb_type_char=kdb_type_char)

    @staticmethod
    def get_kdb_column_names() -> list:
        return [
            RfqColumns.time.value,
            SdrColumns.sym.value,
            RfqColumns.legIndex.value,
            RfqColumns.numLegs.value,
            RfqColumns.legQuotePrice.value,
            RfqColumns.parentQuotePrice.value,
            RfqColumns.date.value,
            SdrColumns.executionTimestamp.value,
            SdrColumns.DisseminationId.value,
            SdrColumns.sdrSize.value,
            SdrColumns.sdrLegPrice.value,
            SdrColumns.sdrParentPrice.value,
            SdrColumns.sdrPriceUnit.value,
            ColumnUtils.matchConfidence.value,
            ColumnUtils.modelVersion.value,
            ColumnUtils.runDateTime.value,
        ]

def filter_out_unmatched_trades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter out requestId that we do not get a match for each of its legs.
    :param df: matched output
    """
    matched_rfq_ids = df.loc[df[MatchAttributes.MATCH.value] == True, RfqColumns.requestId.value].unique()
    df = df.loc[df[RfqColumns.requestId.value].isin(matched_rfq_ids)].reset_index(drop=True)
    return df

def prepare_data(matched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add required columns and cast empty string to correct data type.
    """
    matched_df[RfqColumns.time.value] = pd.Timestamp.utcnow().tz_localize(None)
    matched_df[SdrColumns.sym.value] = matched_df[RfqColumns.requestId.value]
    matched_df[ColumnUtils.runDateTime.value] = (
        pd.Timestamp.utcnow().floor(freq=dmconfig.REALTIME_PUBLICATION_FREQ).tz_localize(None)
    )
    matched_df[ColumnUtils.modelVersion.value] = dmconfig.MODEL_VERSION

    # cast empty string to correct type
    matched_df[SdrColumns.executionTimestamp.value] = matched_df[SdrColumns.executionTimestamp.value].replace(
        {"": pd.NaT}
    )
    for col in [SdrColumns.sdrSize.value, SdrColumns.sdrLegPrice.value, SdrColumns.sdrParentPrice.value]:
        matched_df[col] = matched_df[col].replace({"": np.nan})
        matched_df[col] = matched_df[col].astype(float)

    # cast time columns
    for col in [RfqColumns.time.value, ColumnUtils.runDateTime.value, SdrColumns.executionTimestamp.value]:
        matched_df[col] = matched_df[col].astype("datetime64[ns]")

    columns_to_publish = SdrDataMatchingKdbColumn.get_kdb_column_names()
    return matched_df[columns_to_publish]

def publish_sdr_output_rats(df_to_publish: pd.DataFrame, rats_port: int):
    """
    This class is used to publish the output of SDR data matching to the desired RATS port
    
    :param matched_df: attribute match_output_including_parent_level_data of SdrDataMatchingAlgorithm
    :param rats_port: port to publish the data
    :param publish_only_matched_trades: publish only matched trades
    """
    publisher_sdr_parent_level = RatsDataPublisher(
        ports=[rats_port],
        kdb_tbl_name=dmconfig.KDB_RATS_OUTPUT_TABLE_NAME,
        kdb_column=SdrDataMatchingKdbColumn,
        override_table_if_exists=True,
        load_table_if_not_exists=False,
        save_table_to_file=False,
    )

    publisher_sdr_leg_level = RatsDataPublisher(
        ports=[rats_port],
        kdb_tbl_name=dmconfig.KDB_RATS_OUTPUT_TABLE_NAME_LEG_LEVEL_DATA,
        kdb_column=SdrDataMatchingKdbColumn,
        override_table_if_exists=True,
        load_table_if_not_exists=False,
        save_table_to_file=False,
    )

    # publish leg level data to KDB
    publisher_sdr_leg_level.publish(df=df_to_publish)

    # publish parent level data to KDB
    publisher_sdr_parent_level.publish(df=df_to_publish.drop_duplicates(subset="sym", keep="last"))
    logging.info(
        f"Published to RATS:{rats_port}, tables: {dmconfig.KDB_RATS_OUTPUT_TABLE_NAME}, {dmconfig.KDB_RATS_OUTPUT_TABLE_NAME_LEG_LEVEL_DATA}"
    )

class PublishSdrOutputToKdb:
    """
    This function publishes matched leg level SDR/RFQ matched trades to RDB table.
    TODO: back-population of data to HDB
    """

    CURRENCY_TO_TRIPLET = {
        CurrencyEnum.GBP: SupportedTripletForPublishing.SWAPSTRADING_LDN,
        CurrencyEnum.EUR: SupportedTripletForPublishing.SWAPSTRADING_LDN,
        CurrencyEnum.USD: SupportedTripletForPublishing.SWAPSTRADING
    }

    def __init__(self, currency: CurrencyEnum, matched_df: pd.DataFrame, env_type: EnvType, publish_only_matched_trades: bool = True):
        self.currency = currency
        self.kdb_table_name = KdbTableName.MATCHED_SDR_RFQ_TRADES.value
        self.matched_df = matched_df
        self.env_type = env_type
        if publish_only_matched_trades:
            self.matched_df = filter_out_unmatched_trades(matched_df.copy())
        else:
            self.matched_df = matched_df

    def publish_data(self):
        df = prepare_data(self.matched_df)

        formatting_func_rdb = """{
            [df]
            records:
                update
                    time:"t"$time,
                    sym: `symbol$sym,
                    tradeDate: "d"$tradeDate,
                    sdrDisseminationId: string sdrDisseminationId,
                    sdrPriceUnit: `symbol$sdrPriceUnit,
                    matchConfidence: `symbol$matchConfidence,
                    modelVersion: `symbol$modelVersion
                from df;
            records: value flip records;
            records
        }"""

        triplet = self.CURRENCY_TO_TRIPLET.get(self.currency)
        if triplet is None:
            raise ValueError(f"Unsupported currency: {self.currency}")

        tp_gateway_connections_list = get_supported_kdb_connection_for_publishing(
            self.env_type, AssetClass.RATES, triplet
        )
        
        for tp_gateway_conn in tp_gateway_connections_list:
            publisher = KdbTripletDataPublisher(
                asset_class=AssetClass.RATES,
                kdb_tbl_name=self.kdb_table_name,
                tickerplant_gateway_connection=tp_gateway_conn,
            )
            publisher.publish(formatting_func_rdb=formatting_func_rdb, df=df)
            logging.info(
                f"Matched RFQ/SDR data published to table: {self.kdb_table_name}, connection={tp_gateway_conn}."
            )