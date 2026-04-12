import logging
import numpy as np
import pandas as pd

from swaps_analytics.irs.sdr_matching.sdr_constants import (
    DataMatchingConfiguration as dmconfig,
    EndReason,
    RfqColumns,
    SdrColumns,
    MatchAttributes,
)

def filter_dataframe_based_on_feature_values(
    data_to_be_filtered: pd.DataFrame, reference_data: pd.DataFrame, features_to_match: list
) -> pd.DataFrame:
    """
    Function used to filter a dataFrame based on certain features/column values. The reference dataframe consists of
    only one row of data

    :param data_to_be_filtered:     data to be filtered
    :param reference_data:           data to be used as reference for filtering
    :param features_to_match:       List of features to be used to filter the dataFrame
    :return df_to_filter:           filtered dataFrame
    """

    for feature in features_to_match:
        data_to_be_filtered = data_to_be_filtered[data_to_be_filtered[feature] == reference_data[feature].values[0]]
    return data_to_be_filtered

def filter_dataframe_based_on_price(
    data_to_be_filtered: pd.DataFrame, price_column: str, reference_price: float, reference_dv01: float = None
) -> pd.DataFrame:
    """
    Function used to filter a dataFrame based on price. The reference price is used with a certain precision for
    matching entries in the two tables

    :param data_to_be_filtered:     data to be filtered
    :param price_column:            column name containing the price
    :param reference_price:         reference price to be used for filtering the dataFrame
    :param reference_dv01:          compute price tolerance = bps tolerance * dv01
    :return data_to_be_filtered:    filtered dataFrame
    """

    if (price_column in [RfqColumns.parentQuotePrice.value, SdrColumns.PackageTransactionPrice.value]) & (
        reference_dv01 is not None
    ):
        # NPV quoted trades, price can be negative
        price1 = abs(reference_price) - abs(reference_dv01) * dmconfig.PRICE_PRECISION * 100
        price2 = abs(reference_price) + abs(reference_dv01) * dmconfig.PRICE_PRECISION * 100
    elif price_column in [RfqColumns.legQuotePrice.value, SdrColumns.sdrLegPrice.value]:
        price1 = reference_price - dmconfig.PRICE_PRECISION
        price2 = reference_price + dmconfig.PRICE_PRECISION
    elif (price_column in [RfqColumns.parentQuotePrice.value, SdrColumns.PackageTransactionPrice.value]) & (
        reference_dv01 is None
    ):
        # no filter
        price1 = 0.0
        price2 = 10e20

    min_price = np.minimum(price1, price2) - 0.0000001
    max_price = np.maximum(price1, price2) + 0.0000001
    return data_to_be_filtered.loc[abs(data_to_be_filtered[price_column]).between(min_price, max_price)]

def filter_dataframe_based_on_notional(
    data_to_be_filtered: pd.DataFrame, notional_column: str, reference_notional: float
) -> pd.DataFrame:
    notional1 = reference_notional * (1 - dmconfig.NOTIONAL_PRECISION)
    notional2 = reference_notional * (1 + dmconfig.NOTIONAL_PRECISION)
    return data_to_be_filtered.loc[data_to_be_filtered[notional_column].between(notional1, notional2)]

def filter_dataframe_based_on_regulatory_scope(
    data_to_be_filtered: pd.DataFrame,
    reference_regulatory_scope: str = None,
    reference_execution_venue: str = None,
    regulatory_scope_column_name: str = RfqColumns.regulatoryScope.value,
    execution_venue_column_name: str = SdrColumns.ExecutionVenue.value,
) -> pd.DataFrame:
    """
    Function used to filter a dataFrame based on regulatory scope/execution venue. If reference value of
    regulatory scope is provided, filter based on execution venue and vice versa.
    As of 2024-01, this function is unused because we no longer have ExecutionVenue (ON/OFF) column in SDR table. Also MTF trades are reported.

    :param data_to_be_filtered:            data to be filtered
    :param reference_regulatory_scope:      reference value of regulatory scope to be used for filtering
    :param reference_execution_venue:       reference value of execution venue to be used for filtering
    :param regulatory_scope_column_name:    column name containing the regulatory scope
    :param execution_venue_column_name:     column name containing the execution venue
    """

    # filter data based on execution venue
    if reference_regulatory_scope is None and reference_execution_venue is not None:
        return data_to_be_filtered[
            data_to_be_filtered[regulatory_scope_column_name]
            == dmconfig.REGULATORY_SCOPE_AND_EXECUTION_VENUE_MAP_REV[reference_execution_venue]
        ]
    
    # filter data based on regulatory scope
    elif reference_regulatory_scope is not None and reference_execution_venue is None:
        return data_to_be_filtered[
            data_to_be_filtered[execution_venue_column_name]
            == dmconfig.REGULATORY_SCOPE_AND_EXECUTION_VENUE_MAP[reference_regulatory_scope]
        ]
    else:
        logging.ERROR("Error while filtering based on regulatory scope/execution venue")

def sdr_data_has_identical_legs(dfx: pd.DataFrame) -> bool:
    """
    This is used to verify if the all the legs are identical in the input dataFrame. This is useful for multi-legged
    RFQs where a few legs have identical features
    :param dfx:      dataFrame containing various features of the swap leg
    :return:         boolean whether all legs are identical or not
    """

    # A few columns have been dropped as they could vary for identical legs. For example, PaymentFrequency1 and
    # PaymentFrequency2 could be reversed. Price and dissemination Id has been dropped as it is not a leg based feature
    dfx = dfx.drop(
        columns=[
            SdrColumns.sdrLegPrice.value,
            SdrColumns.PaymentFrequency1.value,
            SdrColumns.PaymentFrequency2.value,
            SdrColumns.ResetFrequency1.value,
            SdrColumns.ResetFrequency2.value,
            SdrColumns.DisseminationId.value,
            SdrColumns.PriceNotation.value,
            SdrColumns.AdditionalPriceNotation.value,
        ]
    )

    return len(dfx.drop_duplicates()) == 1

def rfq_data_has_identical_legs(dfx: pd.DataFrame, feature_columns: list) -> bool:
    """
    This is used to verify if the all the legs are identical in the input dataFrame. This is useful for multi-legged
    RFQs where a few legs have identical features
    :param dfx:               dataFrame containing various features of the swap leg
    :param feature_columns:   column names of RFQ table containing leg features
    :return:                  boolean whether all legs are identical or not
    """

    return len(dfx.drop_duplicates(subset=feature_columns)) == 1

def get_identical_leg_indices_of_rfq(dfx: pd.DataFrame, leg_index_reference: int, feature_columns: list) -> list:
    """
    Get indices of other legs of the RFQ which are identical to the reference leg index
    :param dfx:                  data of various legs of an RFQ
    :param leg_index_reference:  reference leg index
    :param feature_columns:      column names of RFQ table containing leg features
    :return:                     indices of legs which are identical to reference leg index
    """

    list_identical_legs = dfx.groupby(feature_columns)[RfqColumns.legIndex.value].unique()

    return list([k for k in list_identical_legs if leg_index_reference in k][0])

def tie_breaking_logic(reference_time: pd.Timedelta, dfx: pd.DataFrame, time_column: str) -> pd.DataFrame:
    """
    This function acts as a tie-breaker while filtering entries of a table, where we choose the entry
    for which the quote/execution time is closest to the reference time
    :param reference_time:    time for which the closest entry would be chosen
    :param dfx:               data which needs to be filtered based on tie-break logic
    :param time_column:       column containing the time
    :return df_matched_output:          dataFrame containing the output
    """

    ind_min = pd.Series.idxmin(abs((reference_time - dfx[time_column]).dt.total_seconds()))
    return dfx[dfx.index == ind_min]

def derive_parent_price_from_leg_prices_for_rate_quoted_swaps(
    df: pd.DataFrame, leg_price_column: str, parent_price_column: str
) -> pd.DataFrame:
    """
    Derive parent quote price from leg quote prices for curves and flies
    :param df:                    DataFrame containing the leg quote prices
    :param leg_price_column:      name of column containing the leg quote price
    :param parent_price_column:   name of column containing the parent quote price
    :return:                      DataFrame containing leg quote price and parent quote price
    """

    df = df.set_index(RfqColumns.requestId.value)

    # calculation of parent price for a switch/curve
    # the SDR matching algorithm is currently set up for swaps only, and hence excludes swap spreads which are also
    # 2 legged instruments
    df_2_leg_rate_quoted = df.loc[
        ((df[RfqColumns.numLegs.value] == 2) & (df[RfqColumns.legPricingConvention.value] == "RateQuoted"))
    ]
    if df_2_leg_rate_quoted.shape[0] > 0:
        df.loc[
            ((df[RfqColumns.numLegs.value] == 2) & (df[RfqColumns.legPricingConvention.value] == "RateQuoted")),
            parent_price_column,
        ] = df_2_leg_rate_quoted.groupby(RfqColumns.requestId.value).apply(
            lambda x: x.loc[x[RfqColumns.legIndex.value] == 1, leg_price_column].values[0]
            - x.loc[x[RfqColumns.legIndex.value] == 0, leg_price_column].values[0]
        )

    # calculation of parent price for a fly
    df_3_leg_rate_quoted = df.loc[
        ((df[RfqColumns.numLegs.value] == 3) & (df[RfqColumns.legPricingConvention.value] == "RateQuoted"))
    ]
    if df_3_leg_rate_quoted.shape[0] > 0:
        df.loc[
            ((df[RfqColumns.numLegs.value] == 3) & (df[RfqColumns.legPricingConvention.value] == "RateQuoted")),
            parent_price_column,
        ] = df_3_leg_rate_quoted.groupby(RfqColumns.requestId.value).apply(
            lambda x: (
                (2 * x.loc[x[RfqColumns.legIndex.value] == 1, leg_price_column].values[0])
                - x.loc[x[RfqColumns.legIndex.value] == 0, leg_price_column].values[0]
                - x.loc[x[RfqColumns.legIndex.value] == 2, leg_price_column].values[0]
            )
        )

    return df.reset_index()
    
def derive_parent_dealer_side(
    df: pd.DataFrame, leg_dealer_side_column: str, parent_dealer_side_column: str
) -> pd.DataFrame:
    """
    Derive parent dealer side from leg dealer side for curves and flies
    :param df:                         DataFrame containing the leg level data
    :param leg_dealer_side_column:     name of column containing the leg dealer side
    :param parent_dealer_side_column:  name of column containing the parent dealer side
    :return:                            DataFrame containing leg dealer side and parent dealer side
    """
    df = df.set_index(RfqColumns.requestId.value)

    # parent dealer side for curve and fly is the one with leg index 1