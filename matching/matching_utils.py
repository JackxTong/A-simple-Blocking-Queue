import pandas as pd
import numpy as np
from public_trade_matching.constants import DataMatchingConfiguration as config

def filter_by_time_window(df: pd.DataFrame, time_col: str, ref_time: pd.Timestamp) -> pd.DataFrame:
    start_time = ref_time - pd.Timedelta(minutes=config.BACKWARD_MINS)
    end_time = ref_time + pd.Timedelta(minutes=config.FORWARD_MINS)
    return df[(df[time_col] >= start_time) & (df[time_col] <= end_time)]

def filter_dataframe_based_on_notional(df: pd.DataFrame, notional_col: str, ref_notional: float) -> pd.DataFrame:
    notional1 = ref_notional * (1 - config.NOTIONAL_PRECISION)
    notional2 = ref_notional * (1 + config.NOTIONAL_PRECISION)
    return df.loc[df[notional_col].between(notional1, notional2)]

def filter_dataframe_based_on_price(df: pd.DataFrame, price_column: str, reference_price: float, reference_dv01: float = None) -> pd.DataFrame:
    if reference_dv01 is not None:
        # NPV Quoted (DV01 logic from your original sdr code)
        price1 = abs(reference_price) - abs(reference_dv01) * config.PRICE_PRECISION * 100
        price2 = abs(reference_price) + abs(reference_dv01) * config.PRICE_PRECISION * 100
    else:
        # Rate Quoted
        price1 = reference_price - config.PRICE_PRECISION
        price2 = reference_price + config.PRICE_PRECISION

    min_price = np.minimum(price1, price2) - 0.0000001
    max_price = np.maximum(price1, price2) + 0.0000001
    return df.loc[abs(df[price_column]).between(min_price, max_price)]

def filter_mifid_by_maturity_year(df: pd.DataFrame, expiry_col: str, ref_maturity_date: pd.Timestamp) -> pd.DataFrame:
    ref_year = pd.to_datetime(ref_maturity_date).year
    df_filtered = df.copy()
    df_filtered['Drv Expiry Year'] = pd.to_datetime(df_filtered[expiry_col]).dt.year
    return df_filtered[df_filtered['Drv Expiry Year'] == ref_year]

def tie_breaking_closest_time(df: pd.DataFrame, time_col: str, ref_time: pd.Timestamp) -> pd.DataFrame:
    time_diffs = abs((df[time_col] - ref_time).dt.total_seconds())
    min_diff = time_diffs.min()
    return df[time_diffs == min_diff].head(1)
