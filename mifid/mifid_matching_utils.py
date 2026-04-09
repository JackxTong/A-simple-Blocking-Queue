import pandas as pd

def filter_mifid_by_time_window(
    df_mifid: pd.DataFrame, 
    time_col: str, 
    ref_time: pd.Timestamp, 
    backward_mins: int, 
    forward_mins: int
) -> pd.DataFrame:
    """Filter MiFID public trades within a specific timeframe around the internal RFQ time."""
    start_time = ref_time - pd.Timedelta(minutes=backward_mins)
    end_time = ref_time + pd.Timedelta(minutes=forward_mins)
    return df_mifid[(df_mifid[time_col] >= start_time) & (df_mifid[time_col] <= end_time)]


def filter_mifid_by_exact_notional(
    df_mifid: pd.DataFrame, 
    notional_col: str, 
    ref_notional: float, 
    tolerance: float
) -> pd.DataFrame:
    """Filter based on exact notional amount, accounting for floating point precision."""
    return df_mifid[
        (df_mifid[notional_col] >= ref_notional - tolerance) & 
        (df_mifid[notional_col] <= ref_notional + tolerance)
    ]


def filter_mifid_by_maturity_year(
    df_mifid: pd.DataFrame, 
    expiry_col: str, 
    ref_maturity_date: pd.Timestamp
) -> pd.DataFrame:
    """Ensure the MiFID trade's derivative expiry year matches the RFQ maturity year."""
    ref_year = pd.to_datetime(ref_maturity_date).year
    df_filtered = df_mifid.copy()
    df_filtered['Drv Expiry Year'] = pd.to_datetime(df_filtered[expiry_col]).dt.year
    return df_filtered[df_filtered['Drv Expiry Year'] == ref_year]


def tie_breaking_closest_time(
    df_mifid: pd.DataFrame, 
    time_col: str, 
    ref_time: pd.Timestamp
) -> pd.DataFrame:
    """Tie-breaker: Select the MiFID trade closest in time to the RFQ internal time."""
    df_mifid = df_mifid.copy()
    time_diffs = abs((df_mifid[time_col] - ref_time).dt.total_seconds())
    min_diff = time_diffs.min()
    # Return the closest match (if tied on exact time, pick the first one)
    return df_mifid[time_diffs == min_diff].head(1)