import pandas as pd

def filter_by_time_window(df: pd.DataFrame, time_col: str, ref_time: pd.Timestamp, back_mins: int, fwd_mins: int) -> pd.DataFrame:
    """Filter public trades within a specific timeframe around the internal RFQ time."""
    start_time = ref_time - pd.Timedelta(minutes=back_mins)
    end_time = ref_time + pd.Timedelta(minutes=fwd_mins)
    return df[(df[time_col] >= start_time) & (df[time_col] <= end_time)]

def filter_by_exact_notional(df: pd.DataFrame, notional_col: str, ref_notional: float, tolerance: float) -> pd.DataFrame:
    """Filter based on exact notional amount, accounting for floating point precision."""
    return df[(df[notional_col] >= ref_notional - tolerance) & (df[notional_col] <= ref_notional + tolerance)]

def filter_by_maturity_year(df: pd.DataFrame, expiry_year_col: str, ref_maturity_date: pd.Timestamp) -> pd.DataFrame:
    """Ensure the public trade's derivative expiry year matches the RFQ maturity year."""
    ref_year = pd.to_datetime(ref_maturity_date).year
    return df[df[expiry_year_col] == ref_year]

def tie_breaking_closest_time(df: pd.DataFrame, time_col: str, ref_time: pd.Timestamp) -> pd.DataFrame:
    """Tie-breaker: Select the public trade closest in time to the RFQ internal time."""
    time_diffs = abs((df[time_col] - ref_time).dt.total_seconds())
    return df[time_diffs == time_diffs.min()].head(1)