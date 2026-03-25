import pandas as pd

def analyze_client_matching_accuracy(
    df: pd.DataFrame, 
    client_col: str = 'client', 
    req_id_col: str = 'requestId', 
    match_col: str = 'match'
) -> pd.DataFrame:
    """
    Analyzes matching accuracy per client on the final matched DataFrame.
    Calculates accuracy by individual legs and by unique RequestIds (entire package).
    """
    
    # Ensure the match column is treated as boolean for accurate summation
    df[match_col] = df[match_col].astype(bool)

    # ---------------------------------------------------------
    # 1. Calculate Accuracy by Number of Legs
    # ---------------------------------------------------------
    # Group by client to get the total number of leg rows and the sum of True matches
    leg_stats = df.groupby(client_col).agg(
        total_legs=(match_col, 'count'),
        matched_legs=(match_col, 'sum')
    ).reset_index()
    
    leg_stats['leg_match_accuracy_pct'] = (leg_stats['matched_legs'] / leg_stats['total_legs']) * 100

    # ---------------------------------------------------------
    # 2. Calculate Accuracy by Unique RequestId (Package Level)
    # ---------------------------------------------------------
    # First, group by client AND requestId. 
    # A package is only fully matched if ALL its constituent legs are matched.
    package_level = df.groupby([client_col, req_id_col]).agg(
        is_fully_matched=(match_col, 'all') # 'all' returns True only if every row in the group is True
    ).reset_index()

    # Next, group the package-level data by client to get the final RequestId stats
    req_stats = package_level.groupby(client_col).agg(
        total_request_ids=('is_fully_matched', 'count'),
        fully_matched_request_ids=('is_fully_matched', 'sum')
    ).reset_index()

    req_stats['request_id_match_accuracy_pct'] = (req_stats['fully_matched_request_ids'] / req_stats['total_request_ids']) * 100

    # ---------------------------------------------------------
    # 3. Merge and Format the Final Report
    # ---------------------------------------------------------
    accuracy_report = pd.merge(leg_stats, req_stats, on=client_col, how='inner')
    
    # Round percentages for clean reporting
    accuracy_report['leg_match_accuracy_pct'] = accuracy_report['leg_match_accuracy_pct'].round(2)
    accuracy_report['request_id_match_accuracy_pct'] = accuracy_report['request_id_match_accuracy_pct'].round(2)

    return accuracy_report

# ==========================================
# Example Execution
# ==========================================
# df_final = matched_table.match_output_including_parent_level_data
#
# Replace 'client_name_column' with the actual string name of your client column
# client_accuracy_df = analyze_client_matching_accuracy(
#     df=df_final, 
#     client_col='client_name_column', 
#     req_id_col='requestId',   # Update if RfqColumns.requestId.value resolves to something else
#     match_col='match'         # Update if MatchAttributes.MATCH.value resolves to something else
# )
# 
# print(client_accuracy_df)
