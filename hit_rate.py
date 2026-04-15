import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def analyze_and_plot_hit_rates(df):
    """
    Calculates hit rates by request_type and enquiry_type, 
    and plots daily and monthly hit rates over time.
    """
    # 1. Data Preparation
    # Ensure datetime format and extract date/month components
    df = df.copy()
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['date'] = df['datetime'].dt.date
    df['month'] = df['datetime'].dt.to_period('M')

    # 2. Define the core Hit Rate calculation logic
    def calculate_hit_rate(sub_df):
        # Count unique request_ids for Barclays and Away (applies to all request types)
        b_count = sub_df[sub_df['end_reason'] == 'COUNTERPARTY_TRADED_WITH_BARCLAYS']['request_id'].nunique()
        a_count = sub_df[sub_df['end_reason'] == 'COUNTERPARTY_TRADED_AWAY']['request_id'].nunique()
        
        # Count unique request_ids for Rejected ONLY when request_type is 'RFM'
        r_rfm_count = sub_df[(sub_df['end_reason'] == 'CLIENT_REJECTED') & 
                             (sub_df['request_type'] == 'RFM')]['request_id'].nunique()
        
        # Apply the formula
        numerator = b_count
        denominator = b_count + a_count + (0.6 * r_rfm_count)
        
        return (numerator / denominator) if denominator > 0 else np.nan

    # 3. Calculate Hit Rates by Category
    hr_by_request = df.groupby('request_type').apply(calculate_hit_rate).rename('Hit Rate')
    hr_by_enquiry = df.groupby('enquiry_type').apply(calculate_hit_rate).rename('Hit Rate')
    
    print("--- Hit Rate by Request Type ---")
    print(hr_by_request.to_string())
    print("\n--- Hit Rate by Enquiry Type ---")
    print(hr_by_enquiry.to_string())
    print("-" * 32)

    # 4. Calculate Hit Rates over Time
    daily_hr = df.groupby('date').apply(calculate_hit_rate)
    monthly_hr = df.groupby('month').apply(calculate_hit_rate)

    # 5. Plotting
    fig, axes = plt.subplots(2, 1, figsize=(12, 10))

    # Daily Plot (Line chart to show trends and volatility)
    daily_hr.plot(ax=axes[0], color='#1f77b4', marker='o', markersize=4, linewidth=1.5, alpha=0.8)
    axes[0].set_title('Daily Hit Rate', fontsize=14, pad=10)
    axes[0].set_ylabel('Hit Rate')
    axes[0].set_xlabel('Date')
    axes[0].grid(True, linestyle='--', alpha=0.6)

    # Monthly Plot (Bar chart for aggregated performance)
    # Convert period index to string for cleaner x-axis labels on bar charts
    monthly_hr.index = monthly_hr.index.astype(str)
    monthly_hr.plot(kind='bar', ax=axes[1], color='#43a2ca', edgecolor='black', alpha=0.8)
    axes[1].set_title('Monthly Hit Rate', fontsize=14, pad=10)
    axes[1].set_ylabel('Hit Rate')
    axes[1].set_xlabel('Month')
    axes[1].tick_params(axis='x', rotation=45)
    axes[1].grid(axis='y', linestyle='--', alpha=0.6)

    plt.tight_layout()
    plt.show()

    return {
        'hr_by_request_type': hr_by_request,
        'hr_by_enquiry_type': hr_by_enquiry,
        'daily_hr': daily_hr,
        'monthly_hr': monthly_hr
    }

# Example execution:
# results = analyze_and_plot_hit_rates(df)