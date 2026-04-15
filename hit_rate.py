import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def analyze_and_plot_hit_rates(df):
    """
    Calculates hit rates by request_type, enquiry_type, and their combination,
    and plots daily hit rates, monthly hit rates, and combo hit rates.
    """
    # 1. Data Preparation
    df = df.copy()
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['date'] = df['datetime'].dt.date
    df['month'] = df['datetime'].dt.to_period('M')

    # 2. Define the core Hit Rate calculation logic
    def calculate_hit_rate(sub_df):
        # Count unique request_ids for Barclays and Away
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
    
    # Calculate combination Hit Rate (unstacking to make it ready for a grouped bar chart)
    hr_combo = df.groupby(['request_type', 'enquiry_type']).apply(calculate_hit_rate).unstack('enquiry_type')

    print("--- Hit Rate by Request Type ---")
    print(hr_by_request.to_string())
    print("\n--- Hit Rate by Enquiry Type ---")
    print(hr_by_enquiry.to_string())
    print("\n--- Hit Rate by Combo (Request x Enquiry) ---")
    print(hr_combo.to_string())
    print("-" * 32)

    # 4. Calculate Hit Rates over Time
    daily_hr = df.groupby('date').apply(calculate_hit_rate)
    monthly_hr = df.groupby('month').apply(calculate_hit_rate)

    # 5. Plotting (Now 3 subplots)
    fig, axes = plt.subplots(3, 1, figsize=(12, 15))

    # Plot A: Daily Hit Rate (Line chart)
    daily_hr.plot(ax=axes[0], color='#1f77b4', marker='o', markersize=4, linewidth=1.5, alpha=0.8)
    axes[0].set_title('Daily Hit Rate', fontsize=14, pad=10)
    axes[0].set_ylabel('Hit Rate')
    axes[0].set_xlabel('Date')
    axes[0].grid(True, linestyle='--', alpha=0.6)

    # Plot B: Monthly Hit Rate (Bar chart)
    monthly_hr.index = monthly_hr.index.astype(str) # String conversion for cleaner x-axis labels
    monthly_hr.plot(kind='bar', ax=axes[1], color='#43a2ca', edgecolor='black', alpha=0.8)
    axes[1].set_title('Monthly Hit Rate', fontsize=14, pad=10)
    axes[1].set_ylabel('Hit Rate')
    axes[1].set_xlabel('Month')
    axes[1].tick_params(axis='x', rotation=45)
    axes[1].grid(axis='y', linestyle='--', alpha=0.6)

    # Plot C: Combo Request vs Enquiry Type (Grouped Bar chart)
    # This automatically groups bars by request_type with differently colored bars for each enquiry_type
    hr_combo.plot(kind='bar', ax=axes[2], edgecolor='black', alpha=0.8)
    axes[2].set_title('Hit Rate by Combo: Request Type & Enquiry Type', fontsize=14, pad=10)
    axes[2].set_ylabel('Hit Rate')
    axes[2].set_xlabel('Request Type')
    axes[2].tick_params(axis='x', rotation=0)
    axes[2].legend(title='Enquiry Type')
    axes[2].grid(axis='y', linestyle='--', alpha=0.6)

    plt.tight_layout()
    plt.show()

    return {
        'hr_by_request_type': hr_by_request,
        'hr_by_enquiry_type': hr_by_enquiry,
        'hr_combo': hr_combo,
        'daily_hr': daily_hr,
        'monthly_hr': monthly_hr
    }
