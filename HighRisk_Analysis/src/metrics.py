import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class MetricsCalculator:
    @staticmethod
    def calculate_stability_scores(merged_data: pd.DataFrame,weights_composite_stability_score) -> pd.DataFrame:
        """Calculate the overall stability scores and categorize assets."""

        # Calculate composite stability score using weighted averages
        merged_data['composite_stability_score'] = (
            (merged_data['n_usage_score'] * weights_composite_stability_score['overall_usage_score']) +
            (merged_data['overall_incident_score'] * weights_composite_stability_score['overall_incident_score']) +
            (merged_data['overall_maintenance_score'] * weights_composite_stability_score['maintenance_score']) +
            (merged_data['overall_vulnerability_score'] * weights_composite_stability_score['vulnerability_score'])
        )
        
        return merged_data
    
    @staticmethod
    def summarize_risk_categories(complete_df):
        # Filter DataFrames for each risk category
        high_risk_df = complete_df[complete_df['risk_category'] == 'High Risk']
        moderate_risk_df = complete_df[complete_df['risk_category'] == 'Moderate Risk']
        low_risk_df = complete_df[complete_df['risk_category'] == 'Low Risk']

        # Count the total number of assets in each risk category
        high_risk_count = high_risk_df.shape[0]
        moderate_risk_count = moderate_risk_df.shape[0]
        low_risk_count = low_risk_df.shape[0]

        # Create a summary DataFrame
        summary_data = {
            'Risk Category': ['High Risk', 'Moderate Risk', 'Low Risk'],
            'Total Assets': [high_risk_count, moderate_risk_count, low_risk_count]
        }

        summary_df = pd.DataFrame(summary_data)

        return summary_df
    
    @staticmethod
    def display_metrics_summary(merged_data: pd.DataFrame) -> tuple:
        """Create separate summary statistics tables for Usage, Incident, and Maintenance Metrics."""

        # Define the relevant columns for each category
        usage_columns = ['w_cpu_usage', 'w_memory_usage', 'w_disk_usage', 'w_network_bandwidth', 'overall_usage_score','n_usage_score']
        incident_columns = ['incident_count', 'severity_score', 'impact_score', 'incident_score', 'overall_incident_score']
        maintenance_columns = ['maintenance_score','overall_maintenance_score']

        # Create summary statistics DataFrames
        usage_summary = merged_data[usage_columns].describe().T.round(3)
        incident_summary = merged_data[incident_columns].describe().T.round(3)
        maintenance_summary = merged_data[maintenance_columns].describe().T.round(3)

        return usage_summary, incident_summary, maintenance_summary
    
    @staticmethod
    def get_top_10_high_risk_servers_summary(merged_data):

        current_date = datetime.now()
        one_month_from_now = current_date + timedelta(days=30)

        merged_data['end_of_life_date'] = pd.to_datetime(merged_data['end_of_life_date'], errors='coerce')

        # Filter data into three categories
        expired_servers = merged_data[(merged_data['risk_category'] == 'High Risk') & (merged_data['end_of_life_date'] < current_date)]
        expiring_soon_servers = merged_data[(merged_data['risk_category'] == 'High Risk') & (merged_data['end_of_life_date'] >= current_date) & (merged_data['end_of_life_date'] <= one_month_from_now)]
        expiring_later_servers = merged_data[(merged_data['risk_category'] == 'High Risk') & (merged_data['end_of_life_date'] > one_month_from_now)]

        # Group and count the assets by company for each category
        expired_count = expired_servers.groupby('company')['hardware_asset_id'].count().reset_index(name='expired')
        expiring_soon_count = expiring_soon_servers.groupby('company')['hardware_asset_id'].count().reset_index(name='expiring_soon')
        expiring_later_count = expiring_later_servers.groupby('company')['hardware_asset_id'].count().reset_index(name='expiring_later')

        # Merge all the counts into a single DataFrame, fill NaN values with 0
        summary = pd.merge(expired_count, expiring_soon_count, on='company', how='outer')
        summary = pd.merge(summary, expiring_later_count, on='company', how='outer')
        summary = summary.fillna(0)

        # Add a total asset count column
        summary['total_asset_count'] = summary['expired'] + summary['expiring_soon'] + summary['expiring_later']

        # Reorder columns to have 'company', 'total_asset_count', 'expired', 'expiring_soon', 'expiring_later'
        summary = summary[['company', 'total_asset_count', 'expired', 'expiring_soon', 'expiring_later']]

        # Convert counts to integer for better readability
        summary[['total_asset_count', 'expired', 'expiring_soon', 'expiring_later']] = summary[['total_asset_count', 'expired', 'expiring_soon', 'expiring_later']].astype(int)

        # Sort the DataFrame by total asset count in descending order
        summary = summary.sort_values(by='total_asset_count', ascending=False)

        summary = summary.rename(columns={
        'total_asset_count': 'Total Asset Count',
        'expired': 'Expired Assets',
        'expiring_soon': 'Expiring Soon Assets',
        'expiring_later': 'Expiring Later Assets'
        })

        # Return the top 10 companies by total asset count
        return summary.head(10)
    

class RiskCategorizer:

    @staticmethod
    def categorize_asset_risk(df):
        # Ensure the composite stability score is numeric
        df['composite_stability_score'] = pd.to_numeric(df['composite_stability_score'], errors='coerce')

        # Calculate Z-scores for 'composite_stability_score'
        df['zscore_composite_stability'] = (df['composite_stability_score'] - df['composite_stability_score'].mean()) / df['composite_stability_score'].std()

        # Categorize the assets into risk groups based on Z-scores
        conditions = [
            (df['zscore_composite_stability'] > 1.0),  # High Risk
            (df['zscore_composite_stability'] > 0) & (df['zscore_composite_stability'] <= 1.0),  # Moderate Risk
            (df['zscore_composite_stability'] <= 0)     # Low Risk
        ]

        # Define the corresponding risk labels
        risk_labels = ['High Risk', 'Moderate Risk', 'Low Risk']

        # Create a new column 'risk_category' based on the conditions
        df['risk_category'] = np.select(conditions, risk_labels, default='Unknown')

        # Get the count of each risk category
        risk_counts = df['risk_category'].value_counts()
        print(risk_counts)
        return df