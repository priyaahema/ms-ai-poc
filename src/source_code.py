
code_text = """""Summarize the technical process used to analyze hardware asset risk with score and weightage details for each metrics ."import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import random
import seaborn as sns
from datetime import datetime, timedelta
from scipy.stats import zscore
from math import pi
import plotly.express as px

# Function to load datasets
def load_datasets():
    ""Load datasets from CSV files.""
    hw_server_usage = pd.read_csv('/content/hw_servers_usage_5.csv', encoding='ISO-8859-1')
    hw_incidents = pd.read_csv('/content/hw_incidents_5.csv', encoding='ISO-8859-1')
    patchupgrades = pd.read_csv('/content/patchupgrades.csv', encoding='ISO-8859-1')
    hw_warrant = pd.read_csv('/content/hw_warranty_5.csv', encoding='ISO-8859-1')
    hw_server = pd.read_csv('/content/hw_servers_5.csv', encoding='ISO-8859-1')
    return hw_server_usage, hw_incidents, patchupgrades, hw_warrant, hw_server

# Function to convert categorical columns to numeric
def convert_categorical_to_numeric(df: pd.DataFrame) -> pd.DataFrame:
    ""Convert categorical severity and impact to numeric scores.""
    # Define the mappings for severity and impact
    severity_mapping = {'1 - high': 10, 'high': 10, '2 - medium': 4, 'medium': 4, '3 - low': 0.5, 'low': 0.5}
    impact_mapping = {'1 - high': 10, 'high': 10, '2 - medium': 4, 'medium': 4, '3 - low': 0.5, 'low': 0.5}
    uevent_severity_mapping = {'CRITICAL': 3, 'MAJOR': 2, 'MINOR': 1}

    # Convert all the relevant columns to lowercase to ensure proper mapping
    # df['u_event_severity'] = df['u_event_severity'].str.lower()
    df['severity'] = df['severity'].str.lower()
    df['impact'] = df['impact'].str.lower()

    # Map the categorical values to numeric scores using the dictionaries
    df['uevent_severity_score'] = df['u_event_severity'].map(uevent_severity_mapping).fillna(0)
    df['severity_score'] = df['severity'].map(severity_mapping).fillna(0)
    df['impact_score'] = df['impact'].map(impact_mapping).fillna(0)

    return df

# Function to aggregate incident data
def aggregate_incident_data(hw_incidents: pd.DataFrame) -> pd.DataFrame:
    ""Aggregate incident metrics.""
    return hw_incidents.groupby('hardware_asset_id').agg({
        'number': 'count',  # Incident count
        'severity_score': 'mean',  # Average severity score
        'impact_score': 'mean',
        'uevent_severity_score':'mean',

    }).reset_index().rename(columns={
        'number': 'incident_count',
    })

# Function to aggregate maintenance data
def aggregate_maintenance_data(patchupgrades: pd.DataFrame) -> pd.DataFrame:
    ""Aggregate maintenance metrics.""
    return patchupgrades.groupby('hardware_asset_id').agg({
        'maintenance_score': 'mean'
    }).reset_index()

# Function to merge all datasets
def merge_data(hw_server: pd.DataFrame, usage_metrics: pd.DataFrame, incident_metrics: pd.DataFrame,
               maintenance_metrics: pd.DataFrame, warranty_data: pd.DataFrame) -> pd.DataFrame:
    ""Merge all data using full outer join.""
    merged_data = (
        hw_server[['hardware_asset_id']]
        .merge(usage_metrics, on='hardware_asset_id', how='outer')
        .merge(incident_metrics, on='hardware_asset_id', how='outer')
        .merge(maintenance_metrics, on='hardware_asset_id', how='outer')
        .merge(warranty_data, on='hardware_asset_id', how='outer')
    )
    return merged_data

# Function to add company names to the merged data
def add_company_names(merged_data: pd.DataFrame, hw_server: pd.DataFrame, hw_incidents: pd.DataFrame) -> pd.DataFrame:
    ""Add company names to merged data from server and incident files.""
    server_company_mapping = hw_server[['hardware_asset_id', 'company']].drop_duplicates()
    incident_company_mapping = hw_incidents[['hardware_asset_id', 'company']].drop_duplicates()

    # Convert to dictionaries
    server_company_dict = dict(zip(server_company_mapping['hardware_asset_id'], server_company_mapping['company']))
    incident_company_dict = dict(zip(incident_company_mapping['hardware_asset_id'], incident_company_mapping['company']))

    # Add company names from server mapping
    merged_data['company'] = merged_data['hardware_asset_id'].map(server_company_dict)

    # Fill in any missing company names from the incident mapping
    merged_data['company'] = merged_data['company'].combine_first(merged_data['hardware_asset_id'].map(incident_company_dict))

    return merged_data


# Function to handle missing values in merged data
def handle_missing_values(merged_data: pd.DataFrame) -> pd.DataFrame:
    ""Handle missing values in merged data.""
    # Define the columns to handle missing values
    columns_to_fill = [
        'w_cpu_usage',
        'w_memory_usage',
        'w_disk_usage',
        'w_network_bandwidth',
        'overall_usage_score',
        'uevent_severity_score',
        'severity_score',
        'impact_score',
        'maintenance_score',
    ]

    # Fill missing values with the mean of each column
    for column in columns_to_fill:
        mean_value = merged_data[column].mean()
        merged_data[column].fillna(mean_value, inplace=True)
    return merged_data

# Function to calculate min and max values for normalization
def calculate_minmax(df, columns):
    ""Calculate min and max for a given list of columns.""
    mins = df[columns].min().values
    maxs = df[columns].max().values
    return mins, maxs

# Function to normalize an array based on the provided min and max
def normalize_array(array, min_val, max_val):
    ""Normalize array values between 0 and 100.""
    if max_val > min_val:
        return (array - min_val) / (max_val - min_val) * 100
    else:
        return np.zeros_like(array)

# Function to calculate weighted usage scores for each asset
def calculate_weighted_usage_scores(df):
    ""Calculate weighted usage scores based on CPU, memory, IO wait, and network bandwidth.""
    grouped = df.groupby('hardware_asset_id')
    mins, maxs = calculate_minmax(df, ['Network Throughput (Mbps)'])

    # Define weights for each metric (ensure the sum of weights is 1)
    weights = np.array([0.35, 0.35, 0.2, 0.1])  # Weights for CPU, memory, IO, network

    # Initialize a list to store results
    results = []

    for asset_id, group in grouped:
        cpu_usage = group['CPU Usage (%)'].to_numpy()
        memory_usage = group['Memory Usage (%)'].to_numpy()
        disk_usage = group['Disk Usage (%)'].to_numpy()
        network_throughput = group['Network Throughput (Mbps)'].to_numpy()

        # Calculate weighted scores
        weighted_cpu = weights[0] * np.mean(normalize_array(cpu_usage,0,100))
        weighted_memory = weights[1] * np.mean(normalize_array(memory_usage,0,100))
        weighted_disk = weights[2] * np.mean(normalize_array(disk_usage,0,100))
        weighted_network = weights[3] * np.mean(normalize_array(network_throughput, mins[0], maxs[0]))

        # Compute the composite score
        composite_score = weighted_cpu + weighted_memory + weighted_disk + weighted_network

        # Append the result
        results.append({
            'hardware_asset_id': asset_id,
            'w_cpu_usage': weighted_cpu,
            'w_memory_usage': weighted_memory,
            'w_disk_usage': weighted_disk,
            'w_network_bandwidth': weighted_network,
            'overall_usage_score': composite_score
        })

    return pd.DataFrame(results)

# Function to incorporate the weighted usage scores into the merged data
def add_weighted_usage_scores(hw_server_usage):
    ""Add the weighted usage scores into the merged data.""
    weighted_scores_df = calculate_weighted_usage_scores(hw_server_usage)
    return weighted_scores_df

# Function to summarize metrics and save results to CSV
def summarize_and_save_assets(merged_data: pd.DataFrame) -> None:
    ""Summarize metrics and save complete list of assets.""
    # Save complete list of assets with relevant metrics
    dt = datetime.now().strftime('%Y%m%d')
    merged_data.to_csv(f'Servers_Stability_{dt}.csv', index=False)

def assign_random_values_to_existing_column(df, column_name, lower_bound, upper_bound, is_date=False):

    if column_name in df.columns:
        if is_date:
            # Convert the date strings to pandas datetime objects
            start_date = pd.to_datetime(lower_bound)
            end_date = pd.to_datetime(upper_bound)
            # Calculate the time delta in days
            date_range = (end_date - start_date).days
            # Assign random dates within the range
            df[column_name] = start_date + pd.to_timedelta(np.random.randint(0, date_range, size=len(df)), unit='D')
        else:
            # Assign random decimal values
            df[column_name] = np.random.uniform(lower_bound, upper_bound, size=len(df))
    else:
        raise ValueError(f"Column '{column_name}' does not exist in the DataFrame.")
    return df

def assign_random_values_for_missing_values(df):

    df = assign_random_values_to_existing_column(df, 'end_of_life_date', '2021-01-01', '2024-12-31', is_date=True)
    df = assign_random_values_to_existing_column(df, 'end_of_sale_date', '2021-01-01', '2023-12-31', is_date=True)
    df = assign_random_values_to_existing_column(df, 'end_of_support_date', '2021-01-01', '2023-12-31', is_date=True)
    df = assign_random_values_to_existing_column(df, 'end_of_extended_support_date', '2021-01-01', '2023-12-31', is_date=True)


def calculate_stability_scores(merged_data: pd.DataFrame) -> pd.DataFrame:
    ""Calculate the overall stability scores and categorize assets.""

    # Normalize severity and impact scores by their maximum values
    merged_data['severity_score'] = merged_data['severity_score'] / merged_data['severity_score'].max()
    merged_data['impact_score'] = merged_data['impact_score'] / merged_data['impact_score'].max()

    # Calculate incident score based on the incident count, severity, and impact scores
    merged_data['incident_score'] = (
        merged_data['incident_count'] *
        (merged_data['severity_score'] + merged_data['impact_score'])
    )

    # Handle cases where incident count is 0 by setting the incident score to 0
    merged_data.loc[merged_data['incident_count'] == 0, 'incident_score'] = 0

    # Calculate max incident score for normalization
    max_incident_score = merged_data['incident_score'].max()

    # Ensure division by zero is handled by checking if max_incident_score is greater than 0
    if max_incident_score > 0:
        merged_data['overall_incident_score'] = merged_data['incident_score']
    else:
        merged_data['overall_incident_score'] = 0

    # Calculate maintenance score (already aggregated)
    merged_data['maintenance_score'] = merged_data['maintenance_score']

    # Calculate composite stability score using weighted averages
    merged_data['composite_stability_score'] = (
        (merged_data['overall_usage_score'] * 0.3) +
        (merged_data['overall_incident_score'] * 0.4) +
        (merged_data['maintenance_score'] * 0.5)
    )

    # Categorize assets based on the composite stability score, with special handling for zero-incident assets
    # threshold = merged_data['composite_stability_score'].median()

    # merged_data['category'] = np.where(merged_data['composite_stability_score'] >= threshold, 'Stable', 'HighRisk')

    return merged_data

def categorize_asset_risk(df):

    # Calculate Z-scores for 'composite_stability_score'
    df['zscore_composite_stability'] = (df['composite_stability_score'] - df['composite_stability_score'].mean()) / df['composite_stability_score'].std()

    # Categorize the assets into risk groups based on Z-scores
    conditions = [
        (df['zscore_composite_stability'] > 1.0),          # High Risk
        (df['zscore_composite_stability'] > 0) & (df['zscore_composite_stability'] <= 1.0),  # Moderate Risk
        (df['zscore_composite_stability'] <= 0)            # Low Risk
    ]

    # Define the corresponding risk labels
    risk_labels = ['High Risk', 'Moderate Risk', 'Low Risk']

    # Create a new column 'risk_category' based on the conditions
    df['risk_category'] = np.select(conditions, risk_labels)

    # Get the count of each risk category
    risk_counts = df['risk_category'].value_counts()

    return df,df[['hardware_asset_id', 'composite_stability_score', 'zscore_composite_stability', 'risk_category']], risk_counts


def main():
    # Load datasets
    hw_server_usage, hw_incidents, patchupgrades, hw_warrant, hw_server = load_datasets()

    # # Process datasets
    hw_incidents= convert_categorical_to_numeric(hw_incidents)

    incident_metrics = aggregate_incident_data(hw_incidents)

    weighted_usage_scores = add_weighted_usage_scores(hw_server_usage)

    maintenance_metrics = aggregate_maintenance_data(patchupgrades)

    # # Merge datasets
    merged_data = merge_data(hw_server, weighted_usage_scores, incident_metrics, maintenance_metrics, hw_warrant)

    merged_data['incident_count'].fillna(0, inplace=True)

    # # # Handle missing values
    merged_data = handle_missing_values(merged_data)

    # # Add company names
    merged_data = add_company_names(merged_data, hw_server, hw_incidents)

    # # print(merged_data)

    # # # Calculate stability scores
    merged_data = calculate_stability_scores(merged_data)


    complete_df,categorized_df, risk_counts = categorize_asset_risk(merged_data)

# Run the main function
if __name__ == "__main__":
    main()



and don't include Example Workflow like that.
"""""