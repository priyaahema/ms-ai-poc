import pandas as pd
import numpy as np

class DataProcessor:
    def __init__(self):
        pass

    @staticmethod
    def convert_categorical_to_numeric(df: pd.DataFrame,severity_mapping,impact_mapping,u_event_severity_mapping) -> pd.DataFrame:
        """Convert categorical severity and impact to numeric scores."""
        # Define the mappings for severity and impact


        # Convert all the relevant columns to lowercase to ensure proper mapping
        df['severity'] = df['severity'].str.lower()
        df['impact'] = df['impact'].str.lower()

        # Map the categorical values to numeric scores using the dictionaries
        df['uevent_severity_score'] = df['u_event_severity'].map(u_event_severity_mapping).fillna(0)
        df['severity_score'] = df['severity'].map(severity_mapping).fillna(0)
        df['impact_score'] = df['impact'].map(impact_mapping).fillna(0)

        return df

    @staticmethod
    def aggregate_incident_data(hw_incidents: pd.DataFrame) -> pd.DataFrame:
        """Aggregate incident metrics."""
        return hw_incidents.groupby('hardware_asset_id').agg({
            'number': 'count',  # Incident count
            'severity_score': 'mean',  # Average severity score
            'impact_score': 'mean',
            'uevent_severity_score': 'mean',
        }).reset_index().rename(columns={
            'number': 'incident_count',
        })

    @staticmethod
    def aggregate_maintenance_data(patchupgrades: pd.DataFrame) -> pd.DataFrame:
        """Aggregate maintenance metrics."""
        return patchupgrades.groupby('hardware_asset_id').agg({
            'maintenance_score': 'mean'
        }).reset_index()

    @staticmethod
    def merge_data(hw_server: pd.DataFrame, usage_metrics: pd.DataFrame, incident_metrics: pd.DataFrame,
                   maintenance_metrics: pd.DataFrame, warranty_data: pd.DataFrame,vuln_summary: pd.DataFrame) -> pd.DataFrame:
        """Merge all data using full outer join."""
        vuln_summary.rename(columns={'asset_id': 'hardware_asset_id'}, inplace=True)

        merged_data = (
            hw_server[['hardware_asset_id']]
            .merge(usage_metrics, on='hardware_asset_id', how='left')
            .merge(incident_metrics, on='hardware_asset_id', how='left')
            .merge(maintenance_metrics, on='hardware_asset_id', how='left')
            .merge(warranty_data, on='hardware_asset_id', how='left')
            .merge(vuln_summary,on='hardware_asset_id', how='left')
        )
        return merged_data


    @staticmethod
    def add_company_names(merged_data: pd.DataFrame, hw_server: pd.DataFrame) -> pd.DataFrame:
        """Add company names to merged data from server and incident files."""
        server_company_mapping = hw_server[['hardware_asset_id', 'company']].drop_duplicates()
        
        # Convert to dictionaries
        server_company_dict = dict(zip(server_company_mapping['hardware_asset_id'], server_company_mapping['company']))


        # Add company names from server mapping
        merged_data['company'] = merged_data['hardware_asset_id'].map(server_company_dict)
        
        return merged_data

    @staticmethod
    def handle_missing_values(merged_data: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in merged data."""
        # Define the columns to handle missing values
        columns_to_fill = [
            'w_cpu_usage',
            'w_memory_usage',
            'w_disk_usage',
            'w_network_bandwidth',
            'overall_usage_score',
            'maintenance_score',
            'overall_maintenance_score',
            'incident_count',
            'impact_score',
            'severity_score',
            'overall_incident_score',
            'vulnerability_count',
            'vulnerability_severity_score',
            'vulnerability_patchReleased_score',
            'vulnerability_status_score',
            'vulnerability_detectedAge_score',
            'vulnerability_detectedTimes_score',
            'vulnerability_patch_score',
            'vulnerability_score',
            'overall_vulnerability_score',
        ]

        # Fill missing values with zero for the specified columns
        for column in columns_to_fill:
            merged_data[column].fillna(0, inplace=True)

        return merged_data
    
    @staticmethod
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
    
    def assign_random_values_for_missing_values(self,df: pd.DataFrame):

        df = self.assign_random_values_to_existing_column(df, 'end_of_life_date', '2021-01-01', '2024-12-31', is_date=True)
        df = self.assign_random_values_to_existing_column(df, 'end_of_sale_date', '2021-01-01', '2023-12-31', is_date=True)
        df = self.assign_random_values_to_existing_column(df, 'end_of_support_date', '2021-01-01', '2023-12-31', is_date=True)
        df = self.assign_random_values_to_existing_column(df, 'end_of_extended_support_date', '2021-01-01', '2023-12-31', is_date=True)

        return df
