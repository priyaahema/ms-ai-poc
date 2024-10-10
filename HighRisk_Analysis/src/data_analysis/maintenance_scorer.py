class MaintenanceAnalyzer:
    def __init__(self, df):
        """Initialize with a DataFrame."""
        self.df = df

    def validate_columns(self):
        """Ensure necessary columns are present in the DataFrame."""
        required_columns = {'hardware_asset_id', 'maintenance_status', 'maintenance_score'}
        missing_columns = required_columns - set(self.df.columns)
        if missing_columns:
            raise ValueError(f"Missing columns in DataFrame: {missing_columns}")

    def calculate_overall_maintenance_score(self):
        """Calculate overall maintenance score for each row."""
        # Validate the DataFrame columns
        self.validate_columns()

        # Calculate the maximum maintenance score across the entire column
        max_score = self.df['maintenance_score'].max()

        # Calculate overall maintenance score as the ratio of each score to the maximum score
        self.df['overall_maintenance_score'] = self.df['maintenance_score'] / max_score

    def aggregate_by_hardware_asset(self):
        """Aggregate the scores by hardware_asset_id."""
        # Group by hardware_asset_id and aggregate the scores
        aggregated_df = self.df.groupby('hardware_asset_id').agg(
            maintenance_score=('maintenance_score', 'mean'),  # Average maintenance score
            overall_maintenance_score=('overall_maintenance_score', 'mean'),  # Average overall maintenance score
        ).reset_index()
        
        return aggregated_df

    def get_results(self):
        """Return the DataFrame with calculated maintenance scores."""
        return self.df
