import pandas as pd
import numpy as np

class UsageScorer:
    # Class attribute for weights
    WEIGHTS = np.array([0.35, 0.35, 0.2, 0.1])  # Ensure the sum of weights is 1

    def calculate_min_max(self, hw_server_usage, columns):
        """Calculate min and max for a given list of columns."""
        mins = hw_server_usage[columns].min().values
        maxs = hw_server_usage[columns].max().values
        return mins, maxs

    def normalize_array(self, array, min_val, max_val):
        """Normalize array values between 0 and 100."""
        normalized = np.zeros_like(array)
        if max_val > min_val:
            normalized = (array - min_val) / (max_val - min_val) * 100
        return normalized

    def _calculate_weighted_scores(self, group, mins, maxs):
        """Calculate weighted scores for a given group of hardware assets."""
        cpu_usage = group['CPU Usage (%)'].to_numpy()
        memory_usage = group['Memory Usage (%)'].to_numpy()
        disk_usage = group['Disk Usage (%)'].to_numpy()
        network_throughput = group['Network Throughput (Mbps)'].to_numpy()

        # Calculate weighted scores
        weighted_cpu = self.WEIGHTS[0] * np.mean(self.normalize_array(cpu_usage, 0, 100))
        weighted_memory = self.WEIGHTS[1] * np.mean(self.normalize_array(memory_usage, 0, 100))
        weighted_disk = self.WEIGHTS[2] * np.mean(self.normalize_array(disk_usage, 0, 100))
        weighted_network = self.WEIGHTS[3] * np.mean(self.normalize_array(network_throughput, mins[0], maxs[0]))

        # Compute the composite score
        composite_score = weighted_cpu + weighted_memory + weighted_disk + weighted_network

        return {
            'hardware_asset_id': group['hardware_asset_id'].iloc[0],
            'w_cpu_usage': weighted_cpu,
            'w_memory_usage': weighted_memory,
            'w_disk_usage': weighted_disk,
            'w_network_bandwidth': weighted_network,
            'overall_usage_score': composite_score
        }

    def calculate_weighted_usage_scores(self, hw_server_usage):
        """Calculate weighted usage scores based on CPU, memory, IO wait, and network bandwidth."""
        grouped = hw_server_usage.groupby('hardware_asset_id')
        mins, maxs = self.calculate_min_max(hw_server_usage, ['Network Throughput (Mbps)']) 

        # Initialize a list to store results
        results = []

        for asset_id, group in grouped:
            result = self._calculate_weighted_scores(group, mins, maxs)
            results.append(result)
        
        # Convert results to DataFrame
        results_df = pd.DataFrame(results)
        mins_ous, maxs_ous = self.calculate_min_max(results_df,['overall_usage_score'])
        # n_usage_score 
        results_df['n_usage_score'] = (results_df['overall_usage_score']) / (maxs_ous[0])
        
        return  results_df

    def add_weighted_usage_scores(self, hw_server_usage):
        """Add the weighted usage scores into the merged data."""
        return self.calculate_weighted_usage_scores(hw_server_usage)