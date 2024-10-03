import pandas as pd
import numpy as np


class IncidentScoring:
    # Define severity and impact scoring
    
    def __init__(self, df, server_df,severityScoreDef,impactScoreDef):
        self.df = df
        self.server_df = server_df
        self.severityScoreDef = severityScoreDef 
        self.impactScoreDef = impactScoreDef

    def calc_n_incident_score(self, oic, max_oisv):
        return oic / max_oisv

    def incident_score(self, severity_score,  impact_score,incident_count):
        """Calculate score for a single incident based on severity and impact."""
        return incident_count * (severity_score + impact_score)

    
    def incident_stability(self):
        
        # Preprocess 'severity' and 'impact' columns

        self.df['severity'] = self.df['severity'].fillna('').astype(str).str.lower()
        self.df['impact'] = self.df['impact'].fillna('').astype(str).str.lower()
        
        # Map 'severity' and 'impact' to their respective scores
        self.df['severity_score'] = self.df['severity'].apply(
            lambda severity: self.severityScoreDef.get(severity, 0)
        )
        self.df['impact_score'] = self.df['impact'].apply(
            lambda impact: self.impactScoreDef.get(impact, 0)
        )
           
        # Aggregate the data by 'hardware_asset_id'
        aggregated_df = self.df.groupby('hardware_asset_id').agg(
            severity_score=('severity_score', 'mean'),
            impact_score=('impact_score', 'mean'),
            incident_count=('hardware_asset_id', 'size')
        ).reset_index()
        

        aggregated_df['incident_score'] = aggregated_df.apply(
            lambda row: self.incident_score(row['severity_score'], row['impact_score'],row['incident_count']),
            axis=1
        )
        # Calculate the maximum incident_score
        max_incident_score = aggregated_df['incident_score'].max()

        # Calculate overall_incident_score
        aggregated_df['overall_incident_score'] = aggregated_df['incident_score'] / max_incident_score

        return aggregated_df



    def normalize_array(self, array, minv, maxv):
        """
        Normalize a numpy array to the range [0, 1] using the formula: (array - min) / (max - min).
        IN: array is numpy.ndarray, minv, maxv are the minimum and maximum values for normalization.
        OUT: numpy.ndarray: The normalized array.
        """
        # Ensure the input is a NumPy array
        array = np.asarray(array)

        # Avoid division by zero if maxv == minv
        if maxv == minv:
            return np.zeros_like(array)

        # Perform normalization
        normalized_array = (array - minv) / (maxv - minv)

        return normalized_array

    def calculate_minmax(self, df, minmaxcols):
        """
        Calculate the min and max for specified columns in a DataFrame.
        IN: df DataFrame, minmaxcols: list of columns for which to find min/max.
        OUT: mins, maxs: The minimum and maximum values of the columns.
        """
        # Check if all specified columns are in the DataFrame
        for col in minmaxcols:
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found in input df.")

        # Extract the columns and convert them to NumPy arrays
        data = df[minmaxcols].to_numpy()

        # Calculate the min and max for each column
        mins = np.min(data, axis=0)
        maxs = np.max(data, axis=0)

        return mins, maxs

    def incident_df_with_all_scores(self):
        """Generate detailed incident scores and join them with server data."""
        grouped = self.df.groupby('hardware_asset_id')

        results = []

        for hardware_asset_id, group in grouped:
            incident_count = np.mean(group['hardware_asset_id'].count())
            s_impact = np.sum(group['impact_score'].to_numpy())
            s_severity = np.sum(group['severity_score'].to_numpy())
            s_incident_score = np.sum(group['incident_score'].to_numpy())

            # Collect results for each asset
            results.append({
                'hardware_asset_id': hardware_asset_id,
                'incident_count': incident_count,
                's_impact': s_impact,
                's_severity': s_severity,
                's_incident_score': s_incident_score
            })

        # Convert results to DataFrame
        results_df = pd.DataFrame(results)

        # Normalize incident score
        data_max = results_df['s_incident_score'].to_numpy()
        max_ic = np.max(data_max)
        results_df['overall_incident_score'] = (results_df['s_incident_score']) / max_ic

        # Merge with server data
        result_serv = pd.merge(self.server_df, results_df, on='hardware_asset_id', how='left')

        # Specify columns to keep and clean missing data
        columns_to_keep = ['hardware_asset_id', 'incident_count', 's_impact', 's_severity', 's_incident_score', 'overall_incident_score']
        result_serv = result_serv[columns_to_keep]
        result_serv.fillna(0, inplace=True)

        # Normalize overall incident score
        mins_ois, maxs_ois = self.calculate_minmax(result_serv, ['overall_incident_score'])
        max_oisv = maxs_ois[0]
        result_serv['n_incident_score'] = result_serv['overall_incident_score'].apply(self.calc_n_incident_score, args=(max_oisv,))

        return result_serv
