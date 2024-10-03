import matplotlib.pyplot as plt
import base64
import numpy as np
import pandas as pd
from io import BytesIO
from src.utils.report_generator import PlotProcessor
import seaborn as sns

class ReportPlotter:
    def __init__(self, df, html_content_list, OpenAIModel,plot_processor):
        self.df = df
        self.html_content_list = html_content_list
        self.OpenAIModel = OpenAIModel
        self.plot_processor = plot_processor 

    def plot_stability_histograms_with_lines(self):
        """
        Creates histograms showing the distribution of Composite Stability Scores for
        Low Risk, Moderate Risk, and High Risk groups with lines indicating mean values.
        """
        if 'composite_stability_score' not in self.df.columns or 'risk_category' not in self.df.columns:
            raise ValueError("DataFrame must contain 'composite_stability_score' and 'risk_category' columns.")

        # Separate groups based on risk categories
        low_risk_df = self.df[self.df['risk_category'] == 'Low Risk']
        moderate_risk_df = self.df[self.df['risk_category'] == 'Moderate Risk']
        high_risk_df = self.df[self.df['risk_category'] == 'High Risk']

        fig = plt.figure(figsize=(18, 6))

        # Low Risk group
        plt.subplot(1, 3, 1)
        plt.hist(low_risk_df['composite_stability_score'], bins=20, color='blue', alpha=0.7)
        mean_low = low_risk_df['composite_stability_score'].mean()
        plt.axvline(mean_low, color='darkblue', linestyle='dashed', linewidth=2, label='Mean')
        plt.title('Low Risk: Composite Stability Scores')
        plt.xlabel('Composite Stability Score')
        plt.ylabel('Frequency')
        plt.legend()
        plt.grid(axis='y', alpha=0.75)

        # Moderate Risk group
        plt.subplot(1, 3, 2)
        plt.hist(moderate_risk_df['composite_stability_score'], bins=20, color='orange', alpha=0.7)
        mean_moderate = moderate_risk_df['composite_stability_score'].mean()
        plt.axvline(mean_moderate, color='darkorange', linestyle='dashed', linewidth=2, label='Mean')
        plt.title('Moderate Risk: Composite Stability Scores')
        plt.xlabel('Composite Stability Score')
        plt.ylabel('Frequency')
        plt.legend()
        plt.grid(axis='y', alpha=0.75)

        # High Risk group
        plt.subplot(1, 3, 3)
        plt.hist(high_risk_df['composite_stability_score'], bins=20, color='red', alpha=0.7)
        mean_high = high_risk_df['composite_stability_score'].mean()
        plt.axvline(mean_high, color='darkred', linestyle='dashed', linewidth=2, label='Mean')
        plt.title('High Risk: Composite Stability Scores')
        plt.xlabel('Composite Stability Score')
        plt.ylabel('Frequency')
        plt.legend()
        plt.grid(axis='y', alpha=0.75)

        plt.tight_layout()
        return fig

    def plot_average_scores_comparison(self):
        """
        Creates a bar chart comparing the average overall usage, incident, and maintenance
        scores between Low Risk and High Risk groups.
        """
        required_columns = ['overall_usage_score', 'overall_incident_score', 'maintenance_score', 'risk_category']
        if not all(col in self.df.columns for col in required_columns):
            raise ValueError("DataFrame must contain 'overall_usage_score', 'overall_incident_score', 'maintenance_score', and 'risk_category' columns.")

        averages = self.df[self.df['risk_category'].isin(['Low Risk', 'High Risk'])].groupby('risk_category').agg({
            'overall_usage_score': 'mean',
            'overall_incident_score': 'mean',
            'maintenance_score': 'mean'
        }).reset_index()

        fig = plt.figure(figsize=(10, 6))
        bar_width = 0.25
        index = range(len(averages))

        plt.bar(index, averages['overall_usage_score'], width=bar_width, label='Overall Usage Score', alpha=0.7, color='blue')
        plt.bar([i + bar_width for i in index], averages['overall_incident_score'], width=bar_width, label='Overall Incident Score', alpha=0.7, color='orange')
        plt.bar([i + 2 * bar_width for i in index], averages['maintenance_score'], width=bar_width, label='Maintenance Score', alpha=0.7, color='green')

        plt.xlabel('Risk Category')
        plt.ylabel('Average Scores')
        plt.title('Average Scores Comparison: Low Risk vs. High Risk')
        plt.xticks([i + bar_width for i in index], averages['risk_category'])
        plt.legend()
        plt.grid(axis='y', alpha=0.75)
        plt.tight_layout()

        return fig

    def plot_radar_chart(self):
        """
        Creates a radar chart comparing various metrics between Low Risk and High Risk groups.
        """
        metrics = ['w_cpu_usage', 'w_memory_usage', 'w_disk_usage', 'w_network_bandwidth', 
                   'overall_usage_score', 'overall_incident_score', 'maintenance_score']

        if not all(metric in self.df.columns for metric in metrics):
            raise ValueError("DataFrame must contain all specified metrics.")

        low_risk_data = self.df[self.df['risk_category'] == 'Low Risk'][metrics].mean().values
        high_risk_data = self.df[self.df['risk_category'] == 'High Risk'][metrics].mean().values

        num_vars = len(metrics)
        angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()
        low_risk_data = np.concatenate((low_risk_data, [low_risk_data[0]]))
        high_risk_data = np.concatenate((high_risk_data, [high_risk_data[0]]))
        angles += angles[:1]

        fig, ax = plt.subplots(figsize=(9, 9), subplot_kw=dict(polar=True))
        ax.fill(angles, low_risk_data, color='blue', alpha=0.25, label='Low Risk (Stable)')
        ax.fill(angles, high_risk_data, color='red', alpha=0.25, label='High Risk')

        ax.set_yticklabels([])
        plt.xticks(angles[:-1], metrics, color='black', fontsize=12)
        ax.yaxis.grid(color='grey', linestyle='--', linewidth=0.5)
        plt.title('Radar Chart Comparison: Low Risk vs. High Risk')
        plt.legend(loc='upper right', bbox_to_anchor=(1.1, 1.1))

        plt.tight_layout()
        return fig

    def plot_risk_category_distribution(self):
        """Plot a pie chart to visualize the distribution of incident risk categories."""
        risk_distribution = self.df['risk_category'].value_counts()
        colors = plt.get_cmap('Set3').colors

        fig, ax = plt.subplots(figsize=(7, 7))
        ax.pie(risk_distribution, labels=risk_distribution.index, autopct='%1.1f%%',
               startangle=90, colors=colors[:len(risk_distribution)])
        ax.set_title('Incident Risk Category Distribution')
        ax.axis('equal')

        plt.tight_layout()
        return fig

    def plot_top_high_risk_assets(self):

        # Filter for high-risk assets
        high_risk_assets = self.df[self.df['risk_category'] == 'High Risk']

        # Sort by composite stability score (descending) and get the top 10
        top_high_risk = high_risk_assets.sort_values(by='zscore_composite_stability', ascending=False).head(10)

        # Create a color map
        colors = plt.cm.viridis(np.linspace(0, 1, len(top_high_risk)))

        # Create a bar chart with horizontal bars
        fig, ax = plt.subplots(figsize=(12, 6))
        bars = ax.barh(top_high_risk['hardware_asset_id'], top_high_risk['zscore_composite_stability'], color=colors)

        # Add labels and title
        ax.set_xlabel('Z-Score')
        ax.set_ylabel('Hardware Asset ID')
        ax.set_title('Top 10 High-Risk Assets')

        # Optionally, add value labels on the bars
        for bar in bars:
            width = bar.get_width()
            ax.text(width, bar.get_y() + bar.get_height() / 2, f'{width:.2f}', va='center', ha='left')

        # Adjust layout
        plt.tight_layout()

        # Remove grid lines
        ax.grid(False)

        ax.invert_yaxis()

        # plt.show()
        # Return the Figure object
        return fig
    
    def plot_heatmap_using_scores(self):

        # Select relevant columns for the heatmap
        heatmap_data = self.df[['hardware_asset_id',
                            'overall_usage_score',
                            'overall_incident_score',
                            'overall_maintenance_score',
                            'overall_vulnerability_score',
                            'composite_stability_score',
                            'zscore_composite_stability']]

        # Set 'hardware_asset_id' as the index
        heatmap_data.set_index('hardware_asset_id', inplace=True)

        # Compute the correlation matrix
        correlation_matrix = heatmap_data.corr()

        # Create the heatmap
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap='coolwarm', ax=ax, cbar=True)

        # Add title
        plt.title('Heatmap of Incident and Stability Metrics')

        # Adjust layout
        plt.tight_layout()

        # Return the Figure object
        return fig
    
    def plot_graphs_and_charts(self):

        stability_histograms_plot = self.plot_stability_histograms_with_lines()
        self.plot_processor.save_plot_to_html(stability_histograms_plot, 'Composite Stability Scores of Risk Risk Category',self.html_content_list)

        average_scores_comparison_plot = self.plot_average_scores_comparison()
        self.plot_processor.save_plot_to_html(average_scores_comparison_plot, 'Average Scores Comparison',self.html_content_list)

        radar_chart_plot = self.plot_radar_chart()
        self.plot_processor.save_plot_to_html(radar_chart_plot, 'Radar Chart Comparison',self.html_content_list)

        risk_category_distribution_plot = self.plot_risk_category_distribution()
        self.plot_processor.save_plot_to_html(risk_category_distribution_plot, 'Risk Category Distribution',self.html_content_list)

        top_high_risk_assets = self.plot_top_high_risk_assets()
        self.plot_processor.save_plot_to_html(top_high_risk_assets, 'High Risk Assets by Z-score',self.html_content_list)

        heatmap_plot = self.plot_heatmap_using_scores()
        self.plot_processor.save_plot_to_html(heatmap_plot, 'Heatmap of Usage,Incident and Stability Metrics',self.html_content_list)

        return self.html_content_list
