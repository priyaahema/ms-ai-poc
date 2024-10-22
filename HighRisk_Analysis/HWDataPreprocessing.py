import os
import io
import logging
import sys
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import urllib

# Adding the project root (AzureFunctions) to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__)))
sys.path.append(project_root)

from src.data_processor.data_loader import DataLoader
from src.data_analysis.usage_scorer import UsageScorer
from src.data_processor.data_processor import DataProcessor
from src.metrics import MetricsCalculator, RiskCategorizer
from src.data_analysis.vulnerability_scorer import VulnerabilityScorer
from src.data_analysis.incident_scorer import IncidentScoring      
from src.data_analysis.maintenance_scorer import MaintenanceAnalyzer
from src.utils.blob import BlobStorageManager
from src.app_constants.constants import (weights_composite_stability_score,severity_mapping,impact_mapping,
                                        source_folder_name,target_folder_name,csv_report_asset_with_risk_file_location,
                                        code_string_file_name
                                        )
from dotenv import load_dotenv
import inspect

# Load environment variables from the .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataProcessorClass:
    def __init__(self):
        # Initialize Azure Blob storage clients
        self.AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.BLOB_CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')

        # Initialize Azure Blob service client
        self.blob_service_client = BlobServiceClient.from_connection_string(self.AZURE_STORAGE_CONNECTION_STRING)
        self.container_client = self.blob_service_client.get_container_client(self.BLOB_CONTAINER_NAME)
        self.blob_storage_manager = BlobStorageManager()

        self.source_folder_name = source_folder_name
        self.weights_composite_stability_score = weights_composite_stability_score
        self.severity_mapping = severity_mapping
        self.impact_mapping= impact_mapping

        self.data_processor = DataProcessor()
        self.metrics_calculator = MetricsCalculator()
        self.risk_categorizer = RiskCategorizer()

    # Function to capture function source code
    def get_function_code(self,func):
        return inspect.getsource(func)

    def get_code_as_string(self,patch_upgrades_analyzer,incident_scorer,vulnerability_scorer,usage_scorer):

        complete_code_string = ""
        complete_code_string += self.get_function_code(self.process_data) + "\n"
        complete_code_string += self.get_function_code(patch_upgrades_analyzer.calculate_overall_maintenance_score) + "\n"
        complete_code_string += self.get_function_code(patch_upgrades_analyzer.aggregate_by_hardware_asset) + "\n"
        complete_code_string += self.get_function_code(incident_scorer.incident_stability) + "\n"
        complete_code_string += self.get_function_code(vulnerability_scorer.calculate_vulnerability_stability) + "\n"
        complete_code_string += self.get_function_code(usage_scorer.add_weighted_usage_scores) + "\n"
        complete_code_string += self.get_function_code(self.metrics_calculator.calculate_stability_scores) + "\n"
        complete_code_string += self.get_function_code(self.risk_categorizer.categorize_asset_risk)

        return complete_code_string
  
    def process_data(self):

        logger.info("Starting data processing")
        
        # Create list to store dataframes loaded from blob
        hw_server_usage = self.blob_storage_manager.load_from_blob(self.source_folder_name, 'hw_servers_usage_5.csv')
        logger.info("hw_server_usage loaded")

        patch_upgrades = self.blob_storage_manager.load_from_blob(self.source_folder_name, 'patchupgrades.csv')
        logger.info("patch_upgrades loaded")

        hw_warrant = self.blob_storage_manager.load_from_blob(self.source_folder_name, 'hw_warranty_5.csv')
        logger.info("hw_warrant loaded")

        hw_server = self.blob_storage_manager.load_from_blob(self.source_folder_name, 'hw_servers_5.csv')
        logger.info("hw_server loaded")

        hw_vulnerability = self.blob_storage_manager.load_from_blob(self.source_folder_name, 'hw_vulnerabilities_data.csv')
        logger.info("hw_vulnerability loaded")

        hw_incidents = self.blob_storage_manager.load_from_blob(self.source_folder_name, 'hw_incidents_5.csv')
        logger.info("hw_incidents loaded")

        # Calculate and save individual scores for incidents, usage, vulnerabilities, etc.
        patch_upgrades_analyzer = MaintenanceAnalyzer(patch_upgrades)
        patch_upgrades_analyzer.calculate_overall_maintenance_score()
        maintenance_scores_calculated = patch_upgrades_analyzer.aggregate_by_hardware_asset()
        self.blob_storage_manager.upload_dataframe_to_blob(maintenance_scores_calculated, f'{target_folder_name}/hw_maintenance_score.csv')

        incident_scorer = IncidentScoring(hw_incidents, hw_server, self.severity_mapping , self.impact_mapping)
        incident_scores_calculated = incident_scorer.incident_stability()
        self.blob_storage_manager.upload_dataframe_to_blob(incident_scores_calculated, f'{target_folder_name}/hw_incidents_score.csv')

        hw_vulnerability.columns = hw_vulnerability.columns.str.replace('ï»¿', '')
        vulnerability_scorer = VulnerabilityScorer(hw_vulnerability)
        vulnerability_summary = vulnerability_scorer.calculate_vulnerability_stability()
        self.blob_storage_manager.upload_dataframe_to_blob(vulnerability_summary, f'{target_folder_name}/hw_vulnerability_score.csv')

        # Usage scoring
        usage_scorer = UsageScorer()
        weighted_usage_scores = usage_scorer.add_weighted_usage_scores(hw_server_usage)
        self.blob_storage_manager.upload_dataframe_to_blob(weighted_usage_scores, f'{target_folder_name}/hw_usage_score.csv')

        # Merge all individual scores with server data
        logger.info("Merging datasets")
        merged_data = self.data_processor.merge_data(
            hw_server,
            weighted_usage_scores,
            incident_scores_calculated,
            maintenance_scores_calculated,
            hw_warrant,
            vulnerability_summary
        )

        # Handle missing values
        logger.info("Handling missing values")
        merged_data = self.data_processor.handle_missing_values(merged_data)

        # Calculate stability scores
        logger.info("Calculating stability scores")
        merged_data = self.metrics_calculator.calculate_stability_scores(merged_data, self.weights_composite_stability_score)

        # Calculate risk categorization
        logger.info("Categorizing asset risk")
        merged_data_zscore = self.risk_categorizer.categorize_asset_risk(merged_data)

        # Add company names
        merged_data = self.data_processor.add_company_names(merged_data_zscore, hw_server)

        # Save the final merged data
        self.blob_storage_manager.upload_dataframe_to_blob(merged_data, csv_report_asset_with_risk_file_location)
        
        return patch_upgrades_analyzer,incident_scorer,vulnerability_scorer,usage_scorer
      
    def main(self):
        logger.info("Starting data processing via the class.")
        try:
            patch_upgrades_analyzer,incident_scorer,vulnerability_scorer,usage_scorer = self.process_data()
            complete_code_string = self.get_code_as_string(patch_upgrades_analyzer,incident_scorer,vulnerability_scorer,usage_scorer)
            self.blob_storage_manager.upload_blob(complete_code_string, code_string_file_name)
            return {
                'statusCode': 200,
                'body': 'Data processing completed successfully.'
            }
        except Exception as e:
            logger.error(f"Error during data processing: {e}")
            return {
                'statusCode': 500,
                'body': f"Data processing failed: {str(e)}"
            }

