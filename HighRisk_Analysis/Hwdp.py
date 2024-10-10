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
from src.utils.report_generator import CSVProcessor 
from src.data_analysis.vulnerability_scorer import VulnerabilityScorer
from src.data_analysis.incident_scorer import IncidentScoring      
from src.data_analysis.maintenance_scorer import MaintenanceAnalyzer
from src.utils.blob import BlobStorageManager
from dotenv import load_dotenv

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
    
    def upload_blob(self, data, blob_path):
        """Upload data to Azure Blob Storage"""
        blob_client = self.blob_service_client.get_blob_client(container=self.BLOB_CONTAINER_NAME, blob=blob_path)
        blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)
    
    def upload_dataframe_to_blob(self, df, blob_path):
        """Upload DataFrame to Azure Blob Storage as CSV"""
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        self.upload_blob(csv_buffer, blob_path)

    def download_blob_to_file(self, blob_name, download_file_path):
        """Downloads a blob from Azure Blob Storage to a local file"""
        blob_client = self.container_client.get_blob_client(blob_name)
        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        logger.info(f"Downloaded {blob_name} to {download_file_path}")

    def load_from_blob(self, folder_name, blob_name):
        """
        Loads a CSV file directly from Azure Blob Storage into a pandas DataFrame.
        """
        logger.info(f"Attempting to read blob: {blob_name}")

        # URL encode the blob name
        encoded_blob_name = urllib.parse.quote(blob_name)
        logger.info(f"Encoded blob name: {encoded_blob_name}")

        blob_client = self.container_client.get_blob_client(f"{folder_name}/{encoded_blob_name}")
        
        try:
            # Download the blob content
            download_stream = blob_client.download_blob().readall()

            # Decode the content using a different encoding if UTF-8 fails (try 'ISO-8859-1' or 'Windows-1252')
            decoded_content = download_stream.decode('ISO-8859-1')

            # Use io.StringIO to read the CSV content into a pandas DataFrame
            df = pd.read_csv(io.StringIO(decoded_content))
            logger.info(df)
            logger.info(f"Successfully downloaded {blob_name}")
            return df
        except Exception as e:
            logger.error(f"Error downloading blob {blob_name}: {e}")
            raise
  

    def process_data(self):

        logger.info("Starting data processing")

        weights_composite_stability_score = {
            'overall_usage_score': 0.2,
            'overall_incident_score': 0.5,
            'maintenance_score': 0.3,
            'vulnerability_score': 0.5
        }
        
        severity_mapping = {
            '1 - high': 10, 'high': 10,
            '2 - medium': 4, 'medium': 4,
            '3 - low': 0.5, 'low': 0.5
        }
        impact_mapping = {
            '1 - high': 10, 'high': 10,
            '2 - medium': 4, 'medium': 4,
            '3 - low': 0.5, 'low': 0.5
        }

        # Azure Blob Storage setup
        folder_name = 'AssetFiles'
        
        # Load datasets directly from Blob Storage (using streams)
        logger.info("Processing datasets")

        data_loader = DataLoader()
        data_processor = DataProcessor()
        metrics_calculator = MetricsCalculator()
        risk_categorizer = RiskCategorizer()
        csv_processor = CSVProcessor()
        blob_storage_manager = BlobStorageManager()

        # Create list to store dataframes loaded from blob
        hw_server_usage = self.load_from_blob(folder_name, 'hw_servers_usage_5.csv')
        logger.info("hw_server_usage loaded")

        patch_upgrades = self.load_from_blob(folder_name, 'patchupgrades.csv')
        logger.info("patch_upgrades loaded")

        hw_warrant = self.load_from_blob(folder_name, 'hw_warranty_5.csv')
        logger.info("hw_warrant loaded")

        hw_server = self.load_from_blob(folder_name, 'hw_servers_5.csv')
        logger.info("hw_server loaded")

        hw_vulnerability = self.load_from_blob(folder_name, 'hw_vulnerabilities_data.csv')
        logger.info("hw_vulnerability loaded")

        hw_incidents = self.load_from_blob(folder_name, 'hw_incidents_5.csv')
        logger.info("hw_incidents loaded")

        # Calculate and save individual scores for incidents, usage, vulnerabilities, etc.
        patch_upgrades_analyzer = MaintenanceAnalyzer(patch_upgrades)
        patch_upgrades_analyzer.calculate_overall_maintenance_score()
        maintenance_scores_calculated = patch_upgrades_analyzer.aggregate_by_hardware_asset()
        blob_storage_manager.upload_dataframe_to_blob(maintenance_scores_calculated, 'AssetFiles_With_Scores/hw_maintenance_score.csv')

        incident_scorer = IncidentScoring(hw_incidents, hw_server, severity_mapping , impact_mapping)
        incident_scores_calculated = incident_scorer.incident_stability()
        blob_storage_manager.upload_dataframe_to_blob(incident_scores_calculated, 'AssetFiles_With_Scores/hw_incidents_score.csv')

        hw_vulnerability.columns = hw_vulnerability.columns.str.replace('ï»¿', '')
        vulnerability_scorer = VulnerabilityScorer(hw_vulnerability)
        vulnerability_summary = vulnerability_scorer.calculate_vulnerability_stability()
        blob_storage_manager.upload_dataframe_to_blob(vulnerability_summary, 'AssetFiles_With_Scores/hw_vulnerability_score.csv')

        # Usage scoring
        usage_scorer = UsageScorer()
        weighted_usage_scores = usage_scorer.add_weighted_usage_scores(hw_server_usage)
        blob_storage_manager.upload_dataframe_to_blob(weighted_usage_scores, 'AssetFiles_With_Scores/hw_usage_score.csv')

        # Merge all individual scores with server data
        logger.info("Merging datasets")
        merged_data = data_processor.merge_data(
            hw_server,
            weighted_usage_scores,
            incident_scores_calculated,
            maintenance_scores_calculated,
            hw_warrant,
            vulnerability_summary
        )

        # Handle missing values
        logger.info("Handling missing values")
        merged_data = data_processor.handle_missing_values(merged_data)

        # Calculate stability scores
        logger.info("Calculating stability scores")
        merged_data = metrics_calculator.calculate_stability_scores(merged_data, weights_composite_stability_score)

        # Calculate risk categorization
        logger.info("Categorizing asset risk")
        merged_data_zscore = risk_categorizer.categorize_asset_risk(merged_data)

        # Add company names
        merged_data = data_processor.add_company_names(merged_data_zscore, hw_server)

        # Save the final merged data
        blob_storage_manager.upload_dataframe_to_blob(merged_data, 'Reports/csv_report/summarized_asset_scores_with_risk_category.csv')

    def main(self):
        logger.info("Starting data processing via the class.")

        try:
            self.process_data()
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

