import os
import io
import logging
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from src.data_processor.data_loader import DataLoader
from src.data_analysis.usage_scorer import UsageScorer
from src.data_processor.data_processor import DataProcessor
from src.metrics import MetricsCalculator, RiskCategorizer
from src.utils.report_generator import CSVProcessor 
from src.data_analysis.vulnerability_scorer import VulnerabilityScorer
from src.data_analysis.incident_scorer import IncidentScoring      
from src.data_analysis.maintenance_scorer import MaintenanceAnalyzer
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Azure Blob storage clients
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
BLOB_CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')

# Initialize Azure Blob service client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

def upload_blob(data, blob_path):

    # Create a BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

    # Get a client to interact with the specific blob
    blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_path)

    # Upload the data to the blob
    blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)

def upload_dataframe_to_blob(df, blob_path):
    
    # Create an in-memory buffer
    csv_buffer = io.BytesIO()

    # Write the DataFrame to the buffer as CSV
    df.to_csv(csv_buffer, index=False)

    # Move the buffer's position to the beginning
    csv_buffer.seek(0)

    # Upload the CSV buffer to the blob
    upload_blob(csv_buffer, blob_path)

def download_blob_to_file(blob_name, download_file_path):
    """Downloads a blob from Azure Blob Storage to a local file."""
    blob_client = container_client.get_blob_client(blob_name)
    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
    logger.info(f"Downloaded {blob_name} to {download_file_path}")


def process_data():
    logger.info("Starting data processing")

    # Download files from Azure Blob Storage
    download_dir = 'AssetFiles'
    score_output_dir = 'AssetFiles_With_Scores'

    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(score_output_dir, exist_ok=True)

    files_to_download = {
        'hw_servers_usage_5.csv': 'hw_usage_data.csv',
        'hw_warranty_5.csv': 'hw_warranty_data.csv',
        'hw_servers_5.csv': 'hw_servers_data.csv',
        'hw_vulnerabilities_data.csv': 'hw_vulnerabilities_data.csv',
        'patchupgrades.csv': 'patch_upgrades_data.csv',
        'hw_incidents_5.csv': 'hw_incidents_data.csv',
    }

    weights_composite_stability_score = {
        'overall_usage_score': 0.2,
        'overall_incident_score': 0.5,
        'maintenance_score': 0.3,
        'vulnerability_score':0.5
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

    u_event_severity_mapping = {'CRITICAL': 3, 'MAJOR': 2, 'MINOR': 1}
    
    # Folder name inside the container
    folder_name = 'AssetFiles'

    for blob_name, local_name in files_to_download.items():
        blob_path = f"{folder_name}/{blob_name}"
        download_blob_to_file(blob_path, os.path.join(download_dir, local_name))

    # Process datasets
    logger.info("Processing datasets")
    data_loader = DataLoader()
    data_processor = DataProcessor()
    metrics_calculator = MetricsCalculator()
    risk_categorizer = RiskCategorizer()
    csv_processor = CSVProcessor()

    # Load datasets
    hw_server_usage, hw_incidents, patch_upgrades, hw_warrant, hw_server, hw_vulnerability = data_loader.load_datasets([
        os.path.join(download_dir, 'hw_usage_data.csv'),
        os.path.join(download_dir, 'hw_incidents_data.csv'),
        os.path.join(download_dir, 'patch_upgrades_data.csv'),
        os.path.join(download_dir, 'hw_warranty_data.csv'),
        os.path.join(download_dir, 'hw_servers_data.csv'),
        os.path.join(download_dir, 'hw_vulnerabilities_data.csv')
    ])

    # Calculate and save individual scores for incidents, usage, vulnerabilities, etc.
    patch_upgrades_analyzer = MaintenanceAnalyzer(patch_upgrades)
    patch_upgrades_analyzer.calculate_overall_maintenance_score()
    maintenance_scores_calculated = patch_upgrades_analyzer.aggregate_by_hardware_asset()
    # csv_processor.save_to_csv(maintenance_scores_calculated , 'AssetFiles_With_Scores/hw_maintenance_scores.csv')
    upload_dataframe_to_blob(maintenance_scores_calculated,'AssetFiles_With_Scores/hw_maintenance_score.csv')


    incident_scorer = IncidentScoring(hw_incidents, hw_server,severity_mapping,impact_mapping)
    incident_scores_calculated = incident_scorer.incident_stability()
    # csv_processor.save_to_csv(incident_scores_calculated , 'AssetFiles_With_Scores/hw_incidents_score.csv')
    # # incident_scores_calculated = incident_scorer.incident_df_with_all_scores()
    upload_dataframe_to_blob(incident_scores_calculated,'AssetFiles_With_Scores/hw_incidents_score.csv')

  
    hw_vulnerability.columns = hw_vulnerability.columns.str.replace('ï»¿', '')
    vulnerability_scorer = VulnerabilityScorer(hw_vulnerability)
    vulnerability_summary = vulnerability_scorer.calculate_vulnerability_stability()
    # csv_processor.save_to_csv(vulnerability_summary, 'AssetFiles_With_Scores/hw_vulnerability_score.csv')
    upload_dataframe_to_blob(vulnerability_summary,'AssetFiles_With_Scores/hw_vulnerability_score.csv')


    # Usage scoring
    usage_scorer = UsageScorer()
    weighted_usage_scores = usage_scorer.add_weighted_usage_scores(hw_server_usage)
    # csv_processor.save_to_csv(weighted_usage_scores, 'AssetFiles_With_Scores/hw_usage_score.csv')
    upload_dataframe_to_blob(weighted_usage_scores,'AssetFiles_With_Scores/hw_usage_score.csv')
    
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
    merged_data =  metrics_calculator.calculate_stability_scores(merged_data,weights_composite_stability_score)

    # Calculate risk categorization
    logger.info("Categorizing asset risk")
    merged_data_zscore =  risk_categorizer.categorize_asset_risk(merged_data)

    # Add company names
    merged_data= data_processor.add_company_names(merged_data_zscore, hw_server)
    
    # Save the final merged data
    # csv_processor.save_to_csv(merged_data, f'Reports/csv_report/summarized_asset_scores_with_risk_category.csv')
    upload_dataframe_to_blob(merged_data,'Reports/csv_report/summarized_asset_scores_with_risk_category.csv')

    # logger.info("Data processing completed")

# Azure Function entry point
def main():
    logger.info("HTTP trigger function processed a request.")

    try:
        process_data()
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

if __name__ == '__main__':
    main()