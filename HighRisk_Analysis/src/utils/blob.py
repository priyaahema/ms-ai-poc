import os
import io
import logging
import urllib.parse
import pandas as pd
from azure.storage.blob import BlobServiceClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BlobStorageManager:
    def __init__(self):
        # Initialize Azure Blob storage clients
        self.AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.BLOB_CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')

        if not self.AZURE_STORAGE_CONNECTION_STRING or not self.BLOB_CONTAINER_NAME:
            raise ValueError("Azure connection string and container name must be set as environment variables.")

        # Initialize Azure Blob service client
        self.blob_service_client = BlobServiceClient.from_connection_string(self.AZURE_STORAGE_CONNECTION_STRING)
        self.container_client = self.blob_service_client.get_container_client(self.BLOB_CONTAINER_NAME)
    
    def upload_blob(self, data, blob_path):
        """Upload data to Azure Blob Storage"""
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.BLOB_CONTAINER_NAME, blob=blob_path)
            blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)
            logger.info(f"Successfully uploaded to {blob_path}")
        except Exception as e:
            logger.error(f"Failed to upload blob {blob_path}: {e}")
            raise
    

    def upload_dataframe_to_blob(self, df, blob_path):
        """Upload DataFrame to Azure Blob Storage as CSV"""
        try:
            csv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            self.upload_blob(csv_buffer, blob_path)
        except Exception as e:
            logger.error(f"Failed to upload DataFrame to blob {blob_path}: {e}")
            raise

    def download_blob_to_file(self, blob_name, download_file_path):
        """Download a blob from Azure Blob Storage to a local file"""
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            logger.info(f"Downloaded {blob_name} to {download_file_path}")
        except Exception as e:
            logger.error(f"Failed to download blob {blob_name}: {e}")
            raise

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
            # Decode the content (using 'ISO-8859-1' encoding as fallback)
            decoded_content = download_stream.decode('ISO-8859-1')

            # Use io.StringIO to read the CSV content into a pandas DataFrame
            df = pd.read_csv(io.StringIO(decoded_content))
            logger.info(f"Successfully downloaded and loaded DataFrame from {blob_name}")
            return df
        except Exception as e:
            logger.error(f"Error downloading blob {blob_name}: {e}")
            raise

    def load_csv_from_blob(self,container_client,folder_name, blob_name):
        """
        Load the CSV file directly from Azure Blob Storage into a pandas DataFrame.
        """
        try:
            blob_client = container_client.get_blob_client(f"{folder_name}/{blob_name}")
            csv_data = blob_client.download_blob().readall()
            # Convert byte content into a DataFrame
            df = pd.read_csv(io.BytesIO(csv_data))
            logger.info(f"Successfully loaded {blob_name} from container {self.BLOB_CONTAINER_NAME} into memory")
            return df
        except Exception as e:
            logger.error(f"Failed to load {blob_name} from blob storage: {str(e)}")
            raise

    def load_string_from_blob(self,blob_service_client, container_name,blob_path):
    
        # Get a client to interact with the specific blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

        # Download the blob content as a string
        download_stream = blob_client.download_blob()
        downloaded_text = download_stream.readall().decode('utf-8')  # Decoding to ensure it's a string

        return downloaded_text
    