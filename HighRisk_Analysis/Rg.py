import io
import os 
import logging
import sys
import pandas as pd
from azure.storage.blob import BlobServiceClient

# Adding the project root (AzureFunctions) to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__)))
sys.path.append(project_root)

from src.metrics import MetricsCalculator,RiskCategorizer
from src.utils.report_generator import HTMLProcessor,CSVProcessor,PlotProcessor,PDFConverter
from src.source_code import code_text
from src.utils.model import OpenAIModel
from src.data_analysis.visualizations import ReportPlotter
from src.utils.email_sender import EmailSender
from src.utils.blob import BlobStorageManager
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ReportGeneratorClass:

    def __init__(self):
        # Load environment variables for OpenAI API
        self.API_BASE = os.getenv('API_BASE')
        self.API_KEY = os.getenv('API_KEY')
        self.DEPLOYMENT_NAME = os.getenv('DEPLOYMENT_NAME')
        self.API_VERSION = os.getenv('API_VERSION')
        
        if not self.API_BASE or not self.API_KEY or not self.DEPLOYMENT_NAME or not self.API_VERSION:
            logger.error("API credentials are missing")
            return
        
        # Azure Blob Storage details
        self.AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.BLOB_CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')
        self.folder_name = "Reports/csv_report"
        self.blob_name = 'summarized_asset_scores_with_risk_category.csv'
        
        # Check if connection string is available
        if not self.AZURE_STORAGE_CONNECTION_STRING:
            logger.error("Azure Storage connection string is missing")
            return
        
        # Initialize OpenAI Model
        logger.info("Initializing OpenAI Model")
        self.openai_model = OpenAIModel(api_base=self.API_BASE, api_key=self.API_KEY, deployment_name=self.DEPLOYMENT_NAME, api_version=self.API_VERSION)

        self.metrics_calculator = MetricsCalculator()
        self.html_processor = HTMLProcessor()
        self.csv_processor = CSVProcessor()
        self.plot_processor = PlotProcessor(self.openai_model)
        self.pdf_converter = PDFConverter()
        self.blob_storage_manager = BlobStorageManager()


        # Initialize Azure Blob service client
        self.blob_service_client = BlobServiceClient.from_connection_string(self.AZURE_STORAGE_CONNECTION_STRING)
        self.container_client = self.blob_service_client.get_container_client(self.BLOB_CONTAINER_NAME)


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

    def main(self):
        logger.info("Starting AI model and report generation step")

        # Load the CSV directly into memory from Blob Storage
        logger.info("Loading aggregated_calculated_scores.csv from Blob Storage into memory")
        complete_df = self.load_csv_from_blob(self.container_client,self.folder_name,self.blob_name)
        
        # Generate reports
        logger.info("Generating reports")
        html_content_list = []
        report_plotter = ReportPlotter(complete_df,html_content_list,self.openai_model,self.plot_processor)

        # List all data files
        title = "The Approach to Identify the Risk Assets"
        approach_explanation = self.openai_model.explain_risk_asset_identification(code_text)
        approach_explanation_html = self.html_processor.convert_markdown_to_html(approach_explanation)
        formatted_approach_explanation_html = self.html_processor.create_explanation_summary_formatted(title,approach_explanation_html)
        html_content_list.append(formatted_approach_explanation_html)

        # Count of Assets by Category
        logger.info("Summarizing assets by category")
        count_of_assets_by_category_table_title = "Count of Assets by Category"
        count_of_assets_by_category =  self.metrics_calculator.summarize_risk_categories(complete_df)
        count_of_assets_description = self.openai_model.describe_data_frame_with_model(count_of_assets_by_category)
        count_of_assets_description_html = self.html_processor.convert_markdown_to_html(count_of_assets_description)
        count_of_assets_by_category_html = self.html_processor.df_to_html_table(count_of_assets_by_category)
        count_of_assets_by_category_html_formatted = self.html_processor.create_count_assets_by_category_table(count_of_assets_by_category_table_title,count_of_assets_by_category_html,count_of_assets_description_html)

        html_content_list.append(count_of_assets_by_category_html_formatted)

        # Summary of Usage/Incident/Maintenance Metrics
        logger.info("Generating usage, incident, and maintenance summaries")
        usage_summary, incident_summary, maintenance_summary =  self.metrics_calculator.display_metrics_summary(complete_df)

        # Generate HTML for usage summary
        usage_title = "Usage Summary"
        usage_description =  self.openai_model.describe_data_frame_with_model(usage_summary)
        usage_description_html = self.html_processor.convert_markdown_to_html(usage_description)
        usage_summary_html =  self.html_processor.df_to_html_table(usage_summary) 
        usage_summary_html =  self.html_processor.create_usage_table_html(usage_title, usage_summary_html,  usage_description_html)

        # Generate HTML for incident summary
        incident_title = "Incident Summary"
        incident_description = self.openai_model.describe_data_frame_with_model(usage_summary)
        incident_description_html = self.html_processor.convert_markdown_to_html(incident_description)
        incident_summary_html =  self.html_processor.df_to_html_table(incident_summary) 
        incident_summary_html =  self.html_processor.create_incident_table_html(incident_title, incident_summary_html, incident_description_html)

        # Generate HTML for maintenance summary
        maintenance_title = "Maintenance Summary"
        maintenance_description = self.openai_model.describe_data_frame_with_model(usage_summary)
        maintenance_description_html = self.html_processor.convert_markdown_to_html(maintenance_description)
        maintenance_summary_html = self.html_processor.df_to_html_table(maintenance_summary) 
        maintenance_summary_html = self.html_processor.create_maintenance_table_html(maintenance_title, maintenance_summary_html, maintenance_description_html)

        html_content_list.append(usage_summary_html)
        html_content_list.append(incident_summary_html)
        html_content_list.append(maintenance_summary_html)
        
        # logger.info("Plotting graphs and charts")
        # html_content_list = report_plotter.plot_graphs_and_charts()
        
        # Identify high-risk servers
        logger.info("Identifying top 10 high-risk servers by asset count")
        top_10_high_risk_servers_by_asset_count =  self.metrics_calculator.get_top_10_high_risk_servers_summary(complete_df)
        top_10_high_risk_servers_by_asset_html_table = self.plot_processor.high_risk_asset_html_table(top_10_high_risk_servers_by_asset_count)
        html_content_list.append(top_10_high_risk_servers_by_asset_html_table)
        
        # Generate PDF report
        # logger.info("Generating PDF report")
        # self.pdf_converter.generate_pdf(logger,html_content_list, self.container_client ,"Reports/pdf_report/High Risk Hardware Report.pdf")
        
        csv_filename,all_high_risk_servers = self.csv_processor.save_high_risk_servers_with_eol_to_csv(complete_df)
        self.blob_storage_manager.upload_dataframe_to_blob(all_high_risk_servers,csv_filename)
        
        # Initialize the EmailSender class
        email_sender = EmailSender()
        email_sender.send_mail()

