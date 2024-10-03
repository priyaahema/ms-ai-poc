import os
import logging
from src.data_processor.data_loader import DataLoader
from src.data_analysis.usage_scorer import UsageScorer
from src.data_processor.data_processor import DataProcessor
from src.metrics import MetricsCalculator,RiskCategorizer
from src.utils.report_generator import HTMLProcessor,CSVProcessor,PlotProcessor,PDFConverter
from src.source_code import code_text
from src.utils.model import OpenAIModel
from src.data_analysis.visualizations import ReportPlotter
from src.utils.email_sender import EmailSender
from dotenv import load_dotenv
from src.data_analysis.vulnerability_scorer import VulnerabilityScorer
from src.data_analysis.incident_scorer import IncidentScoring      
from src.data_analysis.maintenance_scorer import MaintenanceAnalyzer 

# Load environment variables from the .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting main execution")

    # Access environment variables
    API_BASE = os.getenv('API_BASE')
    API_KEY = os.getenv('API_KEY')
    DEPLOYMENT_NAME = os.getenv('DEPLOYMENT_NAME')
    API_VERSION = os.getenv('API_VERSION')
    
    if not API_BASE or not API_KEY or not DEPLOYMENT_NAME or not API_VERSION:
        logger.error("API credentials are missing from environment variables")
        return

    logger.info("Loaded API credentials successfully")

    html_content_list = []

    data_dir = 'data'
   
    data_files = [
        os.path.join(data_dir, 'hw_servers_usage_5.csv'),
        os.path.join(data_dir, 'hw_incidents_5.csv'),
        os.path.join(data_dir, 'patchupgrades.csv'),
        os.path.join(data_dir, 'hw_warranty_5.csv'),
        os.path.join(data_dir, 'hw_servers_5.csv'),
        os.path.join(data_dir, 'hw_vulnerabilities_data.csv'),
    ]

    weights_composite_stability_score = {
        'overall_usage_score': 0.3,
        'overall_incident_score': 0.4,
        'maintenance_score': 0.5,
        'vulnerability_score':0.2
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

    logger.info("Initializing OpenAI Model")
    openai_model = OpenAIModel(api_base=API_BASE, api_key=API_KEY, deployment_name=DEPLOYMENT_NAME, api_version=API_VERSION)
    
    data_loader = DataLoader()
    data_processor = DataProcessor()
    metrics_calculator = MetricsCalculator()
    risk_categorizer = RiskCategorizer()
    html_processor = HTMLProcessor()
    csv_processor = CSVProcessor()
    plot_processor = PlotProcessor(openai_model)
    pdf_converter = PDFConverter()
    
    logger.info("Loading datasets")
    # Load datasets
    hw_server_usage, hw_incidents, patch_upgrades, hw_warrant, hw_server,hw_vulnerability = data_loader.load_datasets(data_files)
     
    # Process datasets
    logger.info("Processing incidents and metrics")
    
    patch_upgrades_analyzer = MaintenanceAnalyzer(patch_upgrades)
    patch_upgrades_analyzer.calculate_overall_maintenance_score()
    maintenance_scores_calculated = patch_upgrades_analyzer.aggregate_by_hardware_asset()
    
    
    incident_scorer = IncidentScoring(hw_incidents, hw_server)
    incident_scorer.incident_stability()
    incident_scores_calculated = incident_scorer.incident_df_with_all_scores()


    hw_vulnerability.columns = hw_vulnerability.columns.str.replace('ï»¿', '')
    vulnerability_scorer = VulnerabilityScorer(hw_vulnerability)
    vulnerability_scorer.calculate_vulnerability_stability()
    vulnerability_summary = vulnerability_scorer.generate_vulnerability_summary()

    
    usage_scorer = UsageScorer()
    weighted_usage_scores = usage_scorer.add_weighted_usage_scores(hw_server_usage)
    
    # Merge datasets
    logger.info("Merging datasets")
    merged_data = data_processor.merge_data(hw_server, weighted_usage_scores, incident_scores_calculated ,maintenance_scores_calculated, hw_warrant,vulnerability_summary)
    
    # Handle missing values
    logger.info("Handling missing values")
    merged_data = data_processor.handle_missing_values(merged_data)
    
    # # Add company names
    merged_data = data_processor.add_company_names(merged_data, hw_server, hw_incidents)
    
    # Calculate stability scores
    logger.info("Calculating stability scores")
    merged_data =  metrics_calculator.calculate_stability_scores(merged_data,weights_composite_stability_score)

    # Calculate risk categorization
    logger.info("Categorizing asset risk")
    complete_df, categorized_df, risk_counts =  risk_categorizer.categorize_asset_risk(merged_data)
    
    # Generate reports
    logger.info("Generating reports")
    report_plotter = ReportPlotter(complete_df,html_content_list,openai_model,plot_processor)
    
    # List all data files
    title = "The Approach to Identify the Risk Assets"
    approach_explanation = openai_model.explain_risk_asset_identification(code_text)
    approach_explanation_html = html_processor.convert_markdown_to_html(approach_explanation)
    formatted_approach_explanation_html = html_processor.create_explanation_summary_formatted(title,approach_explanation_html)
    html_content_list.append(formatted_approach_explanation_html)

    csv_processor.create_csv_server_stability(complete_df)

    # Count of Assets by Category
    logger.info("Summarizing assets by category")
    count_of_assets_by_category_table_title = "Count of Assets by Category"
    count_of_assets_by_category =  metrics_calculator.summarize_risk_categories(complete_df)
    count_of_assets_description = openai_model.describe_data_frame_with_model(count_of_assets_by_category)
    count_of_assets_description_html = html_processor.convert_markdown_to_html(count_of_assets_description)
    count_of_assets_by_category_html = html_processor.df_to_html_table(count_of_assets_by_category)
    count_of_assets_by_category_html_formatted = html_processor.create_count_assets_by_category_table(count_of_assets_by_category_table_title,count_of_assets_by_category_html,count_of_assets_description_html)

    html_content_list.append(count_of_assets_by_category_html_formatted)

    # Summary of Usage/Incident/Maintenance Metrics
    logger.info("Generating usage, incident, and maintenance summaries")
    usage_summary, incident_summary, maintenance_summary =  metrics_calculator.display_metrics_summary(complete_df)

    # Generate HTML for usage summary
    usage_title = "Usage Summary"
    usage_description =  openai_model.describe_data_frame_with_model(usage_summary)
    usage_description_html = html_processor.convert_markdown_to_html(usage_description)
    usage_summary_html =  html_processor.df_to_html_table(usage_summary) 
    usage_summary_html =  html_processor.create_usage_table_html(usage_title, usage_summary_html,  usage_description_html)

    # Generate HTML for incident summary
    incident_title = "Incident Summary"
    incident_description = openai_model.describe_data_frame_with_model(usage_summary)
    incident_description_html = html_processor.convert_markdown_to_html(incident_description)
    incident_summary_html =  html_processor.df_to_html_table(incident_summary) 
    incident_summary_html =  html_processor.create_incident_table_html(incident_title, incident_summary_html, incident_description_html)

    # Generate HTML for maintenance summary
    maintenance_title = "Maintenance Summary"
    maintenance_description = openai_model.describe_data_frame_with_model(usage_summary)
    maintenance_description_html = html_processor.convert_markdown_to_html(maintenance_description)
    maintenance_summary_html = html_processor.df_to_html_table(maintenance_summary) 
    maintenance_summary_html = html_processor.create_maintenance_table_html(maintenance_title, maintenance_summary_html, maintenance_description_html)

    html_content_list.append(usage_summary_html)
    html_content_list.append(incident_summary_html)
    html_content_list.append(maintenance_summary_html)
    
    logger.info("Plotting graphs and charts")
    html_content_list = report_plotter.plot_graphs_and_charts()
    
    # complete_df = data_processor.assign_random_values_for_missing_values(complete_df)

    # Identify high-risk servers
    logger.info("Identifying top 10 high-risk servers by asset count")
    top_10_high_risk_servers_by_asset_count =  metrics_calculator.get_top_10_high_risk_servers_summary(complete_df)
    top_10_high_risk_servers_by_asset_html_table = plot_processor.high_risk_asset_html_table(top_10_high_risk_servers_by_asset_count)
    html_content_list.append(top_10_high_risk_servers_by_asset_html_table)
     
    # Generate PDF report
    logger.info("Generating PDF report")
    pdf_converter.generate_pdf_from_html_list_pdf_kit(html_content_list, "Reports/pdf_report/High Risk Hardware Report.pdf")
    csv_processor.save_high_risk_servers_with_eol_to_csv(complete_df)

    logger.info("Main execution completed")
    
    # sender = 'hemuhema2000@gmail.com'
    # recipient = 'hariprasath.viswanathan@cdw.com'
    # subject = 'High risk hardware analysis report'
    # body = """
    # Hello,

    # Please find the attached PDF report and the corresponding CSV files for the High Risk Hardware Analysis.
    # This is an automated email that is scheduled.

    # """
    # pdf_folder = 'Reports/pdf_report'
    # csv_folder = 'Reports/csv_report'

    # # Fetch SMTP details from environment variables
    # smtp_server = os.getenv('SMTP_SERVER')
    # smtp_port = int(os.getenv('SMTP_PORT'))  # Port must be an integer
    # smtp_username = os.getenv('SMTP_USERNAME')
    # smtp_password = os.getenv('SMTP_PASSWORD')

    # # Initialize the EmailSender class
    # email_sender = EmailSender(smtp_server, smtp_port, smtp_username, smtp_password)

    # # Send the email with attachments
    # email_sender.send_email(sender, recipient, subject, body, pdf_folder, csv_folder)

if __name__ == '__main__':
    main()