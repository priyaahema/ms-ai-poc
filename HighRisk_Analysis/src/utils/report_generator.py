import markdown2
import base64
from io import BytesIO
import matplotlib.pyplot as plt  
from datetime import datetime, timedelta
import pandas as pd
import pdfkit
import io
from PyPDF2 import PdfReader, PdfWriter
import logging
import os
import weasyprint

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HTMLProcessor:

    def __init__(self) -> None:
        pass

    @staticmethod
    def convert_markdown_to_html(markdown_text: str) -> str:
        """Convert Markdown text to HTML."""
        return markdown2.markdown(markdown_text)
    
    @staticmethod
    def df_to_html_table(dataframe: pd.DataFrame) -> str:
        # Convert DataFrame to HTML table
        html_table = dataframe.to_html(index=True) 
        return html_table

    @staticmethod
    def create_explanation_summary_formatted(title: str, explanation_html: str) -> str:
        """Generate the HTML for the explanation summary."""
        return f"""
        <div class="approach-explanation">
            <h2 class="approach-explanation-title">{title}</h2>
            <div class="approach-explanation-content">
                {explanation_html}
            </div>
        </div>
        """

    @staticmethod
    def create_count_assets_by_category_table(title: str, table_html: str, description: str) -> str:
        """Generate the HTML for the count of assets by category table."""
        return f"""
        <div class="count-assets-table-summary">
            <h2 class="count-assets-table-title">{title}</h2>
            <div class="count-assets-table-content">
                {table_html}
            </div>
            <p class="count-assets-table-description">{description}</p>
        </div>
        """
    
    @staticmethod
    def create_usage_table_html(title: str, table_html: str, description: str) -> str:
        """Generate the HTML for the usage summary table."""
        return f"""
        <div class="usage-summary">
            <h2 class="usage-table-title">{title}</h2>
            <div class="usage-table-content">
                {table_html}
            </div>
            <p class="usage-table-description">{description}</p>
        </div>
        """
    
    @staticmethod
    def create_incident_table_html(title: str, table_html: str, description: str) -> str:
        """Generate the HTML for the incident summary table."""
        return f"""
        <div class="incident-summary">
            <h2 class="incident-table-title">{title}</h2>
            <div class="incident-table-content">
                {table_html}
            </div>
            <p class="incident-table-description">{description}</p>
        </div>
        """
    
    @staticmethod
    def create_maintenance_table_html(title: str, table_html: str, description: str) -> str:
        """Generate the HTML for the maintenance summary table."""
        return f"""
        <div class="maintenance-summary">
            <h2 class="maintenance-table-title">{title}</h2>
            <div class="maintenance-table-content">
                {table_html}
            </div>
            <p class="maintenance-table-description">{description}</p>
        </div>
        """
    
class CSVProcessor:
    @staticmethod
    def save_to_csv(data, file_name):
        """Saves pandas DataFrame to a CSV file."""
        # Check if the data is a pandas DataFrame
        if isinstance(data, pd.DataFrame):
            data.to_csv(file_name, index=False)
            print(f"Data saved to {file_name}")
        else:
            print("Invalid data format. Please provide a pandas DataFrame.")

    @staticmethod
    def create_csv_server_stability(merged_data):
        """Save complete list of assets with relevant metrics."""
        dt = datetime.now().strftime('%Y%m%d')
        merged_data.to_csv(f'Reports/csv_report/Servers_Stability_{dt}.csv', index=False)

    @staticmethod
    def save_high_risk_servers_with_eol_to_csv(merged_data):
        """
        Filters high-risk servers into three categories based on their end-of-life date
        and saves them into a single CSV file, with the category label included.

        :param merged_data: DataFrame containing the server information
        :param current_date: Current date used for filtering
        :param one_month_from_now: Date one month from the current date for filtering
        """

        current_date = datetime.now()
        one_month_from_now = current_date + timedelta(days=30)

        # Filter data into three categories
        expired_servers = merged_data[(merged_data['risk_category'] == 'High Risk') & (merged_data['end_of_life_date'] < current_date)]
        expiring_soon_servers = merged_data[(merged_data['risk_category'] == 'High Risk') & (merged_data['end_of_life_date'] >= current_date) & (merged_data['end_of_life_date'] <= one_month_from_now)]
        expiring_later_servers = merged_data[(merged_data['risk_category'] == 'High Risk') & (merged_data['end_of_life_date'] > one_month_from_now)]

        # Function to add category labels and group data by company and end_of_life_date
        def group_data(data, category_label):
            if not data.empty:
                grouped_data = data.groupby(['company', 'end_of_life_date'])['hardware_asset_id'].apply(list).reset_index()
                grouped_data.columns = ['company', 'end_of_life_date', 'asset_ids']
                grouped_data['category'] = category_label  # Add the category column
                return grouped_data
            else:
                return pd.DataFrame(columns=['company', 'end_of_life_date', 'asset_ids', 'category'])

        # Group and label each category
        expired_grouped = group_data(expired_servers, 'expired')
        expiring_soon_grouped = group_data(expiring_soon_servers, 'expiring_within_a_month')
        expiring_later_grouped = group_data(expiring_later_servers, 'expiring_later')

        # Concatenate all categories into a single DataFrame
        all_high_risk_servers = pd.concat([expired_grouped, expiring_soon_grouped, expiring_later_grouped])

        # If the final DataFrame is not empty, save it to CSV
        if not all_high_risk_servers.empty:
            # Generate a timestamp for the filename
            dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

            # Save the concatenated DataFrame to a single CSV file
            csv_filename = f'Reports/csv_report/High_risk_expiring_and_expired_assets_report.csv'

            return csv_filename,all_high_risk_servers
            # all_high_risk_servers.to_csv(csv_filename, index=False)
           
        else:
            print("No high-risk servers found for the specified date range.")


class PlotProcessor:
    def __init__(self,openai_model):
        self.openai_model = openai_model

    def save_plot_to_html(self, fig, filename: str,html_content_list) -> None:
        """Save a plot to an HTML format with an image and its description."""
        # Save the figure to a BytesIO object
        buffer = BytesIO()
        fig.savefig(buffer, format='png')
        buffer.seek(0)
        img_data = base64.b64encode(buffer.read()).decode()

        # Generate image description (assumed to be defined elsewhere)
        description = self.openai_model.generate_image_description(img_data)
        converted_description = HTMLProcessor.convert_markdown_to_html(description)
        
    
        # Create HTML content with inline CSS
        html_content = f"""
        <div class="plot-container">
            <h3>{filename}</h3>
            <div class="plot-image"><img src="data:image/png;base64,{img_data}  alt={filename}></div>
            <div class="plot-description">{converted_description}</div>
        </div>
        """

        # Append image and description to HTML content list
        html_content_list.append(html_content)

    def high_risk_asset_html_table(self, top_10_expired_df: pd.DataFrame) -> str:
        """Generate HTML table for high-risk assets."""
        html_table = top_10_expired_df.to_html(index=False, border=0, justify='center')

        html_content = f"""
        <div class="highrisk-asset-wrapper">
        
                <div class="highrisk-asset-header">
                    <h2>High Risk Hardware Expiring and Expired Count</h2>
                </div>

                <div class="table-expired-count">
                    {html_table}
                </div>

                <div class="highrisk-asset-table-description">
                    <p>
                        This table highlights the top 10 companies with the highest risk assets based on total asset count. 
                        To view the complete table, refer to the 
                        <strong>highrisk_expiring_and_expired_assets_report.csv</strong>.
                    </p>
                </div>

                <div class="highrisk-asset-table-description">
                    <p>
                        Additionally, further details about all assets can be found in the 
                        <strong>Servers_Stability_`Date`.csv</strong> file, which contains metrics such as 
                        z-score, risk category, and composite stability score for each asset.
                    </p>
                </div>
            </div>
        """
        return html_content


class PDFConverter:
    def __init__(self):
        pass

    # def generate_title_page_pdf(self,logger):
    #     logger.info("generate_title_page")
    #     # Create an HTML string for the title page
    #     title_html = """
    #     <html>
    #         <head>
    #             <title>High Risk Hardware Report</title>
    #             <style>
    #                 body {
    #                     font-family: Arial, sans-serif;
    #                     font-size: 14px;
    #                     margin: 0; 
    #                     padding: 0; 
    #                     height: 100vh; 
    #                     position: relative;
    #                 }
    #                 .pdf-title {
    #                     position: absolute; 
    #                     top: 45%; 
    #                     left: 20%;
    #                     text-align: center; 
    #                     background-color: rgb(6, 6, 65); 
    #                     color: white; 
    #                     font-size: 32px; 
    #                     border-radius: 10px;
    #                     padding: 20px; 
    #                     padding-right: 80px;
    #                     padding-left: 80px;
    #                 }
    #             </style>
    #         </head>
    #         <body>
    #             <div class="pdf-title">High Risk Hardware Report</div>
    #         </body>
    #     </html>
    #     """

    #     # Convert the title HTML string to a PDF and capture it as bytes using stdout
    #     title_pdf_stream = io.BytesIO()
    #     pdf_bytes = pdfkit.from_string(title_html, False, options={
    #         'margin-top': '0.50in',
    #         'margin-right': '0.50in',
    #         'margin-bottom': '0.50in',
    #         'margin-left': '0.50in',
    #         'encoding': 'UTF-8',
    #         'enable-local-file-access': '',
    #         'no-outline': None
    #     },
    #     configuration = self.config
    #     )

    #     title_pdf_stream.write(pdf_bytes)
    #     title_pdf_stream.seek(0)  # Reset stream pointer to start
    #     return title_pdf_stream



    # @staticmethod
    # def generate_content_pdf(html_content_list):
    #     # Create an HTML string to hold the title page and plots with CSS for font and justification
    #     html_string = """
    #     <html>
    #         <head>
    #             <title>High Risk Hardware Report</title>
    #             <style>
    #                 body {
    #                     font-family: Arial, sans-serif;
    #                     font-size: 14px;
    #                     margin: 0; 
    #                     padding: 0; 
    #                     height: 100vh;
    #                     position: relative; 
    #                 }
    #                 .plot-container {
    #                     font-size: 16px;
    #                     # background-color: rgb(6, 6, 65);
    #                     # color: white;
    #                 }
    #                 .plot-image {
    #                     display: flex; 
    #                     justify-content: center; 
    #                     margin-bottom: 15px; 
    #                 }
    #                 .plot-image img {
    #                     max-width: 100%; 
    #                     height: auto;
    #                 }
    #                 .plot-container h3 {
    #                     font-size: 20px;
    #                     font-weight: bold;
    #                     margin-bottom: 20px;
    #                 }
    #                 .plot-description {
    #                     text-align: left;
    #                     margin-top: 15px; 
    #                 }
    #                 table {
    #                     width: 100%;
    #                     border-collapse: collapse;
    #                 }
    #                 th, td {
    #                     border: 1px solid #ccc;
    #                     padding: 8px;
    #                     text-align: left;
    #                 }
    #                 th {
    #                     background-color: rgb(6, 6, 65);
    #                     color: white;
    #                 }
    #                 tr:nth-child(even) {
    #                     background-color: #f2f2f2;
    #                 }
    #                 .summary-container {
    #                     text-align: center;
    #                     margin-bottom: 50px;
    #                 }
    #                 .summary-title {
    #                     margin: 20px 0;
    #                     font-size: 32px;
    #                     color: #063a65;
    #                 }
    #                 .explanation {
    #                     margin-top: 40px;
    #                     margin-bottom: 20px;
    #                 }
    #                 .explanation-title {
    #                     text-align: left;
    #                 }
    #                 .explanation-content {
    #                     border: 1px solid #ccc;
    #                     padding: 10px;
    #                     font-size: 16px;
    #                 }
    #                 .count-assets-table-summary,
    #                 .usage-summary,
    #                 .incident-summary,
    #                 .maintenance-summary {
    #                     margin-top: 40px;
    #                     margin-bottom: 20px;
    #                     font-size: 16px;
    #                 }
    #                 .count-assets-table-title,
    #                 .usage-table-title,
    #                 .incident-table-title,
    #                 .maintenance-table-title,
    #                 .highrisk-asset-table-title {
    #                     text-align: left;
    #                     font-size: 20px;
    #                     margin-bottom: 10px;
    #                 }
    #                 .count-assets-table-content,
    #                 .usage-table-content,
    #                 .incident-table-content,
    #                 .maintenance-table-content {
    #                     padding: 10px;
    #                     overflow-x: auto;
    #                     font-size: 16px;
    #                 }
    #                 .count-assets-table-description,
    #                 .usage-table-description,
    #                 .incident-table-description,
    #                 .maintenance-table-description {
    #                     margin-top: 10px;
    #                     font-size: 14px;
    #                     line-height: 1.5;
    #                 }
    #                 .highrisk-asset-wrapper {
    #                     padding: 20px;
    #                 }
    #                 .highrisk-asset-table-title {
    #                     font-size: 18px;
    #                     font-weight: bold;
    #                     margin-bottom: 10px;
    #                     text-align: left;
    #                 }
    #                 .table-expired-count {
    #                     margin-bottom: 20px;
    #                 }
    #                 .highrisk-asset-table-description {
    #                     font-size: 16px;
    #                     line-height: 1.5;
    #                     margin-top: 20px;
    #                 }
    #                 .asset-table {
    #                     margin-left: auto;
    #                     margin-right: auto;
    #                     border-collapse: collapse;
    #                     width: 100%;
    #                 }
    #                 .asset-table th {
    #                     background-color: #f2f2f2;
    #                     font-weight: bold;
    #                     padding: 10px;
    #                 }
    #                 .asset-table td {
    #                     padding: 8px;
    #                     text-align: center;
    #                 }
    #                 .approach-explanation {
    #                     font-family: Arial, sans-serif;
    #                     margin: 20px;
    #                     padding: 15px;
    #                 }
    #                 .approach-explanation-title {
    #                     font-size: 24px;
    #                     color: rgb(6, 6, 65);
    #                     margin-bottom: 10px;
    #                 }
    #                 .approach-explanation-content h3 {
    #                     font-size: 20px;
    #                     color: rgb(6, 6, 65);
    #                     margin-top: 15px;
    #                 }
    #                 .approach-explanation-content p {
    #                   font-size: 16px;
    #                     margin: 10px 0;
    #                     line-height: 2;
    #                 }
    #                 .approach-explanation-content ul {
    #                     margin: 10px 0;
    #                     padding-left: 20px;
    #                 }
    #                 .approach-explanation-content li {
    #                     margin-bottom: 5px;
    #                 }
    #             </style>
    #         </head>
    #         <body>
    #     """

    #     # Iterate through the list and add each content to a new page
    #     for content in html_content_list:
    #         html_string += f"<div style='page-break-after: always;'>\n{content}\n</div>\n"

    #     html_string += "</body></html>"

    #     # Convert the HTML string to a PDF and capture it as bytes using stdout
    #     content_pdf_stream = io.BytesIO()
    #     pdf_bytes = pdfkit.from_string(html_string, False, options={
    #         'margin-top': '0.75in',
    #         'margin-right': '0.75in',
    #         'margin-bottom': '0.5in',
    #         'margin-left': '0.75in',
    #         'encoding': 'UTF-8',
    #         'enable-local-file-access': '',
    #         'no-outline': None
    #     })
    #     content_pdf_stream.write(pdf_bytes)
    #     content_pdf_stream.seek(0)  # Reset stream pointer to start
    #     return content_pdf_stream

    
    # @staticmethod
    # def combine_pdfs(pdf_streams, output_filename):
    #     # Create a PdfWriter to combine the PDFs
    #     pdf_writer = PdfWriter()

    #     # Loop through each PDF stream and add its pages to the writer
    #     for pdf_stream in pdf_streams:
    #         pdf_reader = PdfReader(pdf_stream)
    #         for page_num in range(len(pdf_reader.pages)):
    #             pdf_writer.add_page(pdf_reader.pages[page_num])

    #     # Write the combined PDF to a file
    #     with open(output_filename, 'wb') as output_file:
    #         pdf_writer.write(output_file)
    
        # @staticmethod
    # def generate_pdf(html_content_list, output_filename):
    #     title_page_pdf = PDFConverter.generate_title_page_pdf()
    #     content_pdf = PDFConverter.generate_content_pdf(html_content_list)
    #     # Combine the PDFs and save the final result
    #     PDFConverter.combine_pdfs([title_page_pdf, content_pdf],output_filename)


    def generate_title_page_pdf(self, logger): 
        logger.info("generate_title_page")

        # Create an HTML string for the title page
        title_html = """
        <html>
            <head>
                <title>High Risk Hardware Report</title>
                 <style>
                    body {
                        font-family: Arial, sans-serif;
                        font-size: 14px;
                        margin: 0; 
                        padding: 0; 
                        position: relative;
                    }
                    .pdf-title {
                        background-color: rgb(6, 6, 65);
                        text-align: center;
                        margin-top: 300px;
                        padding: 50px;
                        border-radius: 10px;
                        color: white;
                        font-size: 24px;
                    }
                </style>
            </head>
            <body>
                <div class="pdf-title">High Risk Hardware Report</div>
            </body>
        </html>
        """

        # Convert the title HTML string to a PDF and capture it as bytes using WeasyPrint
        title_pdf_stream = io.BytesIO()
        
        # WeasyPrint's 'HTML' object can convert HTML to PDF directly
        pdf = weasyprint.HTML(string=title_html).write_pdf()
        
        # Write the PDF data to the BytesIO stream
        title_pdf_stream.write(pdf)
        title_pdf_stream.seek(0)  # Reset stream pointer to start

        return title_pdf_stream

    def generate_content_pdf(self,html_content_list):
        # Create an HTML string to hold the title page and plots with CSS for font and justification
        html_string = """
        <html>
            <head>
                <title>High Risk Hardware Report</title>
            <style>
                    body {
                        font-family: Arial, sans-serif;
                        font-size: 14px;
                        margin: 0; 
                        padding: 0; 
                        position: relative; 
                    }
                    .plot-container {
                        font-size: 14px;
                    }
                    .plot-container h3 {
                        font-size: 20px;
                        font-weight: bold;
                        margin-bottom: 20px;
                    }
                    .plot-description {
                        text-align: left;
                        margin-top: 15px; 
                    }
                    img {
                        width: 600px;
                        margin: 0 auto;
                    }
                    table {
                        width: 100%;
                        border-collapse: collapse;
                    }
                    th, td {
                        border: 1px solid #ccc;
                        padding: 8px;
                        text-align: left;
                    }
                    th {
                        background-color: rgb(6, 6, 65);
                        color: white;
                    }
                    tr:nth-child(even) {
                        background-color: #f2f2f2;
                    }
                    .summary-container {
                        text-align: center;
                        margin-bottom: 50px;
                    }
                    .summary-title {
                        margin: 20px 0;
                        font-size: 32px;
                        color: #063a65;
                    }
                    .explanation {
                        margin-top: 40px;
                        margin-bottom: 20px;
                    }
                    .explanation-title {
                        text-align: left;
                    }
                    .explanation-content {
                        border: 1px solid #ccc;
                        padding: 10px;
                        font-size: 14px;
                    }
                    .count-assets-table-summary,
                    .usage-summary,
                    .incident-summary,
                    .maintenance-summary {
                        margin-top: 40px;
                        margin-bottom: 20px;
                        font-size: 16px;
                    }
                    .count-assets-table-title,
                    .usage-table-title,
                    .incident-table-title,
                    .maintenance-table-title,
                    .highrisk-asset-table-title {
                        text-align: left;
                        font-size: 20px;
                        margin-bottom: 10px;
                    }
                    .count-assets-table-content,
                    .usage-table-content,
                    .incident-table-content,
                    .maintenance-table-content {
                        padding: 10px;
                        font-size: 14px;
                    }
                    .count-assets-table-description,
                    .usage-table-description,
                    .incident-table-description,
                    .maintenance-table-description {
                        margin-top: 10px;
                        font-size: 14px;
                        line-height: 1.5;
                    }
                    .highrisk-asset-wrapper {
                        padding: 20px;
                    }
                    .highrisk-asset-table-title {
                        font-size: 18px;
                        font-weight: bold;
                        margin-bottom: 10px;
                        text-align: left;
                    }
                    .table-expired-count {
                        margin-bottom: 20px;
                    }
                    .highrisk-asset-table-description {
                        font-size: 16px;
                        line-height: 1.5;
                        margin-top: 20px;
                    }
                    .asset-table {
                        margin-left: auto;
                        margin-right: auto;
                        border-collapse: collapse;
                        width: 100%;
                    }
                    .asset-table th {
                        background-color: #f2f2f2;
                        font-weight: bold;
                        padding: 10px;
                    }
                    .asset-table td {
                        padding: 8px;
                        text-align: center;
                    }
                    .approach-explanation {
                        font-family: Arial, sans-serif;
                        margin: 20px;
                        padding: 15px;
                    }
                    .approach-explanation-title {
                        font-size: 24px;
                        color: rgb(6, 6, 65);
                        margin-bottom: 10px;
                    }
                    .approach-explanation-content h3 {
                        font-size: 18px;
                        color: rgb(6, 6, 65);
                        margin-top: 15px;
                    }
                    .approach-explanation-content p {
                      font-size: 14px;
                        margin: 10px 0;
                        line-height: 2;
                    }
                    .approach-explanation-content ul {
                        margin: 10px 0;
                        padding-left: 20px;
                    }
                    .approach-explanation-content li {
                        margin-bottom: 5px;
                    }
            </style>
            </head>
            <body>
        """

        # Iterate through the list and add each content to a new page
        for content in html_content_list:
            html_string += f"<div style='page-break-after: always;'>\n{content}\n</div>\n"

        html_string += "</body></html>"

               # Convert the title HTML string to a PDF and capture it as bytes using WeasyPrint
        title_pdf_stream = io.BytesIO()
        
        # WeasyPrint's 'HTML' object can convert HTML to PDF directly
        pdf = weasyprint.HTML(string=html_string).write_pdf()
        
        # Write the PDF data to the BytesIO stream
        title_pdf_stream.write(pdf)
        title_pdf_stream.seek(0)  # Reset stream pointer to start

        return title_pdf_stream

    
    def upload_pdf_to_blob(self, pdf_stream, container_client, blob_name, logger):
        """
        Uploads a PDF file from a BytesIO stream to Azure Blob Storage.
        """
        try:
            logger.info(f"Entering the uploading function ")
            # Upload the in-memory PDF stream to Azure Blob Storage
            container_client.upload_blob(
                blob_name,  # 'name' argument is passed here
                data=pdf_stream,  # PDF stream as 'data'
                overwrite=True     # 'overwrite' flag if you want to replace the blob if it exists
            )
            logger.info(f"Successfully uploaded PDF to Azure Blob: {blob_name}")
        except Exception as e:
            logger.error(f"Error uploading PDF to Blob Storage: {e}")
            raise

    def combine_pdfs(self,pdf_streams):
        """
        Combine PDF streams and return the result as a BytesIO object.
        """
        # Create a PdfWriter to combine the PDFs
        pdf_writer = PdfWriter()

        # Loop through each PDF stream and add its pages to the writer
        for pdf_stream in pdf_streams:
            pdf_reader = PdfReader(pdf_stream)
            for page_num in range(len(pdf_reader.pages)):
                pdf_writer.add_page(pdf_reader.pages[page_num])
        
        logger.info("pdf steams")

        # Create a BytesIO buffer to hold the combined PDF in memory
        output_stream = io.BytesIO()
        pdf_writer.write(output_stream)
        
        # Move the cursor to the start of the stream
        output_stream.seek(0)
        

        return output_stream       

    def generate_pdf(self,logger,html_content_list, container_client , blob_name):
        """
        Generate PDFs for title and content, combine them, and upload to Azure Blob Storage.
        """
        # Generate PDFs for the title and content in memory
        title_page_pdf = self.generate_title_page_pdf(logger)
        content_pdf =  self.generate_content_pdf(html_content_list)

        # Combine the PDFs in memory
        combined_pdf_stream = self.combine_pdfs([title_page_pdf, content_pdf])
        logger.info("pdf combined")
        # Upload the combined PDF to Azure Blob Storage
        self.upload_pdf_to_blob(combined_pdf_stream, container_client,blob_name,logger)