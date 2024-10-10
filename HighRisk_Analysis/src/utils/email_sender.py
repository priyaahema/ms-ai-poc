import os
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from io import BytesIO
from azure.storage.blob import BlobServiceClient

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EmailSender:
    def __init__(self):
        # Fetch SMTP details from environment variables
        self.smtp_server = os.getenv('SMTP_SERVER')
        self.smtp_port = int(os.getenv('SMTP_PORT'))  # Port must be an integer
        self.smtp_username = os.getenv('SMTP_USERNAME')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.sender = os.getenv('SENDER')
        self.recipient = os.getenv('RECIPIENT')
        self.pdf_folder = 'Reports/pdf_report'
        self.csv_folder = 'Reports/csv_report'
        self.blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_STORAGE_CONNECTION_STRING'))
        self.BLOB_CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')

    def send_email(self, sender_email, recipient_email, subject, body, container_name, pdf_folder, csv_folder):
        """
        Sends an email with attachments fetched from Azure Blob Storage.

        Args:
        - sender_email (str): The sender's email address.
        - recipient_email (str): The recipient's email address.
        - subject (str): The subject of the email.
        - body (str): The body text of the email.
        - container_name (str): The name of the Azure Blob container.
        - pdf_folder (str): Folder name in the container where PDF files are stored.
        - csv_folder (str): Folder name in the container where CSV files are stored.
        """
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject

        # Attach the email body
        msg.attach(MIMEText(body, 'plain'))

        # Create a container client once and reuse it
        container_client = self.blob_service_client.get_container_client(container_name)

        # Attach the first PDF file from Azure Blob Storage
        pdf_files = self.get_blob_files(container_client, pdf_folder, '.pdf')
    
        if pdf_files:
            print(pdf_files)
            pdf_blob_data = self.download_blob_to_memory(container_client, pdf_folder, pdf_files[0])
            self.attach_file_to_email(msg, pdf_files[0], pdf_blob_data)
        else:
            print("No PDF file found in the pdf_report folder")

        # Attach all CSV files from Azure Blob Storage
        csv_files = self.get_blob_files(container_client, csv_folder, '.csv')
        print(csv_files)
        for csv_file in csv_files:
            csv_blob_data = self.download_blob_to_memory(container_client, csv_folder, csv_file)
            self.attach_file_to_email(msg, csv_file, csv_blob_data)

        # Send the email
        self._send_message(msg)

    def get_blob_files(self, container_client, folder_name, file_extension):
        """
        Lists all files in a specified folder in the Azure Blob Storage container with the given extension.

        Args:
        - container_client (ContainerClient): The container client object.
        - folder_name (str): The folder name in the container.
        - file_extension (str): The file extension to filter by (e.g., '.pdf' or '.csv').

        Returns:
        - List of file names in the specified folder.
        """
        blob_list = container_client.list_blobs(name_starts_with=folder_name)
        return [blob.name.split('/')[-1] for blob in blob_list if blob.name.endswith(file_extension)]

    def download_blob_to_memory(self, container_client, folder_name, file_name):
        """
        Downloads a blob file from Azure Blob Storage into memory.

        Args:
        - container_client (ContainerClient): The container client object.
        - folder_name (str): The folder name in the container.
        - file_name (str): The name of the file to download.

        Returns:
        - A BytesIO object containing the file data.
        """
        blob_client = container_client.get_blob_client(blob=f"{folder_name}/{file_name}")
        blob_data = blob_client.download_blob().readall()
        return BytesIO(blob_data)

    def attach_file_to_email(self, msg, file_name, file_data):
        """
        Attaches a file to the email from in-memory data.

        Args:
        - msg (MIMEMultipart): The email message.
        - file_name (str): The name of the file being attached.
        - file_data (BytesIO): The in-memory file data.
        """
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(file_data.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={file_name}')
        msg.attach(part)

    def _send_message(self, msg):
        """
        Connects to the SMTP server and sends the message.

        Args:
        - msg (MIMEMultipart): The email message to send.
        """
        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            server.starttls()  # Secure the connection
            server.login(self.smtp_username, self.smtp_password)
            server.send_message(msg)
        
        logging.info(f"Email sent Successfully to {self.recipient}")
         

    def send_mail(self):
        
        subject = 'High risk hardware analysis report'
        body = """
        Hello,

        Please find the attached PDF report and the corresponding CSV files for the High Risk Hardware Analysis.
        This is an automated email that is scheduled.

        """
        # Send the email with attachments
        self.send_email(self.sender, self.recipient, subject, body, self.BLOB_CONTAINER_NAME,self.pdf_folder, self.csv_folder)

