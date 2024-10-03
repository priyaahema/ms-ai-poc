import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

class EmailSender:
    def __init__(self, smtp_server, smtp_port, smtp_username, smtp_password):
        """
        Initializes the EmailSender object with SMTP server details.
        
        Args:
        - smtp_server (str): The SMTP server address.
        - smtp_port (int): The SMTP server port.
        - smtp_username (str): The username for the SMTP server.
        - smtp_password (str): The password for the SMTP server.
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_username = smtp_username
        self.smtp_password = smtp_password

    def send_email(self, sender_email, recipient_email, subject, body, pdf_folder, csv_folder):
        """
        Sends an email with an attachment.

        Args:
        - sender_email (str): The sender's email address.
        - recipient_email (str): The recipient's email address.
        - subject (str): The subject of the email.
        - body (str): The body text of the email.
        - pdf_folder (str): Path to the folder containing the PDF files.
        - csv_folder (str): Path to the folder containing CSV files.
        """
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject

        # Attach the email body
        msg.attach(MIMEText(body, 'plain'))

        # Attach the first PDF file found in the folder
        pdf_files = [f for f in os.listdir(pdf_folder) if f.endswith('.pdf')]
        if pdf_files:
            pdf_file_path = os.path.join(pdf_folder, pdf_files[0])  # Pick the first PDF file found
            self.attach_file_to_email(msg, pdf_file_path)
        else:
            print("No PDF file found in the pdf_data folder")

        # Attach all CSV files from csv_folder
        csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
        for csv_file in csv_files:
            csv_file_path = os.path.join(csv_folder, csv_file)
            self.attach_file_to_email(msg, csv_file_path)

        # Send the email
        self._send_message(msg)

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

        print('Email sent successfully!')

    def attach_file_to_email(self, message, file_path):
        """
        Attaches a file to the email message.

        Args:
        - message (MIMEMultipart): The email message object.
        - file_path (str): The file path to attach.
        """
        with open(file_path, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
            message.attach(part)
