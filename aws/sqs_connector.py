"""
sqs_connector.py

This module provides a class to interact with AWS SQS. It includes methods for
sending, receiving, and deleting messages, as well as handling batch operations
and FIFO queue requirements.
"""
import boto3
from botocore.exceptions import ClientError
from typing import Optional, Dict, List
import uuid
import logging
from utils.config_loader import config_loader

# Configure basic logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)ss - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class SqsConnector:
    """
    A connector class for AWS SQS operations.
    """

    def __init__(self, profile_name: Optional[str] = None, config_section: str = "S101_SQS"):
        """
        Initializes the SQS client.

        Args:
            profile_name (Optional[str]): The AWS profile name to use. If None,
                                        uses environment variables from config.
            config_section (str): SQS configuration section name (e.g., 'S101_SQS', 'S102_SQS')
        """
        self.profile_name = profile_name
        self.config_section = config_section
        self.sqs_config = None
        if not profile_name:
            self.sqs_config = config_loader.get_sqs_config(config_section)
        self.sqs_client = self._get_sqs_client()

    def _get_sqs_client(self):
        """
        Helper method to create an SQS client.
        """
        try:
            if self.profile_name:
                # Use AWS profile
                session = boto3.Session(profile_name=self.profile_name)
                client = session.client('sqs')
                logger.info(f"SQS client created using AWS profile: {self.profile_name}")
                return client
            elif self.sqs_config:
                # Use environment variables from config.ini
                client = boto3.client(
                    'sqs',
                    region_name=self.sqs_config.get('region', 'us-east-1'),
                    aws_access_key_id=self.sqs_config.get('access_key_id'),
                    aws_secret_access_key=self.sqs_config.get('secret_access_key'),
                    aws_session_token=self.sqs_config.get('session_token')  # Optional for temporary credentials
                )
                logger.info(f"SQS client created using config section: {self.config_section}")
                return client
            else:
                # Fall back to default credentials (environment variables, IAM role, etc.)
                client = boto3.client('sqs')
                logger.info("SQS client created using default credentials")
                return client
        except ClientError as e:
            logger.error(f"Error creating SQS client: {e}")
            raise

    def get_queue_url(self) -> str:
        """
        Get the queue URL from configuration.
        
        Returns:
            Queue URL from configuration
        """
        if self.sqs_config:
            return self.sqs_config.get('queue_url', '')
        return ''

    def test_connection(self, queue_url: str = None) -> bool:
        """
        Tests the connection to an SQS queue by attempting to get its attributes.

        Args:
            queue_url (str): The URL of the SQS queue. If None, uses queue URL from config.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        try:
            # Use queue URL from config if not provided
            if queue_url is None:
                queue_url = self.get_queue_url()
            
            if not queue_url:
                logger.error("No queue URL provided and none found in configuration")
                return False
                
            self.sqs_client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
            )
            logger.info(f"Successfully connected to SQS queue: {queue_url}")
            return True
        except ClientError as e:
            logger.error(f"Connection test failed for queue {queue_url}: {e}")
            return False

    def send_message(self, 
                     queue_url: str, 
                     message_body: str, 
                     message_group_id: Optional[str] = None,
                     message_deduplication_id: Optional[str] = None) -> Dict:
        """
        Sends a single message to an SQS queue.

        Args:
            queue_url (str): The URL of the SQS queue.
            message_body (str): The body of the message.
            message_group_id (Optional[str]): The message group ID for FIFO queues.
            message_deduplication_id (Optional[str]): The deduplication ID for FIFO queues.

        Returns:
            Dict: The response from the SQS send_message API call.
        """
        try:
            params = {
                'QueueUrl': queue_url,
                'MessageBody': message_body,
            }
            if message_group_id:
                params['MessageGroupId'] = message_group_id
            if message_deduplication_id:
                params['MessageDeduplicationId'] = message_deduplication_id
                
            response = self.sqs_client.send_message(**params)
            logger.info(f"Message sent to {queue_url} with ID: {response.get('MessageId')}")
            return response
        except ClientError as e:
            logger.error(f"Failed to send message to queue {queue_url}: {e}")
            raise

    def send_message_with_attributes(self, 
                                     queue_url: str, 
                                     message_body: str, 
                                     message_attributes: Dict) -> Dict:
        """
        Sends a single message with custom attributes.

        Args:
            queue_url (str): The URL of the SQS queue.
            message_body (str): The body of the message.
            message_attributes (Dict): A dictionary of message attributes.

        Returns:
            Dict: The response from the SQS send_message API call.
        """
        try:
            response = self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                MessageAttributes=message_attributes
            )
            logger.info(f"Message with attributes sent to {queue_url} with ID: {response.get('MessageId')}")
            return response
        except ClientError as e:
            logger.error(f"Failed to send message with attributes to queue {queue_url}: {e}")
            raise

    def send_message_batch(self, queue_url: str, entries: List[Dict]) -> Dict:
        """
        Sends a batch of messages to an SQS queue.

        Args:
            queue_url (str): The URL of the SQS queue.
            entries (List[Dict]): A list of dictionaries, where each dictionary
                                  represents a message.

        Returns:
            Dict: The response from the SQS send_message_batch API call.
        """
        try:
            response = self.sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            successful = len(response.get('Successful', []))
            failed = len(response.get('Failed', []))
            logger.info(f"Batch sent to {queue_url}. Successful: {successful}, Failed: {failed}")
            return response
        except ClientError as e:
            logger.error(f"Failed to send batch messages to queue {queue_url}: {e}")
            raise

    def send_file_as_messages(self, queue_url: str, filename: str, line_by_line: bool = True) -> Dict:
        """
        Reads a file and sends its content to an SQS queue.

        Args:
            queue_url (str): The URL of the SQS queue.
            filename (str): The path to the file to be sent.
            line_by_line (bool): If True, sends each line as a separate message.
                                 If False, sends the entire file content as one message.

        Returns:
            Dict: A dictionary with the number of successfully sent messages.
        """
        success_count = 0
        try:
            with open(filename, 'r') as f:
                if line_by_line:
                    for line in f:
                        if line.strip():  # Skip empty lines
                            self.send_message(queue_url, line.strip())
                            success_count += 1
                else:
                    content = f.read()
                    if content.strip():
                        self.send_message(queue_url, content)
                        success_count = 1
        except FileNotFoundError:
            logger.error(f"File not found: {filename}")
            raise
        except Exception as e:
            logger.error(f"Error sending file to SQS: {e}")
            raise
        
        return {'success_count': success_count}


    def receive_messages(self, queue_url: str, max_messages: int = 10) -> List[Dict]:
        """
        Receives messages from an SQS queue.

        Args:
            queue_url (str): The URL of the SQS queue.
            max_messages (int): The maximum number of messages to retrieve (1-10).

        Returns:
            List[Dict]: A list of received messages. Returns an empty list if no messages.
        """
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=20,  # Long polling
                MessageAttributeNames=['All']
            )
            messages = response.get('Messages', [])
            logger.info(f"Received {len(messages)} messages from {queue_url}")
            return messages
        except ClientError as e:
            logger.error(f"Failed to receive messages from queue {queue_url}: {e}")
            return []

    def delete_message(self, queue_url: str, receipt_handle: str):
        """
        Deletes a single message from an SQS queue.

        Args:
            queue_url (str): The URL of the SQS queue.
            receipt_handle (str): The receipt handle of the message to delete.
        """
        try:
            self.sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.info("Message deleted successfully.")
        except ClientError as e:
            logger.error(f"Failed to delete message with receipt handle {receipt_handle}: {e}")
            raise

    def purge_queue(self, queue_url: str):
        """
        Purges all messages from an SQS queue.

        Args:
            queue_url (str): The URL of the SQS queue.
        """
        try:
            self.sqs_client.purge_queue(
                QueueUrl=queue_url
            )
            logger.info(f"Purge request sent for queue {queue_url}")
        except ClientError as e:
            logger.error(f"Failed to purge queue {queue_url}: {e}")
            raise

# Initialize a default SqsConnector instance for convenience
# Note: This assumes a default AWS profile. For specific profiles,
# instantiate the class with the profile name.
sqs_connector = SqsConnector()

