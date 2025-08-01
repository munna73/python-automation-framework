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

    def __init__(self, profile_name: Optional[str] = None):
        """
        Initializes the SQS client.

        Args:
            profile_name (Optional[str]): The AWS profile name to use. If None,
                                        the default profile is used.
        """
        self.profile_name = profile_name
        self.sqs_client = self._get_sqs_client()

    def _get_sqs_client(self):
        """
        Helper method to create an SQS client.
        """
        try:
            if self.profile_name:
                session = boto3.Session(profile_name=self.profile_name)
                return session.client('sqs')
            else:
                return boto3.client('sqs')
        except ClientError as e:
            logger.error(f"Error creating SQS client: {e}")
            raise

    def test_connection(self, queue_url: str) -> bool:
        """
        Tests the connection to an SQS queue by attempting to get its attributes.

        Args:
            queue_url (str): The URL of the SQS queue.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        try:
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

