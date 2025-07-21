"""
AWS SQS connector for sending and receiving messages from SQS queues.
"""
import boto3
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from botocore.exceptions import ClientError, NoCredentialsError
from utils.config_loader import config_loader
from utils.logger import logger

class SQSConnector:
    """AWS SQS connector for message operations."""
    
    def __init__(self):
        """Initialize SQS connector."""
        self.sqs_client = None
        self.sqs_resource = None
        self.aws_config = None
        self.setup_aws_connection()
    
    def setup_aws_connection(self):
        """Setup AWS SQS connection using environment variables."""
        try:
            self.aws_config = config_loader.get_aws_config()
            
            # Create SQS client and resource
            self.sqs_client = boto3.client(
                'sqs',
                region_name=self.aws_config.get('region', 'us-east-1'),
                aws_access_key_id=self.aws_config.get('access_key_id'),
                aws_secret_access_key=self.aws_config.get('secret_access_key')
            )
            
            self.sqs_resource = boto3.resource(
                'sqs',
                region_name=self.aws_config.get('region', 'us-east-1'),
                aws_access_key_id=self.aws_config.get('access_key_id'),
                aws_secret_access_key=self.aws_config.get('secret_access_key')
            )
            
            logger.info("AWS SQS connection established successfully")
            
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
            raise
        except Exception as e:
            logger.error(f"Failed to setup AWS SQS connection: {e}")
            raise
    
    def send_message(self, 
                    queue_url: str, 
                    message_body: str,
                    message_attributes: Dict[str, Any] = None,
                    delay_seconds: int = 0) -> Dict[str, Any]:
        """
        Send a message to SQS queue.
        
        Args:
            queue_url: SQS queue URL
            message_body: Message content
            message_attributes: Optional message attributes
            delay_seconds: Delay before message becomes available
            
        Returns:
            Send message response
        """
        try:
            # Check if it's a FIFO queue
            is_fifo = queue_url.endswith('.fifo')
            
            send_params = {
                'QueueUrl': queue_url,
                'MessageBody': message_body
            }
            
            # Add message attributes if provided
            if message_attributes:
                send_params['MessageAttributes'] = self._format_message_attributes(message_attributes)
            
            # Add delay if specified
            if delay_seconds > 0:
                send_params['DelaySeconds'] = delay_seconds
            
            # FIFO queue specific parameters
            if is_fifo:
                # Generate unique message group ID
                message_group_id = self._generate_group_id()
                send_params['MessageGroupId'] = message_group_id
                
                # Generate deduplication ID to prevent duplicates
                deduplication_id = self._generate_deduplication_id(message_body)
                send_params['MessageDeduplicationId'] = deduplication_id
                
                logger.debug(f"FIFO queue - Group ID: {message_group_id}, Dedup ID: {deduplication_id}")
            
            response = self.sqs_client.send_message(**send_params)
            
            logger.info(f"Message sent to SQS queue - MessageId: {response.get('MessageId')}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to send message to SQS: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error sending message to SQS: {e}")
            raise
    
    def send_file_as_messages(self, 
                             queue_url: str, 
                             file_path: str,
                             line_by_line: bool = True) -> Dict[str, Any]:
        """
        Send file content to SQS queue.
        
        Args:
            queue_url: SQS queue URL
            file_path: Path to file to send
            line_by_line: If True, send each line as separate message; if False, send entire file as one message
            
        Returns:
            Summary of send operations
        """
        try:
            file_path_obj = Path(file_path)
            
            if not file_path_obj.exists():
                # Try looking in data directory
                file_path_obj = Path(__file__).parent.parent / "data" / "input" / file_path
            
            if not file_path_obj.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            logger.info(f"Sending file to SQS: {file_path_obj.name} (line_by_line: {line_by_line})")
            
            success_count = 0
            error_count = 0
            errors = []
            
            with open(file_path_obj, 'r', encoding='utf-8') as f:
                if line_by_line:
                    # Send each line as a separate message
                    for line_number, line in enumerate(f, 1):
                        line = line.rstrip('\n\r')
                        if line:  # Skip empty lines
                            try:
                                message_attributes = {
                                    'source_file': {'StringValue': file_path_obj.name, 'DataType': 'String'},
                                    'line_number': {'StringValue': str(line_number), 'DataType': 'Number'}
                                }
                                
                                self.send_message(queue_url, line, message_attributes)
                                success_count += 1
                                
                            except Exception as e:
                                error_count += 1
                                errors.append(f"Line {line_number}: {str(e)}")
                else:
                    # Send entire file as one message
                    file_content = f.read()
                    try:
                        message_attributes = {
                            'source_file': {'StringValue': file_path_obj.name, 'DataType': 'String'},
                            'file_size': {'StringValue': str(len(file_content)), 'DataType': 'Number'}
                        }
                        
                        self.send_message(queue_url, file_content, message_attributes)
                        success_count = 1
                        
                    except Exception as e:
                        error_count = 1
                        errors.append(f"File send error: {str(e)}")
            
            results = {
                'file_path': str(file_path_obj),
                'mode': 'line_by_line' if line_by_line else 'entire_file',
                'success_count': success_count,
                'error_count': error_count,
                'total_attempts': success_count + error_count,
                'success_rate': (success_count / (success_count + error_count) * 100) if (success_count + error_count) > 0 else 0,
                'errors': errors
            }
            
            logger.info(f"File send completed: {success_count} successful, {error_count} errors")
            return results
            
        except Exception as e:
            logger.error(f"Error sending file to SQS: {e}")
            raise
    
    def receive_messages(self, 
                        queue_url: str,
                        max_messages: int = 10,
                        wait_time_seconds: int = 20,
                        visibility_timeout: int = 30) -> List[Dict[str, Any]]:
        """
        Receive messages from SQS queue.
        
        Args:
            queue_url: SQS queue URL
            max_messages: Maximum number of messages to receive (1-10)
            wait_time_seconds: Long polling wait time
            visibility_timeout: How long messages are hidden from other consumers
            
        Returns:
            List of received messages
        """
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=min(max_messages, 10),  # AWS limit is 10
                WaitTimeSeconds=wait_time_seconds,
                VisibilityTimeoutSeconds=visibility_timeout,
                MessageAttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            logger.info(f"Received {len(messages)} messages from SQS queue")
            
            return messages
            
        except ClientError as e:
            logger.error(f"Failed to receive messages from SQS: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error receiving messages from SQS: {e}")
            raise
    
    def delete_message(self, queue_url: str, receipt_handle: str) -> bool:
        """
        Delete a message from SQS queue.
        
        Args:
            queue_url: SQS queue URL
            receipt_handle: Receipt handle of the message to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            
            logger.debug("Message deleted from SQS queue")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete message from SQS: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting message from SQS: {e}")
            return False
    
    def get_queue_attributes(self, queue_url: str) -> Dict[str, Any]:
        """
        Get queue attributes and statistics.
        
        Args:
            queue_url: SQS queue URL
            
        Returns:
            Queue attributes
        """
        try:
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
            )
            
            attributes = response.get('Attributes', {})
            
            queue_info = {
                'queue_url': queue_url,
                'message_count': int(attributes.get('ApproximateNumberOfMessages', 0)),
                'in_flight_count': int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0)),
                'delayed_count': int(attributes.get('ApproximateNumberOfMessagesDelayed', 0)),
                'created_timestamp': attributes.get('CreatedTimestamp'),
                'visibility_timeout': int(attributes.get('VisibilityTimeout', 30)),
                'max_receive_count': attributes.get('MaxReceiveCount'),
                'is_fifo': queue_url.endswith('.fifo')
            }
            
            logger.info(f"Queue stats - Messages: {queue_info['message_count']}, In-flight: {queue_info['in_flight_count']}")
            return queue_info
            
        except ClientError as e:
            logger.error(f"Failed to get queue attributes: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting queue attributes: {e}")
            raise
    
    def test_connection(self, queue_url: str = None) -> bool:
        """
        Test SQS connection.
        
        Args:
            queue_url: Optional queue URL to test specific queue access
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if queue_url:
                # Test specific queue access
                self.get_queue_attributes(queue_url)
                logger.info(f"SQS connection test successful for queue: {queue_url}")
            else:
                # Test general SQS access by listing queues
                response = self.sqs_client.list_queues()
                queue_count = len(response.get('QueueUrls', []))
                logger.info(f"SQS connection test successful - {queue_count} queues accessible")
            
            return True
            
        except Exception as e:
            logger.error(f"SQS connection test failed: {e}")
            return False
    
    def _format_message_attributes(self, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """Format message attributes for SQS."""
        formatted_attributes = {}
        
        for key, value in attributes.items():
            if isinstance(value, dict) and 'StringValue' in value and 'DataType' in value:
                # Already formatted
                formatted_attributes[key] = value
            elif isinstance(value, str):
                formatted_attributes[key] = {
                    'StringValue': value,
                    'DataType': 'String'
                }
            elif isinstance(value, (int, float)):
                formatted_attributes[key] = {
                    'StringValue': str(value),
                    'DataType': 'Number'
                }
            else:
                formatted_attributes[key] = {
                    'StringValue': str(value),
                    'DataType': 'String'
                }
        
        return formatted_attributes
    
    def _generate_group_id(self) -> str:
        """Generate unique message group ID for FIFO queues."""
        # Use timestamp + random component for uniqueness
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        return f"group_{timestamp}_{unique_id}"
    
    def _generate_deduplication_id(self, message_body: str) -> str:
        """Generate deduplication ID for FIFO queues."""
        import hashlib
        
        # Create hash of message content + timestamp for uniqueness
        content_hash = hashlib.md5(message_body.encode()).hexdigest()[:16]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        return f"{content_hash}_{timestamp}"

# Global SQS connector instance
sqs_connector = SQSConnector()