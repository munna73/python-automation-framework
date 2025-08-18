"""
IBM MQ producer for posting messages to queues.
"""
import pymqi
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from utils.config_loader import ConfigLoader
from utils.logger import logger, mq_logger

class MQProducer:
    """IBM MQ producer for posting messages to queues."""
    
    def __init__(self):
        """Initialize MQ producer."""
        self.config = config_loader.get_mq_config()
        self.queue_manager = None
        self.queue = None
        self.connection_params = None
        self.setup_connection_params()
    
    def setup_connection_params(self):
        """Setup MQ connection parameters."""
        try:
            self.connection_params = {
                'queue_manager': self.config.get('queue_manager'),
                'channel': self.config.get('channel'),
                'host': self.config.get('host'),
                'port': int(self.config.get('port', 1414)),
                'username': self.config.get('username'),
                'password': self.config.get('password'),
                'queue_name': self.config.get('queue_name')
            }
            
            mq_logger.info("MQ connection parameters configured")
            
        except Exception as e:
            mq_logger.error(f"Failed to setup MQ connection parameters: {e}")
            raise
    
    def connect(self):
        """Connect to IBM MQ queue manager and queue."""
        try:
            # Connection details
            conn_info = f"{self.connection_params['host']}({self.connection_params['port']})"
            
            mq_logger.info(f"Connecting to MQ: {self.connection_params['queue_manager']} at {conn_info}")
            
            # Create connection descriptor
            cd = pymqi.CD()
            cd.ChannelName = self.connection_params['channel']
            cd.ConnectionName = conn_info
            cd.ChannelType = pymqi.CMQC.MQCHT_CLNTCONN
            cd.TransportType = pymqi.CMQC.MQXPT_TCP
            
            # Create security exit options if username/password provided
            sco = pymqi.SCO()
            if self.connection_params['username'] and self.connection_params['password']:
                sco.AuthInfos = [pymqi.AuthInfo(authInfoType=pymqi.CMQC.MQAIT_IDPW,
                                              userId=self.connection_params['username'],
                                              password=self.connection_params['password'])]
            
            # Connect to queue manager
            self.queue_manager = pymqi.connect(
                self.connection_params['queue_manager'],
                cd=cd,
                sco=sco if self.connection_params['username'] else None
            )
            
            # Open queue for output
            self.queue = pymqi.Queue(self.queue_manager, self.connection_params['queue_name'])
            
            mq_logger.info(f"Successfully connected to queue: {self.connection_params['queue_name']}")
            
        except pymqi.MQMIError as e:
            mq_logger.error(f"MQ connection failed: {e}")
            raise
        except Exception as e:
            mq_logger.error(f"Unexpected error during MQ connection: {e}")
            raise
    
    def disconnect(self):
        """Disconnect from MQ queue and queue manager."""
        try:
            if self.queue:
                self.queue.close()
                self.queue = None
                mq_logger.info("Queue connection closed")
            
            if self.queue_manager:
                self.queue_manager.disconnect()
                self.queue_manager = None
                mq_logger.info("Queue manager connection closed")
                
        except Exception as e:
            mq_logger.error(f"Error during MQ disconnect: {e}")
    
    def post_message(self, message: str, message_descriptor: Optional[Dict[str, Any]] = None) -> bool:
        """
        Post a single message to the queue.
        
        Args:
            message: Message content to post
            message_descriptor: Optional MQ message descriptor settings
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.queue:
                raise Exception("Not connected to queue. Call connect() first.")
            
            # Create message descriptor
            md = pymqi.MD()
            
            if message_descriptor:
                # Apply custom message descriptor settings
                for key, value in message_descriptor.items():
                    if hasattr(md, key):
                        setattr(md, key, value)
            else:
                # Default message descriptor
                md.Persistence = pymqi.CMQC.MQPER_PERSISTENT
                md.Priority = 5
            
            # Convert message to bytes
            message_bytes = message.encode('utf-8')
            
            # Put message to queue
            self.queue.put(message_bytes, md)
            
            mq_logger.info(f"Message posted successfully - Length: {len(message)} characters")
            mq_logger.debug(f"Message content: {message[:100]}{'...' if len(message) > 100 else ''}")
            
            return True
            
        except pymqi.MQMIError as e:
            mq_logger.error(f"MQ error posting message: {e}")
            return False
        except Exception as e:
            mq_logger.error(f"Error posting message: {e}")
            return False
    
    def post_file_as_single_message(self, file_path: str) -> bool:
        """
        Post entire file content as a single message.
        
        Args:
            file_path: Path to the file to post
            
        Returns:
            True if successful, False otherwise
        """
        try:
            file_path_obj = Path(file_path)
            
            if not file_path_obj.exists():
                # Try looking in data directory
                file_path_obj = Path(__file__).parent.parent / "data" / "input" / file_path
            
            if not file_path_obj.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Read entire file content
            with open(file_path_obj, 'r', encoding='utf-8') as f:
                file_content = f.read()
            
            mq_logger.info(f"Posting file as single message: {file_path_obj.name} ({len(file_content)} characters)")
            
            # Post as single message
            success = self.post_message(file_content)
            
            if success:
                mq_logger.info(f"File posted successfully as single message: {file_path_obj.name}")
            
            return success
            
        except Exception as e:
            mq_logger.error(f"Error posting file as single message: {e}")
            return False
    
    def post_file_line_by_line(self, file_path: str) -> Dict[str, Any]:
        """
        Post file content line by line as separate messages.
        
        Args:
            file_path: Path to the file to post
            
        Returns:
            Dictionary with posting results
        """
        try:
            file_path_obj = Path(file_path)
            
            if not file_path_obj.exists():
                # Try looking in data directory
                file_path_obj = Path(__file__).parent.parent / "data" / "input" / file_path
            
            if not file_path_obj.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            mq_logger.info(f"Posting file line by line: {file_path_obj.name}")
            
            # Read and post line by line
            success_count = 0
            error_count = 0
            total_lines = 0
            errors = []
            
            with open(file_path_obj, 'r', encoding='utf-8') as f:
                for line_number, line in enumerate(f, 1):
                    total_lines += 1
                    line = line.rstrip('\n\r')  # Remove line endings
                    
                    if line:  # Skip empty lines
                        if self.post_message(line):
                            success_count += 1
                        else:
                            error_count += 1
                            errors.append(f"Line {line_number}: Failed to post")
            
            results = {
                'total_lines': total_lines,
                'success_count': success_count,
                'error_count': error_count,
                'success_rate': (success_count / total_lines * 100) if total_lines > 0 else 0,
                'errors': errors
            }
            
            mq_logger.info(f"File posting completed: {success_count}/{total_lines} messages posted successfully")
            
            if error_count > 0:
                mq_logger.warning(f"{error_count} messages failed to post")
                for error in errors[:5]:  # Log first 5 errors
                    mq_logger.error(error)
            
            return results
            
        except Exception as e:
            mq_logger.error(f"Error posting file line by line: {e}")
            return {
                'total_lines': 0,
                'success_count': 0,
                'error_count': 1,
                'success_rate': 0,
                'errors': [str(e)]
            }
    
    def post_messages_batch(self, messages: List[str]) -> Dict[str, Any]:
        """
        Post multiple messages in batch.
        
        Args:
            messages: List of messages to post
            
        Returns:
            Dictionary with batch posting results
        """
        try:
            mq_logger.info(f"Posting batch of {len(messages)} messages")
            
            success_count = 0
            error_count = 0
            errors = []
            
            for i, message in enumerate(messages):
                if self.post_message(message):
                    success_count += 1
                else:
                    error_count += 1
                    errors.append(f"Message {i+1}: Failed to post")
            
            results = {
                'total_messages': len(messages),
                'success_count': success_count,
                'error_count': error_count,
                'success_rate': (success_count / len(messages) * 100) if messages else 0,
                'errors': errors
            }
            
            mq_logger.info(f"Batch posting completed: {success_count}/{len(messages)} messages posted successfully")
            
            return results
            
        except Exception as e:
            mq_logger.error(f"Error in batch posting: {e}")
            return {
                'total_messages': len(messages) if messages else 0,
                'success_count': 0,
                'error_count': len(messages) if messages else 1,
                'success_rate': 0,
                'errors': [str(e)]
            }
    
    def test_connection(self) -> bool:
        """
        Test MQ connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to connect
            self.connect()
            
            # Test by posting a small test message
            test_message = "Connection test message"
            success = self.post_message(test_message)
            
            # Disconnect
            self.disconnect()
            
            if success:
                mq_logger.info("MQ connection test successful")
            else:
                mq_logger.error("MQ connection test failed - could not post message")
            
            return success
            
        except Exception as e:
            mq_logger.error(f"MQ connection test failed: {e}")
            return False
    
    def get_queue_depth(self) -> Optional[int]:
        """
        Get current queue depth (number of messages in queue).
        
        Returns:
            Queue depth or None if unable to retrieve
        """
        try:
            if not self.queue:
                raise Exception("Not connected to queue")
            
            # Inquire queue depth
            attrs = self.queue.inquire(pymqi.CMQC.MQIA_CURRENT_Q_DEPTH)
            depth = attrs[pymqi.CMQC.MQIA_CURRENT_Q_DEPTH]
            
            mq_logger.info(f"Current queue depth: {depth}")
            return depth
            
        except Exception as e:
            mq_logger.error(f"Error getting queue depth: {e}")
            return None
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

    # ========================================
    # MESSAGE-STYLE OPERATIONS FOR MQ
    # ========================================
    
    def send_file_as_mq_messages(self, filename: str, line_by_line: bool = True, 
                                message_prefix: str = None) -> Dict[str, Any]:
        """
        Send file content to MQ as messages with different modes.
        
        Args:
            filename: Local file path to read
            line_by_line: If True, each line becomes a separate MQ message
                         If False, entire file becomes single MQ message
            message_prefix: Optional prefix for message identification
            
        Returns:
            Dictionary with send results and statistics
        """
        try:
            file_path_obj = Path(filename)
            
            if not file_path_obj.exists():
                # Try looking in data directory
                file_path_obj = Path(__file__).parent.parent / "data" / "input" / filename
            
            if not file_path_obj.exists():
                raise FileNotFoundError(f"File not found: {filename}")
            
            mq_logger.info(f"Sending file to MQ {'line by line' if line_by_line else 'as whole file'}: {file_path_obj.name}")
            
            if line_by_line:
                # Use existing line-by-line functionality
                return self.post_file_line_by_line(str(file_path_obj))
            else:
                # Use existing whole file functionality
                success = self.post_file_as_single_message(str(file_path_obj))
                
                # Calculate file stats for consistent return format
                with open(file_path_obj, 'r', encoding='utf-8') as f:
                    content = f.read()
                    total_lines = len(content.splitlines()) if content else 0
                
                return {
                    'success': success,
                    'total_lines': total_lines,
                    'success_count': 1 if success else 0,
                    'error_count': 0 if success else 1,
                    'success_rate': 100 if success else 0,
                    'errors': [] if success else ['Failed to send file as single message']
                }
                
        except Exception as e:
            mq_logger.error(f"Error sending file as MQ messages: {e}")
            return {
                'success': False,
                'total_lines': 0,
                'success_count': 0,
                'error_count': 1,
                'success_rate': 0,
                'errors': [str(e)]
            }
    
    def post_messages_from_list(self, messages: List[str], 
                              add_line_numbers: bool = False) -> Dict[str, Any]:
        """
        Post multiple messages from a list.
        
        Args:
            messages: List of message strings to post
            add_line_numbers: If True, add line numbers to messages
            
        Returns:
            Dictionary with posting results
        """
        try:
            mq_logger.info(f"Posting {len(messages)} messages from list")
            
            success_count = 0
            error_count = 0
            errors = []
            
            for i, message in enumerate(messages, 1):
                try:
                    # Add line number if requested
                    if add_line_numbers:
                        formatted_message = f"[{i:06d}] {message}"
                    else:
                        formatted_message = message
                    
                    if self.post_message(formatted_message):
                        success_count += 1
                    else:
                        error_count += 1
                        errors.append(f"Message {i}: Failed to post")
                        
                except Exception as e:
                    error_count += 1
                    errors.append(f"Message {i}: {str(e)}")
            
            results = {
                'success': error_count == 0,
                'total_messages': len(messages),
                'success_count': success_count,
                'error_count': error_count,
                'success_rate': (success_count / len(messages) * 100) if messages else 0,
                'errors': errors
            }
            
            mq_logger.info(f"Message list posting completed: {success_count}/{len(messages)} posted successfully")
            return results
            
        except Exception as e:
            mq_logger.error(f"Error posting messages from list: {e}")
            return {
                'success': False,
                'total_messages': len(messages) if messages else 0,
                'success_count': 0,
                'error_count': len(messages) if messages else 1,
                'success_rate': 0,
                'errors': [str(e)]
            }
    
    def post_structured_messages(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Post structured messages with custom message descriptors.
        
        Args:
            messages: List of dictionaries with 'content' and optional 'descriptor' keys
            
        Returns:
            Dictionary with posting results
        """
        try:
            mq_logger.info(f"Posting {len(messages)} structured messages")
            
            success_count = 0
            error_count = 0
            errors = []
            
            for i, msg_data in enumerate(messages, 1):
                try:
                    content = msg_data.get('content', '')
                    descriptor = msg_data.get('descriptor', None)
                    
                    if self.post_message(content, descriptor):
                        success_count += 1
                    else:
                        error_count += 1
                        errors.append(f"Structured message {i}: Failed to post")
                        
                except Exception as e:
                    error_count += 1
                    errors.append(f"Structured message {i}: {str(e)}")
            
            results = {
                'success': error_count == 0,
                'total_messages': len(messages),
                'success_count': success_count,
                'error_count': error_count,
                'success_rate': (success_count / len(messages) * 100) if messages else 0,
                'errors': errors
            }
            
            mq_logger.info(f"Structured message posting completed: {success_count}/{len(messages)} posted successfully")
            return results
            
        except Exception as e:
            mq_logger.error(f"Error posting structured messages: {e}")
            return {
                'success': False,
                'total_messages': len(messages) if messages else 0,
                'success_count': 0,
                'error_count': len(messages) if messages else 1,
                'success_rate': 0,
                'errors': [str(e)]
            }

# Global MQ producer instance
mq_producer = MQProducer()