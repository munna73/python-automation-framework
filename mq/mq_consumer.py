"""
IBM MQ consumer for retrieving messages from queues.
"""
import pymqi
import os
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from utils.config_loader import ConfigLoader, config_loader
from utils.logger import logger, mq_logger

class MQConsumer:
    """IBM MQ consumer for retrieving messages from queues."""
    
    def __init__(self, config_section: str = "S101_MQ"):
        """
        Initialize MQ consumer.
        
        Args:
            config_section: MQ configuration section name (e.g., 'S101_MQ', 'LOCAL_MQ_ANONYMOUS')
        """
        self.config_section = config_section
        self.config = config_loader.get_mq_config(config_section)
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
                'username': self.config.get('username', ''),  # Default to empty string
                'password': self.config.get('password', ''),  # Default to empty string
                'queue_name': self.config.get('queue_name')
            }
            
            # Log connection mode
            if self.connection_params['username'] and self.connection_params['password']:
                mq_logger.info("MQ consumer connection parameters configured with authentication")
            else:
                mq_logger.info("MQ consumer connection parameters configured without authentication (anonymous connection)")
            
        except Exception as e:
            mq_logger.error(f"Failed to setup MQ consumer connection parameters: {e}")
            raise
    
    def connect(self):
        """Connect to IBM MQ queue manager and queue."""
        try:
            # Connection details
            conn_info = f"{self.connection_params['host']}({self.connection_params['port']})"
            
            mq_logger.info(f"Connecting MQ consumer to: {self.connection_params['queue_manager']} at {conn_info}")
            
            # Create connection descriptor
            cd = pymqi.CD()
            cd.ChannelName = self.connection_params['channel']
            cd.ConnectionName = conn_info
            cd.ChannelType = pymqi.CMQC.MQCHT_CLNTCONN
            cd.TransportType = pymqi.CMQC.MQXPT_TCP
            
            # Determine authentication mode
            use_authentication = bool(self.connection_params['username'] and self.connection_params['password'])
            
            if use_authentication:
                # Create security exit options for authenticated connection
                sco = pymqi.SCO()
                sco.AuthInfos = [pymqi.AuthInfo(authInfoType=pymqi.CMQC.MQAIT_IDPW,
                                              userId=self.connection_params['username'],
                                              password=self.connection_params['password'])]
                mq_logger.info(f"Connecting MQ consumer with authentication as user: {self.connection_params['username']}")
                
                # Connect to queue manager with authentication
                self.queue_manager = pymqi.connect(
                    self.connection_params['queue_manager'],
                    cd=cd,
                    sco=sco
                )
            else:
                # Connect without authentication (anonymous/unauthenticated connection)
                mq_logger.info("Connecting MQ consumer without authentication (anonymous connection)")
                
                # Connect to queue manager without security context
                self.queue_manager = pymqi.connect(
                    self.connection_params['queue_manager'],
                    cd=cd
                )
            
            # Open queue for input
            self.queue = pymqi.Queue(self.queue_manager, self.connection_params['queue_name'], 
                                   pymqi.CMQC.MQOO_INPUT_AS_Q_DEF)
            
            mq_logger.info(f"MQ consumer successfully connected to queue: {self.connection_params['queue_name']}")
            
        except pymqi.MQMIError as e:
            mq_logger.error(f"MQ consumer connection failed: {e}")
            raise
        except Exception as e:
            mq_logger.error(f"Unexpected error during MQ consumer connection: {e}")
            raise
    
    def disconnect(self):
        """Disconnect from MQ queue and queue manager."""
        try:
            if self.queue:
                self.queue.close()
                self.queue = None
                mq_logger.info("MQ consumer queue connection closed")
            
            if self.queue_manager:
                self.queue_manager.disconnect()
                self.queue_manager = None
                mq_logger.info("MQ consumer queue manager connection closed")
                
        except Exception as e:
            mq_logger.error(f"Error during MQ consumer disconnect: {e}")
    
    def get_message(self, wait_interval: int = 1000) -> Optional[Dict[str, Any]]:
        """
        Get a single message from the queue.
        
        Args:
            wait_interval: Wait interval in milliseconds
            
        Returns:
            Dictionary with message data or None if no message
        """
        try:
            if not self.queue:
                raise Exception("Not connected to queue. Call connect() first.")
            
            # Create message descriptor and get message options
            md = pymqi.MD()
            gmo = pymqi.GMO()
            gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
            gmo.WaitInterval = wait_interval
            
            try:
                # Get message
                message_data = self.queue.get(None, md, gmo)
                
                # Decode message
                message_text = message_data.decode('utf-8')
                
                result = {
                    'message_id': md.MsgId.hex(),
                    'correlation_id': md.CorrelId.hex(),
                    'message_text': message_text,
                    'message_length': len(message_text),
                    'priority': md.Priority,
                    'persistence': md.Persistence,
                    'put_timestamp': md.PutDate + md.PutTime,
                    'format': md.Format.strip(),
                    'expiry': md.Expiry
                }
                
                mq_logger.info(f"Message retrieved - ID: {result['message_id'][:16]}..., Length: {result['message_length']}")
                mq_logger.debug(f"Message content: {message_text[:100]}{'...' if len(message_text) > 100 else ''}")
                
                return result
                
            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    mq_logger.debug("No messages available in queue")
                    return None
                else:
                    mq_logger.error(f"MQ error getting message: {e}")
                    raise
                    
        except Exception as e:
            mq_logger.error(f"Error getting message: {e}")
            raise
    
    def get_messages(self, max_messages: int = 10, wait_interval: int = 1000) -> List[Dict[str, Any]]:
        """
        Get multiple messages from the queue.
        
        Args:
            max_messages: Maximum number of messages to retrieve
            wait_interval: Wait interval in milliseconds for each message
            
        Returns:
            List of message dictionaries
        """
        try:
            mq_logger.info(f"Retrieving up to {max_messages} messages from queue")
            
            messages = []
            messages_retrieved = 0
            
            for i in range(max_messages):
                message = self.get_message(wait_interval)
                
                if message:
                    messages.append(message)
                    messages_retrieved += 1
                else:
                    # No more messages available
                    break
            
            mq_logger.info(f"Retrieved {messages_retrieved} messages from queue")
            return messages
            
        except Exception as e:
            mq_logger.error(f"Error getting multiple messages: {e}")
            raise
    
    def get_all_messages(self, wait_interval: int = 100) -> List[Dict[str, Any]]:
        """
        Get all available messages from the queue.
        
        Args:
            wait_interval: Wait interval in milliseconds for each message
            
        Returns:
            List of all message dictionaries
        """
        try:
            mq_logger.info("Retrieving all available messages from queue")
            
            messages = []
            messages_retrieved = 0
            
            while True:
                message = self.get_message(wait_interval)
                
                if message:
                    messages.append(message)
                    messages_retrieved += 1
                else:
                    # No more messages available
                    break
            
            mq_logger.info(f"Retrieved all {messages_retrieved} messages from queue")
            return messages
            
        except Exception as e:
            mq_logger.error(f"Error getting all messages: {e}")
            raise
    
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
    
    def peek_message(self, message_index: int = 0) -> Optional[Dict[str, Any]]:
        """
        Peek at a message without removing it from the queue.
        
        Args:
            message_index: Index of message to peek at (0-based)
            
        Returns:
            Dictionary with message data or None if message not found
        """
        try:
            if not self.queue:
                raise Exception("Not connected to queue")
            
            md = pymqi.MD()
            gmo = pymqi.GMO()
            gmo.Options = pymqi.CMQC.MQGMO_BROWSE_FIRST if message_index == 0 else pymqi.CMQC.MQGMO_BROWSE_NEXT
            gmo.Options |= pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
            gmo.WaitInterval = 1000
            
            try:
                # Browse message
                message_data = self.queue.get(None, md, gmo)
                message_text = message_data.decode('utf-8')
                
                result = {
                    'message_id': md.MsgId.hex(),
                    'correlation_id': md.CorrelId.hex(),
                    'message_text': message_text,
                    'message_length': len(message_text),
                    'priority': md.Priority,
                    'persistence': md.Persistence,
                    'put_timestamp': md.PutDate + md.PutTime,
                    'format': md.Format.strip(),
                    'expiry': md.Expiry
                }
                
                mq_logger.info(f"Peeked at message {message_index} - ID: {result['message_id'][:16]}...")
                return result
                
            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    mq_logger.debug(f"No message available at index {message_index}")
                    return None
                else:
                    mq_logger.error(f"MQ error peeking at message: {e}")
                    raise
                    
        except Exception as e:
            mq_logger.error(f"Error peeking at message: {e}")
            raise
    
    def test_connection(self) -> bool:
        """
        Test MQ consumer connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to connect
            self.connect()
            
            # Test by getting queue depth
            depth = self.get_queue_depth()
            success = depth is not None
            
            # Disconnect
            self.disconnect()
            
            if success:
                mq_logger.info("MQ consumer connection test successful")
            else:
                mq_logger.error("MQ consumer connection test failed")
            
            return success
            
        except Exception as e:
            mq_logger.error(f"MQ consumer connection test failed: {e}")
            return False
    
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
    
    def retrieve_messages_to_file(self, output_file: str, max_messages: int = None,
                                one_message_per_line: bool = True, 
                                wait_interval: int = 1000) -> Dict[str, Any]:
        """
        Retrieve MQ messages and write to file with different modes.
        
        Args:
            output_file: Local file path to write messages
            max_messages: Maximum number of messages to retrieve (None for all)
            one_message_per_line: If True, each message = one line in file
                                If False, concatenate all messages as single content
            wait_interval: Wait interval in milliseconds for each message
            
        Returns:
            Dictionary with retrieval results and statistics
        """
        try:
            mq_logger.info(f"Retrieving MQ messages to file: {output_file}")
            
            # Get messages
            if max_messages:
                messages = self.get_messages(max_messages, wait_interval)
            else:
                messages = self.get_all_messages(wait_interval)
            
            if not messages:
                mq_logger.info("No messages retrieved from queue")
                return {
                    'success': True,
                    'messages_written': 0,
                    'total_messages': 0,
                    'output_file': output_file,
                    'total_content_size': 0,
                    'failed_messages': []
                }
            
            # Write messages to file
            messages_written = 0
            total_size = 0
            failed_messages = []
            
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                for i, message in enumerate(messages):
                    try:
                        message_text = message['message_text']
                        
                        if one_message_per_line:
                            # Write each message as a separate line
                            # Strip any existing newlines and add single newline
                            clean_content = message_text.strip().replace('\n', ' ').replace('\r', ' ')
                            f.write(clean_content + '\n')
                        else:
                            # Concatenate all messages (preserve original formatting)
                            f.write(message_text)
                            if not message_text.endswith('\n'):
                                f.write('\n')
                        
                        messages_written += 1
                        total_size += len(message_text)
                        
                        mq_logger.debug(f"Processed message {i+1}/{len(messages)}: {message['message_id'][:16]}...")
                        
                    except Exception as e:
                        mq_logger.error(f"Failed to process message {i+1}: {e}")
                        failed_messages.append({
                            'message_index': i+1,
                            'message_id': message.get('message_id', 'unknown'),
                            'error': str(e)
                        })
            
            results = {
                'success': True,
                'messages_written': messages_written,
                'total_messages': len(messages),
                'output_file': str(output_path),
                'total_content_size': total_size,
                'failed_messages': failed_messages,
                'success_rate': (messages_written / len(messages) * 100) if messages else 100
            }
            
            mq_logger.info(f"Retrieved {messages_written}/{len(messages)} messages to file {output_file}")
            return results
            
        except Exception as e:
            mq_logger.error(f"Failed to retrieve MQ messages to file: {e}")
            return {
                'success': False,
                'error': str(e),
                'messages_written': 0,
                'total_messages': 0,
                'output_file': output_file
            }
    
    def get_messages_as_list(self, max_messages: int = None, 
                           include_metadata: bool = False) -> List[str]:
        """
        Get MQ messages as a simple list of strings.
        
        Args:
            max_messages: Maximum number of messages to retrieve
            include_metadata: If True, include message metadata
            
        Returns:
            List of message content strings
        """
        try:
            # Get messages
            if max_messages:
                messages = self.get_messages(max_messages)
            else:
                messages = self.get_all_messages()
            
            if include_metadata:
                # Return messages with metadata
                result = []
                for msg in messages:
                    formatted_msg = (
                        f"[ID: {msg['message_id'][:16]}...] "
                        f"[Priority: {msg['priority']}] "
                        f"[Length: {msg['message_length']}] "
                        f"{msg['message_text']}"
                    )
                    result.append(formatted_msg)
                return result
            else:
                # Return just message content
                return [msg['message_text'] for msg in messages]
                
        except Exception as e:
            mq_logger.error(f"Error getting messages as list: {e}")
            return []
    
    def save_messages_to_json(self, output_file: str, max_messages: int = None) -> Dict[str, Any]:
        """
        Save MQ messages to JSON file with full metadata.
        
        Args:
            output_file: Output JSON file path
            max_messages: Maximum number of messages to retrieve
            
        Returns:
            Dictionary with save results
        """
        try:
            import json
            from datetime import datetime
            
            mq_logger.info(f"Saving MQ messages to JSON file: {output_file}")
            
            # Get messages
            if max_messages:
                messages = self.get_messages(max_messages)
            else:
                messages = self.get_all_messages()
            
            # Prepare JSON data
            json_data = {
                'metadata': {
                    'queue_name': self.connection_params['queue_name'],
                    'queue_manager': self.connection_params['queue_manager'],
                    'retrieval_timestamp': datetime.now().isoformat(),
                    'total_messages': len(messages)
                },
                'messages': messages
            }
            
            # Write to JSON file
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            
            mq_logger.info(f"Saved {len(messages)} messages to JSON file: {output_file}")
            
            return {
                'success': True,
                'messages_saved': len(messages),
                'output_file': str(output_path),
                'file_size': output_path.stat().st_size
            }
            
        except Exception as e:
            mq_logger.error(f"Error saving messages to JSON: {e}")
            return {
                'success': False,
                'error': str(e),
                'messages_saved': 0
            }
    
    def export_messages_with_format(self, output_file: str, export_format: str = 'txt',
                                   max_messages: int = None) -> Dict[str, Any]:
        """
        Export MQ messages to file with different formats.
        
        Args:
            output_file: Output file path
            export_format: Export format ('txt', 'csv', 'json', 'xml')
            max_messages: Maximum number of messages to retrieve
            
        Returns:
            Dictionary with export results
        """
        try:
            mq_logger.info(f"Exporting MQ messages in {export_format.upper()} format to: {output_file}")
            
            # Get messages
            if max_messages:
                messages = self.get_messages(max_messages)
            else:
                messages = self.get_all_messages()
            
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if export_format.lower() == 'txt':
                # Plain text format - one message per line
                with open(output_path, 'w', encoding='utf-8') as f:
                    for msg in messages:
                        f.write(f"{msg['message_text']}\n")
                        
            elif export_format.lower() == 'csv':
                # CSV format with metadata
                import csv
                with open(output_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['message_id', 'correlation_id', 'priority', 'message_length', 'put_timestamp', 'message_text'])
                    for msg in messages:
                        writer.writerow([
                            msg['message_id'], msg['correlation_id'], msg['priority'],
                            msg['message_length'], msg['put_timestamp'], msg['message_text']
                        ])
                        
            elif export_format.lower() == 'json':
                # Use existing JSON export
                return self.save_messages_to_json(str(output_path), max_messages)
                
            elif export_format.lower() == 'xml':
                # XML format
                import xml.etree.ElementTree as ET
                root = ET.Element('mq_messages')
                root.set('total_count', str(len(messages)))
                root.set('queue_name', self.connection_params['queue_name'])
                
                for msg in messages:
                    msg_elem = ET.SubElement(root, 'message')
                    msg_elem.set('id', msg['message_id'])
                    msg_elem.set('priority', str(msg['priority']))
                    msg_elem.set('length', str(msg['message_length']))
                    msg_elem.text = msg['message_text']
                
                tree = ET.ElementTree(root)
                tree.write(str(output_path), encoding='utf-8', xml_declaration=True)
                
            else:
                raise ValueError(f"Unsupported export format: {export_format}")
            
            file_size = output_path.stat().st_size
            
            mq_logger.info(f"Exported {len(messages)} messages in {export_format.upper()} format")
            
            return {
                'success': True,
                'messages_exported': len(messages),
                'output_file': str(output_path),
                'export_format': export_format,
                'file_size': file_size
            }
            
        except Exception as e:
            mq_logger.error(f"Error exporting messages: {e}")
            return {
                'success': False,
                'error': str(e),
                'messages_exported': 0
            }
    
    def drain_queue_to_file(self, output_file: str, batch_size: int = 100) -> Dict[str, Any]:
        """
        Drain all messages from queue to file efficiently.
        
        Args:
            output_file: Output file path
            batch_size: Number of messages to process in each batch
            
        Returns:
            Dictionary with drain results
        """
        try:
            mq_logger.info(f"Draining queue to file: {output_file}")
            
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            total_drained = 0
            batch_count = 0
            
            with open(output_path, 'w', encoding='utf-8') as f:
                while True:
                    # Get batch of messages
                    messages = self.get_messages(batch_size, wait_interval=100)
                    
                    if not messages:
                        # No more messages
                        break
                    
                    batch_count += 1
                    mq_logger.info(f"Processing batch {batch_count}: {len(messages)} messages")
                    
                    # Write batch to file
                    for msg in messages:
                        clean_content = msg['message_text'].strip().replace('\n', ' ').replace('\r', ' ')
                        f.write(clean_content + '\n')
                        total_drained += 1
            
            mq_logger.info(f"Queue drained: {total_drained} messages written to {output_file}")
            
            return {
                'success': True,
                'messages_drained': total_drained,
                'batches_processed': batch_count,
                'output_file': str(output_path),
                'file_size': output_path.stat().st_size
            }
            
        except Exception as e:
            mq_logger.error(f"Error draining queue: {e}")
            return {
                'success': False,
                'error': str(e),
                'messages_drained': 0
            }

# Global MQ consumer instance
mq_consumer = MQConsumer()