"""
Kafka producer for publishing messages to Kafka topics.
"""
from kafka import KafkaProducer
import json
import time
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from utils.config_loader import ConfigLoader, config_loader
from utils.logger import logger
import logging

# Create Kafka-specific logger
kafka_logger = logging.getLogger('kafka')

class KafkaMessageProducer:
    """Kafka producer for publishing messages to topics."""
    
    def __init__(self):
        """Initialize Kafka producer."""
        self.config = None
        self.producer = None
        self.connection_params = None
        self.setup_connection_params()
    
    def setup_connection_params(self):
        """Setup Kafka connection parameters."""
        try:
            self.config = config_loader.get_kafka_config()
            self.connection_params = {
                'bootstrap_servers': self.config.get('bootstrap_servers', 'localhost:9092').split(','),
                'client_id': self.config.get('client_id', 'test-automation-producer'),
                'value_serializer': self._get_serializer(self.config.get('value_serializer', 'string')),
                'key_serializer': self._get_serializer(self.config.get('key_serializer', 'string')),
                'compression_type': self.config.get('compression_type', 'none'),
                'acks': self.config.get('acks', 'all'),
                'retries': int(self.config.get('retries', 3)),
                'batch_size': int(self.config.get('batch_size', 16384)),
                'linger_ms': int(self.config.get('linger_ms', 10)),
                'buffer_memory': int(self.config.get('buffer_memory', 33554432)),
                'max_block_ms': int(self.config.get('max_block_ms', 60000)),
                'request_timeout_ms': int(self.config.get('request_timeout_ms', 30000))
            }
            
            # Add security configuration if enabled
            if self.config.get('security_protocol'):
                self.connection_params['security_protocol'] = self.config.get('security_protocol')
                
                if self.config.get('sasl_mechanism'):
                    self.connection_params['sasl_mechanism'] = self.config.get('sasl_mechanism')
                    self.connection_params['sasl_plain_username'] = self.config.get('sasl_username')
                    self.connection_params['sasl_plain_password'] = self.config.get('sasl_password')
                
                if self.config.get('ssl_cafile'):
                    self.connection_params['ssl_cafile'] = self.config.get('ssl_cafile')
                    self.connection_params['ssl_certfile'] = self.config.get('ssl_certfile')
                    self.connection_params['ssl_keyfile'] = self.config.get('ssl_keyfile')
            
            kafka_logger.info("Kafka producer connection parameters configured")
            
        except Exception as e:
            kafka_logger.error(f"Failed to setup Kafka producer connection parameters: {e}")
            raise
    
    def _get_serializer(self, serializer_type: str):
        """Get serializer function based on type."""
        if serializer_type.lower() == 'json':
            return lambda x: json.dumps(x).encode('utf-8')
        elif serializer_type.lower() == 'string':
            return lambda x: str(x).encode('utf-8') if x is not None else None
        elif serializer_type.lower() == 'bytes':
            return lambda x: x if isinstance(x, bytes) else str(x).encode('utf-8')
        else:
            return lambda x: str(x).encode('utf-8')
    
    def connect(self):
        """Create Kafka producer connection."""
        try:
            kafka_logger.info(f"Connecting to Kafka brokers: {self.connection_params['bootstrap_servers']}")
            
            self.producer = KafkaProducer(**self.connection_params)
            
            # Test connection by getting metadata
            metadata = self.producer.bootstrap_connected()
            if metadata:
                kafka_logger.info("Kafka producer connected successfully")
            else:
                kafka_logger.warning("Kafka producer connection status uncertain")
            
        except Exception as e:
            kafka_logger.error(f"Kafka producer connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close Kafka producer connection."""
        try:
            if self.producer:
                self.producer.flush()  # Ensure all messages are sent
                self.producer.close()
                self.producer = None
                kafka_logger.info("Kafka producer connection closed")
                
        except Exception as e:
            kafka_logger.error(f"Error during Kafka producer disconnect: {e}")
    
    def send_message(self, topic: str, message: Union[str, dict], key: str = None, 
                    partition: int = None, headers: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Send a single message to Kafka topic.
        
        Args:
            topic: Kafka topic name
            message: Message content (string or dict)
            key: Message key for partitioning
            partition: Specific partition to send to
            headers: Message headers
            
        Returns:
            Dictionary with send result
        """
        try:
            if not self.producer:
                raise Exception("Not connected to Kafka. Call connect() first.")
            
            # Convert headers to bytes if provided
            byte_headers = None
            if headers:
                byte_headers = [(k, v.encode('utf-8') if isinstance(v, str) else v) 
                              for k, v in headers.items()]
            
            # Send message
            future = self.producer.send(
                topic=topic,
                value=message,
                key=key,
                partition=partition,
                headers=byte_headers
            )
            
            # Get result with timeout
            record_metadata = future.get(timeout=30)
            
            result = {
                'success': True,
                'topic': record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset,
                'timestamp': record_metadata.timestamp,
                'message_size': record_metadata.serialized_value_size
            }
            
            kafka_logger.info(f"Message sent to {topic} - Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            kafka_logger.debug(f"Message content: {str(message)[:100]}{'...' if len(str(message)) > 100 else ''}")
            
            return result
            
        except Exception as e:
            kafka_logger.error(f"Failed to send message to topic {topic}: {e}")
            return {
                'success': False,
                'error': str(e),
                'topic': topic
            }
    
    def send_file_as_kafka_messages(self, filename: str, topic: str, 
                                   line_by_line: bool = True, 
                                   message_key_prefix: str = None) -> Dict[str, Any]:
        """
        Send file content to Kafka as messages with different modes.
        
        Args:
            filename: Local file path to read
            topic: Kafka topic name
            line_by_line: If True, each line becomes a separate message
                         If False, entire file becomes single message
            message_key_prefix: Optional prefix for message keys
            
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
            
            kafka_logger.info(f"Sending file to Kafka {'line by line' if line_by_line else 'as whole file'}: {file_path_obj.name}")
            
            success_count = 0
            error_count = 0
            total_lines = 0
            errors = []
            sent_messages = []
            
            with open(file_path_obj, 'r', encoding='utf-8') as f:
                if line_by_line:
                    # Send each line as separate message
                    for line_number, line in enumerate(f, 1):
                        total_lines += 1
                        line_content = line.rstrip('\n\r')
                        
                        if line_content:  # Skip empty lines
                            # Generate message key if prefix provided
                            message_key = f"{message_key_prefix}_{line_number:06d}" if message_key_prefix else None
                            
                            result = self.send_message(
                                topic=topic,
                                message=line_content,
                                key=message_key,
                                headers={'line_number': str(line_number), 'source_file': file_path_obj.name}
                            )
                            
                            if result['success']:
                                success_count += 1
                                sent_messages.append({
                                    'line_number': line_number,
                                    'partition': result['partition'],
                                    'offset': result['offset'],
                                    'message_size': result['message_size']
                                })
                            else:
                                error_count += 1
                                errors.append(f"Line {line_number}: {result.get('error', 'Unknown error')}")
                else:
                    # Send entire file as single message
                    f.seek(0)
                    content = f.read()
                    total_lines = len(content.splitlines()) if content else 0
                    
                    message_key = f"{message_key_prefix}_full_file" if message_key_prefix else None
                    
                    result = self.send_message(
                        topic=topic,
                        message=content,
                        key=message_key,
                        headers={'source_file': file_path_obj.name, 'total_lines': str(total_lines)}
                    )
                    
                    if result['success']:
                        success_count = 1
                        sent_messages.append({
                            'line_number': 'all',
                            'partition': result['partition'],
                            'offset': result['offset'],
                            'message_size': result['message_size']
                        })
                    else:
                        error_count = 1
                        errors.append(f"Full file: {result.get('error', 'Unknown error')}")
            
            results = {
                'success': error_count == 0,
                'total_lines': total_lines,
                'success_count': success_count,
                'error_count': error_count,
                'success_rate': (success_count / max(total_lines if line_by_line else 1, 1) * 100),
                'errors': errors,
                'sent_messages': sent_messages,
                'topic': topic
            }
            
            kafka_logger.info(f"File sending completed: {success_count}/{total_lines if line_by_line else 1} messages sent successfully to topic {topic}")
            return results
            
        except Exception as e:
            kafka_logger.error(f"Error sending file as Kafka messages: {e}")
            return {
                'success': False,
                'error': str(e),
                'total_lines': 0,
                'success_count': 0,
                'error_count': 1,
                'topic': topic
            }
    
    def send_messages_batch(self, topic: str, messages: List[Union[str, dict]], 
                           keys: List[str] = None, headers_list: List[Dict] = None) -> Dict[str, Any]:
        """
        Send multiple messages in batch to Kafka topic.
        
        Args:
            topic: Kafka topic name
            messages: List of messages to send
            keys: Optional list of message keys
            headers_list: Optional list of headers for each message
            
        Returns:
            Dictionary with batch send results
        """
        try:
            kafka_logger.info(f"Sending batch of {len(messages)} messages to topic {topic}")
            
            success_count = 0
            error_count = 0
            errors = []
            sent_messages = []
            
            for i, message in enumerate(messages):
                try:
                    key = keys[i] if keys and i < len(keys) else None
                    headers = headers_list[i] if headers_list and i < len(headers_list) else None
                    
                    result = self.send_message(
                        topic=topic,
                        message=message,
                        key=key,
                        headers=headers
                    )
                    
                    if result['success']:
                        success_count += 1
                        sent_messages.append({
                            'message_index': i + 1,
                            'partition': result['partition'],
                            'offset': result['offset'],
                            'message_size': result['message_size']
                        })
                    else:
                        error_count += 1
                        errors.append(f"Message {i + 1}: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    error_count += 1
                    errors.append(f"Message {i + 1}: {str(e)}")
            
            results = {
                'success': error_count == 0,
                'total_messages': len(messages),
                'success_count': success_count,
                'error_count': error_count,
                'success_rate': (success_count / len(messages) * 100) if messages else 0,
                'errors': errors,
                'sent_messages': sent_messages,
                'topic': topic
            }
            
            kafka_logger.info(f"Batch sending completed: {success_count}/{len(messages)} messages sent successfully")
            return results
            
        except Exception as e:
            kafka_logger.error(f"Error in batch sending: {e}")
            return {
                'success': False,
                'error': str(e),
                'total_messages': len(messages) if messages else 0,
                'success_count': 0,
                'error_count': len(messages) if messages else 1,
                'topic': topic
            }
    
    def send_json_messages(self, topic: str, json_objects: List[dict], 
                          key_field: str = None) -> Dict[str, Any]:
        """
        Send JSON objects as messages to Kafka topic.
        
        Args:
            topic: Kafka topic name
            json_objects: List of dictionaries to send as JSON
            key_field: Field name to use as message key
            
        Returns:
            Dictionary with send results
        """
        try:
            kafka_logger.info(f"Sending {len(json_objects)} JSON messages to topic {topic}")
            
            messages = []
            keys = []
            headers_list = []
            
            for i, obj in enumerate(json_objects):
                messages.append(obj)
                
                # Extract key if field specified
                if key_field and key_field in obj:
                    keys.append(str(obj[key_field]))
                else:
                    keys.append(f"json_msg_{i:06d}")
                
                # Add metadata headers
                headers_list.append({
                    'content_type': 'application/json',
                    'message_index': str(i + 1)
                })
            
            return self.send_messages_batch(topic, messages, keys, headers_list)
            
        except Exception as e:
            kafka_logger.error(f"Error sending JSON messages: {e}")
            return {
                'success': False,
                'error': str(e),
                'total_messages': len(json_objects) if json_objects else 0,
                'success_count': 0,
                'error_count': 1,
                'topic': topic
            }
    
    def test_connection(self) -> bool:
        """
        Test Kafka producer connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to connect
            self.connect()
            
            # Test by sending a small test message to a test topic
            test_topic = self.config.get('test_topic', 'connection-test')
            test_message = f"Connection test message - {int(time.time())}"
            
            result = self.send_message(test_topic, test_message)
            success = result['success']
            
            # Disconnect
            self.disconnect()
            
            if success:
                kafka_logger.info("Kafka producer connection test successful")
            else:
                kafka_logger.error("Kafka producer connection test failed")
            
            return success
            
        except Exception as e:
            kafka_logger.error(f"Kafka producer connection test failed: {e}")
            return False
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

# Global Kafka producer instance - lazy initialization to support tag-aware config loading
kafka_producer = None

def get_kafka_producer():
    """Get or create the global Kafka producer instance with lazy initialization."""
    global kafka_producer
    if kafka_producer is None:
        kafka_producer = KafkaMessageProducer()
    return kafka_producer