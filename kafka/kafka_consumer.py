"""
Kafka consumer for consuming messages from Kafka topics.
"""
from kafka import KafkaConsumer, TopicPartition
import json
import time
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Set
from utils.config_loader import ConfigLoader, config_loader
from utils.logger import logger
import logging
from datetime import datetime
import csv
import xml.etree.ElementTree as ET

# Create Kafka-specific logger
kafka_logger = logging.getLogger('kafka')

class KafkaMessageConsumer:
    """Kafka consumer for consuming messages from topics."""
    
    def __init__(self):
        """Initialize Kafka consumer."""
        self.config = None
        self.consumer = None
        self.connection_params = None
        self.setup_connection_params()
    
    def setup_connection_params(self):
        """Setup Kafka consumer connection parameters."""
        try:
            self.config = config_loader.get_kafka_config()
            self.connection_params = {
                'bootstrap_servers': self.config.get('bootstrap_servers', 'localhost:9092').split(','),
                'client_id': self.config.get('client_id', 'test-automation-consumer'),
                'group_id': self.config.get('group_id', 'test-automation-group'),
                'value_deserializer': self._get_deserializer(self.config.get('value_deserializer', 'string')),
                'key_deserializer': self._get_deserializer(self.config.get('key_deserializer', 'string')),
                'auto_offset_reset': self.config.get('auto_offset_reset', 'latest'),
                'enable_auto_commit': self.config.get('enable_auto_commit', 'true').lower() == 'true',
                'auto_commit_interval_ms': int(self.config.get('auto_commit_interval_ms', 5000)),
                'session_timeout_ms': int(self.config.get('session_timeout_ms', 30000)),
                'heartbeat_interval_ms': int(self.config.get('heartbeat_interval_ms', 3000)),
                'max_poll_records': int(self.config.get('max_poll_records', 500)),
                'fetch_min_bytes': int(self.config.get('fetch_min_bytes', 1)),
                'fetch_max_wait_ms': int(self.config.get('fetch_max_wait_ms', 500)),
                'consumer_timeout_ms': int(self.config.get('consumer_timeout_ms', 1000))
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
            
            kafka_logger.info("Kafka consumer connection parameters configured")
            
        except Exception as e:
            kafka_logger.error(f"Failed to setup Kafka consumer connection parameters: {e}")
            raise
    
    def _get_deserializer(self, deserializer_type: str):
        """Get deserializer function based on type."""
        if deserializer_type.lower() == 'json':
            return lambda x: json.loads(x.decode('utf-8')) if x else None
        elif deserializer_type.lower() == 'string':
            return lambda x: x.decode('utf-8') if x else None
        elif deserializer_type.lower() == 'bytes':
            return lambda x: x
        else:
            return lambda x: x.decode('utf-8') if x else None
    
    def connect(self, topics: List[str] = None):
        """
        Create Kafka consumer connection and subscribe to topics.
        
        Args:
            topics: List of topics to subscribe to
        """
        try:
            kafka_logger.info(f"Connecting Kafka consumer to brokers: {self.connection_params['bootstrap_servers']}")
            
            self.consumer = KafkaConsumer(**self.connection_params)
            
            if topics:
                self.consumer.subscribe(topics)
                kafka_logger.info(f"Subscribed to topics: {topics}")
            
            kafka_logger.info("Kafka consumer connected successfully")
            
        except Exception as e:
            kafka_logger.error(f"Kafka consumer connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close Kafka consumer connection."""
        try:
            if self.consumer:
                self.consumer.close()
                self.consumer = None
                kafka_logger.info("Kafka consumer connection closed")
                
        except Exception as e:
            kafka_logger.error(f"Error during Kafka consumer disconnect: {e}")
    
    def consume_messages(self, topics: List[str], max_messages: int = None, 
                        timeout_ms: int = 10000) -> List[Dict[str, Any]]:
        """
        Consume messages from Kafka topics.
        
        Args:
            topics: List of topics to consume from
            max_messages: Maximum number of messages to consume
            timeout_ms: Timeout in milliseconds
            
        Returns:
            List of message dictionaries
        """
        try:
            if not self.consumer:
                self.connect(topics)
            else:
                self.consumer.subscribe(topics)
            
            kafka_logger.info(f"Consuming messages from topics: {topics}")
            
            messages = []
            consumed_count = 0
            start_time = time.time()
            
            for message in self.consumer:
                try:
                    # Convert headers to dict
                    headers = {}
                    if message.headers:
                        headers = {k: v.decode('utf-8') if isinstance(v, bytes) else v 
                                 for k, v in message.headers}
                    
                    message_data = {
                        'topic': message.topic,
                        'partition': message.partition,
                        'offset': message.offset,
                        'timestamp': message.timestamp,
                        'timestamp_type': message.timestamp_type,
                        'key': message.key,
                        'value': message.value,
                        'headers': headers,
                        'checksum': getattr(message, 'checksum', None),
                        'serialized_key_size': getattr(message, 'serialized_key_size', None),
                        'serialized_value_size': getattr(message, 'serialized_value_size', None)
                    }
                    
                    messages.append(message_data)
                    consumed_count += 1
                    
                    kafka_logger.debug(f"Consumed message from {message.topic}:{message.partition}:{message.offset}")
                    
                    # Check limits
                    if max_messages and consumed_count >= max_messages:
                        break
                    
                    # Check timeout
                    if timeout_ms and (time.time() - start_time) * 1000 >= timeout_ms:
                        break
                        
                except Exception as e:
                    kafka_logger.error(f"Error processing message: {e}")
                    continue
            
            kafka_logger.info(f"Consumed {consumed_count} messages from topics: {topics}")
            return messages
            
        except Exception as e:
            kafka_logger.error(f"Error consuming messages: {e}")
            return []
    
    def consume_messages_to_file(self, topics: List[str], output_file: str, 
                               max_messages: int = None, 
                               one_message_per_line: bool = True,
                               timeout_ms: int = 10000) -> Dict[str, Any]:
        """
        Consume messages from Kafka and write to file.
        
        Args:
            topics: List of topics to consume from
            output_file: Output file path
            max_messages: Maximum number of messages to consume
            one_message_per_line: If True, each message = one line in file
            timeout_ms: Timeout in milliseconds
            
        Returns:
            Dictionary with consumption results
        """
        try:
            kafka_logger.info(f"Consuming messages from {topics} to file: {output_file}")
            
            # Consume messages
            messages = self.consume_messages(topics, max_messages, timeout_ms)
            
            if not messages:
                kafka_logger.info("No messages consumed")
                return {
                    'success': True,
                    'messages_written': 0,
                    'total_messages': 0,
                    'output_file': output_file,
                    'topics': topics
                }
            
            # Write messages to file
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            messages_written = 0
            total_size = 0
            
            with open(output_path, 'w', encoding='utf-8') as f:
                for message in messages:
                    try:
                        message_content = str(message['value'])
                        
                        if one_message_per_line:
                            # Write each message as a separate line
                            clean_content = message_content.strip().replace('\n', ' ').replace('\r', ' ')
                            f.write(clean_content + '\n')
                        else:
                            # Concatenate all messages
                            f.write(message_content)
                            if not message_content.endswith('\n'):
                                f.write('\n')
                        
                        messages_written += 1
                        total_size += len(message_content)
                        
                    except Exception as e:
                        kafka_logger.error(f"Failed to write message: {e}")
            
            results = {
                'success': True,
                'messages_written': messages_written,
                'total_messages': len(messages),
                'output_file': str(output_path),
                'total_content_size': total_size,
                'topics': topics
            }
            
            kafka_logger.info(f"Consumed {messages_written} messages to file {output_file}")
            return results
            
        except Exception as e:
            kafka_logger.error(f"Error consuming messages to file: {e}")
            return {
                'success': False,
                'error': str(e),
                'messages_written': 0,
                'total_messages': 0,
                'output_file': output_file,
                'topics': topics
            }
    
    def export_messages_with_format(self, topics: List[str], output_file: str, 
                                   export_format: str = 'txt',
                                   max_messages: int = None,
                                   timeout_ms: int = 10000) -> Dict[str, Any]:
        """
        Export Kafka messages to file with different formats.
        
        Args:
            topics: List of topics to consume from
            output_file: Output file path
            export_format: Export format ('txt', 'csv', 'json', 'xml')
            max_messages: Maximum number of messages to consume
            timeout_ms: Timeout in milliseconds
            
        Returns:
            Dictionary with export results
        """
        try:
            kafka_logger.info(f"Exporting messages from {topics} in {export_format.upper()} format")
            
            # Consume messages
            messages = self.consume_messages(topics, max_messages, timeout_ms)
            
            if not messages:
                return {
                    'success': True,
                    'messages_exported': 0,
                    'output_file': output_file,
                    'export_format': export_format,
                    'topics': topics
                }
            
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if export_format.lower() == 'txt':
                # Plain text format
                with open(output_path, 'w', encoding='utf-8') as f:
                    for msg in messages:
                        f.write(f"[{msg['topic']}:{msg['partition']}:{msg['offset']}] {msg['value']}\n")
            
            elif export_format.lower() == 'csv':
                # CSV format with metadata
                with open(output_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['topic', 'partition', 'offset', 'timestamp', 'key', 'value', 'headers'])
                    for msg in messages:
                        writer.writerow([
                            msg['topic'], msg['partition'], msg['offset'],
                            msg['timestamp'], msg['key'], msg['value'],
                            json.dumps(msg['headers']) if msg['headers'] else ''
                        ])
            
            elif export_format.lower() == 'json':
                # JSON format
                export_data = {
                    'metadata': {
                        'topics': topics,
                        'export_timestamp': datetime.now().isoformat(),
                        'total_messages': len(messages)
                    },
                    'messages': messages
                }
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
            
            elif export_format.lower() == 'xml':
                # XML format
                root = ET.Element('kafka_messages')
                root.set('total_count', str(len(messages)))
                root.set('topics', ','.join(topics))
                
                for msg in messages:
                    msg_elem = ET.SubElement(root, 'message')
                    msg_elem.set('topic', msg['topic'])
                    msg_elem.set('partition', str(msg['partition']))
                    msg_elem.set('offset', str(msg['offset']))
                    msg_elem.set('timestamp', str(msg['timestamp']))
                    
                    if msg['key']:
                        key_elem = ET.SubElement(msg_elem, 'key')
                        key_elem.text = str(msg['key'])
                    
                    value_elem = ET.SubElement(msg_elem, 'value')
                    value_elem.text = str(msg['value'])
                    
                    if msg['headers']:
                        headers_elem = ET.SubElement(msg_elem, 'headers')
                        for k, v in msg['headers'].items():
                            header_elem = ET.SubElement(headers_elem, 'header')
                            header_elem.set('key', k)
                            header_elem.text = str(v)
                
                tree = ET.ElementTree(root)
                tree.write(str(output_path), encoding='utf-8', xml_declaration=True)
            
            else:
                raise ValueError(f"Unsupported export format: {export_format}")
            
            file_size = output_path.stat().st_size
            
            kafka_logger.info(f"Exported {len(messages)} messages in {export_format.upper()} format")
            
            return {
                'success': True,
                'messages_exported': len(messages),
                'output_file': str(output_path),
                'export_format': export_format,
                'file_size': file_size,
                'topics': topics
            }
            
        except Exception as e:
            kafka_logger.error(f"Error exporting messages: {e}")
            return {
                'success': False,
                'error': str(e),
                'messages_exported': 0,
                'output_file': output_file,
                'export_format': export_format,
                'topics': topics
            }
    
    def get_topic_metadata(self, topics: List[str]) -> Dict[str, Any]:
        """
        Get metadata for specified topics.
        
        Args:
            topics: List of topics to get metadata for
            
        Returns:
            Dictionary with topic metadata
        """
        try:
            if not self.consumer:
                self.connect()
            
            metadata = {}
            for topic in topics:
                try:
                    partitions = self.consumer.partitions_for_topic(topic)
                    if partitions:
                        # Get partition info
                        partition_info = []
                        for partition_id in partitions:
                            tp = TopicPartition(topic, partition_id)
                            
                            # Get high water mark (latest offset)
                            high_water_mark = self.consumer.end_offsets([tp])[tp]
                            
                            # Get low water mark (earliest offset)
                            low_water_mark = self.consumer.beginning_offsets([tp])[tp]
                            
                            partition_info.append({
                                'partition_id': partition_id,
                                'high_water_mark': high_water_mark,
                                'low_water_mark': low_water_mark,
                                'message_count': high_water_mark - low_water_mark
                            })
                        
                        metadata[topic] = {
                            'partitions': partition_info,
                            'partition_count': len(partitions),
                            'total_messages': sum(p['message_count'] for p in partition_info)
                        }
                    else:
                        metadata[topic] = {
                            'error': 'Topic not found or no partitions available'
                        }
                        
                except Exception as e:
                    metadata[topic] = {
                        'error': str(e)
                    }
            
            kafka_logger.info(f"Retrieved metadata for topics: {list(metadata.keys())}")
            return metadata
            
        except Exception as e:
            kafka_logger.error(f"Error getting topic metadata: {e}")
            return {topic: {'error': str(e)} for topic in topics}
    
    def seek_to_beginning(self, topics: List[str]):
        """Seek consumer to beginning of topics."""
        try:
            if not self.consumer:
                self.connect(topics)
            
            # Get all topic partitions
            partitions = []
            for topic in topics:
                topic_partitions = self.consumer.partitions_for_topic(topic)
                if topic_partitions:
                    partitions.extend([TopicPartition(topic, p) for p in topic_partitions])
            
            if partitions:
                self.consumer.assign(partitions)
                self.consumer.seek_to_beginning(*partitions)
                kafka_logger.info(f"Seeked to beginning for topics: {topics}")
            
        except Exception as e:
            kafka_logger.error(f"Error seeking to beginning: {e}")
            raise
    
    def seek_to_end(self, topics: List[str]):
        """Seek consumer to end of topics."""
        try:
            if not self.consumer:
                self.connect(topics)
            
            # Get all topic partitions
            partitions = []
            for topic in topics:
                topic_partitions = self.consumer.partitions_for_topic(topic)
                if topic_partitions:
                    partitions.extend([TopicPartition(topic, p) for p in topic_partitions])
            
            if partitions:
                self.consumer.assign(partitions)
                self.consumer.seek_to_end(*partitions)
                kafka_logger.info(f"Seeked to end for topics: {topics}")
            
        except Exception as e:
            kafka_logger.error(f"Error seeking to end: {e}")
            raise
    
    def test_connection(self) -> bool:
        """
        Test Kafka consumer connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to connect
            self.connect()
            
            # Test by getting cluster metadata
            metadata = self.consumer.list_consumer_groups()
            success = metadata is not None
            
            # Disconnect
            self.disconnect()
            
            if success:
                kafka_logger.info("Kafka consumer connection test successful")
            else:
                kafka_logger.error("Kafka consumer connection test failed")
            
            return success
            
        except Exception as e:
            kafka_logger.error(f"Kafka consumer connection test failed: {e}")
            return False
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

# Global Kafka consumer instance
kafka_consumer = KafkaMessageConsumer()