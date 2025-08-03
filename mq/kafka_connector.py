# mq/kafka_connector.py

import json
import logging
from typing import List, Dict, Optional, Any, Tuple

try:
    from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import KafkaError, NoBrokersAvailable, TopicAlreadyExistsError
    from kafka.future import Future
    from kafka.structs import TopicPartition
except ImportError:
    logging.error("The 'kafka-python' library is not installed. Please add it to your requirements.txt.")
    raise

class KafkaConnector:
    """
    An updated connector for interacting with a Kafka broker.
    Handles producing, consuming, and administrative tasks.
    """
    
    def __init__(self, bootstrap_servers: str, client_id: str = 'behave-kafka-client'):
        """
        Initializes the KafkaConnector.

        Args:
            bootstrap_servers (str): Comma-separated string of Kafka brokers.
            client_id (str): A client ID for the producer and consumer.
        """
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.producer = None
        self.consumer = None
        self.admin_client = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self._producer_connected = False
        self._consumer_connected = False
        self._admin_connected = False
        self.message_confirmations = []

    def _get_producer(self) -> KafkaProducer:
        """
        Creates and returns a KafkaProducer instance.
        If a producer is already connected, it returns the existing one.
        """
        if self._producer_connected:
            return self.producer
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1) # Set a compatible API version
            )
            self._producer_connected = True
            self.logger.info("Kafka producer connected successfully.")
            return self.producer
        except (KafkaError, NoBrokersAvailable) as e:
            self.logger.error(f"Failed to connect Kafka producer: {e}")
            raise
    
    def _get_consumer(self, topic: str, group_id: str = 'behave-test-consumer', auto_offset_reset: str = 'earliest') -> KafkaConsumer:
        """
        Creates and returns a KafkaConsumer instance.
        If a consumer is already connected, it returns the existing one.
        """
        if self._consumer_connected and self.consumer.subscription() == {topic}:
            return self.consumer
            
        try:
            self.consumer = KafkaConsumer(
                topic,
                group_id=group_id,
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=True,
                api_version=(0, 10, 1) # Set a compatible API version
            )
            self._consumer_connected = True
            self.logger.info("Kafka consumer connected successfully.")
            return self.consumer
        except (KafkaError, NoBrokersAvailable) as e:
            self.logger.error(f"Failed to connect Kafka consumer: {e}")
            raise

    def _get_admin_client(self) -> KafkaAdminClient:
        """
        Creates and returns a KafkaAdminClient instance.
        """
        if self._admin_connected:
            return self.admin_client

        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                api_version=(0, 10, 1)
            )
            self._admin_connected = True
            self.logger.info("Kafka admin client connected successfully.")
            return self.admin_client
        except (KafkaError, NoBrokersAvailable) as e:
            self.logger.error(f"Failed to connect Kafka admin client: {e}")
            raise

    def get_topics(self) -> List[str]:
        """
        Retrieves a list of all available topics from the broker.
        """
        admin_client = self._get_admin_client()
        return list(admin_client.list_topics())

    def produce_message(self, topic: str, value: Any, key: Optional[str] = None, partition: Optional[int] = None, headers: Optional[Dict[str, str]] = None, timeout_ms: int = 10000) -> Tuple[bool, Optional[Future]]:
        """
        Sends a single message to a Kafka topic.

        Args:
            topic (str): The Kafka topic to send the message to.
            value (Any): The message value (will be serialized to JSON).
            key (Optional[str]): The message key.
            partition (Optional[int]): The partition to send the message to.
            headers (Optional[Dict[str, str]]): Headers to attach to the message.
            timeout_ms (int): The timeout for the send operation in milliseconds.

        Returns:
            Tuple[bool, Optional[Future]]: A tuple with a success boolean and the future object.
        """
        producer = self._get_producer()
        try:
            encoded_key = key.encode('utf-8') if key else None
            encoded_headers = [(k, v.encode('utf-8')) for k, v in headers.items()] if headers else None

            future = producer.send(
                topic,
                key=encoded_key,
                value=value,
                partition=partition,
                headers=encoded_headers
            )
            producer.flush(timeout=timeout_ms/1000)
            self.message_confirmations.append(future)
            self.logger.info(f"Message send initiated for topic '{topic}'. Waiting for confirmation...")
            return True, future
        except KafkaError as e:
            self.logger.error(f"Failed to send message to topic '{topic}': {e}")
            return False, None
    
    def consume_messages(self, topic: str, timeout_ms: int = 5000, max_messages: int = 100,
                         offset: Optional[int] = None) -> List[Dict]:
        """
        Consumes messages from a Kafka topic within a timeout period.

        Args:
            topic (str): The Kafka topic to consume from.
            timeout_ms (int): How long to wait for new messages in milliseconds.
            max_messages (int): The maximum number of messages to retrieve.
            offset (Optional[int]): The specific offset to start consuming from.

        Returns:
            List[Dict]: A list of consumed messages.
        """
        # A simple consumer group ID for testing. We'll rely on the `behave` scenario to control the test run.
        group_id = 'behave-test-consumer-{}'.format(os.getpid())
        consumer = self._get_consumer(topic, group_id)
        
        # If an offset is specified, seek to that offset
        if offset is not None:
            # The consumer must be assigned a partition before seeking
            partitions = consumer.partitions_for_topic(topic)
            if partitions:
                tp = TopicPartition(topic, list(partitions)[0])
                consumer.assign([tp])
                consumer.seek(tp, offset)
                self.logger.info(f"Seeking to offset {offset} for topic {topic}, partition {tp.partition}")
            else:
                self.logger.warning(f"No partitions found for topic {topic}. Cannot seek to offset.")
                return []
        
        messages = []
        for msg in consumer:
            if len(messages) >= max_messages:
                break
            messages.append({
                'value': msg.value,
                'key': msg.key.decode('utf-8') if msg.key else None,
                'headers': {k: v.decode('utf-8') for k, v in msg.headers} if msg.headers else None,
                'partition': msg.partition,
                'offset': msg.offset
            })
        
        self.logger.info(f"Consumed {len(messages)} messages from topic '{topic}'")
        return messages

    def cleanup_topic(self, topic: str):
        """
        Deletes a specific topic from the Kafka broker.
        """
        admin_client = self._get_admin_client()
        try:
            self.logger.info(f"Attempting to delete topic '{topic}'...")
            admin_client.delete_topics(topics=[topic])
            self.logger.info(f"Topic '{topic}' deleted successfully.")
        except Exception as e:
            self.logger.error(f"Failed to delete topic '{topic}': {e}")
            raise
    
    def close(self):
        """
        Closes the producer and consumer connections.
        """
        if self.producer:
            self.producer.close()
            self.logger.info("Kafka producer closed.")
            self._producer_connected = False
        if self.consumer:
            self.consumer.close()
            self.logger.info("Kafka consumer closed.")
            self._consumer_connected = False
        if self.admin_client:
            self.admin_client.close()
            self.logger.info("Kafka admin client closed.")
            self._admin_connected = False
            
