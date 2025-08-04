# ==============================
# features/steps/kafka_steps.py
# ==============================
"""
Step definitions for Behave BDD tests using a Kafka connector.
"""
import json
import os
import time
from behave import given, when, then
from typing import List, Dict, Any

# Import the new Kafka connector
try:
    # We are now importing the 'config_loader' object
    from mq.kafka_connector import KafkaConnector
    from utils.logger import logger
    from utils.config_loader import config_loader
except ImportError as e:
    logger.error(f"Import error in kafka_steps.py: {e}")
    raise

@given('a Kafka connector is configured for environment "{env}"')
def step_configure_kafka_connector(context, env):
    """
    Sets up a Kafka connector instance in the behave context.
    The configuration is loaded from the config_loader using the specified environment.
    """
    logger.info(f"Configuring Kafka connector for environment: {env}")
    try:
        # Corrected call: Use the get_kafka_config method from the imported object
        # The method now accepts the 'env' parameter
        kafka_config_obj = config_loader.get_kafka_config(env=env)
        bootstrap_servers = kafka_config_obj.brokers
        
        if not bootstrap_servers:
            raise ValueError(f"Kafka bootstrap_servers not found in config for '{env}'")
            
        context.kafka_connector = KafkaConnector(bootstrap_servers=bootstrap_servers)
        context.current_env = env
        logger.info(f"Kafka connector initialized for servers: {bootstrap_servers}")
    except Exception as e:
        logger.error(f"Failed to configure Kafka connector: {e}")
        raise

@when('I post messages from file "{filename}" to Kafka topic "{topic}" with key "{key}"')
def step_post_messages_to_kafka_from_file(context, filename, topic, key):
    """
    Reads messages from a file and posts them to a Kafka topic.
    The file should contain one JSON message per line.
    """
    if not hasattr(context, 'kafka_connector'):
        raise ConnectionError("Kafka connector is not initialized. "
                              "Please use 'Given a Kafka connector is configured' first.")
    
    file_path = os.path.join("data", "input", filename)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found at: {file_path}")
        
    try:
        posted_count = 0
        with open(file_path, 'r') as f:
            for line in f:
                if line.strip():
                    message = json.loads(line)
                    success = context.kafka_connector.produce_message(topic, key, message)
                    if success:
                        posted_count += 1
                        
        context.posted_message_count = posted_count
        logger.info(f"Successfully posted {posted_count} messages to topic '{topic}'")
    except Exception as e:
        logger.error(f"Failed to post messages to Kafka: {e}")
        context.last_error = str(e)
        raise

@when('I consume {count:d} messages from Kafka topic "{topic}"')
def step_consume_messages_from_kafka(context, count, topic):
    """
    Consumes a specified number of messages from a Kafka topic.
    """
    if not hasattr(context, 'kafka_connector'):
        raise ConnectionError("Kafka connector is not initialized.")
        
    logger.info(f"Attempting to consume {count} messages from topic '{topic}'")
    
    # We poll for messages and store them in a list
    consumed_messages = []
    # Loop for a certain period to gather messages, as consumer.poll is not blocking
    start_time = time.time()
    timeout = 10  # seconds
    
    while len(consumed_messages) < count and (time.time() - start_time) < timeout:
        messages = context.kafka_connector.consume_messages(topic, timeout_ms=1000)
        consumed_messages.extend(messages)
        if len(messages) > 0:
            logger.info(f"Received {len(messages)} messages, total: {len(consumed_messages)}")
            
    context.consumed_messages = consumed_messages[:count]
    logger.info(f"Finished consuming. Total messages received: {len(context.consumed_messages)}")

@then('the consumed Kafka messages are saved to file "{filename}"')
def step_save_consumed_messages_to_file(context, filename):
    """
    Writes the consumed messages to a file, one message per line.
    """
    if not hasattr(context, 'consumed_messages'):
        raise ValueError("No messages were consumed. 'context.consumed_messages' is empty.")
    
    file_path = os.path.join("output", "exports", filename)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    try:
        with open(file_path, 'w') as f:
            for message in context.consumed_messages:
                # Assuming messages are JSON-serializable dictionaries
                f.write(json.dumps(message) + '\n')
                
        logger.info(f"Successfully saved {len(context.consumed_messages)} messages to {file_path}")
    except Exception as e:
        logger.error(f"Failed to save consumed messages to file: {e}")
        context.last_error = str(e)
        raise
        
# A cleanup step for the Kafka connections
def kafka_after_scenario(context, scenario):
    """
    Close Kafka connections after each scenario.
    This function should be called from the environment.py file.
    """
    if hasattr(context, 'kafka_connector') and context.kafka_connector:
        try:
            context.kafka_connector.close()
            logger.info("Kafka connections closed successfully.")
        except Exception as e:
            logger.warning(f"Error while closing Kafka connections: {e}")
