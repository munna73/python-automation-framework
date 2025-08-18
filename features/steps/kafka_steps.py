"""
Kafka-related step definitions for Behave.
"""
from behave import given, when, then
import os
import time
from pathlib import Path
import sys

# Get project root (go up 3 levels: steps -> features -> project_root)
current_file = Path(__file__)
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root.absolute()))

from kafka.kafka_producer import kafka_producer
from kafka.kafka_consumer import kafka_consumer
from utils.logger import logger

# Create Kafka-specific logger
import logging
kafka_logger = logging.getLogger('kafka')

@given('Kafka connection is configured')
def step_kafka_connection_configured(context):
    """Verify Kafka connection is configured."""
    kafka_logger.info("Verifying Kafka connection configuration")
    context.kafka_producer = kafka_producer
    context.kafka_consumer = kafka_consumer
    assert context.kafka_producer.connection_params, "Kafka producer connection not configured"
    assert context.kafka_consumer.connection_params, "Kafka consumer connection not configured"

# ========================================
# KAFKA MESSAGE PRODUCER STEP DEFINITIONS
# ========================================

@when('I send file "{filename}" to Kafka topic "{topic}" line by line')
def step_send_file_to_kafka_line_by_line(context, filename, topic):
    """Send file to Kafka with each line as a separate message."""
    kafka_logger.info(f"Sending file {filename} to Kafka topic {topic} line by line")
    
    context.kafka_producer.connect()
    try:
        start_time = time.time()
        context.kafka_file_result = context.kafka_producer.send_file_as_kafka_messages(
            filename=filename,
            topic=topic,
            line_by_line=True
        )
        context.kafka_send_duration = time.time() - start_time
        
        success_count = context.kafka_file_result.get('success_count', 0)
        total_lines = context.kafka_file_result.get('total_lines', 0)
        kafka_logger.info(f"Sent {success_count}/{total_lines} lines as Kafka messages in {context.kafka_send_duration:.2f} seconds")
        
    except Exception as e:
        kafka_logger.error(f"Failed to send file to Kafka line by line: {str(e)}")
        raise AssertionError(f"Kafka line-by-line send failed: {str(e)}")
    finally:
        context.kafka_producer.disconnect()

@when('I send file "{filename}" to Kafka topic "{topic}" as whole file')
def step_send_file_to_kafka_whole_file(context, filename, topic):
    """Send entire file to Kafka as a single message."""
    kafka_logger.info(f"Sending file {filename} to Kafka topic {topic} as whole file")
    
    context.kafka_producer.connect()
    try:
        start_time = time.time()
        context.kafka_file_result = context.kafka_producer.send_file_as_kafka_messages(
            filename=filename,
            topic=topic,
            line_by_line=False
        )
        context.kafka_send_duration = time.time() - start_time
        
        success_count = context.kafka_file_result.get('success_count', 0)
        kafka_logger.info(f"Sent {success_count} file as Kafka message in {context.kafka_send_duration:.2f} seconds")
        
    except Exception as e:
        kafka_logger.error(f"Failed to send file to Kafka as whole file: {str(e)}")
        raise AssertionError(f"Kafka whole file send failed: {str(e)}")
    finally:
        context.kafka_producer.disconnect()

@when('I send Kafka message "{message_text}" to topic "{topic}"')
def step_send_kafka_message(context, message_text, topic):
    """Send a single message to Kafka topic."""
    kafka_logger.info(f"Sending Kafka message to topic {topic}: {message_text}")
    
    context.kafka_producer.connect()
    try:
        result = context.kafka_producer.send_message(
            topic=topic,
            message=message_text
        )
        context.kafka_message_result = result
        
        if result['success']:
            kafka_logger.info(f"Kafka message sent successfully to {topic}")
        else:
            raise AssertionError(f"Failed to send Kafka message: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        kafka_logger.error(f"Failed to send Kafka message: {str(e)}")
        raise AssertionError(f"Kafka message send failed: {str(e)}")
    finally:
        context.kafka_producer.disconnect()

@when('I send batch Kafka messages to topic "{topic}"')
def step_send_batch_kafka_messages(context, topic):
    """Send multiple messages to Kafka topic from table data."""
    if not hasattr(context, 'table') or not context.table:
        raise AssertionError("No message table provided for batch sending")
    
    messages = [row['message'] for row in context.table]
    kafka_logger.info(f"Sending batch of {len(messages)} Kafka messages to topic {topic}")
    
    context.kafka_producer.connect()
    try:
        start_time = time.time()
        context.kafka_batch_result = context.kafka_producer.send_messages_batch(
            topic=topic,
            messages=messages
        )
        context.kafka_batch_duration = time.time() - start_time
        
        success_count = context.kafka_batch_result.get('success_count', 0)
        kafka_logger.info(f"Sent {success_count}/{len(messages)} Kafka messages in batch in {context.kafka_batch_duration:.2f} seconds")
        
    except Exception as e:
        kafka_logger.error(f"Failed to send Kafka batch messages: {str(e)}")
        raise AssertionError(f"Kafka batch send failed: {str(e)}")
    finally:
        context.kafka_producer.disconnect()

@when('I send JSON messages to Kafka topic "{topic}"')
def step_send_json_messages_to_kafka(context, topic):
    """Send JSON objects to Kafka topic from table data."""
    if not hasattr(context, 'table') or not context.table:
        raise AssertionError("No JSON table provided for sending")
    
    json_objects = []
    for row in context.table:
        json_obj = {col: row[col] for col in context.table.headings}
        json_objects.append(json_obj)
    
    kafka_logger.info(f"Sending {len(json_objects)} JSON messages to Kafka topic {topic}")
    
    context.kafka_producer.connect()
    try:
        start_time = time.time()
        context.kafka_json_result = context.kafka_producer.send_json_messages(
            topic=topic,
            json_objects=json_objects
        )
        context.kafka_json_duration = time.time() - start_time
        
        success_count = context.kafka_json_result.get('success_count', 0)
        kafka_logger.info(f"Sent {success_count}/{len(json_objects)} JSON messages in {context.kafka_json_duration:.2f} seconds")
        
    except Exception as e:
        kafka_logger.error(f"Failed to send JSON messages to Kafka: {str(e)}")
        raise AssertionError(f"Kafka JSON send failed: {str(e)}")
    finally:
        context.kafka_producer.disconnect()

# ========================================
# KAFKA MESSAGE CONSUMER STEP DEFINITIONS
# ========================================

@when('I consume messages from Kafka topics "{topics}" and write to file "{output_file}" line by line')
def step_consume_kafka_messages_line_by_line(context, topics, output_file):
    """Consume Kafka messages and write each message as a line in file."""
    topic_list = [topic.strip() for topic in topics.split(',')]
    kafka_logger.info(f"Consuming Kafka messages from topics {topic_list} to file {output_file} line by line")
    
    context.kafka_consumer.connect(topic_list)
    try:
        start_time = time.time()
        context.kafka_consume_result = context.kafka_consumer.consume_messages_to_file(
            topics=topic_list,
            output_file=output_file,
            one_message_per_line=True
        )
        context.kafka_consume_duration = time.time() - start_time
        
        messages_count = context.kafka_consume_result.get('messages_written', 0)
        kafka_logger.info(f"Consumed {messages_count} Kafka messages as lines in {context.kafka_consume_duration:.2f} seconds")
        
    except Exception as e:
        kafka_logger.error(f"Failed to consume Kafka messages line by line: {str(e)}")
        raise AssertionError(f"Kafka line-by-line consumption failed: {str(e)}")
    finally:
        context.kafka_consumer.disconnect()

@when('I consume messages from Kafka topics "{topics}" and write to file "{output_file}" as whole file')
def step_consume_kafka_messages_whole_file(context, topics, output_file):
    """Consume Kafka messages and concatenate all content into single file."""
    topic_list = [topic.strip() for topic in topics.split(',')]
    kafka_logger.info(f"Consuming Kafka messages from topics {topic_list} to file {output_file} as whole file")
    
    context.kafka_consumer.connect(topic_list)
    try:
        start_time = time.time()
        context.kafka_consume_result = context.kafka_consumer.consume_messages_to_file(
            topics=topic_list,
            output_file=output_file,
            one_message_per_line=False
        )
        context.kafka_consume_duration = time.time() - start_time
        
        messages_count = context.kafka_consume_result.get('messages_written', 0)
        kafka_logger.info(f"Consumed {messages_count} Kafka messages as whole file in {context.kafka_consume_duration:.2f} seconds")
        
    except Exception as e:
        kafka_logger.error(f"Failed to consume Kafka messages as whole file: {str(e)}")
        raise AssertionError(f"Kafka whole file consumption failed: {str(e)}")
    finally:
        context.kafka_consumer.disconnect()

@when('I consume {max_messages:d} messages from Kafka topics "{topics}" and write to file "{output_file}" line by line')
def step_consume_limited_kafka_messages(context, max_messages, topics, output_file):
    """Consume limited number of Kafka messages and write each as a line in file."""
    topic_list = [topic.strip() for topic in topics.split(',')]
    kafka_logger.info(f"Consuming {max_messages} Kafka messages from topics {topic_list} to file {output_file} line by line")
    
    context.kafka_consumer.connect(topic_list)
    try:
        start_time = time.time()
        context.kafka_consume_result = context.kafka_consumer.consume_messages_to_file(
            topics=topic_list,
            output_file=output_file,
            max_messages=max_messages,
            one_message_per_line=True
        )
        context.kafka_consume_duration = time.time() - start_time
        
        messages_count = context.kafka_consume_result.get('messages_written', 0)
        kafka_logger.info(f"Consumed {messages_count}/{max_messages} Kafka messages as lines in {context.kafka_consume_duration:.2f} seconds")
        
    except Exception as e:
        kafka_logger.error(f"Failed to consume limited Kafka messages: {str(e)}")
        raise AssertionError(f"Kafka limited consumption failed: {str(e)}")
    finally:
        context.kafka_consumer.disconnect()

@when('I export Kafka messages from topics "{topics}" to file "{output_file}" in "{export_format}" format')
def step_export_kafka_messages_with_format(context, topics, output_file, export_format):
    """Export Kafka messages to file with specific format (txt, csv, json, xml)."""
    topic_list = [topic.strip() for topic in topics.split(',')]
    kafka_logger.info(f"Exporting Kafka messages from topics {topic_list} to {output_file} in {export_format} format")
    
    context.kafka_consumer.connect(topic_list)
    try:
        start_time = time.time()
        context.kafka_export_result = context.kafka_consumer.export_messages_with_format(
            topics=topic_list,
            output_file=output_file,
            export_format=export_format
        )
        context.kafka_export_duration = time.time() - start_time
        
        messages_count = context.kafka_export_result.get('messages_exported', 0)
        kafka_logger.info(f"Exported {messages_count} Kafka messages in {export_format} format in {context.kafka_export_duration:.2f} seconds")
        
    except Exception as e:
        kafka_logger.error(f"Failed to export Kafka messages: {str(e)}")
        raise AssertionError(f"Kafka export failed: {str(e)}")
    finally:
        context.kafka_consumer.disconnect()

@when('I get topic metadata for Kafka topics "{topics}"')
def step_get_kafka_topic_metadata(context, topics):
    """Get metadata for specified Kafka topics."""
    topic_list = [topic.strip() for topic in topics.split(',')]
    kafka_logger.info(f"Getting metadata for Kafka topics: {topic_list}")
    
    context.kafka_consumer.connect()
    try:
        context.kafka_metadata = context.kafka_consumer.get_topic_metadata(topic_list)
        
        for topic, metadata in context.kafka_metadata.items():
            if 'error' not in metadata:
                total_messages = metadata.get('total_messages', 0)
                partition_count = metadata.get('partition_count', 0)
                kafka_logger.info(f"Topic {topic}: {partition_count} partitions, {total_messages} total messages")
            else:
                kafka_logger.warning(f"Topic {topic}: {metadata['error']}")
                
    except Exception as e:
        kafka_logger.error(f"Failed to get Kafka topic metadata: {str(e)}")
        raise AssertionError(f"Kafka metadata retrieval failed: {str(e)}")
    finally:
        context.kafka_consumer.disconnect()

@when('I seek to beginning of Kafka topics "{topics}"')
def step_seek_kafka_topics_to_beginning(context, topics):
    """Seek consumer to beginning of Kafka topics."""
    topic_list = [topic.strip() for topic in topics.split(',')]
    kafka_logger.info(f"Seeking to beginning of Kafka topics: {topic_list}")
    
    try:
        context.kafka_consumer.seek_to_beginning(topic_list)
        kafka_logger.info(f"Seeked to beginning for topics: {topic_list}")
        
    except Exception as e:
        kafka_logger.error(f"Failed to seek to beginning: {str(e)}")
        raise AssertionError(f"Kafka seek to beginning failed: {str(e)}")

@when('I seek to end of Kafka topics "{topics}"')
def step_seek_kafka_topics_to_end(context, topics):
    """Seek consumer to end of Kafka topics."""
    topic_list = [topic.strip() for topic in topics.split(',')]
    kafka_logger.info(f"Seeking to end of Kafka topics: {topic_list}")
    
    try:
        context.kafka_consumer.seek_to_end(topic_list)
        kafka_logger.info(f"Seeked to end for topics: {topic_list}")
        
    except Exception as e:
        kafka_logger.error(f"Failed to seek to end: {str(e)}")
        raise AssertionError(f"Kafka seek to end failed: {str(e)}")

# ========================================
# KAFKA VERIFICATION STEP DEFINITIONS
# ========================================

@then('Kafka file should be sent successfully with {expected_messages:d} messages')
def step_verify_kafka_file_sent(context, expected_messages):
    """Verify Kafka file was sent with expected message count."""
    kafka_logger.info(f"Verifying Kafka file sent with {expected_messages} messages")
    
    assert hasattr(context, 'kafka_file_result'), "No Kafka file result available"
    assert context.kafka_file_result.get('success', False), "Kafka file send failed"
    
    success_count = context.kafka_file_result.get('success_count', 0)
    assert success_count == expected_messages, f"Expected {expected_messages} messages, got {success_count}"
    
    # Log performance metrics if available
    if hasattr(context, 'kafka_send_duration') and success_count > 0:
        rate = success_count / context.kafka_send_duration
        kafka_logger.info(f"Kafka send rate: {rate:.2f} messages/second")

@then('Kafka message should be sent successfully')
def step_verify_kafka_message_sent(context):
    """Verify Kafka message was sent successfully."""
    kafka_logger.info("Verifying Kafka message sent successfully")
    
    assert hasattr(context, 'kafka_message_result'), "No Kafka message result available"
    assert context.kafka_message_result.get('success', False), "Kafka message send failed"
    
    topic = context.kafka_message_result.get('topic', 'unknown')
    partition = context.kafka_message_result.get('partition', 'unknown')
    offset = context.kafka_message_result.get('offset', 'unknown')
    kafka_logger.info(f"Kafka message sent successfully to {topic}:{partition}:{offset}")

@then('Kafka batch should be sent successfully with {expected_messages:d} messages')
def step_verify_kafka_batch_sent(context, expected_messages):
    """Verify Kafka batch was sent with expected message count."""
    kafka_logger.info(f"Verifying Kafka batch sent with {expected_messages} messages")
    
    assert hasattr(context, 'kafka_batch_result'), "No Kafka batch result available"
    assert context.kafka_batch_result.get('success', False), "Kafka batch send failed"
    
    success_count = context.kafka_batch_result.get('success_count', 0)
    assert success_count == expected_messages, f"Expected {expected_messages} messages, got {success_count}"
    
    # Log performance metrics if available
    if hasattr(context, 'kafka_batch_duration') and success_count > 0:
        rate = success_count / context.kafka_batch_duration
        kafka_logger.info(f"Kafka batch send rate: {rate:.2f} messages/second")

@then('Kafka JSON messages should be sent successfully with {expected_messages:d} messages')
def step_verify_kafka_json_sent(context, expected_messages):
    """Verify Kafka JSON messages were sent with expected count."""
    kafka_logger.info(f"Verifying Kafka JSON messages sent with {expected_messages} messages")
    
    assert hasattr(context, 'kafka_json_result'), "No Kafka JSON result available"
    assert context.kafka_json_result.get('success', False), "Kafka JSON send failed"
    
    success_count = context.kafka_json_result.get('success_count', 0)
    assert success_count == expected_messages, f"Expected {expected_messages} messages, got {success_count}"
    
    # Log performance metrics if available
    if hasattr(context, 'kafka_json_duration') and success_count > 0:
        rate = success_count / context.kafka_json_duration
        kafka_logger.info(f"Kafka JSON send rate: {rate:.2f} messages/second")

@then('Kafka message consumption should be successful')
def step_verify_kafka_consumption_success(context):
    """Verify Kafka message consumption was successful."""
    kafka_logger.info("Verifying Kafka message consumption success")
    
    assert hasattr(context, 'kafka_consume_result'), "No Kafka consumption result available"
    assert context.kafka_consume_result.get('success', False), "Kafka message consumption failed"
    
    messages_written = context.kafka_consume_result.get('messages_written', 0)
    assert messages_written >= 0, "Invalid message count"
    
    kafka_logger.info(f"Kafka message consumption successful: {messages_written} messages written")

@then('Kafka should consume {expected_messages:d} messages to file')
def step_verify_kafka_messages_consumed_count(context, expected_messages):
    """Verify expected number of messages were consumed from Kafka."""
    kafka_logger.info(f"Verifying Kafka consumed {expected_messages} messages")
    
    assert hasattr(context, 'kafka_consume_result'), "No Kafka consumption result available"
    actual_messages = context.kafka_consume_result.get('messages_written', 0)
    
    assert actual_messages == expected_messages, f"Expected {expected_messages} messages, got {actual_messages}"
    
    # Log performance metrics if available
    if hasattr(context, 'kafka_consume_duration') and actual_messages > 0:
        rate = actual_messages / context.kafka_consume_duration
        kafka_logger.info(f"Kafka consumption rate: {rate:.2f} messages/second")

@then('Kafka export should be successful with {expected_messages:d} messages')
def step_verify_kafka_export_success(context, expected_messages):
    """Verify Kafka export was successful with expected message count."""
    kafka_logger.info(f"Verifying Kafka export with {expected_messages} messages")
    
    assert hasattr(context, 'kafka_export_result'), "No Kafka export result available"
    assert context.kafka_export_result.get('success', False), "Kafka export failed"
    
    exported_count = context.kafka_export_result.get('messages_exported', 0)
    assert exported_count == expected_messages, f"Expected {expected_messages} messages, got {exported_count}"
    
    export_format = context.kafka_export_result.get('export_format', 'unknown')
    file_size = context.kafka_export_result.get('file_size', 0)
    kafka_logger.info(f"Kafka export successful: {exported_count} messages in {export_format} format ({file_size} bytes)")

@then('Kafka topic "{topic}" should have {expected_partitions:d} partitions')
def step_verify_kafka_topic_partitions(context, topic, expected_partitions):
    """Verify Kafka topic has expected number of partitions."""
    kafka_logger.info(f"Verifying Kafka topic {topic} has {expected_partitions} partitions")
    
    assert hasattr(context, 'kafka_metadata'), "No Kafka metadata available"
    assert topic in context.kafka_metadata, f"Topic {topic} not found in metadata"
    
    topic_metadata = context.kafka_metadata[topic]
    assert 'error' not in topic_metadata, f"Error in topic metadata: {topic_metadata.get('error')}"
    
    actual_partitions = topic_metadata.get('partition_count', 0)
    assert actual_partitions == expected_partitions, f"Expected {expected_partitions} partitions, got {actual_partitions}"
    
    kafka_logger.info(f"Kafka topic {topic} verified: {actual_partitions} partitions")

@then('Kafka topic "{topic}" should have at least {min_messages:d} messages')
def step_verify_kafka_topic_min_messages(context, topic, min_messages):
    """Verify Kafka topic has at least minimum number of messages."""
    kafka_logger.info(f"Verifying Kafka topic {topic} has at least {min_messages} messages")
    
    assert hasattr(context, 'kafka_metadata'), "No Kafka metadata available"
    assert topic in context.kafka_metadata, f"Topic {topic} not found in metadata"
    
    topic_metadata = context.kafka_metadata[topic]
    assert 'error' not in topic_metadata, f"Error in topic metadata: {topic_metadata.get('error')}"
    
    actual_messages = topic_metadata.get('total_messages', 0)
    assert actual_messages >= min_messages, f"Expected at least {min_messages} messages, got {actual_messages}"
    
    kafka_logger.info(f"Kafka topic {topic} verified: {actual_messages} messages (minimum {min_messages})")

@then('Kafka processing should complete within {expected_time:d} seconds')
def step_verify_kafka_processing_time(context, expected_time):
    """Verify Kafka processing completed within expected time."""
    duration = (getattr(context, 'kafka_consume_duration', 0) or 
               getattr(context, 'kafka_send_duration', 0) or 
               getattr(context, 'kafka_export_duration', 0) or 
               getattr(context, 'kafka_batch_duration', 0) or
               getattr(context, 'kafka_json_duration', 0))
    
    assert duration <= expected_time, f"Kafka processing took {duration:.2f}s, expected under {expected_time}s"
    
    kafka_logger.info(f"Kafka processing completed in {duration:.2f} seconds")
