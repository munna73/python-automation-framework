"""
MQ-related step definitions for Behave.
"""
from behave import given, when, then
import os
from pathlib import Path
import sys
# Get project root (go up 3 levels: steps -> features -> project_root)
current_file = Path(__file__)
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root.absolute()))

# Conditional imports to avoid pymqi dependency issues
try:
    from mq.mq_producer import mq_producer
    from mq.mq_consumer import mq_consumer
    MQ_AVAILABLE = True
except ImportError as e:
    print(f"MQ modules not available: {e}")
    MQ_AVAILABLE = False
    mq_producer = None
    mq_consumer = None

from utils.logger import logger, mq_logger

@given('MQ connection is configured')
def step_mq_connection_configured(context):
    """Verify MQ connection is configured."""
    mq_logger.info("Verifying MQ connection configuration")
    context.mq_producer = mq_producer
    assert context.mq_producer.connection_params, "MQ connection not configured"

@when('I post message from "{filename}" as single message')
def step_post_file_as_single_message(context, filename):
    """Post file content as single message."""
    mq_logger.info(f"Posting file {filename} as single message")
    context.mq_producer.connect()
    try:
        context.mq_result = context.mq_producer.post_file_as_single_message(filename)
    finally:
        context.mq_producer.disconnect()

@when('I post message from "{filename}" line by line')
def step_post_file_line_by_line(context, filename):
    """Post file content line by line."""
    mq_logger.info(f"Posting file {filename} line by line")
    context.mq_producer.connect()
    try:
        context.mq_result = context.mq_producer.post_file_line_by_line(filename)
    finally:
        context.mq_producer.disconnect()

@when('I post custom message "{message_text}"')
def step_post_custom_message(context, message_text):
    """Post custom message text."""
    mq_logger.info(f"Posting custom message: {message_text}")
    context.mq_producer.connect()
    try:
        context.mq_result = context.mq_producer.post_message(message_text)
    finally:
        context.mq_producer.disconnect()

@then('message should be posted successfully')
def step_verify_message_posted(context):
    """Verify message was posted successfully."""
    mq_logger.info("Verifying message posted successfully")
    assert hasattr(context, 'mq_result'), "No MQ result available"
    assert context.mq_result == True, "Message posting failed"

@then('all lines should be posted successfully')
def step_verify_all_lines_posted(context):
    """Verify all lines were posted successfully."""
    mq_logger.info("Verifying all lines posted successfully")
    assert hasattr(context, 'mq_result'), "No MQ result available"
    assert isinstance(context.mq_result, dict), "Invalid MQ result format"
    assert context.mq_result.get('error_count', 1) == 0, f"Some lines failed to post: {context.mq_result.get('error_count')} errors"

@then('success count should match file line count')
def step_verify_success_count_matches_lines(context):
    """Verify success count matches the number of lines in file."""
    mq_logger.info("Verifying success count matches file line count")
    assert hasattr(context, 'mq_result'), "No MQ result available"
    assert isinstance(context.mq_result, dict), "Invalid MQ result format"
    
    total_lines = context.mq_result.get('total_lines', 0)
    success_count = context.mq_result.get('success_count', 0)
    
    assert success_count == total_lines, f"Success count {success_count} doesn't match total lines {total_lines}"

@then('MQ posting should have {expected_success_rate:d}% success rate')
def step_verify_success_rate(context, expected_success_rate):
    """Verify MQ posting success rate."""
    mq_logger.info(f"Verifying success rate is {expected_success_rate}%")
    assert hasattr(context, 'mq_result'), "No MQ result available"
    assert isinstance(context.mq_result, dict), "Invalid MQ result format"
    
    actual_rate = context.mq_result.get('success_rate', 0)
    assert actual_rate >= expected_success_rate, f"Success rate {actual_rate:.2f}% is below expected {expected_success_rate}%"


# ========================================
# MQ MESSAGE-STYLE STEP DEFINITIONS
# ========================================

@when('I send file "{filename}" to MQ line by line')
def step_send_file_to_mq_line_by_line(context, filename):
    """Send file to MQ with each line as a separate message."""
    mq_logger.info(f"Sending file {filename} to MQ line by line")
    
    context.mq_producer.connect()
    try:
        start_time = time.time()
        context.mq_file_result = context.mq_producer.send_file_as_mq_messages(
            filename=filename,
            line_by_line=True
        )
        context.mq_send_duration = time.time() - start_time
        
        success_count = context.mq_file_result.get('success_count', 0)
        total_lines = context.mq_file_result.get('total_lines', 0)
        mq_logger.info(f"Sent {success_count}/{total_lines} lines as MQ messages in {context.mq_send_duration:.2f} seconds")
        
    except Exception as e:
        mq_logger.error(f"Failed to send file to MQ line by line: {str(e)}")
        raise AssertionError(f"MQ line-by-line send failed: {str(e)}")
    finally:
        context.mq_producer.disconnect()

@when('I send file "{filename}" to MQ as whole file')
def step_send_file_to_mq_whole_file(context, filename):
    """Send entire file to MQ as a single message."""
    mq_logger.info(f"Sending file {filename} to MQ as whole file")
    
    context.mq_producer.connect()
    try:
        start_time = time.time()
        context.mq_file_result = context.mq_producer.send_file_as_mq_messages(
            filename=filename,
            line_by_line=False
        )
        context.mq_send_duration = time.time() - start_time
        
        success_count = context.mq_file_result.get('success_count', 0)
        mq_logger.info(f"Sent {success_count} file as MQ message in {context.mq_send_duration:.2f} seconds")
        
    except Exception as e:
        mq_logger.error(f"Failed to send file to MQ as whole file: {str(e)}")
        raise AssertionError(f"MQ whole file send failed: {str(e)}")
    finally:
        context.mq_producer.disconnect()

@when('I retrieve MQ messages and write to file "{output_file}" line by line')
def step_retrieve_mq_messages_line_by_line(context, output_file):
    """Retrieve MQ messages and write each message as a line in file."""
    mq_logger.info(f"Retrieving MQ messages to file {output_file} line by line")
    
    mq_consumer.connect()
    try:
        start_time = time.time()
        context.mq_retrieve_result = mq_consumer.retrieve_messages_to_file(
            output_file=output_file,
            one_message_per_line=True
        )
        context.mq_retrieve_duration = time.time() - start_time
        
        messages_count = context.mq_retrieve_result.get('messages_written', 0)
        mq_logger.info(f"Retrieved {messages_count} MQ messages as lines in {context.mq_retrieve_duration:.2f} seconds")
        
    except Exception as e:
        mq_logger.error(f"Failed to retrieve MQ messages line by line: {str(e)}")
        raise AssertionError(f"MQ line-by-line retrieval failed: {str(e)}")
    finally:
        mq_consumer.disconnect()

@when('I retrieve MQ messages and write to file "{output_file}" as whole file')
def step_retrieve_mq_messages_whole_file(context, output_file):
    """Retrieve MQ messages and concatenate all content into single file."""
    mq_logger.info(f"Retrieving MQ messages to file {output_file} as whole file")
    
    mq_consumer.connect()
    try:
        start_time = time.time()
        context.mq_retrieve_result = mq_consumer.retrieve_messages_to_file(
            output_file=output_file,
            one_message_per_line=False
        )
        context.mq_retrieve_duration = time.time() - start_time
        
        messages_count = context.mq_retrieve_result.get('messages_written', 0)
        mq_logger.info(f"Retrieved {messages_count} MQ messages as whole file in {context.mq_retrieve_duration:.2f} seconds")
        
    except Exception as e:
        mq_logger.error(f"Failed to retrieve MQ messages as whole file: {str(e)}")
        raise AssertionError(f"MQ whole file retrieval failed: {str(e)}")
    finally:
        mq_consumer.disconnect()

@when('I retrieve {max_messages:d} MQ messages and write to file "{output_file}" line by line')
def step_retrieve_limited_mq_messages(context, max_messages, output_file):
    """Retrieve limited number of MQ messages and write each as a line in file."""
    mq_logger.info(f"Retrieving {max_messages} MQ messages to file {output_file} line by line")
    
    mq_consumer.connect()
    try:
        start_time = time.time()
        context.mq_retrieve_result = mq_consumer.retrieve_messages_to_file(
            output_file=output_file,
            max_messages=max_messages,
            one_message_per_line=True
        )
        context.mq_retrieve_duration = time.time() - start_time
        
        messages_count = context.mq_retrieve_result.get('messages_written', 0)
        mq_logger.info(f"Retrieved {messages_count}/{max_messages} MQ messages as lines in {context.mq_retrieve_duration:.2f} seconds")
        
    except Exception as e:
        mq_logger.error(f"Failed to retrieve limited MQ messages: {str(e)}")
        raise AssertionError(f"MQ limited retrieval failed: {str(e)}")
    finally:
        mq_consumer.disconnect()

@when('I post custom MQ message "{message_text}"')
def step_post_custom_mq_message(context, message_text):
    """Post a single custom message to MQ."""
    mq_logger.info(f"Posting custom MQ message: {message_text}")
    
    context.mq_producer.connect()
    try:
        success = context.mq_producer.post_message(message_text)
        context.mq_message_result = {
            'success': success,
            'message_length': len(message_text)
        }
        
        if success:
            mq_logger.info(f"Custom message posted successfully ({len(message_text)} characters)")
        else:
            raise AssertionError("Failed to post custom message to MQ")
            
    except Exception as e:
        mq_logger.error(f"Failed to post custom MQ message: {str(e)}")
        raise AssertionError(f"MQ custom message post failed: {str(e)}")
    finally:
        context.mq_producer.disconnect()

@when('I export MQ messages to file "{output_file}" in "{export_format}" format')
def step_export_mq_messages_with_format(context, output_file, export_format):
    """Export MQ messages to file with specific format (txt, csv, json, xml)."""
    mq_logger.info(f"Exporting MQ messages to {output_file} in {export_format} format")
    
    mq_consumer.connect()
    try:
        start_time = time.time()
        context.mq_export_result = mq_consumer.export_messages_with_format(
            output_file=output_file,
            export_format=export_format
        )
        context.mq_export_duration = time.time() - start_time
        
        messages_count = context.mq_export_result.get('messages_exported', 0)
        mq_logger.info(f"Exported {messages_count} MQ messages in {export_format} format in {context.mq_export_duration:.2f} seconds")
        
    except Exception as e:
        mq_logger.error(f"Failed to export MQ messages: {str(e)}")
        raise AssertionError(f"MQ export failed: {str(e)}")
    finally:
        mq_consumer.disconnect()

@when('I drain MQ queue to file "{output_file}"')
def step_drain_mq_queue_to_file(context, output_file):
    """Drain all messages from MQ queue to file."""
    mq_logger.info(f"Draining MQ queue to file {output_file}")
    
    mq_consumer.connect()
    try:
        start_time = time.time()
        context.mq_drain_result = mq_consumer.drain_queue_to_file(output_file)
        context.mq_drain_duration = time.time() - start_time
        
        messages_count = context.mq_drain_result.get('messages_drained', 0)
        mq_logger.info(f"Drained {messages_count} MQ messages to file in {context.mq_drain_duration:.2f} seconds")
        
    except Exception as e:
        mq_logger.error(f"Failed to drain MQ queue: {str(e)}")
        raise AssertionError(f"MQ queue drain failed: {str(e)}")
    finally:
        mq_consumer.disconnect()

@when('I get MQ queue depth')
def step_get_mq_queue_depth(context):
    """Get current MQ queue depth."""
    mq_logger.info("Getting MQ queue depth")
    
    mq_consumer.connect()
    try:
        depth = mq_consumer.get_queue_depth()
        context.mq_queue_depth = depth
        
        if depth is not None:
            mq_logger.info(f"MQ queue depth: {depth}")
        else:
            mq_logger.warning("Could not retrieve MQ queue depth")
            
    except Exception as e:
        mq_logger.error(f"Failed to get MQ queue depth: {str(e)}")
        raise AssertionError(f"MQ queue depth retrieval failed: {str(e)}")
    finally:
        mq_consumer.disconnect()

# MQ Message Verification Steps
@then('MQ file should be sent successfully with {expected_messages:d} messages')
def step_verify_mq_file_sent(context, expected_messages):
    """Verify MQ file was sent with expected message count."""
    mq_logger.info(f"Verifying MQ file sent with {expected_messages} messages")
    
    assert hasattr(context, 'mq_file_result'), "No MQ file result available"
    assert context.mq_file_result.get('success', False), "MQ file send failed"
    
    success_count = context.mq_file_result.get('success_count', 0)
    assert success_count == expected_messages, f"Expected {expected_messages} messages, got {success_count}"
    
    # Log performance metrics if available
    if hasattr(context, 'mq_send_duration') and success_count > 0:
        rate = success_count / context.mq_send_duration
        mq_logger.info(f"MQ send rate: {rate:.2f} messages/second")

@then('MQ message retrieval should be successful')
def step_verify_mq_message_retrieval_success(context):
    """Verify MQ message retrieval was successful."""
    mq_logger.info("Verifying MQ message retrieval success")
    
    assert hasattr(context, 'mq_retrieve_result'), "No MQ retrieval result available"
    assert context.mq_retrieve_result.get('success', False), "MQ message retrieval failed"
    
    messages_written = context.mq_retrieve_result.get('messages_written', 0)
    assert messages_written >= 0, "Invalid message count"
    
    mq_logger.info(f"MQ message retrieval successful: {messages_written} messages written")

@then('MQ should retrieve {expected_messages:d} messages to file')
def step_verify_mq_messages_retrieved_count(context, expected_messages):
    """Verify expected number of messages were retrieved from MQ."""
    mq_logger.info(f"Verifying MQ retrieved {expected_messages} messages")
    
    assert hasattr(context, 'mq_retrieve_result'), "No MQ retrieval result available"
    actual_messages = context.mq_retrieve_result.get('messages_written', 0)
    
    assert actual_messages == expected_messages, f"Expected {expected_messages} messages, got {actual_messages}"
    
    # Log performance metrics if available
    if hasattr(context, 'mq_retrieve_duration') and actual_messages > 0:
        rate = actual_messages / context.mq_retrieve_duration
        mq_logger.info(f"MQ retrieval rate: {rate:.2f} messages/second")

@then('MQ custom message should be posted successfully')
def step_verify_mq_custom_message_posted(context):
    """Verify MQ custom message was posted successfully."""
    mq_logger.info("Verifying MQ custom message posted successfully")
    
    assert hasattr(context, 'mq_message_result'), "No MQ message result available"
    assert context.mq_message_result.get('success', False), "MQ custom message post failed"
    
    message_length = context.mq_message_result.get('message_length', 0)
    mq_logger.info(f"MQ custom message posted successfully ({message_length} characters)")

@then('MQ export should be successful with {expected_messages:d} messages')
def step_verify_mq_export_success(context, expected_messages):
    """Verify MQ export was successful with expected message count."""
    mq_logger.info(f"Verifying MQ export with {expected_messages} messages")
    
    assert hasattr(context, 'mq_export_result'), "No MQ export result available"
    assert context.mq_export_result.get('success', False), "MQ export failed"
    
    exported_count = context.mq_export_result.get('messages_exported', 0)
    assert exported_count == expected_messages, f"Expected {expected_messages} messages, got {exported_count}"
    
    export_format = context.mq_export_result.get('export_format', 'unknown')
    file_size = context.mq_export_result.get('file_size', 0)
    mq_logger.info(f"MQ export successful: {exported_count} messages in {export_format} format ({file_size} bytes)")

@then('MQ queue should be drained successfully')
def step_verify_mq_queue_drained(context):
    """Verify MQ queue was drained successfully."""
    mq_logger.info("Verifying MQ queue drained successfully")
    
    assert hasattr(context, 'mq_drain_result'), "No MQ drain result available"
    assert context.mq_drain_result.get('success', False), "MQ queue drain failed"
    
    drained_count = context.mq_drain_result.get('messages_drained', 0)
    batches_processed = context.mq_drain_result.get('batches_processed', 0)
    
    mq_logger.info(f"MQ queue drained successfully: {drained_count} messages in {batches_processed} batches")

@then('MQ queue depth should be {expected_depth:d}')
def step_verify_mq_queue_depth(context, expected_depth):
    """Verify MQ queue depth matches expected value."""
    mq_logger.info(f"Verifying MQ queue depth is {expected_depth}")
    
    assert hasattr(context, 'mq_queue_depth'), "No MQ queue depth available"
    assert context.mq_queue_depth is not None, "MQ queue depth is None"
    
    actual_depth = context.mq_queue_depth
    assert actual_depth == expected_depth, f"Expected queue depth {expected_depth}, got {actual_depth}"
    
    mq_logger.info(f"MQ queue depth verified: {actual_depth}")

@then('MQ processing should complete within {expected_time:d} seconds')
def step_verify_mq_processing_time(context, expected_time):
    """Verify MQ processing completed within expected time."""
    duration = (getattr(context, 'mq_retrieve_duration', 0) or 
               getattr(context, 'mq_send_duration', 0) or 
               getattr(context, 'mq_export_duration', 0) or 
               getattr(context, 'mq_drain_duration', 0))
    
    assert duration <= expected_time, f"MQ processing took {duration:.2f}s, expected under {expected_time}s"
    
    mq_logger.info(f"MQ processing completed in {duration:.2f} seconds")