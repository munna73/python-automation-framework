"""
AWS-related step definitions for Behave BDD testing.
"""
from behave import given, when, then
from aws.sqs_connector import sqs_connector
from aws.s3_connector import s3_connector
from aws.sql_integration import aws_sql_integration
from utils.logger import logger
import json
import time
import uuid
from typing import Optional
import sys
import os
from pathlib import Path

# Get project root (go up 3 levels: steps -> features -> project_root)
current_file = Path(__file__)
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root.absolute()))

# SQS Step Definitions
@given('AWS SQS connection is configured')
def step_sqs_connection_configured(context):
    """Verify AWS SQS connection is configured."""
    logger.info("Verifying AWS SQS connection configuration")
    context.sqs_connector = sqs_connector
    assert context.sqs_connector.sqs_client is not None, "SQS client not configured"

@given('AWS credentials are loaded from profile "{profile_name}"')
def step_load_aws_profile(context, profile_name):
    """Load AWS credentials from specific profile."""
    logger.info(f"Loading AWS credentials from profile: {profile_name}")
    context.aws_profile = profile_name
    # Reinitialize connectors with profile
    context.sqs_connector = sqs_connector(profile_name=profile_name)
    context.s3_connector = s3_connector(profile_name=profile_name)

@given('SQS queue URL is set to "{queue_url}"')
def step_set_sqs_queue_url(context, queue_url):
    """Set SQS queue URL for testing."""
    # Validate queue URL format
    if not queue_url.startswith("https://sqs."):
        raise ValueError(f"Invalid SQS queue URL format: {queue_url}")
    
    logger.info(f"Setting SQS queue URL: {queue_url}")
    context.sqs_queue_url = queue_url
    
    # Set FIFO flag for later use
    context.is_fifo_queue = queue_url.endswith('.fifo')

@given('message group ID is set to "{group_id}"')
def step_set_message_group_id(context, group_id):
    """Set message group ID for FIFO queues."""
    logger.info(f"Setting message group ID: {group_id}")
    context.message_group_id = group_id

@when('I send message "{message_text}" to SQS queue')
def step_send_message_to_sqs(context, message_text):
    """Send a message to SQS queue."""
    logger.info(f"Sending message to SQS: {message_text}")
    
    try:
        # Check if FIFO queue
        if getattr(context, 'is_fifo_queue', False):
            # FIFO queues require message group ID and deduplication ID
            context.sqs_send_result = context.sqs_connector.send_message(
                context.sqs_queue_url, 
                message_text,
                message_group_id=getattr(context, 'message_group_id', 'default-group'),
                message_deduplication_id=str(uuid.uuid4())
            )
        else:
            context.sqs_send_result = context.sqs_connector.send_message(
                context.sqs_queue_url, message_text
            )
    except Exception as e:
        logger.error(f"Failed to send SQS message: {str(e)}")
        raise AssertionError(f"SQS send failed: {str(e)}")

@when('I send message with attributes to SQS queue')
def step_send_message_with_attributes(context):
    """Send message with custom attributes from table."""
    message_text = context.text or "Test message with attributes"
    attributes = {}
    
    if context.table:
        for row in context.table:
            attributes[row['attribute']] = {
                'StringValue': row['value'],
                'DataType': row.get('type', 'String')
            }
    
    logger.info(f"Sending message with attributes: {attributes}")
    try:
        context.sqs_send_result = context.sqs_connector.send_message_with_attributes(
            context.sqs_queue_url, message_text, attributes
        )
    except Exception as e:
        logger.error(f"Failed to send message with attributes: {str(e)}")
        raise AssertionError(f"SQS send with attributes failed: {str(e)}")

@when('I send {count:d} messages to SQS queue in batch')
def step_send_batch_messages(context, count):
    """Send multiple messages in batch."""
    logger.info(f"Sending {count} messages in batch to SQS")
    
    messages = []
    for i in range(count):
        message = {
            'Id': str(i),
            'MessageBody': f'Batch message {i}'
        }
        if getattr(context, 'is_fifo_queue', False):
            message['MessageGroupId'] = getattr(context, 'message_group_id', 'default-group')
            message['MessageDeduplicationId'] = f"{uuid.uuid4()}-{i}"
        messages.append(message)
    
    try:
        context.sqs_batch_result = context.sqs_connector.send_message_batch(
            context.sqs_queue_url, messages
        )
    except Exception as e:
        logger.error(f"Failed to send batch messages: {str(e)}")
        raise AssertionError(f"SQS batch send failed: {str(e)}")

@when('I send message "{message_text}" to SQS queue with {retries:d} retries')
def step_send_message_with_retries(context, message_text, retries):
    """Send message with retry logic."""
    logger.info(f"Sending message with up to {retries} retries")
    
    for attempt in range(retries + 1):
        try:
            context.sqs_send_result = context.sqs_connector.send_message(
                context.sqs_queue_url, message_text
            )
            logger.info(f"Message sent successfully on attempt {attempt + 1}")
            break
        except Exception as e:
            if attempt < retries:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s: {str(e)}")
                time.sleep(wait_time)
            else:
                logger.error(f"All retry attempts failed: {str(e)}")
                raise AssertionError(f"SQS send failed after {retries} retries: {str(e)}")

@when('I send file "{filename}" to SQS queue line by line')
def step_send_file_line_by_line_sqs(context, filename):
    """Send file to SQS queue line by line."""
    logger.info(f"Sending file to SQS line by line: {filename}")
    
    try:
        start_time = time.time()
        context.sqs_file_result = context.sqs_connector.send_file_as_messages(
            context.sqs_queue_url, filename, line_by_line=True
        )
        context.file_send_duration = time.time() - start_time
        logger.info(f"File sent in {context.file_send_duration:.2f} seconds")
    except Exception as e:
        logger.error(f"Failed to send file to SQS: {str(e)}")
        raise AssertionError(f"SQS file send failed: {str(e)}")

@when('I send file "{filename}" to SQS queue as single message')
def step_send_file_as_single_message_sqs(context, filename):
    """Send entire file as single SQS message."""
    logger.info(f"Sending file to SQS as single message: {filename}")
    
    try:
        context.sqs_file_result = context.sqs_connector.send_file_as_messages(
            context.sqs_queue_url, filename, line_by_line=False
        )
    except Exception as e:
        logger.error(f"Failed to send file as single message: {str(e)}")
        raise AssertionError(f"SQS file send failed: {str(e)}")

@when('I receive messages from SQS queue')
def step_receive_messages_from_sqs(context):
    """Receive messages from SQS queue."""
    logger.info("Receiving messages from SQS queue")
    
    # Better default handling
    max_messages = getattr(context, 'sqs_max_messages', 
                          context.config.userdata.get('default_max_messages', 10))
    
    try:
        context.sqs_received_messages = context.sqs_connector.receive_messages(
            context.sqs_queue_url, max_messages
        )
        logger.info(f"Received {len(context.sqs_received_messages)} messages")
    except Exception as e:
        logger.error(f"Failed to receive messages: {str(e)}")
        raise AssertionError(f"SQS receive failed: {str(e)}")

@when('I receive {max_messages:d} messages from SQS queue')
def step_receive_n_messages_from_sqs(context, max_messages):
    """Receive specific number of messages from SQS queue."""
    logger.info(f"Receiving {max_messages} messages from SQS queue")
    
    try:
        start_time = time.time()
        context.sqs_received_messages = context.sqs_connector.receive_messages(
            context.sqs_queue_url, max_messages
        )
        context.receive_duration = time.time() - start_time
        
        logger.info(f"Received {len(context.sqs_received_messages)} messages in {context.receive_duration:.2f} seconds")
    except Exception as e:
        logger.error(f"Failed to receive {max_messages} messages: {str(e)}")
        raise AssertionError(f"SQS receive failed: {str(e)}")

@when('I delete processed messages from SQS')
def step_delete_processed_messages(context):
    """Delete processed messages from SQS."""
    logger.info("Deleting processed messages from SQS")
    
    if not hasattr(context, 'sqs_received_messages'):
        logger.warning("No messages to delete")
        return
    
    deleted_count = 0
    for message in context.sqs_received_messages:
        try:
            context.sqs_connector.delete_message(
                context.sqs_queue_url,
                message['ReceiptHandle']
            )
            deleted_count += 1
        except Exception as e:
            logger.error(f"Failed to delete message: {str(e)}")
    
    logger.info(f"Deleted {deleted_count} messages")
    context.deleted_message_count = deleted_count

@then('SQS message should be sent successfully')
def step_verify_sqs_message_sent(context):
    """Verify SQS message was sent successfully."""
    logger.info("Verifying SQS message sent successfully")
    
    assert hasattr(context, 'sqs_send_result'), "No SQS send result available"
    assert 'MessageId' in context.sqs_send_result, "Message ID not found in send result"
    
    # Add more validations
    assert 'ResponseMetadata' in context.sqs_send_result, "No response metadata"
    assert context.sqs_send_result['ResponseMetadata']['HTTPStatusCode'] == 200, "Invalid HTTP status"
    
    logger.info(f"Message sent successfully with ID: {context.sqs_send_result['MessageId']}")

@then('SQS batch should send {expected_success:d} messages successfully')
def step_verify_sqs_batch_sent(context, expected_success):
    """Verify SQS batch send results."""
    logger.info(f"Verifying SQS batch sent {expected_success} messages")
    
    assert hasattr(context, 'sqs_batch_result'), "No SQS batch result available"
    
    successful = len(context.sqs_batch_result.get('Successful', []))
    failed = len(context.sqs_batch_result.get('Failed', []))
    
    assert successful == expected_success, f"Expected {expected_success} successful, got {successful}"
    
    if failed > 0:
        logger.warning(f"{failed} messages failed to send")

@then('SQS file should be sent with {expected_success:d} successful messages')
def step_verify_sqs_file_sent(context, expected_success):
    """Verify SQS file was sent with expected success count."""
    logger.info(f"Verifying SQS file sent with {expected_success} successful messages")
    
    assert hasattr(context, 'sqs_file_result'), "No SQS file result available"
    actual_success = context.sqs_file_result.get('success_count', 0)
    
    assert actual_success == expected_success, f"Expected {expected_success} successful messages, got {actual_success}"
    
    # Log performance if available
    if hasattr(context, 'file_send_duration'):
        rate = actual_success / context.file_send_duration
        logger.info(f"Send rate: {rate:.2f} messages/second")

@then('SQS should receive {expected_count:d} messages')
def step_verify_sqs_received_count(context, expected_count):
    """Verify expected number of messages received from SQS."""
    logger.info(f"Verifying SQS received {expected_count} messages")
    
    assert hasattr(context, 'sqs_received_messages'), "No SQS received messages available"
    actual_count = len(context.sqs_received_messages)
    
    assert actual_count == expected_count, f"Expected {expected_count} messages, got {actual_count}"
    
    # Log performance metrics if available
    if hasattr(context, 'receive_duration') and actual_count > 0:
        rate = actual_count / context.receive_duration
        logger.info(f"Receive rate: {rate:.2f} messages/second")

@then('processing should complete within {expected_time:d} seconds')
def step_verify_processing_time(context, expected_time):
    """Verify processing completed within expected time."""
    duration = getattr(context, 'receive_duration', 0) or getattr(context, 'file_send_duration', 0)
    
    assert duration <= expected_time, f"Processing took {duration:.2f}s, expected under {expected_time}s"

# S3 Step Definitions
@given('AWS S3 connection is configured')
def step_s3_connection_configured(context):
    """Verify AWS S3 connection is configured."""
    logger.info("Verifying AWS S3 connection configuration")
    context.s3_connector = s3_connector
    assert context.s3_connector.s3_client is not None, "S3 client not configured"

@given('S3 bucket is set to "{bucket_name}"')
def step_set_s3_bucket(context, bucket_name):
    """Set S3 bucket name for testing."""
    logger.info(f"Setting S3 bucket: {bucket_name}")
    context.s3_bucket = bucket_name

@given('S3 prefix is set to "{prefix}"')
def step_set_s3_prefix(context, prefix):
    """Set S3 prefix for testing."""
    logger.info(f"Setting S3 prefix: {prefix}")
    context.s3_prefix = prefix

@given('local download directory is set to "{directory}"')
def step_set_local_directory(context, directory):
    """Set local download directory."""
    logger.info(f"Setting local download directory: {directory}")
    context.local_directory = directory
    
    # Ensure directory exists
    import os
    os.makedirs(directory, exist_ok=True)

@when('I download file "{s3_key}" from S3 to "{local_path}"')
def step_download_s3_file(context, s3_key, local_path):
    """Download single file from S3."""
    logger.info(f"Downloading S3 file: {s3_key} -> {local_path}")
    
    try:
        start_time = time.time()
        context.s3_download_result = context.s3_connector.download_file(
            context.s3_bucket, s3_key, local_path
        )
        context.download_duration = time.time() - start_time
        logger.info(f"Download completed in {context.download_duration:.2f} seconds")
    except Exception as e:
        logger.error(f"Failed to download S3 file: {str(e)}")
        raise AssertionError(f"S3 download failed: {str(e)}")

@when('I download S3 directory to local directory')
def step_download_s3_directory(context):
    """Download S3 directory to local directory."""
    logger.info("Downloading S3 directory to local directory")
    
    try:
        start_time = time.time()
        context.s3_download_results = context.s3_connector.download_directory(
            context.s3_bucket, 
            context.s3_prefix, 
            context.local_directory
        )
        context.download_duration = time.time() - start_time
        
        file_count = context.s3_download_results.get('downloaded_count', 0)
        logger.info(f"Downloaded {file_count} files in {context.download_duration:.2f} seconds")
    except Exception as e:
        logger.error(f"Failed to download S3 directory: {str(e)}")
        raise AssertionError(f"S3 directory download failed: {str(e)}")

@when('I upload file "{local_path}" to S3 as "{s3_key}"')
def step_upload_file_to_s3(context, local_path, s3_key):
    """Upload file to S3."""
    logger.info(f"Uploading file to S3: {local_path} -> {s3_key}")
    
    try:
        start_time = time.time()
        context.s3_upload_result = context.s3_connector.upload_file(
            local_path, context.s3_bucket, s3_key
        )
        context.upload_duration = time.time() - start_time
        logger.info(f"Upload completed in {context.upload_duration:.2f} seconds")
    except Exception as e:
        logger.error(f"Failed to upload file to S3: {str(e)}")
        raise AssertionError(f"S3 upload failed: {str(e)}")

@when('I list S3 objects with prefix')
def step_list_s3_objects(context):
    """List S3 objects with prefix."""
    logger.info("Listing S3 objects with prefix")
    
    try:
        context.s3_objects = context.s3_connector.list_objects(
            context.s3_bucket, context.s3_prefix
        )
        logger.info(f"Found {len(context.s3_objects)} objects")
    except Exception as e:
        logger.error(f"Failed to list S3 objects: {str(e)}")
        raise AssertionError(f"S3 list objects failed: {str(e)}")

@then('S3 file download should be successful')
def step_verify_s3_download_success(context):
    """Verify S3 file download was successful."""
    logger.info("Verifying S3 file download success")
    
    assert hasattr(context, 's3_download_result'), "No S3 download result available"
    assert context.s3_download_result == True, "S3 file download failed"

@then('S3 directory download should complete with {expected_files:d} files')
def step_verify_s3_directory_download(context, expected_files):
    """Verify S3 directory download completed with expected file count."""
    logger.info(f"Verifying S3 directory download with {expected_files} files")
    
    assert hasattr(context, 's3_download_results'), "No S3 download results available"
    actual_files = context.s3_download_results.get('downloaded_count', 0)
    
    assert actual_files == expected_files, f"Expected {expected_files} files, got {actual_files}"
    
    # Log performance metrics
    if hasattr(context, 'download_duration') and actual_files > 0:
        rate = actual_files / context.download_duration
        logger.info(f"Download rate: {rate:.2f} files/second")

@then('S3 upload should be successful')
def step_verify_s3_upload_success(context):
    """Verify S3 upload was successful."""
    logger.info("Verifying S3 upload success")
    
    assert hasattr(context, 's3_upload_result'), "No S3 upload result available"
    assert context.s3_upload_result == True, "S3 upload failed"

@then('S3 file "{s3_key}" should exist in bucket')
def step_verify_s3_file_exists(context, s3_key):
    """Verify file exists in S3 bucket."""
    logger.info(f"Verifying S3 file exists: {s3_key}")
    
    try:
        exists = context.s3_connector.object_exists(context.s3_bucket, s3_key)
        assert exists, f"S3 object {s3_key} not found in bucket {context.s3_bucket}"
    except Exception as e:
        logger.error(f"Failed to check S3 object existence: {str(e)}")
        raise AssertionError(f"S3 object check failed: {str(e)}")

@then('S3 object count should be {expected_count:d}')
def step_verify_s3_object_count(context, expected_count):
    """Verify S3 object count matches expected."""
    assert hasattr(context, 's3_objects'), "No S3 objects list available"
    actual_count = len(context.s3_objects)
    
    assert actual_count == expected_count, f"Expected {expected_count} objects, got {actual_count}"

# AWS-SQL Integration Step Definitions
@given('AWS-SQL integration is configured')
def step_aws_sql_integration_configured(context):
    """Verify AWS-SQL integration is configured."""
    logger.info("Verifying AWS-SQL integration configuration")
    context.aws_sql_integration = aws_sql_integration

@given('message table "{table_name}" exists in "{environment}" "{db_type}" database')
def step_ensure_message_table_exists(context, table_name, environment, db_type):
    """Ensure message table exists in database."""
    logger.info(f"Ensuring message table exists: {table_name}")
    context.message_table = table_name
    context.db_environment = environment
    context.db_type = db_type
    
    try:
        # Create table if it doesn't exist
        context.aws_sql_integration.create_message_table(
            environment, db_type, table_name
        )
    except Exception as e:
        logger.error(f"Failed to create message table: {str(e)}")
        raise AssertionError(f"Table creation failed: {str(e)}")

@when('I process SQS queue to SQL database')
def step_process_sqs_to_sql(context):
    """Process SQS queue messages to SQL database."""
    logger.info("Processing SQS queue to SQL database")
    
    try:
        start_time = time.time()
        context.process_results = context.aws_sql_integration.process_queue_to_sql(
            context.sqs_queue_url,
            context.db_environment,
            context.db_type,
            table_name=context.message_table
        )
        context.process_duration = time.time() - start_time
        
        processed = context.process_results.get('processed_count', 0)
        logger.info(f"Processed {processed} messages in {context.process_duration:.2f} seconds")
    except Exception as e:
        logger.error(f"Failed to process SQS to SQL: {str(e)}")
        raise AssertionError(f"SQS to SQL processing failed: {str(e)}")

@when('I save SQS messages to SQL database')
def step_save_sqs_messages_to_sql(context):
    """Save received SQS messages to SQL database."""
    logger.info("Saving SQS messages to SQL database")
    
    messages = getattr(context, 'sqs_received_messages', [])
    if not messages:
        logger.warning("No messages to save")
        context.save_results = {'success_count': 0}
        return
    
    try:
        context.save_results = context.aws_sql_integration.save_messages_to_sql(
            messages,
            context.db_environment,
            context.db_type,
            context.sqs_queue_url,
            context.message_table
        )
    except Exception as e:
        logger.error(f"Failed to save messages to SQL: {str(e)}")
        raise AssertionError(f"SQL save failed: {str(e)}")

@when('I export messages from SQL to file "{filename}"')
def step_export_messages_to_file(context, filename):
    """Export messages from SQL database to file."""
    logger.info(f"Exporting messages from SQL to file: {filename}")
    
    try:
        context.export_results = context.aws_sql_integration.export_messages_to_file_from_sql(
            context.db_environment,
            context.db_type,
            filename,
            table_name=context.message_table
        )
    except Exception as e:
        logger.error(f"Failed to export messages: {str(e)}")
        raise AssertionError(f"SQL export failed: {str(e)}")

@then('SQS messages should be saved to SQL successfully')
def step_verify_sqs_sql_save(context):
    """Verify SQS messages were saved to SQL successfully."""
    logger.info("Verifying SQS messages saved to SQL successfully")
    
    assert hasattr(context, 'save_results') or hasattr(context, 'process_results'), \
        "No save or process results available"
    
    results = getattr(context, 'save_results', context.process_results)
    success_count = results.get('success_count', results.get('processed_count', 0))
    
    assert success_count > 0, "No messages were saved successfully"
    logger.info(f"Successfully saved {success_count} messages")

@then('SQL message export should be successful')
def step_verify_sql_export_success(context):
    """Verify SQL message export was successful."""
    logger.info("Verifying SQL message export success")
    
    assert hasattr(context, 'export_results'), "No export results available"
    assert context.export_results['success'] == True, "SQL message export failed"

@then('exported file should contain {expected_messages:d} messages')
def step_verify_exported_message_count(context, expected_messages):
    """Verify exported file contains expected number of messages."""
    logger.info(f"Verifying exported file contains {expected_messages} messages")
    
    assert hasattr(context, 'export_results'), "No export results available"
    actual_messages = context.export_results.get('messages_exported', 0)
    
    assert actual_messages == expected_messages, f"Expected {expected_messages} messages, got {actual_messages}"

# Connection Test Steps
@then('AWS SQS connection should be successful')
def step_verify_sqs_connection(context):
    """Verify AWS SQS connection is successful."""
    logger.info("Verifying AWS SQS connection")
    
    queue_url = getattr(context, 'sqs_queue_url', None)
    if not queue_url:
        raise AssertionError("No SQS queue URL set")
    
    try:
        success = context.sqs_connector.test_connection(queue_url)
        assert success, "AWS SQS connection test failed"
    except Exception as e:
        logger.error(f"SQS connection test failed: {str(e)}")
        raise AssertionError(f"SQS connection test error: {str(e)}")

@then('AWS S3 connection should be successful')
def step_verify_s3_connection(context):
    """Verify AWS S3 connection is successful."""
    logger.info("Verifying AWS S3 connection")
    
    bucket_name = getattr(context, 's3_bucket', None)
    if not bucket_name:
        raise AssertionError("No S3 bucket name set")
    
    try:
        success = context.s3_connector.test_connection(bucket_name)
        assert success, "AWS S3 connection test failed"
    except Exception as e:
        logger.error(f"S3 connection test failed: {str(e)}")
        raise AssertionError(f"S3 connection test error: {str(e)}")

# Cleanup Steps
@then('I cleanup test messages from database')
def step_cleanup_test_messages(context):
    """Cleanup test messages from database."""
    logger.info("Cleaning up test messages from database")
    
    try:
        context.aws_sql_integration.cleanup_test_messages(
            context.db_environment,
            context.db_type,
            context.message_table
        )
    except Exception as e:
        logger.warning(f"Cleanup failed: {str(e)}")

@then('I purge the SQS queue')
def step_purge_sqs_queue(context):
    """Purge all messages from SQS queue."""
    logger.info("Purging SQS queue")
    
    try:
        context.sqs_connector.purge_queue(context.sqs_queue_url)
        logger.info("Queue purged successfully")
    except Exception as e:
        logger.warning(f"Failed to purge queue: {str(e)}")


# ========================================
# S3 MESSAGE-STYLE STEP DEFINITIONS
# ========================================

@when('I retrieve S3 objects from prefix "{prefix}" and write to file "{output_file}" line by line')
def step_retrieve_s3_objects_line_by_line(context, prefix, output_file):
    """Retrieve S3 objects and write each object content as a line in file."""
    logger.info(f"Retrieving S3 objects from prefix {prefix} to file {output_file} line by line")
    
    try:
        start_time = time.time()
        context.s3_message_results = context.s3_connector.retrieve_s3_messages_to_file(
            bucket_name=context.s3_bucket,
            prefix=prefix,
            output_file=output_file,
            retrieve_mode='line_by_line'
        )
        context.s3_retrieve_duration = time.time() - start_time
        
        messages_count = context.s3_message_results.get('messages_written', 0)
        logger.info(f"Retrieved {messages_count} S3 objects as lines in {context.s3_retrieve_duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Failed to retrieve S3 objects line by line: {str(e)}")
        raise AssertionError(f"S3 line-by-line retrieval failed: {str(e)}")

@when('I retrieve S3 objects from prefix "{prefix}" and write to file "{output_file}" as whole file')
def step_retrieve_s3_objects_whole_file(context, prefix, output_file):
    """Retrieve S3 objects and concatenate all content into single file."""
    logger.info(f"Retrieving S3 objects from prefix {prefix} to file {output_file} as whole file")
    
    try:
        start_time = time.time()
        context.s3_message_results = context.s3_connector.retrieve_s3_messages_to_file(
            bucket_name=context.s3_bucket,
            prefix=prefix,
            output_file=output_file,
            retrieve_mode='whole_file'
        )
        context.s3_retrieve_duration = time.time() - start_time
        
        messages_count = context.s3_message_results.get('messages_written', 0)
        logger.info(f"Retrieved {messages_count} S3 objects as whole file in {context.s3_retrieve_duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Failed to retrieve S3 objects as whole file: {str(e)}")
        raise AssertionError(f"S3 whole file retrieval failed: {str(e)}")

@when('I retrieve {max_messages:d} S3 objects from prefix "{prefix}" and write to file "{output_file}" line by line')
def step_retrieve_limited_s3_objects_line_by_line(context, max_messages, prefix, output_file):
    """Retrieve limited number of S3 objects and write each as a line in file."""
    logger.info(f"Retrieving {max_messages} S3 objects from prefix {prefix} to file {output_file} line by line")
    
    try:
        start_time = time.time()
        context.s3_message_results = context.s3_connector.download_objects_as_messages(
            bucket_name=context.s3_bucket,
            prefix=prefix,
            output_file=output_file,
            one_message_per_line=True,
            max_objects=max_messages
        )
        context.s3_retrieve_duration = time.time() - start_time
        
        messages_count = context.s3_message_results.get('messages_written', 0)
        logger.info(f"Retrieved {messages_count}/{max_messages} S3 objects as lines in {context.s3_retrieve_duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Failed to retrieve limited S3 objects: {str(e)}")
        raise AssertionError(f"S3 limited retrieval failed: {str(e)}")

@when('I send file "{filename}" to S3 prefix "{prefix}" line by line')
def step_send_file_to_s3_line_by_line(context, filename, prefix):
    """Send file to S3 with each line as a separate S3 object."""
    logger.info(f"Sending file {filename} to S3 prefix {prefix} line by line")
    
    try:
        start_time = time.time()
        context.s3_upload_results = context.s3_connector.send_file_to_s3_messages(
            filename=filename,
            bucket_name=context.s3_bucket,
            prefix=prefix,
            send_mode='line_by_line'
        )
        context.s3_upload_duration = time.time() - start_time
        
        uploaded_count = context.s3_upload_results.get('uploaded_count', 0)
        total_lines = context.s3_upload_results.get('total_lines', 0)
        logger.info(f"Uploaded {uploaded_count}/{total_lines} lines as S3 objects in {context.s3_upload_duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Failed to send file to S3 line by line: {str(e)}")
        raise AssertionError(f"S3 line-by-line upload failed: {str(e)}")

@when('I send file "{filename}" to S3 prefix "{prefix}" as whole file')
def step_send_file_to_s3_whole_file(context, filename, prefix):
    """Send entire file to S3 as a single S3 object."""
    logger.info(f"Sending file {filename} to S3 prefix {prefix} as whole file")
    
    try:
        start_time = time.time()
        context.s3_upload_results = context.s3_connector.send_file_to_s3_messages(
            filename=filename,
            bucket_name=context.s3_bucket,
            prefix=prefix,
            send_mode='whole_file'
        )
        context.s3_upload_duration = time.time() - start_time
        
        uploaded_count = context.s3_upload_results.get('uploaded_count', 0)
        logger.info(f"Uploaded {uploaded_count} file as S3 object in {context.s3_upload_duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Failed to send file to S3 as whole file: {str(e)}")
        raise AssertionError(f"S3 whole file upload failed: {str(e)}")

@when('I post message "{message_text}" to S3 as "{s3_key}"')
def step_post_message_to_s3(context, message_text, s3_key):
    """Post a single message directly to S3 as object content."""
    logger.info(f"Posting message to S3: {s3_key}")
    
    try:
        # Ensure s3_key includes the prefix if set
        if hasattr(context, 's3_prefix') and context.s3_prefix:
            if not s3_key.startswith(context.s3_prefix):
                s3_key = f"{context.s3_prefix}/{s3_key}"
        
        success = context.s3_connector.put_object_content(
            bucket_name=context.s3_bucket,
            s3_key=s3_key,
            content=message_text
        )
        
        context.s3_message_post_result = {
            'success': success,
            's3_key': s3_key,
            'message_length': len(message_text)
        }
        
        if success:
            logger.info(f"Message posted to s3://{context.s3_bucket}/{s3_key}")
        else:
            raise AssertionError("Failed to post message to S3")
            
    except Exception as e:
        logger.error(f"Failed to post message to S3: {str(e)}")
        raise AssertionError(f"S3 message post failed: {str(e)}")

@when('I get S3 object content from "{s3_key}"')
def step_get_s3_object_content(context, s3_key):
    """Get content from S3 object as message."""
    logger.info(f"Getting S3 object content: {s3_key}")
    
    try:
        # Ensure s3_key includes the prefix if set
        if hasattr(context, 's3_prefix') and context.s3_prefix:
            if not s3_key.startswith(context.s3_prefix):
                s3_key = f"{context.s3_prefix}/{s3_key}"
        
        content = context.s3_connector.get_object_content(
            bucket_name=context.s3_bucket,
            s3_key=s3_key
        )
        
        context.s3_object_content = content
        context.s3_retrieved_key = s3_key
        
        if content is not None:
            logger.info(f"Retrieved content from s3://{context.s3_bucket}/{s3_key} ({len(content)} characters)")
        else:
            logger.warning(f"No content found for s3://{context.s3_bucket}/{s3_key}")
            
    except Exception as e:
        logger.error(f"Failed to get S3 object content: {str(e)}")
        raise AssertionError(f"S3 object content retrieval failed: {str(e)}")

# S3 Message Verification Steps
@then('S3 message retrieval should be successful')
def step_verify_s3_message_retrieval_success(context):
    """Verify S3 message retrieval was successful."""
    logger.info("Verifying S3 message retrieval success")
    
    assert hasattr(context, 's3_message_results'), "No S3 message results available"
    assert context.s3_message_results.get('success', False), "S3 message retrieval failed"
    
    messages_written = context.s3_message_results.get('messages_written', 0)
    assert messages_written > 0, "No messages were written to file"
    
    logger.info(f"S3 message retrieval successful: {messages_written} messages written")

@then('S3 should retrieve {expected_messages:d} messages to file')
def step_verify_s3_messages_retrieved_count(context, expected_messages):
    """Verify expected number of messages were retrieved from S3."""
    logger.info(f"Verifying S3 retrieved {expected_messages} messages")
    
    assert hasattr(context, 's3_message_results'), "No S3 message results available"
    actual_messages = context.s3_message_results.get('messages_written', 0)
    
    assert actual_messages == expected_messages, f"Expected {expected_messages} messages, got {actual_messages}"
    
    # Log performance metrics if available
    if hasattr(context, 's3_retrieve_duration') and actual_messages > 0:
        rate = actual_messages / context.s3_retrieve_duration
        logger.info(f"S3 retrieval rate: {rate:.2f} messages/second")

@then('S3 file upload should be successful with {expected_objects:d} objects')
def step_verify_s3_file_upload_success(context, expected_objects):
    """Verify S3 file upload was successful with expected object count."""
    logger.info(f"Verifying S3 file upload with {expected_objects} objects")
    
    assert hasattr(context, 's3_upload_results'), "No S3 upload results available"
    assert context.s3_upload_results.get('success', False), "S3 file upload failed"
    
    uploaded_count = context.s3_upload_results.get('uploaded_count', 0)
    assert uploaded_count == expected_objects, f"Expected {expected_objects} objects, got {uploaded_count}"
    
    # Log performance metrics if available
    if hasattr(context, 's3_upload_duration') and uploaded_count > 0:
        rate = uploaded_count / context.s3_upload_duration
        logger.info(f"S3 upload rate: {rate:.2f} objects/second")

@then('S3 message should be posted successfully')
def step_verify_s3_message_posted(context):
    """Verify S3 message was posted successfully."""
    logger.info("Verifying S3 message posted successfully")
    
    assert hasattr(context, 's3_message_post_result'), "No S3 message post result available"
    assert context.s3_message_post_result.get('success', False), "S3 message post failed"
    
    s3_key = context.s3_message_post_result.get('s3_key')
    message_length = context.s3_message_post_result.get('message_length', 0)
    
    logger.info(f"S3 message posted successfully to {s3_key} ({message_length} characters)")

@then('S3 object content should be retrieved successfully')
def step_verify_s3_object_content_retrieved(context):
    """Verify S3 object content was retrieved successfully."""
    logger.info("Verifying S3 object content retrieved successfully")
    
    assert hasattr(context, 's3_object_content'), "No S3 object content available"
    assert context.s3_object_content is not None, "S3 object content is None"
    assert len(context.s3_object_content) > 0, "S3 object content is empty"
    
    content_length = len(context.s3_object_content)
    s3_key = getattr(context, 's3_retrieved_key', 'unknown')
    
    logger.info(f"S3 object content retrieved successfully from {s3_key} ({content_length} characters)")

@then('S3 object content should contain "{expected_text}"')
def step_verify_s3_object_content_contains(context, expected_text):
    """Verify S3 object content contains expected text."""
    logger.info(f"Verifying S3 object content contains: {expected_text}")
    
    assert hasattr(context, 's3_object_content'), "No S3 object content available"
    assert context.s3_object_content is not None, "S3 object content is None"
    assert expected_text in context.s3_object_content, f"Expected text '{expected_text}' not found in S3 object content"
    
    logger.info("S3 object content contains expected text")

@then('S3 message processing should complete within {expected_time:d} seconds')
def step_verify_s3_processing_time(context, expected_time):
    """Verify S3 message processing completed within expected time."""
    duration = getattr(context, 's3_retrieve_duration', 0) or getattr(context, 's3_upload_duration', 0)
    
    assert duration <= expected_time, f"S3 processing took {duration:.2f}s, expected under {expected_time}s"
    
    logger.info(f"S3 message processing completed in {duration:.2f} seconds")