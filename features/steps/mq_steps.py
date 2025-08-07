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

from mq.mq_producer import mq_producer
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