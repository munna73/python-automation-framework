"""
API-related step definitions for Behave.
"""
from behave import given, when, then
from web.api_client import api_client
from utils.logger import logger, api_logger

@given('API base URL is configured')
def step_api_base_url_configured(context):
    """Verify API base URL is configured."""
    api_logger.info("Verifying API base URL configuration")
    context.api_client = api_client
    assert context.api_client.config.get('base_url'), "API base URL not configured"

@given('request payload is loaded from "{filename}"')
def step_load_request_payload(context, filename):
    """Load request payload from file."""
    api_logger.info(f"Loading request payload from {filename}")
    context.request_payload = context.api_client.load_payload_from_file(filename)

@when('I send GET request to "{endpoint}" endpoint')
def step_send_get_request(context, endpoint):
    """Send GET request to specified endpoint."""
    api_logger.info(f"Sending GET request to {endpoint}")
    context.response = context.api_client.get(endpoint)

@when('I send POST request to "{endpoint}" endpoint')
def step_send_post_request(context, endpoint):
    """Send POST request to specified endpoint."""
    api_logger.info(f"Sending POST request to {endpoint}")
    payload = getattr(context, 'request_payload', None)
    context.response = context.api_client.post(endpoint, json_data=payload)

@when('I send PUT request to "{endpoint}" endpoint')
def step_send_put_request(context, endpoint):
    """Send PUT request to specified endpoint."""
    api_logger.info(f"Sending PUT request to {endpoint}")
    payload = getattr(context, 'request_payload', None)
    context.response = context.api_client.put(endpoint, json_data=payload)

@when('I send DELETE request to "{endpoint}" endpoint')
def step_send_delete_request(context, endpoint):
    """Send DELETE request to specified endpoint."""
    api_logger.info(f"Sending DELETE request to {endpoint}")
    context.response = context.api_client.delete(endpoint)

@then('response status code should be {expected_status:d}')
def step_verify_status_code(context, expected_status):
    """Verify response status code."""
    api_logger.info(f"Verifying response status code: {expected_status}")
    assert hasattr(context, 'response'), "No response available"
    actual_status = context.response.status_code
    assert actual_status == expected_status, f"Expected status {expected_status}, got {actual_status}"

@then('response should contain valid JSON')
def step_verify_valid_json(context):
    """Verify response contains valid JSON."""
    api_logger.info("Verifying response contains valid JSON")
    assert hasattr(context, 'response'), "No response available"
    assert context.api_client.validate_json_response(context.response), "Response does not contain valid JSON"

@then('response should match expected schema')
def step_verify_response_schema(context):
    """Verify response matches expected schema."""
    api_logger.info("Verifying response schema")
    assert hasattr(context, 'response'), "No response available"
    # Schema would be loaded from context or file
    schema = getattr(context, 'expected_schema', {})
    if schema:
        assert context.api_client.validate_response_schema(context.response, schema), "Response schema validation failed"

@then('response field "{field_path}" should be "{expected_value}"')
def step_verify_response_field(context, field_path, expected_value):
    """Verify specific field in response."""
    api_logger.info(f"Verifying response field {field_path} = {expected_value}")
    assert hasattr(context, 'response'), "No response available"
    assert context.api_client.validate_response_field(context.response, field_path, expected_value), f"Field validation failed for {field_path}"

@then('response time should be less than {max_time:d} milliseconds')
def step_verify_response_time(context, max_time):
    """Verify response time is within acceptable limits."""
    api_logger.info(f"Verifying response time is less than {max_time}ms")
    stats = context.api_client.get_response_time_stats()
    if stats and 'average' in stats:
        assert stats['average'] < max_time, f"Response time {stats['average']:.2f}ms exceeds limit {max_time}ms"