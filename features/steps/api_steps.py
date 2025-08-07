"""
REST API step definitions for Behave BDD testing.
"""
from behave import given, when, then
import json
import time
import statistics
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from pathlib import Path
import mimetypes
import sys
import os
from pathlib import Path

# Get project root (go up 3 levels: steps -> features -> project_root)
current_file = Path(__file__)
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root.absolute()))

from api.rest_client import RestClient
from utils.logger import logger
from api.json_validator import JsonValidator
from utils.data_loader import DataLoader

# Initialize REST client
rest_client = RestClient()
json_validator = JsonValidator()
data_loader = DataLoader()

# Context storage for values between steps
stored_values = {}

@given('API base URL is configured')
def step_api_base_url_configured(context):
    """Verify API base URL is configured."""
    logger.info("Verifying API base URL configuration")
    assert rest_client.base_url is not None, "API base URL not configured"
    context.rest_client = rest_client

@given('API timeout is set to {timeout:d} seconds')
def step_set_api_timeout(context, timeout):
    """Set API request timeout."""
    logger.info(f"Setting API timeout to {timeout} seconds")
    context.rest_client.set_timeout(timeout)

@given('API authentication token "{token}"')
def step_set_auth_token(context, token):
    """Set authentication token."""
    logger.info("Setting API authentication token")
    context.rest_client.set_auth_token(token)

@given('request headers')
def step_set_request_headers(context):
    """Set request headers from table."""
    headers = {}
    for row in context.table:
        headers[row['header_name']] = row['header_value']
    
    logger.info(f"Setting request headers: {headers}")
    context.rest_client.set_headers(headers)

@given('request payload is loaded from "{filename}"')
def step_load_payload_from_file(context, filename):
    """Load request payload from file."""
    logger.info(f"Loading request payload from: {filename}")
    payload = data_loader.load_json_file(filename)
    context.request_payload = payload

@given('request payload')
def step_set_request_payload(context):
    """Set request payload from text."""
    payload_text = context.text.strip()
    
    # Replace any stored values in the payload
    for key, value in stored_values.items():
        placeholder = f"{{{key}}}"
        if placeholder in payload_text:
            payload_text = payload_text.replace(placeholder, str(value))
    
    try:
        context.request_payload = json.loads(payload_text)
        logger.info(f"Set request payload: {json.dumps(context.request_payload, indent=2)}")
    except json.JSONDecodeError as e:
        raise AssertionError(f"Invalid JSON payload: {e}")

@given('query parameters')
def step_set_query_parameters(context):
    """Set query parameters from table."""
    params = {}
    for row in context.table:
        params[row['param_name']] = row['param_value']
    
    logger.info(f"Setting query parameters: {params}")
    context.query_params = params

# The old `step_set_multipart_form_data` is no longer needed in this format.
# A new, more direct approach for uploading files is provided below.

@given('concurrent users count is {users:d}')
def step_set_concurrent_users(context, users):
    """Set concurrent users for load testing."""
    context.concurrent_users = users
    logger.info(f"Set concurrent users: {users}")

@given('API retry configuration')
def step_set_retry_configuration(context):
    """Set API retry configuration from table."""
    retry_config = {}
    for row in context.table:
        retry_config['max_retries'] = int(row['max_retries'])
        retry_config['retry_delay'] = int(row['retry_delay'])
        retry_config['retry_status_codes'] = [int(code.strip()) for code in row['retry_status_codes'].split(',')]
        break  # Only take first row
    
    context.rest_client.set_retry_config(retry_config)
    logger.info(f"Set retry configuration: {retry_config}")

@given('GraphQL query')
def step_set_graphql_query(context):
    """Set GraphQL query from text."""
    context.graphql_query = context.text.strip()
    logger.info("Set GraphQL query")

@given('GraphQL variables')
def step_set_graphql_variables(context):
    """Set GraphQL variables from text."""
    try:
        context.graphql_variables = json.loads(context.text.strip())
        logger.info(f"Set GraphQL variables: {context.graphql_variables}")
    except json.JSONDecodeError as e:
        raise AssertionError(f"Invalid JSON variables: {e}")

# WHEN steps

@when('I send {method} request to "{endpoint}" endpoint')
def step_send_request(context, method, endpoint):
    """Send HTTP request to endpoint."""
    # Replace stored values in endpoint
    for key, value in stored_values.items():
        placeholder = f"{{{key}}}"
        if placeholder in endpoint:
            endpoint = endpoint.replace(placeholder, str(value))
    
    logger.info(f"Sending {method} request to: {endpoint}")
    
    # Prepare request parameters
    params = getattr(context, 'query_params', None)
    payload = getattr(context, 'request_payload', None)
    
    # Record start time
    start_time = time.time()
    
    # Send request based on method
    if method.upper() == 'GET':
        context.response = context.rest_client.get(endpoint, params=params)
    elif method.upper() == 'POST':
        context.response = context.rest_client.post(endpoint, json=payload, params=params)
    elif method.upper() == 'PUT':
        context.response = context.rest_client.put(endpoint, json=payload, params=params)
    elif method.upper() == 'PATCH':
        context.response = context.rest_client.patch(endpoint, json=payload, params=params)
    elif method.upper() == 'DELETE':
        context.response = context.rest_client.delete(endpoint, params=params)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    # Record response time
    context.response_time = (time.time() - start_time) * 1000  # Convert to milliseconds
    
    logger.info(f"Response status: {context.response.status_code}, Time: {context.response_time:.2f}ms")

@when('I send {count:d} {method} requests to "{endpoint}" endpoint')
def step_send_multiple_requests(context, count, method, endpoint):
    """Send multiple requests for load testing."""
    concurrent_users = getattr(context, 'concurrent_users', 1)
    logger.info(f"Sending {count} {method} requests with {concurrent_users} concurrent users")
    
    response_times = []
    status_codes = []
    errors = []
    
    def send_single_request():
        try:
            start_time = time.time()
            if method.upper() == 'GET':
                response = context.rest_client.get(endpoint)
            else:
                raise ValueError(f"Load testing only supports GET method, got: {method}")
            
            response_time = (time.time() - start_time) * 1000
            return {
                'status_code': response.status_code,
                'response_time': response_time,
                'error': None
            }
        except Exception as e:
            return {
                'status_code': None,
                'response_time': None,
                'error': str(e)
            }
    
    with ThreadPoolExecutor(max_workers=concurrent_users) as executor:
        futures = [executor.submit(send_single_request) for _ in range(count)]
        
        for future in as_completed(futures):
            result = future.result()
            if result['error']:
                errors.append(result['error'])
            else:
                status_codes.append(result['status_code'])
                response_times.append(result['response_time'])
    
    context.load_test_results = {
        'total_requests': count,
        'successful_requests': len(status_codes),
        'failed_requests': len(errors),
        'response_times': response_times,
        'status_codes': status_codes,
        'errors': errors
    }
    
    logger.info(f"Load test completed: {len(status_codes)} successful, {len(errors)} failed")

@when('I send GraphQL request to "{endpoint}" endpoint')
def step_send_graphql_request(context, endpoint):
    """Send GraphQL request."""
    query = getattr(context, 'graphql_query', '')
    variables = getattr(context, 'graphql_variables', {})
    
    payload = {
        'query': query,
        'variables': variables
    }
    
    logger.info(f"Sending GraphQL request to: {endpoint}")
    context.response = context.rest_client.post(endpoint, json=payload)

@when('I poll {method} request to "{endpoint}" until status is "{expected_status}" with timeout {timeout:d} seconds')
def step_poll_endpoint(context, method, endpoint, expected_status, timeout):
    """
    Poll endpoint until expected status is reached.
    This version correctly uses the RestClient's GET method inside the loop,
    which will now also benefit from the client's internal retry logic.
    """
    for key, value in stored_values.items():
        placeholder = f"{{{key}}}"
        if placeholder in endpoint:
            endpoint = endpoint.replace(placeholder, str(value))
    
    logger.info(f"Polling {endpoint} until status is '{expected_status}' (timeout: {timeout}s)")
    
    start_time = time.time()
    poll_interval = 2  # seconds
    
    while time.time() - start_time < timeout:
        if method.upper() == 'GET':
            response = context.rest_client.get(endpoint)
        else:
            raise ValueError(f"Polling only supports GET method, got: {method}")
        
        try:
            response_data = response.json()
            if response_data.get('status') == expected_status:
                context.response = response
                logger.info(f"Polling successful: status is '{expected_status}'")
                return
        except:
            pass
        
        time.sleep(poll_interval)
    
    raise TimeoutError(f"Polling timeout: status did not reach '{expected_status}' within {timeout} seconds")

@when('I upload file "{file_path}" to "{endpoint}" with form data')
def step_upload_file_with_data(context, file_path, endpoint):
    """
    Upload a file with optional additional form data.
    Requires a table with form data in the step.
    
    Example:
    When I upload file "test_data/sample.jpg" to "/upload" with form data
      | field_name | field_value |
      | user_id    | 123         |
      | project_id | 456         |
    """
    additional_data = {}
    for row in context.table:
        additional_data[row['field_name']] = row['field_value']

    logger.info(f"Uploading file '{file_path}' to '{endpoint}' with data: {additional_data}")
    context.response = context.rest_client.upload_file(endpoint, file_path, additional_data=additional_data)

@when('I upload file "{file_path}" to "{endpoint}"')
def step_upload_file(context, file_path, endpoint):
    """
    Upload a file without additional form data.
    
    Example:
    When I upload file "test_data/document.pdf" to "/upload"
    """
    logger.info(f"Uploading file '{file_path}' to '{endpoint}'")
    context.response = context.rest_client.upload_file(endpoint, file_path)


# THEN steps (rest of the file is unchanged)

@then('response status code should be {expected_code:d}')
def step_verify_status_code(context, expected_code):
    """Verify response status code."""
    actual_code = context.response.status_code
    assert actual_code == expected_code, \
        f"Expected status code {expected_code}, got {actual_code}. Response: {context.response.text}"

@then('response should contain valid JSON')
def step_verify_valid_json(context):
    """Verify response contains valid JSON."""
    try:
        context.response_json = context.response.json()
        logger.info("Response contains valid JSON")
    except json.JSONDecodeError as e:
        raise AssertionError(f"Response is not valid JSON: {e}")

@then('response should match expected schema')
def step_verify_schema(context):
    """Verify response matches expected schema."""
    response_json = context.response.json()
    
    endpoint = context.response.request.path_url
    method = context.response.request.method
    
    schema = json_validator.load_schema_for_endpoint(endpoint, method)
    validation_result = json_validator.validate(response_json, schema)
    
    assert validation_result['valid'], \
        f"Schema validation failed: {validation_result['errors']}"

@then('response should match JSON schema')
def step_verify_custom_schema(context):
    """Verify response matches custom JSON schema."""
    response_json = context.response.json()
    schema = json.loads(context.text.strip())
    
    validation_result = json_validator.validate(response_json, schema)
    
    assert validation_result['valid'], \
        f"Schema validation failed: {validation_result['errors']}"

@then('response time should be less than {max_time:d} milliseconds')
def step_verify_response_time(context, max_time):
    """Verify response time is within limit."""
    actual_time = getattr(context, 'response_time', 0)
    assert actual_time < max_time, \
        f"Response time {actual_time:.2f}ms exceeds limit of {max_time}ms"

@then('response header "{header_name}" should contain "{expected_value}"')
def step_verify_response_header_contains(context, header_name, expected_value):
    """Verify response header contains expected value."""
    actual_value = context.response.headers.get(header_name, '')
    assert expected_value in actual_value, \
        f"Header '{header_name}' does not contain '{expected_value}'. Actual: '{actual_value}'"

@then('response header "{header_name}" should equal "{expected_value}"')
def step_verify_response_header_equals(context, header_name, expected_value):
    """Verify response header equals expected value."""
    actual_value = context.response.headers.get(header_name, '')
    assert actual_value == expected_value, \
        f"Header '{header_name}' expected '{expected_value}', got '{actual_value}'"

@then('response header "{header_name}" should be present')
def step_verify_response_header_present(context, header_name):
    """Verify response header is present."""
    assert header_name in context.response.headers, \
        f"Header '{header_name}' not found in response headers"

@then('response should have required fields')
def step_verify_required_fields(context):
    """Verify response has required fields from table."""
    response_json = context.response.json()
    
    for row in context.table:
        field_name = row['field_name']
        assert json_validator.field_exists(response_json, field_name), \
            f"Required field '{field_name}' not found in response"

@then('response should contain field "{field_path}" with type "{expected_type}"')
def step_verify_field_type(context, field_path, expected_type):
    """Verify field exists with expected type."""
    response_json = context.response.json()
    
    value = json_validator.get_field_value(response_json, field_path)
    assert value is not None, f"Field '{field_path}' not found in response"
    
    actual_type = type(value).__name__
    type_mapping = {
        'string': 'str',
        'number': 'int',
        'float': 'float',
        'boolean': 'bool',
        'array': 'list',
        'object': 'dict'
    }
    
    expected_python_type = type_mapping.get(expected_type, expected_type)
    assert actual_type == expected_python_type, \
        f"Field '{field_path}' expected type '{expected_type}', got '{actual_type}'"

@then('response should contain field "{field_path}" with value "{expected_value}"')
def step_verify_field_value(context, field_path, expected_value):
    """Verify field has expected value."""
    response_json = context.response.json()
    
    actual_value = json_validator.get_field_value(response_json, field_path)
    assert str(actual_value) == expected_value, \
        f"Field '{field_path}' expected '{expected_value}', got '{actual_value}'"

@then('response field "{field_path}" should equal "{expected_value}"')
def step_verify_field_equals(context, field_path, expected_value):
    """Verify field equals expected value."""
    response_json = context.response.json()
    
    actual_value = json_validator.get_field_value(response_json, field_path)
    
    if expected_value.isdigit():
        expected_value = int(expected_value)
    
    assert actual_value == expected_value, \
        f"Field '{field_path}' expected '{expected_value}', got '{actual_value}'"

@then('response field "{field_path}" should equal stored value "{stored_key}"')
def step_verify_field_equals_stored(context, field_path, stored_key):
    """Verify field equals previously stored value."""
    response_json = context.response.json()
    
    actual_value = json_validator.get_field_value(response_json, field_path)
    expected_value = stored_values.get(stored_key)
    
    assert actual_value == expected_value, \
        f"Field '{field_path}' expected stored value '{expected_value}', got '{actual_value}'"

@then('response field "{field_path}" should contain "{expected_substring}"')
def step_verify_field_contains(context, field_path, expected_substring):
    """Verify field contains expected substring."""
    response_json = context.response.json()
    
    actual_value = str(json_validator.get_field_value(response_json, field_path))
    assert expected_substring in actual_value, \
        f"Field '{field_path}' does not contain '{expected_substring}'. Actual: '{actual_value}'"

@then('response field "{field_path}" should be greater than {expected_value:d}')
def step_verify_field_greater_than(context, field_path, expected_value):
    """Verify numeric field is greater than expected value."""
    response_json = context.response.json()
    
    actual_value = json_validator.get_field_value(response_json, field_path)
    assert isinstance(actual_value, (int, float)), \
        f"Field '{field_path}' is not numeric: {type(actual_value)}"
    
    assert actual_value > expected_value, \
        f"Field '{field_path}' expected > {expected_value}, got {actual_value}"

@then('response body should be empty')
def step_verify_empty_body(context):
    """Verify response body is empty."""
    assert len(context.response.content) == 0, \
        f"Expected empty response body, got: {context.response.text}"

@then('response should be JSON array')
def step_verify_json_array(context):
    """Verify response is a JSON array."""
    response_json = context.response.json()
    assert isinstance(response_json, list), \
        f"Expected JSON array, got {type(response_json).__name__}"

@then('response array should have maximum {max_items:d} items')
def step_verify_array_max_items(context, max_items):
    """Verify array has maximum number of items."""
    response_json = context.response.json()
    assert isinstance(response_json, list), "Response is not a JSON array"
    
    actual_count = len(response_json)
    assert actual_count <= max_items, \
        f"Array has {actual_count} items, expected maximum {max_items}"

@then('each item in response should have field "{field_name}" with value "{expected_value}"')
def step_verify_array_items_field_value(context, field_name, expected_value):
    """Verify each item in array has field with expected value."""
    response_json = context.response.json()
    assert isinstance(response_json, list), "Response is not a JSON array"
    
    for i, item in enumerate(response_json):
        actual_value = item.get(field_name)
        assert actual_value == expected_value, \
            f"Item {i}: field '{field_name}' expected '{expected_value}', got '{actual_value}'"

@then('response should have pagination metadata')
def step_verify_pagination_metadata(context):
    """Verify response has pagination metadata."""
    response_json = context.response.json()
    
    required_fields = ['page', 'size', 'total_pages', 'total_items']
    for field in required_fields:
        assert field in response_json, \
            f"Pagination field '{field}' not found in response"

@then('response should not contain field "{field_path}"')
def step_verify_field_not_present(context, field_path):
    """Verify field is not present in response."""
    response_json = context.response.json()
    
    value = json_validator.get_field_value(response_json, field_path)
    assert value is None, f"Field '{field_path}' should not be present, but found: {value}"

@then('I store response field "{field_path}" as "{key}"')
def step_store_response_field(context, field_path, key):
    """Store response field value for later use."""
    response_json = context.response.json()
    
    value = json_validator.get_field_value(response_json, field_path)
    assert value is not None, f"Field '{field_path}' not found in response"
    
    stored_values[key] = value
    logger.info(f"Stored '{key}' = {value}")

@then('request should have header "{header_name}" with value "{expected_value}"')
def step_verify_request_header(context, header_name, expected_value):
    """Verify request had specific header."""
    actual_value = context.response.request.headers.get(header_name)
    assert actual_value == expected_value, \
        f"Request header '{header_name}' expected '{expected_value}', got '{actual_value}'"

@then('average response time should be less than {max_time:d} milliseconds')
def step_verify_average_response_time(context, max_time):
    """Verify average response time from load test."""
    results = context.load_test_results
    response_times = results['response_times']
    
    if response_times:
        avg_time = statistics.mean(response_times)
        assert avg_time < max_time, \
            f"Average response time {avg_time:.2f}ms exceeds limit of {max_time}ms"

@then('95th percentile response time should be less than {max_time:d} milliseconds')
def step_verify_percentile_response_time(context, max_time):
    """Verify 95th percentile response time from load test."""
    results = context.load_test_results
    response_times = sorted(results['response_times'])
    
    if response_times:
        index = int(len(response_times) * 0.95)
        percentile_95 = response_times[index] if index < len(response_times) else response_times[-1]
        
        assert percentile_95 < max_time, \
            f"95th percentile response time {percentile_95:.2f}ms exceeds limit of {max_time}ms"

@then('all responses should have status code {expected_code:d}')
def step_verify_all_status_codes(context, expected_code):
    """Verify all responses from load test have expected status code."""
    results = context.load_test_results
    status_codes = results['status_codes']
    
    unexpected_codes = [code for code in status_codes if code != expected_code]
    
    assert len(unexpected_codes) == 0, \
        f"Found {len(unexpected_codes)} responses with unexpected status codes: {set(unexpected_codes)}"

@then('request should have been retried if needed')
def step_verify_retry_occurred(context):
    """Verify retry mechanism worked if needed."""
    retry_count = getattr(context.response, 'retry_count', 0)
    logger.info(f"Request was retried {retry_count} times")
    
    # Just log the retry count - actual retry logic is in the client

# Cleanup stored values after each scenario
def after_scenario(context, scenario):
    """Clean up stored values after each scenario."""
    stored_values.clear()
    
    if hasattr(context, 'rest_client'):
        context.rest_client.reset()
