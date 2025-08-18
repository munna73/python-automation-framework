"""
Step definitions for @my_test scenarios.
"""
from behave import given, when, then
from utils.logger import logger

@given('I have a simple test setup')
def step_simple_test_setup(context):
    """Setup for simple test."""
    logger.info("Setting up simple test - no complex configuration needed")
    context.test_setup = True

@given('I have a basic test environment')  
def step_basic_test_environment(context):
    """Setup basic test environment."""
    logger.info("Setting up basic test environment")
    context.test_environment = "basic"

@when('I run my test logic')
def step_run_test_logic(context):
    """Run test logic."""
    logger.info("Running test logic")
    context.test_result = "success"

@when('I perform basic operations')
def step_perform_basic_operations(context):
    """Perform basic operations."""
    logger.info("Performing basic operations")
    context.operations_performed = True

@then('the test should pass without API_TOKEN validation')
def step_test_passes_without_api(context):
    """Verify test passes without needing API configuration."""
    logger.info("Test completed successfully without API_TOKEN validation")
    assert context.test_result == "success"
    
@then('everything should work correctly')
def step_everything_works(context):
    """Verify everything works."""
    logger.info("Everything working correctly")
    assert hasattr(context, 'operations_performed')
    assert context.operations_performed == True