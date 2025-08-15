"""
Demo step definitions for HTML report testing
"""
from behave import given, when, then, but
import time
from utils.logger import logger


@given('I have a passing test step')
def given_passing_step(context):
    """Demo step that always passes"""
    logger.info("Executing passing test step")
    context.test_data = "success"
    

@given('I have a failing test step') 
def given_failing_step(context):
    """Demo step that sets up for failure"""
    logger.info("Setting up failing test step")
    context.test_data = "failure"


@given('I have multiple test steps')
def given_multiple_steps(context):
    """Demo step for complex scenario"""
    logger.info("Setting up multiple test steps")
    context.test_data = "multiple"
    context.step_count = 0


@when('I execute the test step')
def when_execute_step(context):
    """Demo step execution"""
    logger.info("Executing test step")
    time.sleep(0.1)  # Simulate some processing time
    if context.test_data == "success":
        context.result = "passed"
    else:
        context.result = "unknown"


@when('I execute the test step that will fail')
def when_execute_failing_step(context):
    """Demo step that will fail"""
    logger.info("Executing step that will fail")
    time.sleep(0.05)
    # This step intentionally fails to demonstrate error reporting
    raise AssertionError("This is a demo failure for HTML report testing")


@when('I execute a passing step')
def when_execute_passing_step(context):
    """Demo passing step in complex scenario"""
    logger.info("Executing passing step in complex scenario")
    context.step_count += 1
    time.sleep(0.02)


@when('I execute another passing step')  
def when_execute_another_passing_step(context):
    """Another demo passing step"""
    logger.info("Executing another passing step")
    context.step_count += 1
    time.sleep(0.03)


@then('the test should pass successfully')
def then_test_passes(context):
    """Verification step that passes"""
    logger.info("Verifying test passed successfully")
    assert context.result == "passed", f"Expected 'passed' but got '{context.result}'"
    

@then('the test should fail with an error message')
def then_test_fails(context):
    """This step should not be reached due to previous failure"""
    logger.info("This step should not be reached")
    assert False, "Previous step should have failed"


@then('all steps should complete')
def then_all_steps_complete(context):
    """Verification for complex scenario"""
    logger.info("Verifying all steps completed")
    assert context.step_count >= 2, f"Expected at least 2 steps, got {context.step_count}"


@but('one step might have warnings')
def but_step_warnings(context):
    """Demo step that shows warnings can be logged"""
    logger.warning("This is a demo warning message in the test")
    logger.info("Step completed with warnings logged")
    # This step passes but shows how warnings appear in logs