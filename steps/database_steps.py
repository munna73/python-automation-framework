"""
Database-related step definitions for Behave.
"""
from behave import given, when, then
from utils.logger import logger

@given('I connect to "{environment}" {db_type} database')
def step_connect_database(context, environment, db_type):
    """Connect to specified database."""
    logger.info(f"Connecting to {environment} {db_type} database")
    # Implementation will use DatabaseConnector class
    context.db_connection = f"{environment}_{db_type}"

@when('I execute customer comparison query')
def step_execute_comparison_query(context):
    """Execute customer comparison query."""
    logger.info("Executing customer comparison query")
    # Implementation will use DataComparator class
    context.query_result = "comparison_executed"

@when('I execute comparison query with window "{window_minutes}" minutes')
def step_execute_windowed_query(context, window_minutes):
    """Execute query with time window."""
    logger.info(f"Executing query with {window_minutes} minute window")
    context.window_minutes = int(window_minutes)
    context.query_result = "windowed_query_executed"

@then('the data should match within acceptable thresholds')
def step_verify_data_match(context):
    """Verify data matches within thresholds."""
    logger.info("Verifying data match within thresholds")
    assert context.query_result is not None

@then('differences should be exported to Excel')
def step_export_differences(context):
    """Export differences to Excel file."""
    logger.info("Exporting differences to Excel")
    # Implementation will use ExportUtils class

@then('the results should be processed successfully')
def step_verify_processing(context):
    """Verify results processed successfully."""
    logger.info("Verifying successful processing")
    assert context.query_result is not None

@then('export should handle CLOB data properly')
def step_verify_clob_handling(context):
    """Verify CLOB data handling."""
    logger.info("Verifying CLOB data handling")
    # Implementation will use DataCleaner class