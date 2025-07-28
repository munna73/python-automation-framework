"""
Step definitions for reading and executing SQL queries from config.ini
"""
from behave import given, when, then
from db.database_connector import db_connector
from utils.config_loader import config_loader
from utils.logger import logger
from utils.data_comparator import data_comparator
import pandas as pd
import configparser
import os

# Store dataframes for comparison
context_dataframes = {}

@given('I read SQL query "{query_key}" from config section "{section}"')
def step_read_query_from_config(context, query_key, section):
    """
    Read SQL query from config.ini file.
    
    Args:
        context: Behave context
        query_key: Key name in the config section (e.g., 'customer_comparison')
        section: Section name in config.ini (e.g., 'DATABASE_QUERIES')
    """
    try:
        # Read config.ini
        config = configparser.ConfigParser()
        config_path = os.path.join('config', 'config.ini')
        config.read(config_path)
        
        # Get query from specified section and key
        if section not in config:
            raise ValueError(f"Section '{section}' not found in config.ini")
        
        if query_key not in config[section]:
            raise ValueError(f"Query key '{query_key}' not found in section '{section}'")
        
        query = config[section][query_key]
        
        # Store in context
        context.sql_query = query
        context.query_key = query_key
        
        logger.info(f"Read query '{query_key}' from config section '{section}'")
        logger.debug(f"Query: {query}")
        
    except Exception as e:
        logger.error(f"Failed to read query from config: {e}")
        raise

@when('I execute the config query on "{environment}" "{db_type}" database')
def step_execute_config_query(context, environment, db_type):
    """
    Execute the SQL query read from config on specified database.
    
    Args:
        context: Behave context
        environment: Database environment (DEV, QA, PROD)
        db_type: Database type (ORACLE, POSTGRES)
    """
    try:
        if not hasattr(context, 'sql_query'):
            raise ValueError("No SQL query found in context. Run 'I read SQL query' step first")
        
        # Execute query
        df = db_connector.execute_query(environment, db_type, context.sql_query)
        
        # Store dataframe with unique key
        storage_key = f"{environment}_{db_type}_{context.query_key}"
        context_dataframes[storage_key] = df
        
        # Also store in context for immediate use
        context.query_result_df = df
        context.query_result_key = storage_key
        
        logger.info(f"Executed query on {environment} {db_type}: {len(df)} rows returned")
        
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise

@when('I execute the config query on "{environment}" "{db_type}" database and store as "{alias}"')
def step_execute_config_query_with_alias(context, environment, db_type, alias):
    """
    Execute SQL query and store result with a custom alias.
    
    Args:
        context: Behave context
        environment: Database environment
        db_type: Database type
        alias: Custom alias for storing the dataframe
    """
    try:
        if not hasattr(context, 'sql_query'):
            raise ValueError("No SQL query found in context. Run 'I read SQL query' step first")
        
        # Execute query
        df = db_connector.execute_query(environment, db_type, context.sql_query)
        
        # Store with custom alias
        context_dataframes[alias] = df
        
        # Also store in context
        context.query_result_df = df
        context.query_result_alias = alias
        
        logger.info(f"Executed query and stored as '{alias}': {len(df)} rows")
        
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise

@given('I have executed query "{query_key}" from section "{section}" on "{environment}" "{db_type}" database')
def step_read_and_execute_query(context, query_key, section, environment, db_type):
    """
    Combined step to read query from config and execute it.
    
    Args:
        context: Behave context
        query_key: Query key in config
        section: Config section
        environment: Database environment
        db_type: Database type
    """
    # Read query
    step_read_query_from_config(context, query_key, section)
    
    # Execute query
    step_execute_config_query(context, environment, db_type)

@then('I compare the query results between "{source_alias}" and "{target_alias}"')
def step_compare_query_results(context, source_alias, target_alias):
    """
    Compare two dataframes stored with aliases.
    
    Args:
        context: Behave context
        source_alias: Alias of source dataframe
        target_alias: Alias of target dataframe
    """
    try:
        # Get dataframes
        if source_alias not in context_dataframes:
            raise ValueError(f"Source dataframe '{source_alias}' not found")
        
        if target_alias not in context_dataframes:
            raise ValueError(f"Target dataframe '{target_alias}' not found")
        
        source_df = context_dataframes[source_alias]
        target_df = context_dataframes[target_alias]
        
        # Perform comparison
        comparison_result = data_comparator.compare_dataframes(
            source_df,
            target_df,
            key_columns=getattr(context, 'key_columns', None),
            exclude_columns=getattr(context, 'exclude_columns', [])
        )
        
        # Store result
        context.comparison_result = comparison_result
        
        # Log summary
        logger.info(f"Comparison complete: {comparison_result['summary']['total_differences']} differences found")
        
    except Exception as e:
        logger.error(f"Failed to compare dataframes: {e}")
        raise

@given('I set comparison key columns as "{columns}"')
def step_set_key_columns(context, columns):
    """
    Set key columns for dataframe comparison.
    
    Args:
        context: Behave context
        columns: Comma-separated list of column names
    """
    context.key_columns = [col.strip() for col in columns.split(',')]
    logger.info(f"Set key columns: {context.key_columns}")

@given('I set columns to exclude from comparison as "{columns}"')
def step_set_exclude_columns(context, columns):
    """
    Set columns to exclude from comparison.
    
    Args:
        context: Behave context
        columns: Comma-separated list of column names to exclude
    """
    context.exclude_columns = [col.strip() for col in columns.split(',')]
    logger.info(f"Set exclude columns: {context.exclude_columns}")

@then('the query result should have {expected_count:d} rows')
def step_verify_row_count(context, expected_count):
    """
    Verify the row count of query result.
    
    Args:
        context: Behave context
        expected_count: Expected number of rows
    """
    if not hasattr(context, 'query_result_df'):
        raise ValueError("No query result found in context")
    
    actual_count = len(context.query_result_df)
    assert actual_count == expected_count, \
        f"Expected {expected_count} rows, but got {actual_count}"

@then('the query result should have columns "{expected_columns}"')
def step_verify_columns(context, expected_columns):
    """
    Verify the columns in query result.
    
    Args:
        context: Behave context
        expected_columns: Comma-separated list of expected column names
    """
    if not hasattr(context, 'query_result_df'):
        raise ValueError("No query result found in context")
    
    expected_cols = [col.strip() for col in expected_columns.split(',')]
    actual_cols = list(context.query_result_df.columns)
    
    missing_cols = set(expected_cols) - set(actual_cols)
    assert not missing_cols, \
        f"Missing columns: {missing_cols}. Actual columns: {actual_cols}"

@when('I export the query result to "{file_path}" as "{format_type}"')
def step_export_query_result(context, file_path, format_type):
    """
    Export query result to file.
    
    Args:
        context: Behave context
        file_path: Output file path
        format_type: Export format (csv, excel, json)
    """
    try:
        if not hasattr(context, 'query_result_df'):
            raise ValueError("No query result found in context")
        
        df = context.query_result_df
        
        # Create directory if needed
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Export based on format
        if format_type.lower() == 'csv':
            df.to_csv(file_path, index=False)
        elif format_type.lower() == 'excel':
            df.to_excel(file_path, index=False)
        elif format_type.lower() == 'json':
            df.to_json(file_path, orient='records', indent=2)
        else:
            raise ValueError(f"Unsupported format: {format_type}")
        
        logger.info(f"Exported {len(df)} rows to {file_path} as {format_type}")
        
    except Exception as e:
        logger.error(f"Failed to export query result: {e}")
        raise

@then('I should see no differences in the comparison')
def step_verify_no_differences(context):
    """Verify that comparison found no differences."""
    if not hasattr(context, 'comparison_result'):
        raise ValueError("No comparison result found. Run comparison step first")
    
    total_diff = context.comparison_result['summary']['total_differences']
    assert total_diff == 0, \
        f"Expected no differences, but found {total_diff} differences"

@then('I should see {expected_diff:d} differences in the comparison')
def step_verify_difference_count(context, expected_diff):
    """Verify specific number of differences."""
    if not hasattr(context, 'comparison_result'):
        raise ValueError("No comparison result found. Run comparison step first")
    
    total_diff = context.comparison_result['summary']['total_differences']
    assert total_diff == expected_diff, \
        f"Expected {expected_diff} differences, but found {total_diff}"

# Cleanup function
def clear_stored_dataframes():
    """Clear all stored dataframes."""
    global context_dataframes
    context_dataframes.clear()
    logger.info("Cleared all stored dataframes")