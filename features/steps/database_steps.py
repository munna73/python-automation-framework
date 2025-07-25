"""Enhanced database steps for BDD testing with proper typing and error handling."""
from typing import Dict, List, Any, Optional
from behave import given, when, then, step
from behave.runner import Context
import json
import pandas as pd
from datetime import datetime
import logging

from db.database_connector import db_connector
from db.data_comparator import DataComparator
from utils.custom_exceptions import (
    DatabaseConnectionError, 
    QueryExecutionError, 
    DataValidationError,
    ComparisonError
)
from utils.logger import logger
from utils.config_loader import config_loader


class DatabaseContext:
    """Helper class to manage database context in BDD steps."""
    
    def __init__(self, context: Context):
        self.context = context
        self._ensure_db_context()
    
    def _ensure_db_context(self) -> None:
        """Ensure database context exists."""
        if not hasattr(self.context, 'db'):
            self.context.db = {}
        if not hasattr(self.context, 'query_results'):
            self.context.query_results = {}
        if not hasattr(self.context, 'comparison_results'):
            self.context.comparison_results = {}
    
    def store_connection(self, env: str, db_type: str, connection: Any) -> None:
        """Store database connection in context."""
        key = f"{env}_{db_type}"
        self.context.db[key] = connection
        logger.info(f"Stored connection for {key}")
    
    def get_connection(self, env: str, db_type: str) -> Any:
        """Get database connection from context."""
        key = f"{env}_{db_type}"
        if key not in self.context.db:
            raise DatabaseConnectionError(f"No connection found for {key}")
        return self.context.db[key]
    
    def store_results(self, key: str, results: List[Dict[str, Any]]) -> None:
        """Store query results in context."""
        self.context.query_results[key] = results
        logger.info(f"Stored {len(results)} results for key: {key}")
    
    def get_results(self, key: str) -> List[Dict[str, Any]]:
        """Get query results from context."""
        if key not in self.context.query_results:
            raise DataValidationError(f"No results found for key: {key}")
        return self.context.query_results[key]


@given('I connect to "{env}" "{db_type}" database')
def step_connect_to_database(context: Context, env: str, db_type: str) -> None:
    """
    Connect to a database.
    
    Args:
        context: Behave context
        env: Environment (DEV, QA, PROD)
        db_type: Database type (ORACLE, POSTGRES)
    """
    db_context = DatabaseContext(context)
    
    try:
        logger.info(f"Connecting to {env} {db_type} database")
        config = config_loader.get_database_config(env, db_type)
        
        connection = db_connector.connect(
            db_type=db_type,
            host=config.host,
            port=config.port,
            database=config.database,
            username=config.username,
            password=config.password
        )
        
        db_context.store_connection(env, db_type, connection)
        logger.info(f"Successfully connected to {env} {db_type} database")
        
    except Exception as e:
        logger.error(f"Failed to connect to {env} {db_type}: {str(e)}")
        raise DatabaseConnectionError(f"Connection failed: {str(e)}")


@when('I execute query "{query}" on "{env}" "{db_type}" database')
def step_execute_query(context: Context, query: str, env: str, db_type: str) -> None:
    """Execute a query on specified database."""
    db_context = DatabaseContext(context)
    
    try:
        connection = db_context.get_connection(env, db_type)
        
        # Handle multi-line queries from feature files
        if context.text:
            query = context.text
        
        logger.info(f"Executing query on {env} {db_type}: {query[:100]}...")
        results = db_connector.execute_query(connection, query)
        
        # Store results with a unique key
        result_key = f"{env}_{db_type}_latest"
        db_context.store_results(result_key, results)
        
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        raise QueryExecutionError(f"Query failed: {str(e)}", query=query)


@when('I execute query from file "{filename}" on "{env}" "{db_type}" database')
def step_execute_query_from_file(context: Context, filename: str, env: str, db_type: str) -> None:
    """Execute a query from a SQL file."""
    db_context = DatabaseContext(context)
    
    try:
        with open(f"sql/{filename}", 'r') as f:
            query = f.read()
        
        connection = db_context.get_connection(env, db_type)
        logger.info(f"Executing query from {filename} on {env} {db_type}")
        
        results = db_connector.execute_query(connection, query)
        
        # Store with filename as part of key
        result_key = f"{env}_{db_type}_{filename.replace('.sql', '')}"
        db_context.store_results(result_key, results)
        
    except FileNotFoundError:
        raise DataValidationError(f"SQL file not found: {filename}")
    except Exception as e:
        logger.error(f"Failed to execute query from file: {str(e)}")
        raise QueryExecutionError(f"Query from file failed: {str(e)}", query=filename)


@when('I execute parameterized query on "{env}" "{db_type}" database')
def step_execute_parameterized_query(context: Context, env: str, db_type: str) -> None:
    """Execute a parameterized query with parameters from table."""
    db_context = DatabaseContext(context)
    
    if not context.table:
        raise DataValidationError("No parameters provided for parameterized query")
    
    try:
        # Extract query and parameters
        query = context.text
        params = dict(context.table[0])
        
        connection = db_context.get_connection(env, db_type)
        logger.info(f"Executing parameterized query with params: {params}")
        
        results = db_connector.execute_query(connection, query, params)
        
        result_key = f"{env}_{db_type}_parameterized"
        db_context.store_results(result_key, results)
        
    except Exception as e:
        logger.error(f"Parameterized query failed: {str(e)}")
        raise QueryExecutionError(f"Parameterized query failed: {str(e)}", query=query, params=params)


@then('the query result should have {expected_count:d} rows')
def step_verify_row_count(context: Context, expected_count: int) -> None:
    """Verify the number of rows in query result."""
    db_context = DatabaseContext(context)
    
    # Get the latest results
    results = list(db_context.context.query_results.values())[-1]
    actual_count = len(results)
    
    if actual_count != expected_count:
        raise DataValidationError(
            f"Row count mismatch. Expected: {expected_count}, Actual: {actual_count}"
        )
    
    logger.info(f"Row count verification passed: {actual_count} rows")


@then('the query result should contain')
def step_verify_result_contains(context: Context) -> None:
    """Verify query results contain expected data from table."""
    db_context = DatabaseContext(context)
    
    if not context.table:
        raise DataValidationError("No expected data provided")
    
    # Get the latest results
    results = list(db_context.context.query_results.values())[-1]
    
    for expected_row in context.table:
        expected_dict = dict(expected_row)
        
        # Check if any result matches the expected row
        match_found = any(
            all(str(result.get(k)) == str(v) for k, v in expected_dict.items())
            for result in results
        )
        
        if not match_found:
            raise DataValidationError(f"Expected row not found in results: {expected_dict}")
    
    logger.info("Result content verification passed")


@then('the query result should be empty')
def step_verify_empty_result(context: Context) -> None:
    """Verify query result is empty."""
    db_context = DatabaseContext(context)
    
    results = list(db_context.context.query_results.values())[-1]
    
    if results:
        raise DataValidationError(f"Expected empty result, but got {len(results)} rows")
    
    logger.info("Empty result verification passed")


@when('I compare data between "{source_env}" and "{target_env}" for "{db_type}" using query')
def step_compare_data_between_environments(
    context: Context, source_env: str, target_env: str, db_type: str
) -> None:
    """Compare data between two environments."""
    db_context = DatabaseContext(context)
    
    try:
        query = context.text
        if not query:
            raise DataValidationError("No query provided for comparison")
        
        # Execute query in both environments
        source_conn = db_context.get_connection(source_env, db_type)
        target_conn = db_context.get_connection(target_env, db_type)
        
        logger.info(f"Executing comparison query in {source_env} and {target_env}")
        
        source_results = db_connector.execute_query(source_conn, query)
        target_results = db_connector.execute_query(target_conn, query)
        
        # Store individual results
        db_context.store_results(f"{source_env}_{db_type}_comparison", source_results)
        db_context.store_results(f"{target_env}_{db_type}_comparison", target_results)
        
        # Perform comparison
        comparator = DataComparator()
        comparison = comparator.compare_datasets(
            source_results, 
            target_results,
            source_name=source_env,
            target_name=target_env
        )
        
        # Store comparison results
        db_context.context.comparison_results['latest'] = comparison
        
    except Exception as e:
        logger.error(f"Data comparison failed: {str(e)}")
        raise ComparisonError(f"Comparison failed: {str(e)}")


@then('the data comparison should show no differences')
def step_verify_no_differences(context: Context) -> None:
    """Verify that data comparison shows no differences."""
    db_context = DatabaseContext(context)
    
    if 'latest' not in db_context.context.comparison_results:
        raise DataValidationError("No comparison results found")
    
    comparison = db_context.context.comparison_results['latest']
    
    if comparison['differences_count'] > 0:
        raise ComparisonError(
            f"Found {comparison['differences_count']} differences in data comparison",
            source_data=comparison.get('source_only', []),
            target_data=comparison.get('target_only', [])
        )
    
    logger.info("Data comparison passed - no differences found")


@then('I export the query results to "{format}" file "{filename}"')
def step_export_results(context: Context, format: str, filename: str) -> None:
    """Export query results to file."""
    db_context = DatabaseContext(context)
    
    try:
        # Get the latest results
        results = list(db_context.context.query_results.values())[-1]
        
        if not results:
            logger.warning("No results to export")
            return
        
        df = pd.DataFrame(results)
        output_path = f"output/{filename}"
        
        if format.lower() == 'csv':
            df.to_csv(output_path, index=False)
        elif format.lower() == 'excel':
            df.to_excel(output_path, index=False, engine='xlsxwriter')
        elif format.lower() == 'json':
            df.to_json(output_path, orient='records', indent=2)
        else:
            raise DataValidationError(f"Unsupported export format: {format}")
        
        logger.info(f"Exported {len(results)} rows to {output_path}")
        
    except Exception as e:
        logger.error(f"Export failed: {str(e)}")
        raise DataValidationError(f"Failed to export results: {str(e)}")


@step('I close all database connections')
def step_close_all_connections(context: Context) -> None:
    """Close all open database connections."""
    db_context = DatabaseContext(context)
    
    if hasattr(db_context.context, 'db'):
        for key, connection in db_context.context.db.items():
            try:
                db_connector.disconnect(connection)
                logger.info(f"Closed connection: {key}")
            except Exception as e:
                logger.warning(f"Failed to close connection {key}: {str(e)}")
        
        # Clear the connections
        db_context.context.db.clear()


# Hook to ensure connections are closed after scenario
def after_scenario(context: Context, scenario) -> None:
    """Clean up database connections after each scenario."""
    step_close_all_connections(context)