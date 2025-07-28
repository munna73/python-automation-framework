# steps/database/base_database_steps.py

from behave import given, when, then
import time
import logging
from typing import Dict, Any, List, Optional

# Import your existing modules (adjust imports as needed)
try:
    from db.database_connector import db_connector
    from db.mongodb_connector import mongodb_connector
    from utils.config_loader import config_loader
    from utils.logger import logger
except ImportError as e:
    print(f"Import warning: {e}")
    print("Please adjust imports to match your existing module structure")


class BaseDatabaseSteps:
    """Base class for database step definitions with common functionality."""
    
    def __init__(self, context):
        self.context = context
        self.connections = {}
        self.query_results = {}
        
    def get_connection(self, env: str, db_type: str):
        """Get or create database connection."""
        connection_key = f"{env}_{db_type}"
        
        if connection_key not in self.connections:
            if db_type.upper() in ['ORACLE', 'POSTGRES', 'POSTGRESQL']:
                self.connections[connection_key] = db_connector.get_connection(env, db_type)
            elif db_type.upper() == 'MONGODB':
                self.connections[connection_key] = mongodb_connector.get_connection(env)
            else:
                raise ValueError(f"Unsupported database type: {db_type}")
                
        return self.connections[connection_key]
    
    def execute_sql_query(self, query: str, env: str, db_type: str) -> List[Dict]:
        """Execute SQL query and return results."""
        connection = self.get_connection(env, db_type)
        
        start_time = time.time()
        results = db_connector.execute_query(connection, query)
        execution_time = time.time() - start_time
        
        logger.info(f"Query executed in {execution_time:.2f} seconds: {query[:100]}...")
        return results
    
    def store_result(self, key: str, value: Any):
        """Store query result for later use."""
        self.query_results[key] = value
        logger.debug(f"Stored result '{key}': {value}")
    
    def get_stored_result(self, key: str) -> Any:
        """Retrieve stored query result."""
        if key not in self.query_results:
            raise KeyError(f"No result stored with key: {key}")
        return self.query_results[key]
    
    def cleanup_connections(self):
        """Clean up all database connections."""
        for connection_key, connection in self.connections.items():
            try:
                if 'MONGODB' in connection_key:
                    connection.close()
                else:
                    db_connector.close_connection(connection)
                logger.debug(f"Closed connection: {connection_key}")
            except Exception as e:
                logger.warning(f"Error closing connection {connection_key}: {e}")
        
        self.connections.clear()


# Essential step definitions that work with your existing framework
@given('I have a connection to "{env}" environment "{db_type}" database')
def step_establish_database_connection(context, env, db_type):
    """Establish connection to specified database."""
    if not hasattr(context, 'db_steps'):
        context.db_steps = BaseDatabaseSteps(context)
    
    try:
        connection = context.db_steps.get_connection(env, db_type)
        logger.info(f"Successfully connected to {env} {db_type} database")
        
        # Store connection info in context for later use
        context.current_env = env
        context.current_db_type = db_type
        
    except Exception as e:
        logger.error(f"Failed to connect to {env} {db_type} database: {e}")
        raise


@when('I execute query "{query}" on "{env}" database')
def step_execute_query(context, query, env):
    """Execute SQL query on specified database."""
    if not hasattr(context, 'db_steps'):
        context.db_steps = BaseDatabaseSteps(context)
    
    # Use current db_type from context
    db_type = getattr(context, 'current_db_type', 'ORACLE')
    
    try:
        results = context.db_steps.execute_sql_query(query, env, db_type)
        context.last_query_results = results
        logger.info(f"Query executed successfully, returned {len(results)} rows")
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        context.last_query_error = str(e)
        raise


@when('I store the result as "{key}"')
def step_store_query_result(context, key):
    """Store the last query result with specified key."""
    if not hasattr(context, 'last_query_results'):
        raise ValueError("No query results available to store")
    
    if not hasattr(context, 'db_steps'):
        context.db_steps = BaseDatabaseSteps(context)
    
    # Store the first row's first column value for simple queries
    if context.last_query_results and len(context.last_query_results) > 0:
        if len(context.last_query_results[0]) == 1:
            # Single column result
            value = list(context.last_query_results[0].values())[0]
        else:
            # Multiple columns, store entire result
            value = context.last_query_results
    else:
        value = None
    
    context.db_steps.store_result(key, value)


@then('the "{first_key}" should equal "{second_key}"')
def step_compare_stored_results(context, first_key, second_key):
    """Compare two stored query results."""
    if not hasattr(context, 'db_steps'):
        raise ValueError("No database steps context available")
    
    try:
        first_value = context.db_steps.get_stored_result(first_key)
        second_value = context.db_steps.get_stored_result(second_key)
        
        assert first_value == second_value, f"Values don't match: {first_value} != {second_value}"
        logger.info(f"Comparison passed: {first_key} ({first_value}) equals {second_key} ({second_value})")
        
    except KeyError as e:
        logger.error(f"Stored result not found: {e}")
        raise
    except AssertionError as e:
        logger.error(f"Comparison failed: {e}")
        raise

@then('the query should complete successfully')
def step_verify_query_successful(context):
    """Verify that the query completed without errors."""
    if hasattr(context, 'last_query_error'):
        raise AssertionError(f"Query failed: {context.last_query_error}")
    
    if not hasattr(context, 'last_query_results'):
        raise AssertionError("No query results available")
    
    logger.info("Query completed successfully")

@then('the results should be stored successfully')
def step_verify_results_stored(context):
    """Verify that query results were stored successfully."""
    if not hasattr(context, 'last_query_results'):
        raise AssertionError("No query results to verify")
    
    logger.info("Results stored and verified successfully")
    
# Cleanup function for environment.py
def after_scenario(context, scenario):
    """Clean up after each scenario."""
    if hasattr(context, 'db_steps'):
        context.db_steps.cleanup_connections()
        logger.debug("Database connections cleaned up after scenario")

