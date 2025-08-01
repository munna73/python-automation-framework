# steps/database/base_database_steps.py

from behave import given, when, then
import logging

# Import your existing modules (adjust imports as needed)
try:
    from db.database_manager import DatabaseManager
    from utils.logger import logger
except ImportError as e:
    print(f"Import warning: {e}")
    print("Please adjust imports to match your existing module structure")


@given('I have a connection to "{env}" environment "{db_type}" database')
def step_establish_database_connection(context, env, db_type):
    """Establish connection to specified database."""
    if not hasattr(context, 'db_manager'):
        context.db_manager = DatabaseManager()
    
    try:
        connection = context.db_manager.get_connection(env, db_type)
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
    if not hasattr(context, 'db_manager'):
        context.db_manager = DatabaseManager()
    
    # Use current db_type from context
    db_type = getattr(context, 'current_db_type', 'ORACLE')
    
    try:
        results = context.db_manager.execute_sql_query(query, env, db_type)
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
    
    if not hasattr(context, 'db_manager'):
        context.db_manager = DatabaseManager()
    
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
    
    context.db_manager.store_result(key, value)


@then('the "{first_key}" should equal "{second_key}"')
def step_compare_stored_results(context, first_key, second_key):
    """Compare two stored query results."""
    if not hasattr(context, 'db_manager'):
        raise ValueError("No database manager context available")
    
    try:
        first_value = context.db_manager.get_stored_result(first_key)
        second_value = context.db_manager.get_stored_result(second_key)
        
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
    if hasattr(context, 'db_manager'):
        context.db_manager.cleanup_connections()
        logger.debug("Database connections cleaned up after scenario")