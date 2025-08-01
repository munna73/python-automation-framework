"""
Step definitions for reading and executing SQL queries from config.ini
"""
from behave import given, when, then
import pandas as pd
import os
from typing import Dict, Any, List, Optional

# Import your existing modules
try:
    from db.database_manager import DatabaseManager
    from utils.config_loader import config_loader
    from utils.logger import logger, db_logger
    # Import data comparator if it exists, otherwise create basic comparison
    try:
        from utils.data_comparator import data_comparator
    except ImportError:
        data_comparator = None
        logger.warning("data_comparator module not found, using basic comparison")
except ImportError as e:
    print(f"Import warning: {e}")
    print("Please adjust imports to match your existing module structure")


class QueryDatabaseSteps:
    """Query execution and management step definitions."""
    
    def __init__(self, context):
        self.context = context
        self.dataframes = {}  # Store dataframes for comparison
        
        # Initialize database manager if not exists
        if not hasattr(context, 'db_manager'):
            context.db_manager = DatabaseManager()
        
        self.db_manager = context.db_manager
    
    def execute_query(self, environment: str, db_type: str, query: str) -> pd.DataFrame:
        """Execute query and return results as DataFrame."""
        try:
            results = self.db_manager.execute_sql_query(query, environment, db_type)
            df = pd.DataFrame(results)
            db_logger.info(f"Query executed on {environment} {db_type}: {len(df)} rows returned")
            return df
        except Exception as e:
            db_logger.error(f"Query execution failed: {e}")
            raise
    
    def basic_dataframe_comparison(self, source_df: pd.DataFrame, target_df: pd.DataFrame, 
                                  key_columns: Optional[List[str]] = None,
                                  exclude_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """Basic dataframe comparison when data_comparator is not available."""
        exclude_columns = exclude_columns or []
        
        # Filter out excluded columns
        source_cols = [col for col in source_df.columns if col not in exclude_columns]
        target_cols = [col for col in target_df.columns if col not in exclude_columns]
        
        source_filtered = source_df[source_cols] if source_cols else source_df
        target_filtered = target_df[target_cols] if target_cols else target_df
        
        comparison_result = {
            'summary': {
                'source_rows': len(source_filtered),
                'target_rows': len(target_filtered),
                'source_columns': len(source_filtered.columns),
                'target_columns': len(target_filtered.columns),
                'total_differences': 0
            },
            'differences': []
        }
        
        # Compare row counts
        if len(source_filtered) != len(target_filtered):
            comparison_result['differences'].append({
                'type': 'row_count_mismatch',
                'source_count': len(source_filtered),
                'target_count': len(target_filtered)
            })
            comparison_result['summary']['total_differences'] += 1
        
        # Compare column names
        source_col_set = set(source_filtered.columns)
        target_col_set = set(target_filtered.columns)
        
        if source_col_set != target_col_set:
            missing_in_target = source_col_set - target_col_set
            missing_in_source = target_col_set - source_col_set
            
            if missing_in_target:
                comparison_result['differences'].append({
                    'type': 'columns_missing_in_target',
                    'columns': list(missing_in_target)
                })
                comparison_result['summary']['total_differences'] += len(missing_in_target)
            
            if missing_in_source:
                comparison_result['differences'].append({
                    'type': 'columns_missing_in_source',
                    'columns': list(missing_in_source)
                })
                comparison_result['summary']['total_differences'] += len(missing_in_source)
        
        return comparison_result


# Global instance for storing dataframes across steps
query_steps_instance = None


def get_query_steps(context):
    """Get or create QueryDatabaseSteps instance."""
    global query_steps_instance
    if query_steps_instance is None:
        query_steps_instance = QueryDatabaseSteps(context)
    return query_steps_instance


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
        # Use the enhanced config_loader to get custom configuration
        query = config_loader.get_custom_config(section, query_key)
        
        # Store in context
        context.sql_query = query
        context.query_key = query_key
        context.config_section = section
        
        logger.info(f"Read query '{query_key}' from config section '{section}'")
        db_logger.debug(f"Query: {query}")
        
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
        
        query_steps = get_query_steps(context)
        
        # Execute query
        df = query_steps.execute_query(environment, db_type, context.sql_query)
        
        # Store dataframe with unique key
        storage_key = f"{environment}_{db_type}_{context.query_key}"
        query_steps.dataframes[storage_key] = df
        
        # Also store in context for immediate use
        context.query_result_df = df
        context.query_result_key = storage_key
        context.current_env = environment
        context.current_db_type = db_type
        
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
        
        query_steps = get_query_steps(context)
        
        # Execute query
        df = query_steps.execute_query(environment, db_type, context.sql_query)
        
        # Store with custom alias
        query_steps.dataframes[alias] = df
        
        # Also store in context
        context.query_result_df = df
        context.query_result_alias = alias
        context.current_env = environment
        context.current_db_type = db_type
        
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


@given('I have executed query "{query_key}" from section "{section}" on "{environment}" "{db_type}" database and stored as "{alias}"')
def step_read_and_execute_query_with_alias(context, query_key, section, environment, db_type, alias):
    """
    Combined step to read query from config, execute it, and store with alias.
    """
    # Read query
    step_read_query_from_config(context, query_key, section)
    
    # Execute query with alias
    step_execute_config_query_with_alias(context, environment, db_type, alias)


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
        query_steps = get_query_steps(context)
        
        # Get dataframes
        if source_alias not in query_steps.dataframes:
            raise ValueError(f"Source dataframe '{source_alias}' not found")
        
        if target_alias not in query_steps.dataframes:
            raise ValueError(f"Target dataframe '{target_alias}' not found")
        
        source_df = query_steps.dataframes[source_alias]
        target_df = query_steps.dataframes[target_alias]
        
        # Perform comparison
        if data_comparator:
            comparison_result = data_comparator.compare_dataframes(
                source_df,
                target_df,
                key_columns=getattr(context, 'key_columns', None),
                exclude_columns=getattr(context, 'exclude_columns', [])
            )
        else:
            # Use basic comparison
            comparison_result = query_steps.basic_dataframe_comparison(
                source_df,
                target_df,
                key_columns=getattr(context, 'key_columns', None),
                exclude_columns=getattr(context, 'exclude_columns', [])
            )
        
        # Store result
        context.comparison_result = comparison_result
        
        # Log summary
        total_differences = comparison_result['summary']['total_differences']
        logger.info(f"Comparison complete: {total_differences} differences found")
        
        if total_differences > 0:
            logger.warning(f"Differences found between '{source_alias}' and '{target_alias}':")
            for diff in comparison_result['differences'][:5]:  # Show first 5 differences
                logger.warning(f"  {diff}")
        
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
    
    logger.info(f"Row count verification passed: {actual_count} rows")


@then('the query result should have more than {min_count:d} rows')
def step_verify_min_row_count(context, min_count):
    """Verify the query result has more than minimum rows."""
    if not hasattr(context, 'query_result_df'):
        raise ValueError("No query result found in context")
    
    actual_count = len(context.query_result_df)
    assert actual_count > min_count, \
        f"Expected more than {min_count} rows, but got {actual_count}"
    
    logger.info(f"Minimum row count verification passed: {actual_count} > {min_count}")


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
    
    logger.info(f"Column verification passed: All expected columns found")


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
            df.to_excel(file_path, index=False, engine='openpyxl')
        elif format_type.lower() == 'json':
            df.to_json(file_path, orient='records', indent=2)
        else:
            raise ValueError(f"Unsupported format: {format_type}. Supported: csv, excel, json")
        
        logger.info(f"Exported {len(df)} rows to {file_path} as {format_type}")
        
    except Exception as e:
        logger.error(f"Failed to export query result: {e}")
        raise


@when('I export dataframe "{alias}" to "{file_path}" as "{format_type}"')
def step_export_stored_dataframe(context, alias, file_path, format_type):
    """Export a stored dataframe to file."""
    try:
        query_steps = get_query_steps(context)
        
        if alias not in query_steps.dataframes:
            raise ValueError(f"Dataframe '{alias}' not found")
        
        df = query_steps.dataframes[alias]
        
        # Create directory if needed
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Export based on format
        if format_type.lower() == 'csv':
            df.to_csv(file_path, index=False)
        elif format_type.lower() == 'excel':
            df.to_excel(file_path, index=False, engine='openpyxl')
        elif format_type.lower() == 'json':
            df.to_json(file_path, orient='records', indent=2)
        else:
            raise ValueError(f"Unsupported format: {format_type}")
        
        logger.info(f"Exported dataframe '{alias}' ({len(df)} rows) to {file_path}")
        
    except Exception as e:
        logger.error(f"Failed to export dataframe: {e}")
        raise


@then('I should see no differences in the comparison')
def step_verify_no_differences(context):
    """Verify that comparison found no differences."""
    if not hasattr(context, 'comparison_result'):
        raise ValueError("No comparison result found. Run comparison step first")
    
    total_diff = context.comparison_result['summary']['total_differences']
    assert total_diff == 0, \
        f"Expected no differences, but found {total_diff} differences"
    
    logger.info("Comparison verification passed: No differences found")


@then('I should see {expected_diff:d} differences in the comparison')
def step_verify_difference_count(context, expected_diff):
    """Verify specific number of differences."""
    if not hasattr(context, 'comparison_result'):
        raise ValueError("No comparison result found. Run comparison step first")
    
    total_diff = context.comparison_result['summary']['total_differences']
    assert total_diff == expected_diff, \
        f"Expected {expected_diff} differences, but found {total_diff}"
    
    logger.info(f"Difference count verification passed: {total_diff} differences")


@then('I should see at most {max_diff:d} differences in the comparison')
def step_verify_max_differences(context, max_diff):
    """Verify that differences don't exceed maximum threshold."""
    if not hasattr(context, 'comparison_result'):
        raise ValueError("No comparison result found. Run comparison step first")
    
    total_diff = context.comparison_result['summary']['total_differences']
    assert total_diff <= max_diff, \
        f"Expected at most {max_diff} differences, but found {total_diff}"
    
    logger.info(f"Maximum difference threshold verification passed: {total_diff} <= {max_diff}")


@when('I generate comparison report to "{file_path}"')
def step_generate_comparison_report(context, file_path):
    """Generate a detailed comparison report."""
    try:
        if not hasattr(context, 'comparison_result'):
            raise ValueError("No comparison result found. Run comparison step first")
        
        comparison = context.comparison_result
        
        # Create report data
        report_data = {
            'Summary': comparison['summary'],
            'Differences': comparison['differences'],
            'Generated_At': pd.Timestamp.now().isoformat()
        }
        
        # Export as JSON for detailed structure
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        import json
        with open(file_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        logger.info(f"Comparison report generated: {file_path}")
        
    except Exception as e:
        logger.error(f"Failed to generate comparison report: {e}")
        raise


# Cleanup functions
def clear_stored_dataframes():
    """Clear all stored dataframes."""
    global query_steps_instance
    if query_steps_instance:
        query_steps_instance.dataframes.clear()
        logger.info("Cleared all stored dataframes")


def query_database_cleanup(context):
    """Cleanup function for query database steps."""
    try:
        clear_stored_dataframes()
        db_logger.debug("Query database steps cleanup completed")
    except Exception as e:
        db_logger.warning(f"Error during query database cleanup: {e}")


# Additional utility steps
@when('I execute direct query "{query}" on "{environment}" "{db_type}" database')
def step_execute_direct_query(context, query, environment, db_type):
    """Execute a direct SQL query without reading from config."""
    try:
        query_steps = get_query_steps(context)
        
        # Execute query
        df = query_steps.execute_query(environment, db_type, query)
        
        # Store in context
        context.query_result_df = df
        context.current_env = environment
        context.current_db_type = db_type
        
        logger.info(f"Executed direct query on {environment} {db_type}: {len(df)} rows returned")
        
    except Exception as e:
        logger.error(f"Failed to execute direct query: {e}")
        raise


@when('I store current query result as "{alias}"')
def step_store_current_result(context, alias):
    """Store the current query result with an alias."""
    if not hasattr(context, 'query_result_df'):
        raise ValueError("No query result found in context")
    
    query_steps = get_query_steps(context)
    query_steps.dataframes[alias] = context.query_result_df.copy()
    
    logger.info(f"Stored current query result as '{alias}' ({len(context.query_result_df)} rows)")


@then('I should see dataframe "{alias}" with {expected_count:d} rows')
def step_verify_stored_dataframe_count(context, alias, expected_count):
    """Verify row count of a stored dataframe."""
    query_steps = get_query_steps(context)
    
    if alias not in query_steps.dataframes:
        raise ValueError(f"Dataframe '{alias}' not found")
    
    actual_count = len(query_steps.dataframes[alias])
    assert actual_count == expected_count, \
        f"Dataframe '{alias}': expected {expected_count} rows, but got {actual_count}"
    
    logger.info(f"Stored dataframe verification passed: '{alias}' has {actual_count} rows")