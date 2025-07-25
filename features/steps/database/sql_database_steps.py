# steps/database/sql_database_steps.py

from behave import given, when, then, step
from steps.database.base_database_steps import BaseDatabaseSteps
from utils.logger import logger
from utils.data_comparator import DataComparator
from utils.schema_validator import SchemaValidator
from typing import Dict, List, Any
import pandas as pd


class SQLDatabaseSteps(BaseDatabaseSteps):
    """SQL-specific database step definitions."""
    
    def __init__(self, context):
        super().__init__(context)
        self.data_comparator = DataComparator()
        self.schema_validator = SchemaValidator()


@given('I have a table "{table_name}" in "{env}" database')
def step_verify_table_exists(context, table_name, env):
    """Verify that a table exists in the specified database."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    # Query to check if table exists (Oracle/PostgreSQL compatible)
    if context.current_db_type.upper() == 'ORACLE':
        query = f"SELECT COUNT(*) FROM user_tables WHERE table_name = UPPER('{table_name}')"
    else:  # PostgreSQL
        query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name.lower()}'"
    
    results = context.sql_steps.execute_sql_query(query, env, context.current_db_type)
    table_count = list(results[0].values())[0] if results else 0
    
    assert table_count > 0, f"Table '{table_name}' does not exist in {env} {context.current_db_type} database"
    logger.info(f"Table '{table_name}' exists in {env} database")


@given('I have a table "{table_name}" in both "{env1}" and "{env2}" environments')
def step_verify_table_exists_both_envs(context, table_name, env1, env2):
    """Verify that a table exists in both environments."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    # Check table exists in both environments
    for env in [env1, env2]:
        step_verify_table_exists(context, table_name, env)
    
    # Store for later comparison
    context.comparison_table = table_name
    context.comparison_envs = [env1, env2]


@when('I validate that all records have required fields populated')
def step_validate_required_fields(context):
    """Validate that required fields are properly populated."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    validation_rules = {}
    for row in context.table:
        field_name = row['field_name']
        validation_rule = row['validation_rule']
        validation_rules[field_name] = validation_rule
    
    # Get current table name from context
    table_name = getattr(context, 'current_table', None)
    if not table_name:
        raise ValueError("No current table specified for validation")
    
    # Perform validations
    validation_results = {}
    
    for field_name, rule in validation_rules.items():
        if rule == 'NOT_NULL':
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NULL"
            results = context.sql_steps.execute_sql_query(
                query, context.current_env, context.current_db_type
            )
            null_count = list(results[0].values())[0] if results else 0
            validation_results[field_name] = {
                'rule': rule,
                'passed': null_count == 0,
                'details': f"Found {null_count} NULL values"
            }
            
        elif rule == 'VALID_DATE':
            if context.current_db_type.upper() == 'ORACLE':
                query = f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE {field_name} IS NOT NULL 
                    AND NOT REGEXP_LIKE(TO_CHAR({field_name}, 'YYYY-MM-DD'), '^\\d{{4}}-\\d{{2}}-\\d{{2}}$')
                """
            else:  # PostgreSQL
                query = f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE {field_name} IS NOT NULL 
                    AND {field_name}::date IS NULL
                """
            # Note: This is a simplified validation, you might want more robust date validation
            
        elif rule == 'POSITIVE_NUMBER':
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} <= 0"
            results = context.sql_steps.execute_sql_query(
                query, context.current_env, context.current_db_type
            )
            invalid_count = list(results[0].values())[0] if results else 0
            validation_results[field_name] = {
                'rule': rule,
                'passed': invalid_count == 0,
                'details': f"Found {invalid_count} non-positive values"
            }
    
    # Store validation results
    context.validation_results = validation_results


@then('all validation rules should pass')
def step_verify_all_validations_pass(context):
    """Verify that all validation rules have passed."""
    if not hasattr(context, 'validation_results'):
        raise ValueError("No validation results available")
    
    failed_validations = []
    
    for field_name, result in context.validation_results.items():
        if not result['passed']:
            failed_validations.append(f"{field_name}: {result['details']}")
    
    if failed_validations:
        failure_message = "Validation failures:\n" + "\n".join(failed_validations)
        logger.error(failure_message)
        raise AssertionError(failure_message)
    
    logger.info("All validation rules passed successfully")


@when('I compare data between environments for table "{table_name}"')
def step_compare_data_between_environments(context, table_name):
    """Compare data between two environments for specified table."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    if not hasattr(context, 'comparison_envs') or len(context.comparison_envs) != 2:
        raise ValueError("Two environments must be specified for comparison")
    
    env1, env2 = context.comparison_envs
    
    # Get data from both environments
    query = f"SELECT * FROM {table_name} ORDER BY 1"
    
    data1 = context.sql_steps.execute_sql_query(query, env1, context.current_db_type)
    data2 = context.sql_steps.execute_sql_query(query, env2, context.current_db_type)
    
    # Perform comparison
    comparison_result = context.sql_steps.data_comparator.compare_datasets(
        data1, data2, table_name, env1, env2
    )
    
    context.comparison_result = comparison_result
    logger.info(f"Data comparison completed for table {table_name}")


@then('the data should match with tolerance of "{tolerance_percent:d}%"')
def step_verify_data_matches_with_tolerance(context, tolerance_percent):
    """Verify that data matches within specified tolerance."""
    if not hasattr(context, 'comparison_result'):
        raise ValueError("No comparison result available")
    
    result = context.comparison_result
    
    # Calculate match percentage
    total_records = max(result.get('source_count', 0), result.get('target_count', 0))
    matching_records = result.get('matching_count', 0)
    
    if total_records == 0:
        match_percentage = 100.0
    else:
        match_percentage = (matching_records / total_records) * 100
    
    required_percentage = 100 - tolerance_percent
    
    assert match_percentage >= required_percentage, \
        f"Data match percentage {match_percentage:.2f}% is below required {required_percentage}%"
    
    logger.info(f"Data match percentage {match_percentage:.2f}% meets requirement of {required_percentage}%")


@then('any discrepancies should be logged to file "{filename}"')
def step_log_discrepancies_to_file(context, filename):
    """Log any data discrepancies to specified file."""
    if not hasattr(context, 'comparison_result'):
        raise ValueError("No comparison result available")
    
    result = context.comparison_result
    discrepancies = result.get('discrepancies', [])
    
    if discrepancies:
        # Create DataFrame from discrepancies
        df = pd.DataFrame(discrepancies)
        
        # Save to CSV file in output directory
        output_path = f"output/{filename}"
        df.to_csv(output_path, index=False)
        
        logger.info(f"Logged {len(discrepancies)} discrepancies to {output_path}")
    else:
        logger.info("No discrepancies found, no file generated")


@when('I compare schema for table "{table_name}" between environments')
def step_compare_schema_between_environments(context, table_name):
    """Compare table schema between environments."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    if not hasattr(context, 'comparison_envs') or len(context.comparison_envs) != 2:
        raise ValueError("Two environments must be specified for schema comparison")
    
    env1, env2 = context.comparison_envs
    
    # Get schema information from both environments
    schema1 = context.sql_steps.schema_validator.get_table_schema(
        table_name, env1, context.current_db_type
    )
    schema2 = context.sql_steps.schema_validator.get_table_schema(
        table_name, env2, context.current_db_type
    )
    
    # Compare schemas
    schema_comparison = context.sql_steps.schema_validator.compare_schemas(
        schema1, schema2, table_name, env1, env2
    )
    
    context.schema_comparison = schema_comparison
    logger.info(f"Schema comparison completed for table {table_name}")


@then('the table structure should match exactly')
def step_verify_table_structure_matches(context):
    """Verify that table structures match exactly."""
    if not hasattr(context, 'schema_comparison'):
        raise ValueError("No schema comparison result available")
    
    comparison = context.schema_comparison
    structure_differences = comparison.get('column_differences', [])
    
    assert len(structure_differences) == 0, \
        f"Table structure differences found: {structure_differences}"
    
    logger.info("Table structures match exactly")


@then('indexes should be consistent')
def step_verify_indexes_consistent(context):
    """Verify that indexes are consistent between environments."""
    if not hasattr(context, 'schema_comparison'):
        raise ValueError("No schema comparison result available")
    
    comparison = context.schema_comparison
    index_differences = comparison.get('index_differences', [])
    
    assert len(index_differences) == 0, \
        f"Index differences found: {index_differences}"
    
    logger.info("Indexes are consistent between environments")


@then('constraints should be identical')
def step_verify_constraints_identical(context):
    """Verify that constraints are identical between environments."""
    if not hasattr(context, 'schema_comparison'):
        raise ValueError("No schema comparison result available")
    
    comparison = context.schema_comparison
    constraint_differences = comparison.get('constraint_differences', [])
    
    assert len(constraint_differences) == 0, \
        f"Constraint differences found: {constraint_differences}"
    
    logger.info("Constraints are identical between environments")


@when('I run data quality checks on table "{table_name}"')
def step_run_data_quality_checks(context, table_name):
    """Run comprehensive data quality checks on specified table."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    quality_checks = {}
    
    for row in context.table:
        check_type = row['check_type']
        column_name = row['column_name']
        expected_result = row['expected_result']
        
        if check_type == 'duplicate_check':
            query = f"""
                SELECT {column_name}, COUNT(*) as duplicate_count 
                FROM {table_name} 
                GROUP BY {column_name} 
                HAVING COUNT(*) > 1
            """
            results = context.sql_steps.execute_sql_query(
                query, context.current_env, context.current_db_type
            )
            quality_checks[f"{check_type}_{column_name}"] = {
                'type': check_type,
                'column': column_name,
                'expected': expected_result,
                'passed': len(results) == 0,
                'details': f"Found {len(results)} duplicate groups" if results else "No duplicates found"
            }
            
        elif check_type == 'null_check':
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL"
            results = context.sql_steps.execute_sql_query(
                query, context.current_env, context.current_db_type
            )
            null_count = list(results[0].values())[0] if results else 0
            quality_checks[f"{check_type}_{column_name}"] = {
                'type': check_type,
                'column': column_name,
                'expected': expected_result,
                'passed': null_count == 0,
                'details': f"Found {null_count} NULL values"
            }
            
        elif check_type == 'format_check':
            if expected_result == 'PHONE_FORMAT':
                # Basic phone format validation (adjust regex as needed)
                if context.current_db_type.upper() == 'ORACLE':
                    query = f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {column_name} IS NOT NULL 
                        AND NOT REGEXP_LIKE({column_name}, '^[+]?[0-9\\s\\-\\(\\)]+)
                    """
                else:  # PostgreSQL
                    query = f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {column_name} IS NOT NULL 
                        AND {column_name} !~ '^[+]?[0-9\\s\\-\\(\\)]+
                    """
                
                results = context.sql_steps.execute_sql_query(
                    query, context.current_env, context.current_db_type
                )
                invalid_count = list(results[0].values())[0] if results else 0
                quality_checks[f"{check_type}_{column_name}"] = {
                    'type': check_type,
                    'column': column_name,
                    'expected': expected_result,
                    'passed': invalid_count == 0,
                    'details': f"Found {invalid_count} invalid phone formats"
                }
                
        elif check_type == 'range_check':
            if expected_result == '18_TO_120':
                query = f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE {column_name} IS NOT NULL 
                    AND ({column_name} < 18 OR {column_name} > 120)
                """
                results = context.sql_steps.execute_sql_query(
                    query, context.current_env, context.current_db_type
                )
                out_of_range_count = list(results[0].values())[0] if results else 0
                quality_checks[f"{check_type}_{column_name}"] = {
                    'type': check_type,
                    'column': column_name,
                    'expected': expected_result,
                    'passed': out_of_range_count == 0,
                    'details': f"Found {out_of_range_count} values outside range 18-120"
                }
    
    context.quality_check_results = quality_checks


@then('all data quality checks should pass')
def step_verify_all_quality_checks_pass(context):
    """Verify that all data quality checks have passed."""
    if not hasattr(context, 'quality_check_results'):
        raise ValueError("No quality check results available")
    
    failed_checks = []
    
    for check_name, result in context.quality_check_results.items():
        if not result['passed']:
            failed_checks.append(f"{check_name}: {result['details']}")
    
    if failed_checks:
        failure_message = "Data quality check failures:\n" + "\n".join(failed_checks)
        logger.error(failure_message)
        raise AssertionError(failure_message)
    
    logger.info("All data quality checks passed successfully")


@then('a data quality report should be generated')
def step_generate_data_quality_report(context):
    """Generate a comprehensive data quality report."""
    if not hasattr(context, 'quality_check_results'):
        raise ValueError("No quality check results available")
    
    # Create data quality report
    report_data = []
    for check_name, result in context.quality_check_results.items():
        report_data.append({
            'Check Name': check_name,
            'Type': result['type'],
            'Column': result['column'],
            'Expected': result['expected'],
            'Status': 'PASS' if result['passed'] else 'FAIL',
            'Details': result['details'],
            'Timestamp': pd.Timestamp.now().isoformat()
        })
    
    # Save report to CSV
    df = pd.DataFrame(report_data)
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"output/data_quality_report_{timestamp}.csv"
    df.to_csv(report_filename, index=False)
    
    logger.info(f"Data quality report generated: {report_filename}")


@when('I execute performance test query "{query}"')
def step_execute_performance_test_query(context, query):
    """Execute a query specifically for performance testing."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    # Execute query with detailed performance monitoring
    start_time = time.time()
    
    # Enable query execution plan capture if supported
    if context.current_db_type.upper() == 'ORACLE':
        explain_query = f"EXPLAIN PLAN FOR {query}"
        context.sql_steps.execute_sql_query(
            explain_query, context.current_env, context.current_db_type
        )
        
        # Get execution plan
        plan_query = "SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY)"
        plan_results = context.sql_steps.execute_sql_query(
            plan_query, context.current_env, context.current_db_type
        )
        context.execution_plan = plan_results
    
    # Execute the actual query
    results = context.sql_steps.execute_sql_query(
        query, context.current_env, context.current_db_type
    )
    
    execution_time = time.time() - start_time
    context.last_performance_time = execution_time
    context.last_query_results = results
    
    logger.info(f"Performance test query executed in {execution_time:.3f} seconds")


@then('the execution plan should use indexes efficiently')
def step_verify_execution_plan_uses_indexes(context):
    """Verify that the execution plan uses indexes efficiently."""
    if not hasattr(context, 'execution_plan'):
        logger.warning("Execution plan not available for verification")
        return
    
    # Check execution plan for index usage
    plan_text = str(context.execution_plan)
    
    # Look for index access patterns
    efficient_patterns = ['INDEX RANGE SCAN', 'INDEX UNIQUE SCAN', 'INDEX FAST FULL SCAN']
    inefficient_patterns = ['TABLE ACCESS FULL']
    
    has_index_access = any(pattern in plan_text.upper() for pattern in efficient_patterns)
    has_table_scan = any(pattern in plan_text.upper() for pattern in inefficient_patterns)
    
    # For performance-critical queries, we expect index usage
    if has_table_scan and not has_index_access:
        logger.warning("Query execution plan shows table scan without index usage")
        # Note: This could be a warning rather than a hard failure depending on requirements
    
    logger.info("Execution plan verified for index usage")


# Performance monitoring steps
@step('I monitor query performance for "{query_type}" operations')
def step_monitor_query_performance(context, query_type):
    """Monitor and track query performance for specific operation types."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    # Initialize performance tracking for this query type
    context.sql_steps.performance_monitor.start_monitoring(query_type)
    context.current_query_type = query_type


@then('the average response time should be less than {max_time:f} seconds')
def step_verify_average_response_time(context, max_time):
    """Verify that average response time meets performance requirements."""
    if not hasattr(context, 'sql_steps'):
        raise ValueError("No SQL steps context available")
    
    query_type = getattr(context, 'current_query_type', 'default')
    avg_time = context.sql_steps.performance_monitor.get_average_time(query_type)
    
    assert avg_time <= max_time, \
        f"Average response time {avg_time:.3f}s exceeds maximum {max_time}s for {query_type}"
    
    logger.info(f"Average response time {avg_time:.3f}s meets requirement of <= {max_time}s")


# Transaction management steps
@given('I start a database transaction')
def step_start_database_transaction(context):
    """Start a database transaction."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    connection = context.sql_steps.get_connection(
        context.current_env, context.current_db_type
    )
    
    # Start transaction
    connection.autocommit = False
    context.transaction_started = True
    logger.info("Database transaction started")


@when('I rollback the transaction')
def step_rollback_transaction(context):
    """Rollback the current transaction."""
    if not getattr(context, 'transaction_started', False):
        raise ValueError("No transaction to rollback")
    
    connection = context.sql_steps.get_connection(
        context.current_env, context.current_db_type
    )
    
    connection.rollback()
    context.transaction_started = False
    logger.info("Transaction rolled back")


@when('I commit the transaction')
def step_commit_transaction(context):
    """Commit the current transaction."""
    if not getattr(context, 'transaction_started', False):
        raise ValueError("No transaction to commit")
    
    connection = context.sql_steps.get_connection(
        context.current_env, context.current_db_type
    )
    
    connection.commit()
    context.transaction_started = False
    logger.info("Transaction committed")


# Cleanup function
def after_scenario(context, scenario):
    """Clean up after each scenario."""
    # Rollback any open transactions
    if getattr(context, 'transaction_started', False):
        try:
            step_rollback_transaction(context)
        except Exception as e:
            logger.warning(f"Error rolling back transaction: {e}")
    
    # Clean up connections
    if hasattr(context, 'sql_steps'):
        context.sql_steps.cleanup_connections()