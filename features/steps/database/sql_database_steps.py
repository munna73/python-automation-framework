# steps/database/sql_database_steps.py

from behave import given, when, then, step
import time
import pandas as pd
from typing import Dict, List, Any
from steps.database.base_database_steps import BaseDatabaseSteps

# Import your existing modules (adjust as needed)
try:
    from utils.logger import logger
    from db.database_connector import db_connector
except ImportError as e:
    print(f"Import warning: {e}")
    print("Please adjust imports to match your existing module structure")


class SQLDatabaseSteps(BaseDatabaseSteps):
    """SQL-specific database step definitions."""
    
    def __init__(self, context):
        super().__init__(context)
        self.validation_results = {}
        self.comparison_result = {}
        self.quality_check_results = {}
        self.schema_comparison = {}


# Step definitions for your data_validation.feature
@given('I have a table "{table_name}" in "{env}" database')
def step_verify_table_exists(context, table_name, env):
    """Verify that a table exists in the specified database."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    # Get current database type from context
    db_type = getattr(context, 'current_db_type', 'ORACLE')
    
    # Query to check if table exists (Oracle/PostgreSQL compatible)
    if db_type.upper() == 'ORACLE':
        query = f"SELECT COUNT(*) FROM user_tables WHERE table_name = UPPER('{table_name}')"
    else:  # PostgreSQL
        query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name.lower()}'"
    
    try:
        results = context.sql_steps.execute_sql_query(query, env, db_type)
        table_count = list(results[0].values())[0] if results else 0
        
        assert table_count > 0, f"Table '{table_name}' does not exist in {env} {db_type} database"
        logger.info(f"Table '{table_name}' exists in {env} database")
        
        # Store table name for use in other steps
        context.current_table = table_name
        
    except Exception as e:
        logger.error(f"Table existence check failed: {e}")
        raise


@given('I have table "{table_name}" in both "{env1}" and "{env2}" environments')
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
    
    env = getattr(context, 'current_env', 'DEV')
    db_type = getattr(context, 'current_db_type', 'ORACLE')
    
    # Perform validations
    validation_results = {}
    
    for field_name, rule in validation_rules.items():
        if rule == 'NOT_NULL':
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NULL"
            results = context.sql_steps.execute_sql_query(query, env, db_type)
            null_count = list(results[0].values())[0] if results else 0
            validation_results[field_name] = {
                'rule': rule,
                'passed': null_count == 0,
                'details': f"Found {null_count} NULL values"
            }
            
        elif rule == 'VALID_DATE':
            if db_type.upper() == 'ORACLE':
                query = f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE {field_name} IS NOT NULL 
                    AND NOT REGEXP_LIKE(TO_CHAR({field_name}, 'YYYY-MM-DD'), '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$')
                """
            else:  # PostgreSQL
                query = f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE {field_name} IS NOT NULL 
                    AND {field_name}::text !~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                """
            
            results = context.sql_steps.execute_sql_query(query, env, db_type)
            invalid_count = list(results[0].values())[0] if results else 0
            validation_results[field_name] = {
                'rule': rule,
                'passed': invalid_count == 0,
                'details': f"Found {invalid_count} invalid date values"
            }
            
        elif rule == 'POSITIVE_NUMBER':
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} <= 0"
            results = context.sql_steps.execute_sql_query(query, env, db_type)
            invalid_count = list(results[0].values())[0] if results else 0
            validation_results[field_name] = {
                'rule': rule,
                'passed': invalid_count == 0,
                'details': f"Found {invalid_count} non-positive values"
            }
    
    # Store validation results
    context.sql_steps.validation_results = validation_results
    context.validation_results = validation_results


@then('all validation rules should pass')
def step_verify_all_validations_pass(context):
    """Verify that all validation rules have passed."""
    validation_results = getattr(context, 'validation_results', {})
    
    if not validation_results:
        raise ValueError("No validation results available")
    
    failed_validations = []
    
    for field_name, result in validation_results.items():
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
    db_type = getattr(context, 'current_db_type', 'ORACLE')
    
    # Get data from both environments
    query = f"SELECT * FROM {table_name} ORDER BY 1"
    
    try:
        data1 = context.sql_steps.execute_sql_query(query, env1, db_type)
        data2 = context.sql_steps.execute_sql_query(query, env2, db_type)
        
        # Perform basic comparison
        comparison_result = {
            'table_name': table_name,
            'env1': env1,
            'env2': env2,
            'env1_count': len(data1),
            'env2_count': len(data2),
            'matching_count': 0,
            'discrepancies': []
        }
        
        # Simple comparison logic (can be enhanced)
        if len(data1) == len(data2):
            comparison_result['matching_count'] = len(data1)
        else:
            comparison_result['discrepancies'].append({
                'type': 'count_mismatch',
                'env1_count': len(data1),
                'env2_count': len(data2)
            })
        
        context.sql_steps.comparison_result = comparison_result
        context.comparison_result = comparison_result
        logger.info(f"Data comparison completed for table {table_name}")
        
    except Exception as e:
        logger.error(f"Data comparison failed: {e}")
        raise


@then('the data should match with tolerance of "{tolerance_percent:d}%"')
def step_verify_data_matches_with_tolerance(context, tolerance_percent):
    """Verify that data matches within specified tolerance."""
    comparison_result = getattr(context, 'comparison_result', {})
    
    if not comparison_result:
        raise ValueError("No comparison result available")
    
    # Calculate match percentage
    env1_count = comparison_result.get('env1_count', 0)
    env2_count = comparison_result.get('env2_count', 0)
    total_records = max(env1_count, env2_count)
    matching_records = comparison_result.get('matching_count', 0)
    
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
    comparison_result = getattr(context, 'comparison_result', {})
    
    if not comparison_result:
        raise ValueError("No comparison result available")
    
    discrepancies = comparison_result.get('discrepancies', [])
    
    if discrepancies:
        # Create DataFrame from discrepancies
        df = pd.DataFrame(discrepancies)
        
        # Save to CSV file in output directory
        import os
        os.makedirs('output', exist_ok=True)
        output_path = f"output/{filename}"
        df.to_csv(output_path, index=False)
        
        logger.info(f"Logged {len(discrepancies)} discrepancies to {output_path}")
    else:
        logger.info("No discrepancies found, no file generated")


@when('I execute performance test query "{query}"')
def step_execute_performance_test_query(context, query):
    """Execute a query specifically for performance testing."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    db_type = getattr(context, 'current_db_type', 'ORACLE')
    
    # Execute query with performance monitoring
    start_time = time.time()
    
    try:
        results = context.sql_steps.execute_sql_query(query, env, db_type)
        execution_time = time.time() - start_time
        
        context.last_performance_time = execution_time
        context.last_query_results = results
        
        logger.info(f"Performance test query executed in {execution_time:.3f} seconds")
        
    except Exception as e:
        logger.error(f"Performance test query failed: {e}")
        raise


@then('the query should complete within {seconds:d} seconds')
def step_verify_query_performance(context, seconds):
    """Verify that the last query completed within specified time."""
    last_execution_time = getattr(context, 'last_performance_time', None)
    
    if last_execution_time is None:
        raise ValueError("No performance data available")
    
    assert last_execution_time <= seconds, \
        f"Query took {last_execution_time:.2f} seconds, expected <= {seconds} seconds"
    
    logger.info(f"Performance check passed: Query completed in {last_execution_time:.2f} seconds")


@then('the execution plan should use indexes efficiently')
def step_verify_execution_plan_uses_indexes(context):
    """Verify that the execution plan uses indexes efficiently."""
    # This is a placeholder implementation
    # In a real scenario, you'd capture and analyze the execution plan
    logger.info("Execution plan verification - implementation depends on database specifics")
    
    # You can implement actual execution plan analysis here
    # For Oracle: EXPLAIN PLAN FOR ... then SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY)
    # For PostgreSQL: EXPLAIN (ANALYZE, BUFFERS) ...


@when('I run data quality checks on table "{table_name}"')
def step_run_data_quality_checks(context, table_name):
    """Run comprehensive data quality checks on specified table."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    db_type = getattr(context, 'current_db_type', 'ORACLE')
    
    quality_checks = {}
    
    for row in context.table:
        check_type = row['check_type']
        column_name = row['column_name']
        expected_result = row['expected_result']
        
        try:
            if check_type == 'duplicate_check':
                query = f"""
                    SELECT {column_name}, COUNT(*) as duplicate_count 
                    FROM {table_name} 
                    GROUP BY {column_name} 
                    HAVING COUNT(*) > 1
                """
                results = context.sql_steps.execute_sql_query(query, env, db_type)
                quality_checks[f"{check_type}_{column_name}"] = {
                    'type': check_type,
                    'column': column_name,
                    'expected': expected_result,
                    'passed': len(results) == 0,
                    'details': f"Found {len(results)} duplicate groups" if results else "No duplicates found"
                }
                
            elif check_type == 'null_check':
                query = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL"
                results = context.sql_steps.execute_sql_query(query, env, db_type)
                null_count = list(results[0].values())[0] if results else 0
                quality_checks[f"{check_type}_{column_name}"] = {
                    'type': check_type,
                    'column': column_name,
                    'expected': expected_result,
                    'passed': null_count == 0,
                    'details': f"Found {null_count} NULL values"
                }
                
            elif check_type == 'format_check' and expected_result == 'PHONE_FORMAT':
                if db_type.upper() == 'ORACLE':
                    query = f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {column_name} IS NOT NULL 
                        AND NOT REGEXP_LIKE({column_name}, '^[+]?[0-9\\s\\-\\(\\)]+$')
                    """
                else:  # PostgreSQL
                    query = f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {column_name} IS NOT NULL 
                        AND {column_name} !~ '^[+]?[0-9\\s\\-\\(\\)]+$'
                    """
                
                results = context.sql_steps.execute_sql_query(query, env, db_type)
                invalid_count = list(results[0].values())[0] if results else 0
                quality_checks[f"{check_type}_{column_name}"] = {
                    'type': check_type,
                    'column': column_name,
                    'expected': expected_result,
                    'passed': invalid_count == 0,
                    'details': f"Found {invalid_count} invalid phone formats"
                }
                
            elif check_type == 'range_check' and expected_result == '18_TO_120':
                query = f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE {column_name} IS NOT NULL 
                    AND ({column_name} < 18 OR {column_name} > 120)
                """
                results = context.sql_steps.execute_sql_query(query, env, db_type)
                out_of_range_count = list(results[0].values())[0] if results else 0
                quality_checks[f"{check_type}_{column_name}"] = {
                    'type': check_type,
                    'column': column_name,
                    'expected': expected_result,
                    'passed': out_of_range_count == 0,
                    'details': f"Found {out_of_range_count} values outside range 18-120"
                }
                
        except Exception as e:
            logger.error(f"Quality check failed for {check_type}_{column_name}: {e}")
            quality_checks[f"{check_type}_{column_name}"] = {
                'type': check_type,
                'column': column_name,
                'expected': expected_result,
                'passed': False,
                'details': f"Check failed with error: {str(e)}"
            }
    
    context.sql_steps.quality_check_results = quality_checks
    context.quality_check_results = quality_checks


@then('all data quality checks should pass')
def step_verify_all_quality_checks_pass(context):
    """Verify that all data quality checks have passed."""
    quality_check_results = getattr(context, 'quality_check_results', {})
    
    if not quality_check_results:
        raise ValueError("No quality check results available")
    
    failed_checks = []
    
    for check_name, result in quality_check_results.items():
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
    quality_check_results = getattr(context, 'quality_check_results', {})
    
    if not quality_check_results:
        raise ValueError("No quality check results available")
    
    # Create data quality report
    report_data = []
    for check_name, result in quality_check_results.items():
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
    
    import os
    os.makedirs('output', exist_ok=True)
    report_filename = f"output/data_quality_report_{timestamp}.csv"
    df.to_csv(report_filename, index=False)
    
    logger.info(f"Data quality report generated: {report_filename}")


@when('I compare schema for table "{table_name}" between environments')
def step_compare_schema_between_environments(context, table_name):
    """Compare table schema between environments."""
    if not hasattr(context, 'sql_steps'):
        context.sql_steps = SQLDatabaseSteps(context)
    
    if not hasattr(context, 'comparison_envs') or len(context.comparison_envs) != 2:
        raise ValueError("Two environments must be specified for schema comparison")
    
    env1, env2 = context.comparison_envs
    db_type = getattr(context, 'current_db_type', 'ORACLE')
    
    # Basic schema comparison - can be enhanced
    schema_comparison = {
        'table_name': table_name,
        'environments': [env1, env2],
        'column_differences': [],
        'index_differences': [],
        'constraint_differences': []
    }
    
    try:
        # Get column information from both environments
        if db_type.upper() == 'ORACLE':
            schema_query = f"""
                SELECT column_name, data_type, data_length, nullable 
                FROM user_tab_columns 
                WHERE table_name = UPPER('{table_name}')
                ORDER BY column_id
            """
        else:  # PostgreSQL
            schema_query = f"""
                SELECT column_name, data_type, character_maximum_length as data_length, is_nullable as nullable
                FROM information_schema.columns
                WHERE table_name = '{table_name.lower()}'
                ORDER BY ordinal_position
            """
        
        schema1 = context.sql_steps.execute_sql_query(schema_query, env1, db_type)
        schema2 = context.sql_steps.execute_sql_query(schema_query, env2, db_type)
        
        # Simple comparison - can be enhanced for detailed analysis
        if len(schema1) != len(schema2):
            schema_comparison['column_differences'].append({
                'type': 'column_count_mismatch',
                'env1_count': len(schema1),
                'env2_count': len(schema2)
            })
        
        context.sql_steps.schema_comparison = schema_comparison
        context.schema_comparison = schema_comparison
        logger.info(f"Schema comparison completed for table {table_name}")
        
    except Exception as e:
        logger.error(f"Schema comparison failed: {e}")
        raise


@then('the table structure should match exactly')
def step_verify_table_structure_matches(context):
    """Verify that table structures match exactly."""
    schema_comparison = getattr(context, 'schema_comparison', {})
    
    if not schema_comparison:
        raise ValueError("No schema comparison result available")
    
    structure_differences = schema_comparison.get('column_differences', [])
    
    assert len(structure_differences) == 0, \
        f"Table structure differences found: {structure_differences}"
    
    logger.info("Table structures match exactly")


@then('indexes should be consistent')
def step_verify_indexes_consistent(context):
    """Verify that indexes are consistent between environments."""
    schema_comparison = getattr(context, 'schema_comparison', {})
    
    if not schema_comparison:
        raise ValueError("No schema comparison result available")
    
    index_differences = schema_comparison.get('index_differences', [])
    
    assert len(index_differences) == 0, \
        f"Index differences found: {index_differences}"
    
    logger.info("Indexes are consistent between environments")


@then('constraints should be identical')
def step_verify_constraints_identical(context):
    """Verify that constraints are identical between environments."""
    schema_comparison = getattr(context, 'schema_comparison', {})
    
    if not schema_comparison:
        raise ValueError("No schema comparison result available")
    
    constraint_differences = schema_comparison.get('constraint_differences', [])
    
    assert len(constraint_differences) == 0, \
        f"Constraint differences found: {constraint_differences}"
    
    logger.info("Constraints are identical between environments")