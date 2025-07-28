# steps/database/cross_database_steps.py

from behave import given, when, then
from steps.database.base_database_steps import BaseDatabaseSteps
from steps.database.mongodb_steps import MongoDBSteps

# Import your existing modules (adjust as needed)
try:
    from utils.logger import logger
except ImportError as e:
    print(f"Import warning: {e}")
    import logging
    logger = logging.getLogger(__name__)


class CrossDatabaseSteps:
    """Cross-database operation step definitions."""
    
    def __init__(self, context):
        self.context = context
        self.sql_steps = BaseDatabaseSteps(context)
        self.mongodb_steps = MongoDBSteps(context)
    
    def compare_data_consistency(self, sql_data: list, nosql_data: list, key_field: str) -> dict:
        """Compare data consistency between SQL and NoSQL results."""
        comparison_result = {
            'sql_count': len(sql_data),
            'nosql_count': len(nosql_data),
            'matches': 0,
            'sql_only': [],
            'nosql_only': [],
            'mismatches': []
        }
        
        # Create lookup dictionaries
        sql_lookup = {str(row.get(key_field, '')): row for row in sql_data}
        nosql_lookup = {str(doc.get(key_field, '')): doc for doc in nosql_data}
        
        # Find matches and mismatches
        all_keys = set(sql_lookup.keys()) | set(nosql_lookup.keys())
        
        for key in all_keys:
            if key in sql_lookup and key in nosql_lookup:
                comparison_result['matches'] += 1
                # You can add more detailed field comparison here if needed
            elif key in sql_lookup:
                comparison_result['sql_only'].append(sql_lookup[key])
            else:
                comparison_result['nosql_only'].append(nosql_lookup[key])
        
        return comparison_result


# Cross-database step definitions
@then('the customer data should be consistent between databases')
def step_verify_customer_data_consistency(context):
    """Verify customer data consistency between SQL and NoSQL databases."""
    if not hasattr(context, 'cross_db_steps'):
        context.cross_db_steps = CrossDatabaseSteps(context)
    
    # Get stored results
    try:
        sql_customers = context.cross_db_steps.sql_steps.get_stored_result('sql_customers')
        nosql_customers = context.cross_db_steps.mongodb_steps.get_stored_result('nosql_customers')
        
        # Compare data consistency
        comparison = context.cross_db_steps.compare_data_consistency(
            sql_customers, nosql_customers, 'customer_id'
        )
        
        # Validate results
        total_discrepancies = len(comparison['sql_only']) + len(comparison['nosql_only'])
        
        if total_discrepancies > 0:
            logger.warning(f"Found {total_discrepancies} data discrepancies between databases")
            logger.warning(f"SQL only records: {len(comparison['sql_only'])}")
            logger.warning(f"NoSQL only records: {len(comparison['nosql_only'])}")
        
        # Allow for some tolerance or make it configurable
        max_allowed_discrepancies = 0  # Adjust as needed
        assert total_discrepancies <= max_allowed_discrepancies, \
            f"Too many data discrepancies: {total_discrepancies} > {max_allowed_discrepancies}"
        
        logger.info(f"Data consistency check passed: {comparison['matches']} matching records")
        context.data_consistency_result = comparison
        
    except KeyError as e:
        logger.error(f"Required data not found for consistency check: {e}")
        raise
    except Exception as e:
        logger.error(f"Data consistency check failed: {e}")
        raise


@given('I have test data in Oracle database')
def step_setup_test_data_in_oracle(context):
    """Set up test data in Oracle database for migration testing."""
    if not hasattr(context, 'cross_db_steps'):
        context.cross_db_steps = CrossDatabaseSteps(context)
    
    # Create test data in Oracle
    test_data_sql = """
        INSERT INTO test_migration_table (id, name, status, created_date) 
        VALUES (999, 'Test Migration Record', 'ACTIVE', SYSDATE)
    """
    
    try:
        env = getattr(context, 'current_env', 'DEV')
        context.cross_db_steps.sql_steps.execute_sql_query(test_data_sql, env, 'ORACLE')
        
        # Store test data info for cleanup
        context.test_migration_id = 999
        logger.info("Test data created in Oracle database for migration testing")
        
    except Exception as e:
        logger.error(f"Failed to create test data in Oracle: {e}")
        raise


@when('I migrate the test data to MongoDB')
def step_migrate_test_data_to_mongodb(context):
    """Migrate test data from Oracle to MongoDB."""
    if not hasattr(context, 'cross_db_steps'):
        context.cross_db_steps = CrossDatabaseSteps(context)
    
    try:
        # Get test data from Oracle
        env = getattr(context, 'current_env', 'DEV')
        migration_id = getattr(context, 'test_migration_id', 999)
        
        sql_query = f"SELECT id, name, status, created_date FROM test_migration_table WHERE id = {migration_id}"
        sql_results = context.cross_db_steps.sql_steps.execute_sql_query(sql_query, env, 'ORACLE')
        
        if not sql_results:
            raise ValueError("No test data found in Oracle for migration")
        
        # Convert SQL result to MongoDB document
        sql_record = sql_results[0]
        mongo_document = {
            'id': sql_record['id'],
            'name': sql_record['name'],
            'status': sql_record['status'],
            'created_date': sql_record['created_date'],
            'migrated_from': 'Oracle',
            'migration_timestamp': time.time()
        }
        
        # Insert into MongoDB
        doc_id = context.cross_db_steps.mongodb_steps.insert_test_document(
            'test_migration_collection', env, mongo_document
        )
        
        context.migrated_doc_id = doc_id
        logger.info(f"Successfully migrated test data to MongoDB: {doc_id}")
        
    except Exception as e:
        logger.error(f"Data migration failed: {e}")
        raise


@then('the data should be successfully migrated')
def step_verify_data_migration_success(context):
    """Verify that data migration was successful."""
    if not hasattr(context, 'migrated_doc_id'):
        raise ValueError("No migration record found")
    
    try:
        # Verify the document exists in MongoDB
        env = getattr(context, 'current_env', 'DEV')
        migration_id = getattr(context, 'test_migration_id', 999)
        
        query = {'id': migration_id}
        results = context.cross_db_steps.mongodb_steps.execute_mongodb_find(
            'test_migration_collection', env, query
        )
        
        assert len(results) > 0, "Migrated data not found in MongoDB"
        assert results[0]['name'] == 'Test Migration Record', "Migrated data content mismatch"
        
        logger.info("Data migration verification successful")
        
    except Exception as e:
        logger.error(f"Migration verification failed: {e}")
        raise


@then('the record count should match between databases')
def step_verify_record_count_match(context):
    """Verify that record counts match between databases after migration."""
    try:
        env = getattr(context, 'current_env', 'DEV')
        migration_id = getattr(context, 'test_migration_id', 999)
        
        # Count in Oracle
        sql_count_query = f"SELECT COUNT(*) FROM test_migration_table WHERE id = {migration_id}"
        sql_results = context.cross_db_steps.sql_steps.execute_sql_query(sql_count_query, env, 'ORACLE')
        sql_count = list(sql_results[0].values())[0]
        
        # Count in MongoDB
        mongo_count = context.cross_db_steps.mongodb_steps.execute_mongodb_count(
            'test_migration_collection', env, {'id': migration_id}
        )
        
        assert sql_count == mongo_count, f"Record count mismatch: SQL={sql_count}, MongoDB={mongo_count}"
        logger.info(f"Record count verification passed: {sql_count} records in both databases")
        
    except Exception as e:
        logger.error(f"Record count verification failed: {e}")
        raise


# Cleanup function for cross-database operations
def cross_database_cleanup(context):
    """Clean up cross-database test resources."""
    try:
        # Clean up Oracle test data
        if hasattr(context, 'test_migration_id'):
            env = getattr(context, 'current_env', 'DEV')
            migration_id = context.test_migration_id
            
            cleanup_sql = f"DELETE FROM test_migration_table WHERE id = {migration_id}"
            if hasattr(context, 'cross_db_steps'):
                context.cross_db_steps.sql_steps.execute_sql_query(cleanup_sql, env, 'ORACLE')
                logger.debug(f"Cleaned up Oracle test data: {migration_id}")
        
        # MongoDB cleanup is handled by mongodb_steps.cleanup_test_documents()
        
    except Exception as e:
        logger.warning(f"Error during cross-database cleanup: {e}")