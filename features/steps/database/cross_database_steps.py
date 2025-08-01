# steps/database/cross_database_steps.py

from behave import given, when, then
import time
from typing import Dict, List, Any

# Import your existing modules
try:
    from db.database_manager import DatabaseManager
    from steps.database.mongodb_steps import MongoDBSteps
    from utils.logger import logger, db_logger
except ImportError as e:
    print(f"Import warning: {e}")
    import logging
    logger = logging.getLogger(__name__)
    db_logger = logger


class CrossDatabaseSteps:
    """Cross-database operation step definitions."""
    
    def __init__(self, context):
        self.context = context
        
        # Initialize database manager if not exists
        if not hasattr(context, 'db_manager'):
            context.db_manager = DatabaseManager()
        
        # Initialize MongoDB steps if not exists
        if not hasattr(context, 'mongodb_steps'):
            context.mongodb_steps = MongoDBSteps(context)
        
        self.db_manager = context.db_manager
        self.mongodb_steps = context.mongodb_steps
    
    def compare_data_consistency(self, sql_data: List[Dict], nosql_data: List[Dict], key_field: str) -> Dict[str, Any]:
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
                sql_record = sql_lookup[key]
                nosql_record = nosql_lookup[key]
                
                # Compare key fields for data integrity
                mismatched_fields = []
                for field in ['name', 'status']:  # Add other fields as needed
                    if sql_record.get(field) != nosql_record.get(field):
                        mismatched_fields.append(field)
                
                if mismatched_fields:
                    comparison_result['mismatches'].append({
                        'key': key,
                        'fields': mismatched_fields,
                        'sql_record': sql_record,
                        'nosql_record': nosql_record
                    })
                    
            elif key in sql_lookup:
                comparison_result['sql_only'].append(sql_lookup[key])
            else:
                comparison_result['nosql_only'].append(nosql_lookup[key])
        
        return comparison_result
    
    def execute_sql_query(self, query: str, env: str, db_type: str) -> List[Dict]:
        """Execute SQL query using database manager."""
        return self.db_manager.execute_sql_query(query, env, db_type)
    
    def cleanup_migration_test_data(self, migration_id: int, env: str):
        """Clean up test data from both databases."""
        try:
            # Clean up Oracle test data
            cleanup_sql = f"DELETE FROM test_migration_table WHERE id = {migration_id}"
            self.execute_sql_query(cleanup_sql, env, 'ORACLE')
            db_logger.debug(f"Cleaned up Oracle test data: {migration_id}")
            
            # Clean up MongoDB test documents (handled by mongodb_steps)
            self.mongodb_steps.cleanup_test_documents()
            db_logger.debug(f"Cleaned up MongoDB test data")
            
        except Exception as e:
            db_logger.warning(f"Error during migration test cleanup: {e}")


# Cross-database step definitions
@then('the customer data should be consistent between databases')
def step_verify_customer_data_consistency(context):
    """Verify customer data consistency between SQL and NoSQL databases."""
    if not hasattr(context, 'cross_db_steps'):
        context.cross_db_steps = CrossDatabaseSteps(context)
    
    # Get stored results
    try:
        sql_customers = context.db_manager.get_stored_result('sql_customers')
        nosql_customers = context.db_manager.get_stored_result('nosql_customers')
        
        # Compare data consistency
        comparison = context.cross_db_steps.compare_data_consistency(
            sql_customers, nosql_customers, 'customer_id'
        )
        
        # Log detailed comparison results
        db_logger.info(f"Data consistency comparison:")
        db_logger.info(f"  SQL records: {comparison['sql_count']}")
        db_logger.info(f"  NoSQL records: {comparison['nosql_count']}")
        db_logger.info(f"  Matching records: {comparison['matches']}")
        db_logger.info(f"  SQL-only records: {len(comparison['sql_only'])}")
        db_logger.info(f"  NoSQL-only records: {len(comparison['nosql_only'])}")
        db_logger.info(f"  Field mismatches: {len(comparison['mismatches'])}")
        
        # Validate results
        total_discrepancies = len(comparison['sql_only']) + len(comparison['nosql_only']) + len(comparison['mismatches'])
        
        if total_discrepancies > 0:
            logger.warning(f"Found {total_discrepancies} data discrepancies between databases")
            
            # Log specific discrepancies for debugging
            if comparison['sql_only']:
                logger.warning(f"Records only in SQL: {comparison['sql_only'][:5]}...")  # Show first 5
            if comparison['nosql_only']:
                logger.warning(f"Records only in NoSQL: {comparison['nosql_only'][:5]}...")  # Show first 5
            if comparison['mismatches']:
                logger.warning(f"Field mismatches: {comparison['mismatches'][:5]}...")  # Show first 5
        
        # Allow for some tolerance or make it configurable
        max_allowed_discrepancies = int(getattr(context, 'max_allowed_discrepancies', 0))
        assert total_discrepancies <= max_allowed_discrepancies, \
            f"Too many data discrepancies: {total_discrepancies} > {max_allowed_discrepancies}"
        
        logger.info(f"Data consistency check passed: {comparison['matches']} matching records")
        context.data_consistency_result = comparison
        
    except KeyError as e:
        logger.error(f"Required data not found for consistency check: {e}")
        logger.error("Make sure to store SQL results as 'sql_customers' and NoSQL results as 'nosql_customers'")
        raise
    except Exception as e:
        logger.error(f"Data consistency check failed: {e}")
        raise


@given('I have test data in Oracle database')
def step_setup_test_data_in_oracle(context):
    """Set up test data in Oracle database for migration testing."""
    if not hasattr(context, 'cross_db_steps'):
        context.cross_db_steps = CrossDatabaseSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    migration_id = int(time.time() * 1000) % 1000000  # Generate unique ID
    
    # Create test data in Oracle
    test_data_sql = f"""
        INSERT INTO test_migration_table (id, name, status, created_date) 
        VALUES ({migration_id}, 'Test Migration Record', 'ACTIVE', SYSDATE)
    """
    
    try:
        context.cross_db_steps.execute_sql_query(test_data_sql, env, 'ORACLE')
        
        # Store test data info for cleanup
        context.test_migration_id = migration_id
        logger.info(f"Test data created in Oracle database: ID {migration_id}")
        
    except Exception as e:
        logger.error(f"Failed to create test data in Oracle: {e}")
        raise


@when('I migrate the test data to MongoDB')
def step_migrate_test_data_to_mongodb(context):
    """Migrate test data from Oracle to MongoDB."""
    if not hasattr(context, 'cross_db_steps'):
        context.cross_db_steps = CrossDatabaseSteps(context)
    
    try:
        env = getattr(context, 'current_env', 'DEV')
        migration_id = getattr(context, 'test_migration_id')
        
        if not migration_id:
            raise ValueError("No test migration ID found - ensure test data was created first")
        
        # Get test data from Oracle
        sql_query = f"SELECT id, name, status, created_date FROM test_migration_table WHERE id = {migration_id}"
        sql_results = context.cross_db_steps.execute_sql_query(sql_query, env, 'ORACLE')
        
        if not sql_results:
            raise ValueError(f"No test data found in Oracle for migration ID: {migration_id}")
        
        # Convert SQL result to MongoDB document
        sql_record = sql_results[0]
        mongo_document = {
            'id': sql_record['id'],
            'name': sql_record['name'],
            'status': sql_record['status'],
            'created_date': str(sql_record['created_date']),  # Convert to string for MongoDB
            'migrated_from': 'Oracle',
            'migration_timestamp': time.time()
        }
        
        # Insert into MongoDB
        doc_id = context.cross_db_steps.mongodb_steps.insert_test_document(
            'test_migration_collection', env, mongo_document
        )
        
        context.migrated_doc_id = doc_id
        context.last_used_collection = 'test_migration_collection'
        logger.info(f"Successfully migrated test data to MongoDB: {doc_id}")
        
    except Exception as e:
        logger.error(f"Data migration failed: {e}")
        raise


@then('the data should be successfully migrated')
def step_verify_data_migration_success(context):
    """Verify that data migration was successful."""
    if not hasattr(context, 'migrated_doc_id'):
        raise ValueError("No migration record found")
    
    if not hasattr(context, 'cross_db_steps'):
        context.cross_db_steps = CrossDatabaseSteps(context)
    
    try:
        env = getattr(context, 'current_env', 'DEV')
        migration_id = getattr(context, 'test_migration_id')
        
        # Verify the document exists in MongoDB
        query = {'id': migration_id}
        results = context.cross_db_steps.mongodb_steps.execute_mongodb_find(
            'test_migration_collection', env, query
        )
        
        assert len(results) > 0, "Migrated data not found in MongoDB"
        
        migrated_doc = results[0]
        assert migrated_doc['name'] == 'Test Migration Record', f"Migrated data content mismatch: expected 'Test Migration Record', got '{migrated_doc['name']}'"
        assert migrated_doc['status'] == 'ACTIVE', f"Migrated status mismatch: expected 'ACTIVE', got '{migrated_doc['status']}'"
        assert migrated_doc['migrated_from'] == 'Oracle', "Migration source not recorded correctly"
        
        logger.info("Data migration verification successful")
        context.migration_verified = True
        
    except Exception as e:
        logger.error(f"Migration verification failed: {e}")
        raise


@then('the record count should match between databases')
def step_verify_record_count_match(context):
    """Verify that record counts match between databases after migration."""
    if not hasattr(context, 'cross_db_steps'):
        context.cross_db_steps = CrossDatabaseSteps(context)
    
    try:
        env = getattr(context, 'current_env', 'DEV')
        migration_id = getattr(context, 'test_migration_id')
        
        if not migration_id:
            raise ValueError("No test migration ID available")
        
        # Count in Oracle
        sql_count_query = f"SELECT COUNT(*) as record_count FROM test_migration_table WHERE id = {migration_id}"
        sql_results = context.cross_db_steps.execute_sql_query(sql_count_query, env, 'ORACLE')
        sql_count = sql_results[0]['record_count']
        
        # Count in MongoDB
        mongo_count = context.cross_db_steps.mongodb_steps.execute_mongodb_count(
            'test_migration_collection', env, {'id': migration_id}
        )
        
        assert sql_count == mongo_count, f"Record count mismatch: SQL={sql_count}, MongoDB={mongo_count}"
        logger.info(f"Record count verification passed: {sql_count} records in both databases")
        
        # Store counts for further analysis if needed
        context.migration_counts = {
            'sql_count': sql_count,
            'mongodb_count': mongo_count
        }
        
    except Exception as e:
        logger.error(f"Record count verification failed: {e}")
        raise


@when('I compare data between Oracle and MongoDB')
def step_compare_data_between_databases(context):
    """Compare data between Oracle and MongoDB databases."""
    if not hasattr(context, 'cross_db_steps'):
        context.cross_db_steps = CrossDatabaseSteps(context)
    
    try:
        env = getattr(context, 'current_env', 'DEV')
        
        # Get data from Oracle
        oracle_query = "SELECT customer_id, name, email, status FROM customers WHERE status = 'ACTIVE'"
        oracle_results = context.cross_db_steps.execute_sql_query(oracle_query, env, 'ORACLE')
        
        # Get data from MongoDB
        mongo_query = {'status': 'ACTIVE'}
        mongo_results = context.cross_db_steps.mongodb_steps.execute_mongodb_find(
            'customers', env, mongo_query
        )
        
        # Store results for comparison
        context.db_manager.store_result('sql_customers', oracle_results)
        context.db_manager.store_result('nosql_customers', mongo_results)
        
        logger.info(f"Retrieved {len(oracle_results)} records from Oracle and {len(mongo_results)} from MongoDB")
        
    except Exception as e:
        logger.error(f"Data comparison setup failed: {e}")
        raise


# Cleanup functions
@when('I clean up the migration test data')
@then('I clean up the migration test data')
def step_cleanup_migration_test_data(context):
    """Clean up migration test data from both databases."""
    if hasattr(context, 'cross_db_steps') and hasattr(context, 'test_migration_id'):
        env = getattr(context, 'current_env', 'DEV')
        context.cross_db_steps.cleanup_migration_test_data(context.test_migration_id, env)
        logger.info("Migration test data cleaned up successfully")


def cross_database_cleanup(context):
    """Clean up cross-database test resources."""
    try:
        if hasattr(context, 'cross_db_steps'):
            # Clean up any remaining migration test data
            if hasattr(context, 'test_migration_id'):
                env = getattr(context, 'current_env', 'DEV')
                context.cross_db_steps.cleanup_migration_test_data(context.test_migration_id, env)
            
            db_logger.debug("Cross-database cleanup completed")
            
    except Exception as e:
        db_logger.warning(f"Error during cross-database cleanup: {e}")


# Additional step for setting comparison tolerance
@given('I allow up to {max_discrepancies:d} data discrepancies')
def step_set_discrepancy_tolerance(context, max_discrepancies):
    """Set tolerance for data discrepancies in cross-database comparisons."""
    context.max_allowed_discrepancies = max_discrepancies
    logger.info(f"Set maximum allowed discrepancies to: {max_discrepancies}")


# Performance comparison step
@then('the migration should complete within {timeout:d} seconds')
def step_verify_migration_performance(context, timeout):
    """Verify that migration completed within specified time."""
    if not hasattr(context, 'migration_start_time'):
        logger.warning("No migration start time recorded")
        return
    
    migration_duration = time.time() - context.migration_start_time
    
    assert migration_duration <= timeout, \
        f"Migration took too long: {migration_duration:.2f}s > {timeout}s"
    
    logger.info(f"Migration performance check passed: {migration_duration:.2f}s <= {timeout}s")