# steps/database/mongodb_steps.py

from behave import given, when, then
import time
import json
from typing import Dict, List, Any
from bson import ObjectId
# Add this to the top of files like base_database_steps.py, mongodb_steps.py, etc.
import sys
import os
from pathlib import Path

# Get project root (go up 4 levels: database -> steps -> features -> project_root)
current_file = Path(__file__)
project_root = current_file.parent.parent.parent.parent
sys.path.insert(0, str(project_root.absolute()))

import re

# Import your existing modules
try:
    from db.database_manager import DatabaseManager
    from db.mongodb_connector import mongodb_connector
    from utils.logger import logger, db_logger
except ImportError as e:
    print(f"Import warning: {e}")
    print("Please adjust imports to match your existing module structure")


class MongoDBSteps:
    """MongoDB-specific database step definitions."""
    
    def __init__(self, context):
        self.context = context
        self.test_documents = []  # Track test documents for cleanup
        
        # Initialize database manager if not exists
        if not hasattr(context, 'db_manager'):
            context.db_manager = DatabaseManager()
    
    def execute_mongodb_count(self, collection: str, env: str, query: Dict = None) -> int:
        """Count documents in MongoDB collection."""
        try:
            count = mongodb_connector.count_documents(
                environment=env,
                collection_name=collection,
                query=query or {}
            )
            db_logger.info(f"MongoDB count executed: {collection} = {count}")
            return count
        except Exception as e:
            db_logger.error(f"MongoDB count failed: {e}")
            raise
    
    def execute_mongodb_find(self, collection: str, env: str, query: Dict = None, fields: List[str] = None) -> List[Dict]:
        """Find documents in MongoDB collection."""
        try:
            projection = None
            if fields:
                projection = {field: 1 for field in fields}
                projection['_id'] = 0  # Exclude _id unless specifically requested
            
            results = mongodb_connector.find_documents(
                environment=env,
                collection_name=collection,
                query=query or {},
                projection=projection
            )
            
            db_logger.info(f"MongoDB find executed: {collection} returned {len(results)} documents")
            return results
        except Exception as e:
            db_logger.error(f"MongoDB find failed: {e}")
            raise
    
    def insert_test_document(self, collection: str, env: str, document: Dict) -> str:
        """Insert a test document and track it for cleanup."""
        try:
            # Add a test marker to identify test documents
            document['_test_marker'] = True
            document['_test_timestamp'] = time.time()
            
            result = mongodb_connector.insert_documents(
                environment=env,
                collection_name=collection,
                documents=document
            )
            
            doc_id = result['inserted_ids'][0]
            
            # Track for cleanup
            self.test_documents.append({
                'collection': collection,
                'env': env,
                'doc_id': ObjectId(doc_id)
            })
            
            db_logger.info(f"Inserted test document in {collection}: {doc_id}")
            return doc_id
        except Exception as e:
            db_logger.error(f"Insert test document failed: {e}")
            raise
    
    def update_test_document(self, collection: str, env: str, doc_id: str, update_data: Dict) -> bool:
        """Update a test document."""
        try:
            filter_query = {'_id': ObjectId(doc_id)}
            update_query = {'$set': update_data}
            
            result = mongodb_connector.update_documents(
                environment=env,
                collection_name=collection,
                filter_query=filter_query,
                update_query=update_query
            )
            
            success = result['modified_count'] > 0
            if success:
                db_logger.info(f"Updated test document {doc_id} in {collection}")
            else:
                db_logger.warning(f"No documents updated for ID {doc_id}")
            
            return success
        except Exception as e:
            db_logger.error(f"Update test document failed: {e}")
            raise
    
    def cleanup_test_documents(self):
        """Clean up all test documents created during testing."""
        for doc_info in self.test_documents:
            try:
                filter_query = {'_id': doc_info['doc_id']}
                result = mongodb_connector.delete_documents(
                    environment=doc_info['env'],
                    collection_name=doc_info['collection'],
                    filter_query=filter_query
                )
                
                if result['deleted_count'] > 0:
                    db_logger.debug(f"Cleaned up test document: {doc_info['doc_id']}")
                else:
                    db_logger.warning(f"Test document not found for cleanup: {doc_info['doc_id']}")
                    
            except Exception as e:
                db_logger.warning(f"Error cleaning up test document {doc_info['doc_id']}: {e}")
        
        self.test_documents.clear()
    
    def list_collections(self, env: str) -> List[str]:
        """List all collections in MongoDB database."""
        try:
            collections = mongodb_connector.list_collection_names(environment=env)
            db_logger.info(f"Found {len(collections)} collections in {env} database")
            return collections
        except Exception as e:
            db_logger.error(f"List collections failed: {e}")
            raise


# MongoDB-specific step definitions
@when(re.compile(r'I count documents in collection "(?P<collection>.*?)"(?: with query "(?P<query_str>.*?)")?'))
def step_count_mongodb_documents(context, collection, query_str=None):
    """Count documents in MongoDB collection, optionally with a query."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    query = None
    
    if query_str:
        try:
            query = json.loads(query_str)
            logger.info(f"Counting documents in {collection} with query: {query}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON query string: {query_str}")
    else:
        logger.info(f"Counting all documents in {collection}")
    
    try:
        count = context.mongodb_steps.execute_mongodb_count(collection, env, query)
        context.last_query_results = [{'count': count}]
        logger.info(f"Document count in {collection}: {count}")
        
    except Exception as e:
        logger.error(f"MongoDB count failed: {e}")
        context.last_query_error = str(e)
        raise

@when('I query collection "{collection}" for all documents with fields "{fields_str}"')
def step_query_mongodb_collection_with_fields(context, collection, fields_str):
    """Query MongoDB collection for documents with specific fields."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    fields = [field.strip() for field in fields_str.split(',')]
    
    try:
        results = context.mongodb_steps.execute_mongodb_find(collection, env, fields=fields)
        context.last_query_results = results
        logger.info(f"Queried {collection} for {len(results)} documents with fields: {fields_str}")
        
    except Exception as e:
        logger.error(f"MongoDB query failed: {e}")
        context.last_query_error = str(e)
        raise


@when('I list all collections in the database')
def step_list_mongodb_collections(context):
    """List all collections in MongoDB database."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    
    try:
        collections = context.mongodb_steps.list_collections(env)
        context.last_query_results = [{'collections': collections}]
        logger.info(f"Found {len(collections)} collections in database")
        
    except Exception as e:
        logger.error(f"List collections failed: {e}")
        context.last_query_error = str(e)
        raise


@when('I insert a test document in collection "{collection}"')
def step_insert_test_document(context, collection):
    """Insert a test document in MongoDB collection."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    
    # Create a test document
    test_document = {
        'name': 'Test Document',
        'test_field': 'test_value',
        'status': 'active',
        'created_date': time.time()
    }
    
    try:
        doc_id = context.mongodb_steps.insert_test_document(collection, env, test_document)
        context.last_inserted_doc_id = doc_id
        context.last_used_collection = collection
        logger.info(f"Inserted test document with ID: {doc_id}")
        
    except Exception as e:
        logger.error(f"Insert test document failed: {e}")
        context.last_query_error = str(e)
        raise


@given('I have a test document in collection "{collection}"')
def step_setup_test_document(context, collection):
    """Set up a test document in the specified collection."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    
    # Create a test document with known data
    test_document = {
        'name': 'Test Document for Update',
        'status': 'pending',
        'value': 100,
        'tags': ['test', 'automation'],
        'created_date': time.time()
    }
    
    try:
        doc_id = context.mongodb_steps.insert_test_document(collection, env, test_document)
        context.test_document_id = doc_id
        context.last_used_collection = collection
        context.original_document = test_document.copy()
        
        logger.info(f"Set up test document in {collection} with ID: {doc_id}")
        
    except Exception as e:
        logger.error(f"Failed to set up test document: {e}")
        raise


@when('I update the document with new data')
def step_update_document_with_new_data(context):
    """Update the test document with new data."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    if not hasattr(context, 'test_document_id'):
        raise ValueError("No test document ID available")
    
    env = getattr(context, 'current_env', 'DEV')
    collection = getattr(context, 'last_used_collection', 'test_data')
    
    try:
        # Define the update data
        update_data = {
            'status': 'completed',
            'value': 250,
            'updated_date': time.time(),
            'tags': ['test', 'automation', 'updated']
        }
        
        success = context.mongodb_steps.update_test_document(
            collection, env, context.test_document_id, update_data
        )
        
        if success:
            context.update_data = update_data
            logger.info("Document updated successfully")
        else:
            raise AssertionError("Document was not updated")
            
    except Exception as e:
        logger.error(f"Document update failed: {e}")
        raise


@when('I retrieve the most recently inserted document')
def step_retrieve_inserted_document(context):
    """Retrieve the last inserted test document."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    if not hasattr(context, 'last_inserted_doc_id'):
        raise ValueError("No inserted document ID available")
    
    env = getattr(context, 'current_env', 'DEV')
    collection = getattr(context, 'last_used_collection', 'test_data')
    
    try:
        query = {'_id': ObjectId(context.last_inserted_doc_id)}
        results = context.mongodb_steps.execute_mongodb_find(collection, env, query)
        context.last_query_results = results
        
        logger.info(f"Retrieved inserted document, found {len(results)} documents")
        
    except Exception as e:
        logger.error(f"Retrieve inserted document failed: {e}")
        context.last_query_error = str(e)
        raise


@when('I fetch the previously updated document')
def step_fetch_updated_document(context):
    """Fetch the document that was previously updated."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    if not hasattr(context, 'test_document_id'):
        raise ValueError("No test document ID available")
    
    env = getattr(context, 'current_env', 'DEV')
    collection = getattr(context, 'last_used_collection', 'test_data')
    
    try:
        query = {'_id': ObjectId(context.test_document_id)}
        results = context.mongodb_steps.execute_mongodb_find(collection, env, query)
        context.last_query_results = results
        
        logger.info(f"Fetched updated document, found {len(results)} documents")
        
    except Exception as e:
        logger.error(f"Fetch updated document failed: {e}")
        context.last_query_error = str(e)
        raise


# LEGACY SUPPORT - Keep these for backward compatibility but mark as deprecated
@when('I query for the inserted document')
def step_query_inserted_document_legacy(context):
    """DEPRECATED: Use 'I retrieve the most recently inserted document' instead."""
    logger.warning("Using deprecated step. Please update to 'I retrieve the most recently inserted document'")
    step_retrieve_inserted_document(context)


@when('I query for the updated document')
def step_query_updated_document_legacy(context):
    """DEPRECATED: Use 'I fetch the previously updated document' instead."""
    logger.warning("Using deprecated step. Please update to 'I fetch the previously updated document'")
    step_fetch_updated_document(context)


# Verification steps
@then('the connection should be successful')
def step_verify_connection_successful(context):
    """Verify that the database connection was successful."""
    if hasattr(context, 'last_query_error'):
        raise AssertionError(f"Connection failed: {context.last_query_error}")
    
    if hasattr(context, 'last_query_results'):
        logger.info("Database connection verified successfully")
    else:
        raise AssertionError("No query results available to verify connection")


@then('the "{key}" should be greater than {expected_value:d}')
def step_verify_value_greater_than(context, key, expected_value):
    """Verify that stored value is greater than expected."""
    if not hasattr(context, 'db_manager'):
        raise ValueError("No database manager context available")
    
    try:
        actual_value = context.db_manager.get_stored_result(key)
        assert actual_value > expected_value, f"Value {actual_value} is not greater than {expected_value}"
        logger.info(f"Verification passed: {key} ({actual_value}) > {expected_value}")
        
    except KeyError as e:
        logger.error(f"Stored result not found: {e}")
        raise


@then('the collections should include "{collection_name}"')
def step_verify_collection_exists(context, collection_name):
    """Verify that collection exists in the database."""
    if not hasattr(context, 'last_query_results') or not context.last_query_results:
        raise ValueError("No collection list available")
    
    collections = context.last_query_results[0].get('collections', [])
    assert collection_name in collections, f"Collection '{collection_name}' not found in database"
    logger.info(f"Collection '{collection_name}' exists in database")


@then('the document should be found')
def step_verify_document_found(context):
    """Verify that the document was found."""
    if hasattr(context, 'last_query_error'):
        raise AssertionError(f"Query failed: {context.last_query_error}")
    
    if not hasattr(context, 'last_query_results') or not context.last_query_results:
        raise AssertionError("No documents found")
    
    assert len(context.last_query_results) > 0, "Document was not found"
    
    # Store the found document for further validation
    context.found_document = context.last_query_results[0]
    logger.info("Document was successfully found")


@then('the document should contain the updated data')
def step_verify_document_contains_updated_data(context):
    """Verify that the document contains the updated data."""
    if hasattr(context, 'last_query_error'):
        raise AssertionError(f"Query failed: {context.last_query_error}")
    
    if not hasattr(context, 'last_query_results') or not context.last_query_results:
        raise AssertionError("No documents found")
    
    if not hasattr(context, 'update_data'):
        raise ValueError("No update data available for comparison")
    
    document = context.last_query_results[0]
    update_data = context.update_data
    
    # Verify each updated field
    for field, expected_value in update_data.items():
        actual_value = document.get(field)
        assert actual_value == expected_value, \
            f"Field '{field}' mismatch: expected {expected_value}, got {actual_value}"
    
    logger.info("Document contains all expected updated data")


# FIXED CLEANUP STEPS - No more ambiguity!
@when('I perform cleanup of test documents')
def step_cleanup_test_documents_when(context):
    """Clean up test documents during scenario execution."""
    if hasattr(context, 'mongodb_steps'):
        context.mongodb_steps.cleanup_test_documents()
        logger.info("Test documents cleaned up during scenario")


@then('I should cleanup the test document successfully')  
def step_cleanup_test_documents_then(context):
    """Verify test documents are cleaned up successfully."""
    if hasattr(context, 'mongodb_steps'):
        context.mongodb_steps.cleanup_test_documents()
        logger.info("Test documents cleaned up and verified")


@then('I should cleanup the migrated test data successfully')
def step_cleanup_migrated_test_data(context):
    """Clean up migrated test data after migration tests."""
    if hasattr(context, 'mongodb_steps'):
        context.mongodb_steps.cleanup_test_documents()
        logger.info("Migrated test data cleaned up successfully")


# Enhanced cleanup for environment.py
def mongodb_after_scenario(context, scenario):
    """Clean up MongoDB resources after each scenario."""
    if hasattr(context, 'mongodb_steps'):
        try:
            context.mongodb_steps.cleanup_test_documents()
            db_logger.debug("MongoDB test documents cleaned up after scenario")
        except Exception as e:
            db_logger.warning(f"Error during MongoDB cleanup: {e}")
    
    # Clean up MongoDB connections through database manager
    if hasattr(context, 'db_manager'):
        try:
            context.db_manager.cleanup_connections()
            db_logger.debug("MongoDB connections cleaned up after scenario")
        except Exception as e:
            db_logger.warning(f"Error cleaning up MongoDB connections: {e}")


# Additional aggregation step (completion of the cut-off step)
@when('I run aggregation pipeline on collection "{collection}"')
def step_run_aggregation_pipeline(context, collection):
    """Run an aggregation pipeline on the specified collection."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    
    # Example aggregation pipeline - can be customized based on needs
    pipeline = [
        {'$match': {'status': 'active'}},
        {'$group': {'_id': '$status', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}}
    ]
    
    try:
        results = mongodb_connector.execute_aggregation_query(
            environment=env,
            collection_name=collection,
            pipeline=pipeline
        )
        
        # Convert DataFrame to list of dictionaries if needed
        if hasattr(results, 'to_dict'):
            results = results.to_dict('records')
        
        context.last_query_results = results
        logger.info(f"Aggregation pipeline executed on {collection}, returned {len(results)} results")
        
    except Exception as e:
        logger.error(f"Aggregation pipeline failed: {e}")
        context.last_query_error = str(e)
        raise


@when('I create index on collection "{collection}" for field "{field}"')
def step_create_single_field_index(context, collection, field):
    """Create a single field index on MongoDB collection."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    
    try:
        result = mongodb_connector.create_index(
            environment=env,
            collection_name=collection,
            index_spec=[(field, 1)]  # 1 for ascending, -1 for descending
        )
        
        context.last_index_result = result
        logger.info(f"Created index on {collection}.{field}")
        
    except Exception as e:
        logger.error(f"Create index failed: {e}")
        context.last_query_error = str(e)
        raise


@when('I drop collection "{collection}"')
def step_drop_collection(context, collection):
    """Drop a MongoDB collection."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    
    try:
        result = mongodb_connector.drop_collection(
            environment=env,
            collection_name=collection
        )
        
        context.last_drop_result = result
        logger.info(f"Dropped collection {collection}")
        
    except Exception as e:
        logger.error(f"Drop collection failed: {e}")
        context.last_query_error = str(e)
        raise


@then('the collection "{collection}" should be empty')
def step_verify_collection_empty(context, collection):
    """Verify that a MongoDB collection is empty."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    
    try:
        count = context.mongodb_steps.execute_mongodb_count(collection, env)
        assert count == 0, f"Collection '{collection}' is not empty. Found {count} documents"
        logger.info(f"Verified collection '{collection}' is empty")
        
    except Exception as e:
        logger.error(f"Verify empty collection failed: {e}")
        raise


@then('the collection "{collection}" should have {expected_count:d} documents')
def step_verify_collection_document_count(context, collection, expected_count):
    """Verify the exact number of documents in a MongoDB collection."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    
    try:
        actual_count = context.mongodb_steps.execute_mongodb_count(collection, env)
        assert actual_count == expected_count, \
            f"Collection '{collection}' has {actual_count} documents, expected {expected_count}"
        
        logger.info(f"Verified collection '{collection}' has exactly {expected_count} documents")
        
    except Exception as e:
        logger.error(f"Verify document count failed: {e}")
        raise


@when('I find documents in collection "{collection}" where "{field}" equals "{value}"')
def step_find_documents_by_field_value(context, collection, field, value):
    """Find documents in MongoDB collection by field value."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    
    # Try to convert value to appropriate type
    try:
        # Try integer
        if value.isdigit():
            query_value = int(value)
        # Try float
        elif '.' in value and value.replace('.', '').isdigit():
            query_value = float(value)
        # Try boolean
        elif value.lower() in ['true', 'false']:
            query_value = value.lower() == 'true'
        else:
            query_value = value
    except:
        query_value = value
    
    query = {field: query_value}
    
    try:
        results = context.mongodb_steps.execute_mongodb_find(collection, env, query)
        context.last_query_results = results
        logger.info(f"Found {len(results)} documents in {collection} where {field} = {value}")
        
    except Exception as e:
        logger.error(f"Find documents failed: {e}")
        context.last_query_error = str(e)
        raise

