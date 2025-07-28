# steps/database/mongodb_steps.py

from behave import given, when, then
import time
from typing import Dict, List, Any
from steps.database.base_database_steps import BaseDatabaseSteps

# Import your existing modules (adjust as needed)
try:
    from db.mongodb_connector import mongodb_connector
    from utils.logger import logger
except ImportError as e:
    print(f"Import warning: {e}")
    print("Please adjust imports to match your existing module structure")


class MongoDBSteps(BaseDatabaseSteps):
    """MongoDB-specific database step definitions."""
    
    def __init__(self, context):
        super().__init__(context)
        self.test_documents = []  # Track test documents for cleanup
    
    def execute_mongodb_count(self, collection: str, env: str, query: Dict = None) -> int:
        """Count documents in MongoDB collection."""
        connection = self.get_connection(env, 'MONGODB')
        db = connection[mongodb_connector.get_database_name(env)]
        
        start_time = time.time()
        if query:
            count = db[collection].count_documents(query)
        else:
            count = db[collection].estimated_document_count()
        execution_time = time.time() - start_time
        
        logger.info(f"MongoDB count executed in {execution_time:.2f} seconds: {collection}")
        return count
    
    def execute_mongodb_find(self, collection: str, env: str, query: Dict = None, fields: List[str] = None) -> List[Dict]:
        """Find documents in MongoDB collection."""
        connection = self.get_connection(env, 'MONGODB')
        db = connection[mongodb_connector.get_database_name(env)]
        
        start_time = time.time()
        
        projection = None
        if fields:
            projection = {field: 1 for field in fields}
            projection['_id'] = 0  # Exclude _id unless specifically requested
        
        if query:
            cursor = db[collection].find(query, projection)
        else:
            cursor = db[collection].find({}, projection)
        
        results = list(cursor)
        execution_time = time.time() - start_time
        
        logger.info(f"MongoDB find executed in {execution_time:.2f} seconds: {collection}")
        return results
    
    def insert_test_document(self, collection: str, env: str, document: Dict) -> str:
        """Insert a test document and track it for cleanup."""
        connection = self.get_connection(env, 'MONGODB')
        db = connection[mongodb_connector.get_database_name(env)]
        
        # Add a test marker to identify test documents
        document['_test_marker'] = True
        document['_test_timestamp'] = time.time()
        
        result = db[collection].insert_one(document)
        doc_id = str(result.inserted_id)
        
        # Track for cleanup
        self.test_documents.append({
            'collection': collection,
            'env': env,
            'doc_id': result.inserted_id
        })
        
        logger.info(f"Inserted test document in {collection}: {doc_id}")
        return doc_id
    
    def cleanup_test_documents(self):
        """Clean up all test documents created during testing."""
        for doc_info in self.test_documents:
            try:
                connection = self.get_connection(doc_info['env'], 'MONGODB')
                db = connection[mongodb_connector.get_database_name(doc_info['env'])]
                
                db[doc_info['collection']].delete_one({'_id': doc_info['doc_id']})
                logger.debug(f"Cleaned up test document: {doc_info['doc_id']}")
            except Exception as e:
                logger.warning(f"Error cleaning up test document {doc_info['doc_id']}: {e}")
        
        self.test_documents.clear()


# MongoDB-specific step definitions
@when('I count documents in collection "{collection}"')
def step_count_mongodb_documents(context, collection):
    """Count documents in MongoDB collection."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    
    try:
        count = context.mongodb_steps.execute_mongodb_count(collection, env)
        context.last_query_results = [{'count': count}]
        logger.info(f"Document count in {collection}: {count}")
        
    except Exception as e:
        logger.error(f"MongoDB count failed: {e}")
        context.last_query_error = str(e)
        raise


@when('I count documents in collection "{collection}" with query "{query_str}"')
def step_count_mongodb_documents_with_query(context, collection, query_str):
    """Count documents in MongoDB collection with query."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    env = getattr(context, 'current_env', 'DEV')
    
    try:
        # Parse query string (basic JSON parsing)
        import json
        query = json.loads(query_str)
        
        count = context.mongodb_steps.execute_mongodb_count(collection, env, query)
        context.last_query_results = [{'count': count}]
        logger.info(f"Document count in {collection} with query: {count}")
        
    except Exception as e:
        logger.error(f"MongoDB count with query failed: {e}")
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
        connection = context.mongodb_steps.get_connection(env, 'MONGODB')
        db = connection[mongodb_connector.get_database_name(env)]
        
        collections = db.list_collection_names()
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
        logger.info(f"Inserted test document with ID: {doc_id}")
        
    except Exception as e:
        logger.error(f"Insert test document failed: {e}")
        context.last_query_error = str(e)
        raise


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
    if not hasattr(context, 'mongodb_steps'):
        raise ValueError("No MongoDB steps context available")
    
    try:
        actual_value = context.mongodb_steps.get_stored_result(key)
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


@when('I clean up the test document')
@then('I clean up the test document')
@then('I clean up the migrated test data')
def step_cleanup_test_documents(context):
    """Clean up test documents."""
    if hasattr(context, 'mongodb_steps'):
        context.mongodb_steps.cleanup_test_documents()
        logger.info("Test documents cleaned up successfully")


# Enhanced cleanup for environment.py
def mongodb_after_scenario(context, scenario):
    """Clean up MongoDB resources after each scenario."""
    if hasattr(context, 'mongodb_steps'):
        context.mongodb_steps.cleanup_test_documents()
        context.mongodb_steps.cleanup_connections()
        logger.debug("MongoDB resources cleaned up after scenario")

# Add these step definitions to your steps/database/mongodb_steps.py file

@when('I query for the inserted document')
def step_query_inserted_document(context):
    """Query for the last inserted test document."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    if not hasattr(context, 'last_inserted_doc_id'):
        raise ValueError("No inserted document ID available")
    
    env = getattr(context, 'current_env', 'DEV')
    
    try:
        # Find the document by its ObjectId
        from bson import ObjectId
        query = {'_id': ObjectId(context.last_inserted_doc_id)}
        
        # Use a default collection or the last used collection
        collection = getattr(context, 'last_used_collection', 'test_data')
        
        results = context.mongodb_steps.execute_mongodb_find(collection, env, query)
        context.last_query_results = results
        
        logger.info(f"Queried for inserted document, found {len(results)} documents")
        
    except Exception as e:
        logger.error(f"Query for inserted document failed: {e}")
        context.last_query_error = str(e)
        raise


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
        connection = context.mongodb_steps.get_connection(env, 'MONGODB')
        db = connection[mongodb_connector.get_database_name(env)]
        
        # Define the update data
        update_data = {
            'status': 'completed',
            'value': 250,
            'updated_date': time.time(),
            'tags': ['test', 'automation', 'updated']
        }
        
        # Perform the update
        from bson import ObjectId
        filter_query = {'_id': ObjectId(context.test_document_id)}
        update_query = {'$set': update_data}
        
        result = db[collection].update_one(filter_query, update_query)
        
        if result.modified_count == 1:
            context.update_data = update_data
            logger.info("Document updated successfully")
        else:
            raise AssertionError("Document was not updated")
            
    except Exception as e:
        logger.error(f"Document update failed: {e}")
        raise


@when('I query for the updated document')
def step_query_updated_document(context):
    """Query for the updated document."""
    if not hasattr(context, 'mongodb_steps'):
        context.mongodb_steps = MongoDBSteps(context)
    
    if not hasattr(context, 'test_document_id'):
        raise ValueError("No test document ID available")
    
    env = getattr(context, 'current_env', 'DEV')
    collection = getattr(context, 'last_used_collection', 'test_data')
    
    try:
        from bson import ObjectId
        query = {'_id': ObjectId(context.test_document_id)}
        
        results = context.mongodb_steps.execute_mongodb_find(collection, env, query)
        context.last_query_results = results
        
        logger.info(f"Queried for updated document, found {len(results)} documents")
        
    except Exception as e:
        logger.error(f"Query for updated document failed: {e}")
        context.last_query_error = str(e)
        raise


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


@when('I run aggregation pipeline on collection "{collection}"')
def step_run_aggregation_pipeline(context, collection):
    """Run an aggregation pipeline on the specified collection."""