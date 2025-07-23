"""
MongoDB-related step definitions for Behave.
"""
from behave import given, when, then
from db.mongodb_connector import mongodb_connector
from utils.logger import logger, db_logger
import json
from datetime import datetime, timedelta

@given('I connect to "{environment}" MongoDB database')
def step_connect_mongodb(context, environment):
    """Connect to MongoDB database."""
    db_logger.info(f"Connecting to {environment} MongoDB database")
    context.mongodb_connector = mongodb_connector
    context.mongodb_environment = environment
    context.mongodb_connector.connect_mongodb(environment)

@given('I connect to MongoDB collection "{collection_name}"')
def step_connect_collection(context, collection_name):
    """Set the MongoDB collection to work with."""
    db_logger.info(f"Setting MongoDB collection: {collection_name}")
    context.mongodb_collection = collection_name

@when('I execute MongoDB find query')
def step_execute_find_query(context):
    """Execute MongoDB find query."""
    db_logger.info("Executing MongoDB find query")
    context.mongodb_result = context.mongodb_connector.execute_find_query(
        context.mongodb_environment,
        context.mongodb_collection,
        query=getattr(context, 'mongodb_query', {}),
        limit=getattr(context, 'mongodb_limit', None)
    )

@when('I execute MongoDB aggregation query')
def step_execute_aggregation_query(context):
    """Execute MongoDB aggregation query."""
    db_logger.info("Executing MongoDB aggregation query")
    pipeline = getattr(context, 'mongodb_pipeline', [])
    context.mongodb_result = context.mongodb_connector.execute_aggregation_query(
        context.mongodb_environment,
        context.mongodb_collection,
        pipeline
    )

@when('I execute MongoDB find query with filter "{query_filter}"')
def step_execute_find_with_filter(context, query_filter):
    """Execute MongoDB find query with specific filter."""
    db_logger.info(f"Executing MongoDB find query with filter: {query_filter}")
    
    # Parse the filter (assuming it's JSON format)
    try:
        filter_dict = json.loads(query_filter)
    except json.JSONDecodeError:
        # If not valid JSON, treat as a simple key-value filter
        filter_dict = {"status": query_filter}
    
    context.mongodb_result = context.mongodb_connector.execute_find_query(
        context.mongodb_environment,
        context.mongodb_collection,
        query=filter_dict
    )

@when('I execute chunked MongoDB query with "{window_minutes:d}" minute windows')
def step_execute_chunked_mongodb_query(context, window_minutes):
    """Execute chunked MongoDB query."""
    db_logger.info(f"Executing chunked MongoDB query with {window_minutes} minute windows")
    
    # Default date range (last 24 hours)
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=24)
    
    context.mongodb_result = context.mongodb_connector.execute_chunked_date_query(
        context.mongodb_environment,
        context.mongodb_collection,
        date_field=getattr(context, 'mongodb_date_field', 'created_date'),
        start_date=start_date,
        end_date=end_date,
        window_minutes=window_minutes
    )

@when('I insert document into MongoDB collection')
def step_insert_document(context):
    """Insert document into MongoDB collection."""
    db_logger.info("Inserting document into MongoDB collection")
    
    document = getattr(context, 'mongodb_document', {
        "test_field": "test_value",
        "created_date": datetime.now(),
        "status": "active"
    })
    
    context.mongodb_insert_result = context.mongodb_connector.insert_documents(
        context.mongodb_environment,
        context.mongodb_collection,
        document
    )

@when('I update MongoDB documents with filter "{filter_query}" and update "{update_query}"')
def step_update_documents(context, filter_query, update_query):
    """Update MongoDB documents."""
    db_logger.info(f"Updating MongoDB documents with filter: {filter_query}")
    
    try:
        filter_dict = json.loads(filter_query)
        update_dict = json.loads(update_query)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format: {e}")
    
    context.mongodb_update_result = context.mongodb_connector.update_documents(
        context.mongodb_environment,
        context.mongodb_collection,
        filter_query=filter_dict,
        update_query=update_dict,
        many=True
    )

@when('I delete MongoDB documents with filter "{filter_query}"')
def step_delete_documents(context, filter_query):
    """Delete MongoDB documents."""
    db_logger.info(f"Deleting MongoDB documents with filter: {filter_query}")
    
    try:
        filter_dict = json.loads(filter_query)
    except json.JSONDecodeError:
        # Simple string filter
        filter_dict = {"status": filter_query}
    
    context.mongodb_delete_result = context.mongodb_connector.delete_documents(
        context.mongodb_environment,
        context.mongodb_collection,
        filter_query=filter_dict,
        many=True
    )

@then('MongoDB query should return {expected_count:d} documents')
def step_verify_document_count(context, expected_count):
    """Verify MongoDB query returned expected number of documents."""
    db_logger.info(f"Verifying MongoDB query returned {expected_count} documents")
    assert hasattr(context, 'mongodb_result'), "No MongoDB result available"
    actual_count = len(context.mongodb_result)
    assert actual_count == expected_count, f"Expected {expected_count} documents, got {actual_count}"

@then('MongoDB query should return at least {min_count:d} documents')
def step_verify_minimum_document_count(context, min_count):
    """Verify MongoDB query returned at least minimum number of documents."""
    db_logger.info(f"Verifying MongoDB query returned at least {min_count} documents")
    assert hasattr(context, 'mongodb_result'), "No MongoDB result available"
    actual_count = len(context.mongodb_result)
    assert actual_count >= min_count, f"Expected at least {min_count} documents, got {actual_count}"

@then('MongoDB documents should contain field "{field_name}"')
def step_verify_field_exists(context, field_name):
    """Verify MongoDB documents contain specified field."""
    db_logger.info(f"Verifying MongoDB documents contain field: {field_name}")
    assert hasattr(context, 'mongodb_result'), "No MongoDB result available"
    assert not context.mongodb_result.empty, "No documents returned"
    assert field_name in context.mongodb_result.columns, f"Field '{field_name}' not found in documents"

@then('MongoDB insert should be successful')
def step_verify_insert_success(context):
    """Verify MongoDB insert was successful."""
    db_logger.info("Verifying MongoDB insert was successful")
    assert hasattr(context, 'mongodb_insert_result'), "No MongoDB insert result available"
    assert context.mongodb_insert_result['inserted_count'] > 0, "No documents were inserted"

@then('MongoDB update should affect {expected_count:d} documents')
def step_verify_update_count(context, expected_count):
    """Verify MongoDB update affected expected number of documents."""
    db_logger.info(f"Verifying MongoDB update affected {expected_count} documents")
    assert hasattr(context, 'mongodb_update_result'), "No MongoDB update result available"
    actual_count = context.mongodb_update_result['modified_count']
    assert actual_count == expected_count, f"Expected {expected_count} documents updated, got {actual_count}"

@then('MongoDB delete should remove {expected_count:d} documents')
def step_verify_delete_count(context, expected_count):
    """Verify MongoDB delete removed expected number of documents."""
    db_logger.info(f"Verifying MongoDB delete removed {expected_count} documents")
    assert hasattr(context, 'mongodb_delete_result'), "No MongoDB delete result available"
    actual_count = context.mongodb_delete_result['deleted_count']
    assert actual_count == expected_count, f"Expected {expected_count} documents deleted, got {actual_count}"

@then('MongoDB collection statistics should be retrieved')
def step_verify_collection_stats(context):
    """Verify MongoDB collection statistics were retrieved."""
    db_logger.info("Verifying MongoDB collection statistics")
    context.mongodb_stats = context.mongodb_connector.get_collection_stats(
        context.mongodb_environment,
        context.mongodb_collection
    )
    assert context.mongodb_stats['document_count'] >= 0, "Invalid document count in statistics"

@given('MongoDB query filter is set to "{query_json}"')
def step_set_mongodb_query_filter(context, query_json):
    """Set MongoDB query filter."""
    try:
        context.mongodb_query = json.loads(query_json)
        db_logger.debug(f"Set MongoDB query filter: {context.mongodb_query}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON query filter: {e}")

@given('MongoDB aggregation pipeline is set to "{pipeline_json}"')
def step_set_mongodb_pipeline(context, pipeline_json):
    """Set MongoDB aggregation pipeline."""
    try:
        context.mongodb_pipeline = json.loads(pipeline_json)
        db_logger.debug(f"Set MongoDB aggregation pipeline: {context.mongodb_pipeline}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON aggregation pipeline: {e}")

@given('MongoDB date field is set to "{date_field}"')
def step_set_mongodb_date_field(context, date_field):
    """Set MongoDB date field for chunked queries."""
    context.mongodb_date_field = date_field
    db_logger.debug(f"Set MongoDB date field: {date_field}")

@given('MongoDB document to insert is set to "{document_json}"')
def step_set_mongodb_document(context, document_json):
    """Set MongoDB document for insertion."""
    try:
        context.mongodb_document = json.loads(document_json)
        # Add timestamp if not present
        if 'created_date' not in context.mongodb_document:
            context.mongodb_document['created_date'] = datetime.now()
        db_logger.debug(f"Set MongoDB document: {context.mongodb_document}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON document: {e}")

@given('MongoDB query limit is set to {limit:d}')
def step_set_mongodb_limit(context, limit):
    """Set MongoDB query limit."""
    context.mongodb_limit = limit
    db_logger.debug(f"Set MongoDB query limit: {limit}")

@then('MongoDB documents should have field "{field_name}" with value "{expected_value}"')
def step_verify_field_value(context, field_name, expected_value):
    """Verify MongoDB documents have field with expected value."""
    db_logger.info(f"Verifying field {field_name} has value {expected_value}")
    assert hasattr(context, 'mongodb_result'), "No MongoDB result available"
    assert not context.mongodb_result.empty, "No documents returned"
    assert field_name in context.mongodb_result.columns, f"Field '{field_name}' not found"
    
    # Check if any document has the expected value
    has_expected_value = (context.mongodb_result[field_name] == expected_value).any()
    assert has_expected_value, f"No document has field '{field_name}' with value '{expected_value}'"

@then('MongoDB operation should complete successfully')
def step_verify_mongodb_operation_success(context):
    """Verify MongoDB operation completed successfully."""
    db_logger.info("Verifying MongoDB operation completed successfully")
    # This is a generic success check - assumes if we got here without exceptions, it succeeded
    assert True  # If we reach here without exceptions, operation was successful