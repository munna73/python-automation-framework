Feature: MongoDB Database Operations
    As a test automation engineer
    I want to test MongoDB database operations
    So that I can validate MongoDB connectivity, document operations, and data integrity

    Background:
        Given I set the MongoDB configuration section to "S101_MONGODB"
        And I initialize the MongoDB connection

    @mongodb @smoke
    Scenario: Test MongoDB connection and list collections
        When I list all collections in the database
        Then the connection should be successful
        And the collections should include "test_collection"

    @mongodb @document_count
    Scenario: Count documents in MongoDB collection
        When I count documents in collection "users"
        Then the connection should be successful
        And the "count" should be greater than 0

    @mongodb @document_count @query
    Scenario: Count documents with query filter
        When I count documents in collection "users" with query "{"status": "active"}"
        Then the connection should be successful
        And the "count" should be greater than 0

    @mongodb @document_query
    Scenario: Query MongoDB collection for specific fields
        When I query collection "users" for all documents with fields "name, email, status"
        Then the connection should be successful
        And the document should be found

    @mongodb @document_search
    Scenario: Find documents by field value
        When I find documents in collection "users" where "status" equals "active"
        Then the connection should be successful
        And the document should be found

    @mongodb @document_insert
    Scenario: Insert test document into MongoDB collection
        When I insert a test document in collection "test_data"
        Then the connection should be successful
        And I retrieve the most recently inserted document
        And the document should be found

    @mongodb @document_operations
    Scenario: Complete document lifecycle - insert, update, retrieve
        Given I have a test document in collection "test_data"
        When I update the document with new data
        And I fetch the previously updated document
        Then the document should be found
        And the document should contain the updated data
        When I perform cleanup of test documents
        Then I should cleanup the test document successfully

    @mongodb @aggregation
    Scenario: Run aggregation pipeline on collection
        When I run aggregation pipeline on collection "users"
        Then the connection should be successful

    @mongodb @index_management
    Scenario: Create index on MongoDB collection
        When I create index on collection "test_data" for field "status"
        Then the connection should be successful

    @mongodb @collection_management
    Scenario: Collection management operations
        When I drop collection "temp_test_collection"
        Then the connection should be successful
        And the collection "temp_test_collection" should be empty

    @mongodb @document_validation
    Scenario: Validate document count in collection
        When I count documents in collection "users"
        Then the collection "users" should have 5 documents

    @mongodb @performance
    Scenario: Performance test - bulk document operations
        Given I have a test document in collection "performance_test"
        When I insert a test document in collection "performance_test"
        And I insert a test document in collection "performance_test"
        And I count documents in collection "performance_test"
        Then the collection "performance_test" should have 3 documents
        When I perform cleanup of test documents
        Then I should cleanup the test document successfully

    @mongodb @data_migration
    Scenario: Test data migration cleanup
        Given I have a test document in collection "migration_test"
        When I update the document with new data
        And I retrieve the most recently inserted document
        Then the document should be found
        And I should cleanup the migrated test data successfully

    @mongodb @legacy_support
    Scenario: Test legacy step definitions (backward compatibility)
        Given I have a test document in collection "legacy_test"
        When I query for the inserted document
        And I update the document with new data
        And I query for the updated document
        Then the document should be found
        And the document should contain the updated data

    @mongodb @error_handling
    Scenario: Test error handling with invalid collection
        When I count documents in collection "non_existent_collection"
        Then the connection should be successful

    @mongodb @field_types
    Scenario: Test different field value types
        When I find documents in collection "users" where "age" equals "25"
        And I find documents in collection "users" where "active" equals "true"
        And I find documents in collection "users" where "score" equals "85.5"
        Then the connection should be successful

    @mongodb @comprehensive
    Scenario: Comprehensive MongoDB operations test
        # Test connection and basic operations
        When I list all collections in the database
        Then the connection should be successful
        
        # Test document insertion and retrieval
        When I insert a test document in collection "comprehensive_test"
        And I retrieve the most recently inserted document
        Then the document should be found
        
        # Test document counting
        When I count documents in collection "comprehensive_test"
        Then the "count" should be greater than 0
        
        # Test querying with fields
        When I query collection "comprehensive_test" for all documents with fields "name, status"
        Then the document should be found
        
        # Test aggregation
        When I run aggregation pipeline on collection "comprehensive_test"
        Then the connection should be successful
        
        # Test index creation
        When I create index on collection "comprehensive_test" for field "name"
        Then the connection should be successful
        
        # Cleanup
        When I perform cleanup of test documents
        Then I should cleanup the test document successfully