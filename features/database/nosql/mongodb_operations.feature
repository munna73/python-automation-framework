Feature: MongoDB Database Operations
  As a QA engineer
  I want to test MongoDB database operations
  So that I can validate NoSQL data functionality

  @smoke @mongodb @database
  Scenario: Connect to MongoDB and query documents
    Given I connect to "DEV" MongoDB database
    And I connect to MongoDB collection "customers"
    When I execute MongoDB find query
    Then MongoDB operation should complete successfully
    
  @regression @mongodb @database
  Scenario: Query MongoDB documents with filter
    Given I connect to "DEV" MongoDB database
    And I connect to MongoDB collection "orders"
    And MongoDB query filter is set to '{"status": "active"}'
    When I execute MongoDB find query
    Then MongoDB documents should contain field "status"
    And MongoDB documents should have field "status" with value "active"
    
  @smoke @mongodb @database
  Scenario: Execute MongoDB aggregation query
    Given I connect to "DEV" MongoDB database
    And I connect to MongoDB collection "sales"
    And MongoDB aggregation pipeline is set to '[{"$group": {"_id": "$customer_id", "total": {"$sum": "$amount"}}}]'
    When I execute MongoDB aggregation query
    Then MongoDB operation should complete successfully
    And MongoDB documents should contain field "_id"
    
  @regression @mongodb @database
  Scenario: Execute chunked MongoDB query
    Given I connect to "DEV" MongoDB database
    And I connect to MongoDB collection "transactions"
    And MongoDB date field is set to "transaction_date"
    When I execute chunked MongoDB query with "30" minute windows
    Then MongoDB operation should complete successfully
    
  @mongodb @database @crud
  Scenario: Insert document into MongoDB collection
    Given I connect to "DEV" MongoDB database
    And I connect to MongoDB collection "test_collection"
    And MongoDB document to insert is set to '{"name": "Test User", "email": "test@example.com", "status": "active"}'
    When I insert document into MongoDB collection
    Then MongoDB insert should be successful
    
  @mongodb @database @crud
  Scenario: Update MongoDB documents
    Given I connect to "DEV" MongoDB database
    And I connect to MongoDB collection "users"
    When I update MongoDB documents with filter '{"status": "pending"}' and update '{"$set": {"status": "active"}}'
    Then MongoDB operation should complete successfully
    
  @mongodb @database @crud
  Scenario: Delete MongoDB documents
    Given I connect to "DEV" MongoDB database
    And I connect to MongoDB collection "temp_data"
    When I delete MongoDB documents with filter '{"status": "expired"}'
    Then MongoDB operation should complete successfully
    
  @mongodb @database @stats
  Scenario: Retrieve MongoDB collection statistics
    Given I connect to "DEV" MongoDB database
    And I connect to MongoDB collection "products"
    When I execute MongoDB find query
    Then MongoDB collection statistics should be retrieved
    
  @integration @mongodb @database
  Scenario Outline: Query different MongoDB collections
    Given I connect to "<environment>" MongoDB database
    And I connect to MongoDB collection "<collection>"
    And MongoDB query limit is set to <limit>
    When I execute MongoDB find query
    Then MongoDB query should return at least 0 documents
    
    Examples:
      | environment | collection | limit |
      | DEV         | customers  | 10    |
      | DEV         | orders     | 20    |
      | QA          | products   | 15    |