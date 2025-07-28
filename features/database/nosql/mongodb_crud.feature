@database @mongodb @crud
Feature: MongoDB CRUD Operations
    As a QA engineer
    I want to test MongoDB CRUD operations
    So that I can validate data manipulation functionality

    Background:
        Given I have a connection to "DEV" environment "MONGODB" database

    @smoke @fast
    Scenario: Insert and retrieve document
        When I insert a test document in collection "test_data"
        And I query for the inserted document
        Then the document should be found
        And I clean up the test document

    @regression @medium
    Scenario: Update document operation
        Given I have a test document in collection "test_data"
        When I update the document with new data
        And I query for the updated document
        Then the document should contain the updated data
        And I clean up the test document

    @integration @medium
    Scenario: Aggregate data validation
        When I run aggregation pipeline on collection "orders"
        And I store the aggregation result as "order_summary"
        Then the "order_summary" should contain valid data