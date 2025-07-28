@database @mongodb @smoke
Feature: MongoDB Connection and Basic Operations
    As a QA engineer
    I want to validate MongoDB connectivity and basic operations
    So that I can ensure NoSQL database functionality

    Background:
        Given I have a connection to "DEV" environment "MONGODB" database

    @critical @fast
    Scenario: MongoDB connectivity test
        When I count documents in collection "test_collection"
        Then the connection should be successful

    @smoke @fast
    Scenario: Basic document count
        When I count documents in collection "customers"
        And I store the result as "customer_count"
        Then the "customer_count" should be greater than 0

    @regression @medium
    Scenario: Collection existence validation
        When I list all collections in the database
        Then the collections should include "customers"
        And the collections should include "orders"