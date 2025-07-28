# features/database/cross_database/data_sync_validation.feature

@database @mongodb @cross_database @integration
Feature: Cross-Database Data Synchronization Validation
    As a QA engineer
    I want to validate data synchronization between SQL and NoSQL databases
    So that I can ensure data consistency across different database types

    Background:
        Given I have a connection to "DEV" environment "ORACLE" database
        And I have a connection to "DEV" environment "MONGODB" database

    @integration @medium
    Scenario: Customer count comparison between Oracle and MongoDB
        When I execute query "SELECT COUNT(*) FROM customers" on "DEV" database
        And I store the result as "sql_customer_count"
        When I count documents in collection "customers"
        And I store the result as "nosql_customer_count"
        Then the "sql_customer_count" should equal "nosql_customer_count"

    @regression @slow
    Scenario: Data consistency check between databases
        When I execute query "SELECT customer_id, email FROM customers ORDER BY customer_id" on "DEV" database
        And I store the result as "sql_customers"
        When I query collection "customers" for all documents with fields "customer_id,email"
        And I store the result as "nosql_customers"
        Then the customer data should be consistent between databases

    @e2e @slow
    Scenario: Cross-platform data migration validation
        Given I have test data in Oracle database
        When I migrate the test data to MongoDB
        Then the data should be successfully migrated
        And the record count should match between databases
        And I clean up the migrated test data