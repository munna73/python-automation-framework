# features/database/sql/data_validation.feature

@database @sql @validation @smoke
Feature: SQL Database Data Validation
    As a QA engineer
    I want to validate data integrity in SQL databases
    So that I can ensure data quality across environments

    Background:
        Given I have a connection to "DEV" environment "ORACLE" database
        And I have a connection to "QA" environment "ORACLE" database

    @critical @fast
    Scenario: Validate record count between environments
        Given I execute query "SELECT COUNT(*) FROM customers" on "DEV" database
        And I store the result as "dev_count"
        When I execute query "SELECT COUNT(*) FROM customers" on "QA" database
        And I store the result as "qa_count"
        Then the "dev_count" should equal "qa_count"

    @regression @medium
    Scenario: Validate data integrity with business rules
        Given I have a table "orders" in "DEV" database
        When I validate that all records have required fields populated
            | field_name    | validation_rule |
            | order_id      | NOT_NULL        |
            | customer_id   | NOT_NULL        |
            | order_date    | VALID_DATE      |
            | total_amount  | POSITIVE_NUMBER |
        Then all validation rules should pass

    @integration @slow
    Scenario Outline: Cross-environment data comparison
        Given I have table "<table_name>" in both "DEV" and "QA" environments
        When I compare data between environments for table "<table_name>"
        Then the data should match with tolerance of "<tolerance>%"
        And any discrepancies should be logged to file "data_comparison_<table_name>.csv"

        Examples:
            | table_name | tolerance |
            | customers  | 0         |
            | orders     | 5         |
            | products   | 2         |

    @performance @medium
    Scenario: Database performance validation
        Given I have a connection to "DEV" environment "ORACLE" database
        When I execute performance test query "SELECT * FROM large_table WHERE indexed_column = 'value'"
        Then the query should complete within 5 seconds
        And the execution plan should use indexes efficiently

    @data-quality @regression
    Scenario: Data quality checks
        Given I have a connection to "DEV" environment "ORACLE" database
        When I run data quality checks on table "customers"
            | check_type        | column_name | expected_result |
            | duplicate_check   | customer_id | NO_DUPLICATES   |
            | null_check        | email       | NO_NULLS        |
            | format_check      | phone       | PHONE_FORMAT    |
            | range_check       | age         | 18_TO_120       |
        Then all data quality checks should pass
        And a data quality report should be generated

    @schema @validation
    Scenario: Schema validation between environments
        Given I have a connection to "DEV" environment "ORACLE" database
        And I have a connection to "QA" environment "ORACLE" database
        When I compare schema for table "customers" between environments
        Then the table structure should match exactly
        And indexes should be consistent
        And constraints should be identical