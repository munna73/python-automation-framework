Feature: Enhanced Database Testing
  As a QA engineer
  I want to test database operations with improved error handling and type safety
  So that I can ensure data integrity across environments

  Background:
    Given I connect to "DEV" "ORACLE" database
    And I connect to "QA" "ORACLE" database

  @database @smoke @fast
  Scenario: Basic query execution with row count validation
    When I execute query "SELECT * FROM customers WHERE status = 'ACTIVE'" on "DEV" "ORACLE" database
    Then the query result should have 150 rows
    And I close all database connections

  @database @regression
  Scenario: Query with expected data validation
    When I execute query "SELECT id, name, email FROM customers WHERE id IN (1, 2, 3)" on "DEV" "ORACLE" database
    Then the query result should contain
      | id | name          | email                    |
      | 1  | John Doe      | john.doe@example.com    |
      | 2  | Jane Smith    | jane.smith@example.com  |
      | 3  | Bob Johnson   | bob.johnson@example.com |

  @database @parameterized
  Scenario: Parameterized query execution
    When I execute parameterized query on "DEV" "ORACLE" database
      | status | created_date |
      | ACTIVE | 2024-01-01   |
      """
      SELECT COUNT(*) as count 
      FROM customers 
      WHERE status = :status 
      AND created_date >= TO_DATE(:created_date, 'YYYY-MM-DD')
      """
    Then the query result should have 1 rows

  @database @comparison @slow
  Scenario: Compare data between environments
    When I compare data between "DEV" and "QA" for "ORACLE" using query
      """
      SELECT 
        customer_id,
        order_id,
        product_name,
        quantity,
        total_amount
      FROM orders
      WHERE order_date >= SYSDATE - 30
      ORDER BY customer_id, order_id
      """
    Then the data comparison should show no differences

  @database @export
  Scenario: Export query results to multiple formats
    When I execute query from file "complex_report.sql" on "DEV" "ORACLE" database
    Then I export the query results to "CSV" file "report_2024.csv"
    And I export the query results to "Excel" file "report_2024.xlsx"
    And I export the query results to "JSON" file "report_2024.json"

  @database @validation @integration
  Scenario: Validate referential integrity
    When I execute query "SELECT customer_id FROM customers" on "DEV" "ORACLE" database
    And I execute query "SELECT DISTINCT customer_id FROM orders" on "DEV" "ORACLE" database
    Then all customer_ids in orders should exist in customers

  @database @postgres @multi-db
  Scenario: Cross-database type testing
    Given I connect to "DEV" "POSTGRES" database
    When I execute query "SELECT version()" on "DEV" "POSTGRES" database
    And I execute query "SELECT * FROM v$version" on "DEV" "ORACLE" database
    Then both databases should be accessible

  @database @error-handling
  Scenario: Handle query execution errors gracefully
    When I execute query "SELECT * FROM non_existent_table" on "DEV" "ORACLE" database
    Then the query should fail with error containing "table or view does not exist"

  @database @performance @slow
  Scenario: Query performance validation
    When I execute query "SELECT COUNT(*) FROM large_table" on "DEV" "ORACLE" database with 5 second timeout
    Then the query execution time should be less than 5 seconds

  @database @schema-validation
  Scenario: Validate table schema
    When I execute query "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = 'CUSTOMERS'" on "DEV" "ORACLE" database
    Then the query result should contain
      | column_name | data_type |
      | ID          | NUMBER    |
      | NAME        | VARCHAR2  |
      | EMAIL       | VARCHAR2  |
      | STATUS      | VARCHAR2  |
      | CREATED_DATE| DATE      |