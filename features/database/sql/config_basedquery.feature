Feature: Database Query Configuration
  As a QA engineer
  I want to execute SQL queries from configuration files
  So that I can maintain queries centrally and compare data across environments

  Background:
    Given database connections are configured

  @config @query @smoke
  Scenario: Execute query from config and verify results
    Given I read SQL query "customer_count" from config section "DATABASE_QUERIES"
    When I execute the config query on "DEV" "POSTGRES" database
    Then the query result should have columns "environment, table_name, row_count"
    And the query result should have 5 rows

  @config @query @comparison
  Scenario: Compare customer data between environments using config queries
    # Set comparison parameters
    Given I set comparison key columns as "customer_id"
    And I set columns to exclude from comparison as "last_updated, created_date"
    
    # Execute same query on different environments
    Given I read SQL query "customer_comparison" from config section "DATABASE_QUERIES"
    When I execute the config query on "DEV" "POSTGRES" database and store as "dev_customers"
    And I execute the config query on "QA" "POSTGRES" database and store as "qa_customers"
    
    # Compare results
    Then I compare the query results between "dev_customers" and "qa_customers"
    And I should see no differences in the comparison

  @config @query @export
  Scenario: Export query results to file
    Given I read SQL query "active_customers" from config section "DATABASE_QUERIES"
    When I execute the config query on "PROD" "ORACLE" database
    And I export the query result to "output/exports/active_customers.csv" as "csv"
    Then the query result should have columns "customer_id, name, email, status"

  @config @query @cross_db
  Scenario: Compare data across different database types
    # Oracle query
    Given I have executed query "order_summary" from section "DATABASE_QUERIES" on "DEV" "ORACLE" database
    And I export the query result to "output/exports/oracle_orders.csv" as "csv"
    
    # PostgreSQL query  
    Given I have executed query "order_summary" from section "DATABASE_QUERIES" on "DEV" "POSTGRES" database
    And I export the query result to "output/exports/postgres_orders.csv" as "csv"
    
    # Compare
    When I execute the config query on "DEV" "ORACLE" database and store as "oracle_data"
    And I execute the config query on "DEV" "POSTGRES" database and store as "postgres_data"
    Then I compare the query results between "oracle_data" and "postgres_data"

  @config @query @dynamic
  Scenario Outline: Execute various queries from config
    Given I read SQL query "<query_key>" from config section "<section>"
    When I execute the config query on "<environment>" "<db_type>" database
    Then the query result should have <min_rows> rows

    Examples:
      | query_key          | section           | environment | db_type  | min_rows |
      | customer_count     | DATABASE_QUERIES  | DEV        | POSTGRES | 1        |
      | order_summary      | DATABASE_QUERIES  | QA         | ORACLE   | 10       |
      | inventory_check    | ANALYTICS_QUERIES | PROD       | POSTGRES | 50       |

  @config @query @validation
  Scenario: Validate query results against business rules
    Given I read SQL query "daily_sales_summary" from config section "REPORT_QUERIES"
    When I execute the config query on "PROD" "ORACLE" database
    Then the query result should have columns "date, total_sales, order_count"
    And I validate that "total_sales" is greater than 0 for all rows
    And I validate that "order_count" is not null for any row