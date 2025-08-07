Feature: Database Comparison and Data Validation
  As a data engineer
  I want to compare data between different databases
  So that I can validate data migration and synchronization

  Background:
    Given I load configuration from "config.ini"

  @database @oracle @postgres
  Scenario: Basic Oracle to PostgreSQL comparison using config tables
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I load source data using table from config section "comparison_settings" key "SRCE_TABLE" on Oracle
    And I load target data using table from config section "comparison_settings" key "TRGT_TABLE" on PostgreSQL
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key from config section "comparison_settings"
    Then I print the comparison summary
    And I export all results to Excel file "oracle_to_postgres_comparison.xlsx"
    And I export comparison results to CSV file "detailed_comparison.csv"
    And I save comparison results as JSON file "comparison_results.json"

  @database @oracle @postgres @custom_query
  Scenario: Compare using custom queries from config
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I read query from config section "queries" key "source_query"
    And I execute query on Oracle and store as source DataFrame
    When I read query from config section "queries" key "target_query"
    And I execute query on PostgreSQL and store as target DataFrame
    And I compare DataFrames using primary key "id"
    Then I print the comparison summary
    And I export comparison summary to CSV file "query_comparison_summary.csv"

  @database @oracle @direct_query
  Scenario: Oracle to Oracle comparison with direct queries
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    When I execute direct query "SELECT emp_id, first_name, last_name, salary FROM employees WHERE dept_id = 10" on Oracle as source
    And I execute direct query "SELECT emp_id, first_name, last_name, salary FROM employees_backup WHERE dept_id = 10" on Oracle as target
    And I compare DataFrames using primary key "emp_id"
    Then there should be no missing records in either DataFrame
    And all fields should match between source and target DataFrames
    And I export all results to Excel file "oracle_oracle_comparison.xlsx"

  @database @postgres @postgres @validation
  Scenario: PostgreSQL to PostgreSQL comparison with data validation
    Given I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I execute direct query "SELECT customer_id, customer_name, email, registration_date FROM customers" on PostgreSQL as source
    And I execute direct query "SELECT customer_id, customer_name, email, registration_date FROM customers_replica" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Then source DataFrame should have no duplicate records
    And target DataFrame should have no duplicate records
    When I compare DataFrames using primary key "customer_id"
    Then I print the comparison summary
    And I print DataFrame info for source
    And I print DataFrame info for target

  @database @oracle @postgres @expected_differences
  Scenario: Compare with expected differences and verify counts
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I execute direct query "SELECT product_id, product_name, price, category FROM products WHERE status = 'ACTIVE'" on Oracle as source
    And I execute direct query "SELECT product_id, product_name, price, category FROM products WHERE status = 'ACTIVE'" on PostgreSQL as target
    And I compare DataFrames using primary key "product_id"
    Then the source DataFrame should have "150" records
    And the target DataFrame should have "148" records
    And there should be "2" records missing in target
    And there should be "0" records missing in source
    And field "price" should have "5" delta records
    And I export comparison results to CSV file "expected_differences.csv"

  @database @oracle @data_export
  Scenario: Data extraction and export from Oracle
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    When I execute direct query "SELECT order_id, customer_id, order_date, total_amount FROM orders WHERE order_date >= SYSDATE - 30" on Oracle as source
    And I validate data quality for source DataFrame
    Then I print DataFrame info for source
    And I export source DataFrame to CSV "oracle_orders_export.csv"

  @database @postgres @data_export
  Scenario: Data extraction and export from PostgreSQL
    Given I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I execute direct query "SELECT user_id, username, email, created_at FROM users WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'" on PostgreSQL as source
    And I validate data quality for source DataFrame
    Then source DataFrame should have no duplicate records
    And I print DataFrame info for source
    And I export source DataFrame to CSV "postgres_users_export.csv"

  @database @oracle @postgres @full_comparison
  Scenario: Complete data migration validation with all export formats
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I read query from config section "queries" key "migration_source_query"
    And I execute query on Oracle and store as source DataFrame
    When I read query from config section "queries" key "migration_target_query"
    And I execute query on PostgreSQL and store as target DataFrame
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key from config section "comparison_settings"
    Then I print the comparison summary
    And I print DataFrame info for source
    And I print DataFrame info for target
    And I export all results to Excel file "migration_validation_complete.xlsx"
    And I export comparison results to CSV file "migration_detailed_results.csv"
    And I export comparison summary to CSV file "migration_summary.csv"
    And I save comparison results as JSON file "migration_results.json"

  @database @oracle @postgres @large_dataset
  Scenario: Compare large datasets with memory optimization
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I execute direct query "SELECT transaction_id, account_id, transaction_type, amount, transaction_date FROM transactions WHERE transaction_date >= SYSDATE - 7" on Oracle as source
    And I execute direct query "SELECT transaction_id, account_id, transaction_type, amount, transaction_date FROM transactions WHERE transaction_date >= CURRENT_DATE - INTERVAL '7 days'" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Then source DataFrame should have no duplicate records
    And target DataFrame should have no duplicate records
    When I compare DataFrames using primary key "transaction_id"
    Then I print the comparison summary
    And I export all results to Excel file "large_dataset_comparison.xlsx"

  @database @oracle @oracle @same_db_comparison
  Scenario: Compare tables within the same Oracle database
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    When I execute direct query "SELECT emp_id, first_name, last_name, department, salary FROM employees" on Oracle as source
    And I execute direct query "SELECT emp_id, first_name, last_name, department, salary FROM employees_archive" on Oracle as target
    And I compare DataFrames using primary key "emp_id"
    Then I print the comparison summary
    And I export comparison results to CSV file "oracle_employee_comparison.csv"

  @database @postgres @postgres @same_db_comparison
  Scenario: Compare tables within the same PostgreSQL database
    Given I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I execute direct query "SELECT product_id, product_name, category, price, stock_quantity FROM products" on PostgreSQL as source
    And I execute direct query "SELECT product_id, product_name, category, price, stock_quantity FROM products_staging" on PostgreSQL as target
    And I compare DataFrames using primary key "product_id"
    Then I print the comparison summary
    And I export all results to Excel file "postgres_product_comparison.xlsx"

  @database @oracle @postgres @config_driven
  Scenario: Configuration-driven comparison test
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I load source data using table from config section "comparison_settings" key "SRCE_TABLE" on Oracle
    And I load target data using table from config section "comparison_settings" key "TRGT_TABLE" on PostgreSQL
    And I compare DataFrames using primary key from config section "comparison_settings"
    Then there should be no missing records in either DataFrame
    And all fields should match between source and target DataFrames
    And I export all results to Excel file "config_driven_comparison.xlsx"

  @database @oracle @postgres @data_quality
  Scenario: Data quality validation and comparison
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I execute direct query "SELECT customer_id, customer_name, email, phone, address FROM customer_master" on Oracle as source
    And I execute direct query "SELECT customer_id, customer_name, email, phone, address FROM customer_master" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Then source DataFrame should have no duplicate records
    And target DataFrame should have no duplicate records
    When I compare DataFrames using primary key "customer_id"
    Then I print the comparison summary
    And I export all results to Excel file "data_quality_comparison.xlsx"
    And I save comparison results as JSON file "data_quality_results.json"

  @database @oracle @postgres @specific_validation
  Scenario: Validate specific field differences and counts
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I execute direct query "SELECT account_id, account_type, balance, status, last_updated FROM accounts" on Oracle as source
    And I execute direct query "SELECT account_id, account_type, balance, status, last_updated FROM accounts" on PostgreSQL as target
    And I compare DataFrames using primary key "account_id"
    Then the source DataFrame should have "1000" records
    And the target DataFrame should have "995" records
    And there should be "5" records missing in target
    And there should be "0" records missing in source
    And field "balance" should have "0" delta records
    And field "status" should have "2" delta records
    And I export comparison results to CSV file "specific_validation_results.csv"

  @database @oracle @postgres @error_handling
  Scenario: Test error handling and edge cases
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I execute direct query "SELECT id, name, description FROM test_table WHERE id BETWEEN 1 AND 100" on Oracle as source
    And I execute direct query "SELECT id, name, description FROM test_table WHERE id BETWEEN 1 AND 100" on PostgreSQL as target
    And I compare DataFrames using primary key "id"
    Then I print the comparison summary
    And I print DataFrame info for source
    And I print DataFrame info for target
    And I export all results to Excel file "error_handling_test.xlsx"

  @database @oracle @postgres @mixed_queries
  Scenario: Mix of config queries and direct queries
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I read query from config section "queries" key "complex_source_query"
    And I execute query on Oracle and store as source DataFrame
    And I execute direct query "SELECT report_id, report_name, created_by, created_date FROM reports" on PostgreSQL as target
    And I compare DataFrames using primary key "report_id"
    Then I print the comparison summary
    And I export comparison summary to CSV file "mixed_queries_summary.csv"

  @database @oracle @postgres @clob_xml_handling
  Scenario: Test CLOB and XML data handling
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I execute direct query "SELECT doc_id, title, xml_content, clob_data FROM documents WHERE doc_type = 'XML'" on Oracle as source
    And I execute direct query "SELECT doc_id, title, xml_content, clob_data FROM documents WHERE doc_type = 'XML'" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "doc_id"
    Then I print the comparison summary
    And I export all results to Excel file "clob_xml_comparison.xlsx"

  # Scenarios for different environments
  @database @oracle @dev_environment
  Scenario: Development environment comparison
    Given I load configuration from "config.ini"
    And I connect to Oracle database using "DEV_ORACLE" configuration
    When I execute direct query "SELECT test_id, test_name, test_result FROM dev_tests" on Oracle as source
    And I validate data quality for source DataFrame
    Then I print DataFrame info for source
    And I export source DataFrame to CSV "dev_test_data.csv"

  @database @postgres @qa_environment  
  Scenario: QA environment comparison
    Given I load configuration from "config.ini"
    And I connect to PostgreSQL database using "QA_POSTGRES" configuration
    When I execute direct query "SELECT qa_id, test_case, result, executed_by FROM qa_results" on PostgreSQL as source
    And I validate data quality for source DataFrame
    Then I print DataFrame info for source
    And I export source DataFrame to CSV "qa_test_results.csv"