Feature: Enhanced Database Comparison and Data Validation
  As a data engineer
  I want to compare data between different databases with advanced features
  So that I can validate data migration and synchronization with comprehensive analysis

  Background:
    Given I load configuration from "config.ini"

  @database @oracle @postgres @enhanced
  Scenario: Enhanced Oracle to PostgreSQL comparison with performance monitoring
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I load source data using table from config section "comparison_settings" key "SRCE_TABLE" on Oracle
    And I load target data using table from config section "comparison_settings" key "TRGT_TABLE" on PostgreSQL
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key from config section "comparison_settings"
    Then I generate data quality report
    And data quality score should be above "90.0"
    And I print performance metrics
    And I export all results to Excel file "enhanced_oracle_to_postgres_comparison.xlsx"
    And I export all comparison results with timestamp
    And I save comparison results as JSON file "enhanced_comparison_results.json"

  @database @oracle @postgres @enhanced_comparison @quality_reporting
  Scenario: Enhanced comparison with omit columns, values, and quality reporting
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT emp_id, name, salary, last_updated, status, created_date FROM employees" on Oracle as source
    And I execute direct query "SELECT emp_id, name, salary, last_updated, status, created_date FROM employees" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "emp_id" omitting columns "last_updated,created_date" and values "N,None,NULL,---,INACTIVE,inactive"
    Then I generate data quality report
    And data quality score should be above "95.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @oracle @postgres @numeric_precision @enhanced
  Scenario: Enhanced numeric precision handling with performance tracking
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT id, amount, quantity, rate FROM transactions" on Oracle as source
    And I execute direct query "SELECT id, amount, quantity, rate FROM transactions" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "id"
    Then I generate data quality report
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @oracle @postgres @omit_columns_only @enhanced
  Scenario: Enhanced comparison omitting timestamp columns with quality validation
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT customer_id, name, email, status, created_date, modified_date FROM customers" on Oracle as source
    And I execute direct query "SELECT customer_id, name, email, status, created_date, modified_date FROM customers" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Then source DataFrame should have no duplicate records
    And target DataFrame should have no duplicate records
    When I compare DataFrames using primary key "customer_id" omitting columns "created_date,modified_date"
    Then I generate data quality report
    And data quality score should be above "85.0"
    And I print the comparison summary
    And I export all comparison results with timestamp

  @database @oracle @postgres @omit_values_only @enhanced
  Scenario: Enhanced comparison treating NULL variants as equal with comprehensive reporting
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT product_id, description, category, status FROM products" on Oracle as source
    And I execute direct query "SELECT product_id, description, category, status FROM products" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "product_id" omitting values "NULL,None,N/A,---,N,NONE,null,NaN"
    Then I generate data quality report
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @oracle @postgres @full_audit_comparison @enhanced
  Scenario: Enhanced full audit comparison with all advanced features
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I load source data using table from config section "comparison_settings" key "SRCE_TABLE" on Oracle
    And I load target data using table from config section "comparison_settings" key "TRGT_TABLE" on PostgreSQL
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Then source DataFrame should have no duplicate records
    And target DataFrame should have no duplicate records
    When I compare DataFrames using primary key from config section "comparison_settings" omitting columns "last_updated_timestamp,created_date,audit_user" and values "N,None,NULL,---,INACTIVE,inactive,SYSTEM"
    Then I generate data quality report
    And data quality score should be above "92.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp
    And I save comparison results as JSON file "enhanced_full_audit_results.json"

  @database @oracle @direct_query @enhanced
  Scenario: Enhanced Oracle to Oracle comparison with advanced validation
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT emp_id, first_name, last_name, salary, department FROM employees WHERE dept_id = 10" on Oracle as source
    And I execute direct query "SELECT emp_id, first_name, last_name, salary, department FROM employees_backup WHERE dept_id = 10" on Oracle as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "emp_id"
    Then I generate data quality report
    And data quality score should be above "98.0"
    And there should be no missing records in either DataFrame
    And all fields should match between source and target DataFrames
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @postgres @postgres @validation @enhanced
  Scenario: Enhanced PostgreSQL to PostgreSQL comparison with comprehensive data validation
    Given I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT customer_id, customer_name, email, registration_date, status FROM customers" on PostgreSQL as source
    And I execute direct query "SELECT customer_id, customer_name, email, registration_date, status FROM customers_replica" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Then source DataFrame should have no duplicate records
    And target DataFrame should have no duplicate records
    When I compare DataFrames using primary key "customer_id"
    Then I generate data quality report
    And data quality score should be above "95.0"
    And I print the comparison summary
    And I print DataFrame info for source
    And I print DataFrame info for target
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @oracle @postgres @expected_differences @enhanced
  Scenario: Enhanced comparison with expected differences and comprehensive validation
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT product_id, product_name, price, category, status FROM products WHERE status = 'ACTIVE'" on Oracle as source
    And I execute direct query "SELECT product_id, product_name, price, category, status FROM products WHERE status = 'ACTIVE'" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "product_id"
    Then the source DataFrame should have "150" records
    And the target DataFrame should have "148" records
    And there should be "2" records missing in target
    And there should be "0" records missing in source
    And field "price" should have "5" delta records
    And I generate data quality report
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @oracle @data_export @enhanced
  Scenario: Enhanced data extraction and export from Oracle with quality validation
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT order_id, customer_id, order_date, total_amount, status FROM orders WHERE order_date >= SYSDATE - 30" on Oracle as source
    And I validate data quality for source DataFrame
    Then source DataFrame should have no duplicate records
    And I print DataFrame info for source
    And I print performance metrics
    And I export source DataFrame to CSV "enhanced_oracle_orders_export.csv"

  @database @postgres @data_export @enhanced
  Scenario: Enhanced data extraction and export from PostgreSQL with comprehensive validation
    Given I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT user_id, username, email, created_at, status FROM users WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'" on PostgreSQL as source
    And I validate data quality for source DataFrame
    Then source DataFrame should have no duplicate records
    And I print DataFrame info for source
    And I print performance metrics
    And I export source DataFrame to CSV "enhanced_postgres_users_export.csv"

  @database @oracle @postgres @full_comparison @enhanced
  Scenario: Enhanced complete data migration validation with all export formats and quality reporting
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I read query from config section "queries" key "migration_source_query"
    And I execute query on Oracle and store as source DataFrame
    When I read query from config section "queries" key "migration_target_query"
    And I execute query on PostgreSQL and store as target DataFrame
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Then source DataFrame should have no duplicate records
    And target DataFrame should have no duplicate records
    When I compare DataFrames using primary key from config section "comparison_settings"
    Then I generate data quality report
    And data quality score should be above "90.0"
    And I print the comparison summary
    And I print DataFrame info for source
    And I print DataFrame info for target
    And I print performance metrics
    And I export all comparison results with timestamp
    And I save comparison results as JSON file "enhanced_migration_results.json"

  @database @oracle @postgres @large_dataset @enhanced @performance
  Scenario: Enhanced large dataset comparison with memory optimization and performance monitoring
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT transaction_id, account_id, transaction_type, amount, transaction_date, status FROM transactions WHERE transaction_date >= SYSDATE - 7" on Oracle as source
    And I execute direct query "SELECT transaction_id, account_id, transaction_type, amount, transaction_date, status FROM transactions WHERE transaction_date >= CURRENT_DATE - INTERVAL '7 days'" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Then source DataFrame should have no duplicate records
    And target DataFrame should have no duplicate records
    When I compare DataFrames using primary key "transaction_id" omitting columns "transaction_date" and values "NULL,None,---"
    Then I generate data quality report
    And data quality score should be above "88.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @oracle @oracle @same_db_comparison @enhanced
  Scenario: Enhanced comparison of tables within the same Oracle database
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT emp_id, first_name, last_name, department, salary, status FROM employees" on Oracle as source
    And I execute direct query "SELECT emp_id, first_name, last_name, department, salary, status FROM employees_archive" on Oracle as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "emp_id" omitting values "INACTIVE,inactive,NULL,None"
    Then I generate data quality report
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @postgres @postgres @same_db_comparison @enhanced
  Scenario: Enhanced comparison of tables within the same PostgreSQL database
    Given I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT product_id, product_name, category, price, stock_quantity, status FROM products" on PostgreSQL as source
    And I execute direct query "SELECT product_id, product_name, category, price, stock_quantity, status FROM products_staging" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "product_id"
    Then I generate data quality report
    And data quality score should be above "93.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @oracle @postgres @config_driven @enhanced
  Scenario: Enhanced configuration-driven comparison test with quality validation
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I load source data using table from config section "comparison_settings" key "SRCE_TABLE" on Oracle
    And I load target data using table from config section "comparison_settings" key "TRGT_TABLE" on PostgreSQL
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key from config section "comparison_settings"
    Then I generate data quality report
    And data quality score should be above "95.0"
    And there should be no missing records in either DataFrame
    And all fields should match between source and target DataFrames
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @oracle @postgres @data_quality @enhanced
  Scenario: Enhanced data quality validation and comparison with comprehensive reporting
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT customer_id, customer_name, email, phone, address, status FROM customer_master" on Oracle as source
    And I execute direct query "SELECT customer_id, customer_name, email, phone, address, status FROM customer_master" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Then source DataFrame should have no duplicate records
    And target DataFrame should have no duplicate records
    When I compare DataFrames using primary key "customer_id" omitting values "NULL,None,N/A,---"
    Then I generate data quality report
    And data quality score should be above "97.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp
    And I save comparison results as JSON file "enhanced_data_quality_results.json"

  @database @oracle @postgres @specific_validation @enhanced
  Scenario: Enhanced validation of specific field differences and counts with quality metrics
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT account_id, account_type, balance, status, last_updated FROM accounts" on Oracle as source
    And I execute direct query "SELECT account_id, account_type, balance, status, last_updated FROM accounts" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "account_id" omitting columns "last_updated"
    Then the source DataFrame should have "1000" records
    And the target DataFrame should have "995" records
    And there should be "5" records missing in target
    And there should be "0" records missing in source
    And field "balance" should have "0" delta records
    And field "status" should have "2" delta records
    And I generate data quality report
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @oracle @postgres @error_handling @enhanced
  Scenario: Enhanced error handling and edge cases with comprehensive validation
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT id, name, description, status FROM test_table WHERE id BETWEEN 1 AND 100" on Oracle as source
    And I execute direct query "SELECT id, name, description, status FROM test_table WHERE id BETWEEN 1 AND 100" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "id"
    Then I generate data quality report
    And I print the comparison summary
    And I print DataFrame info for source
    And I print DataFrame info for target
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @oracle @postgres @mixed_queries @enhanced
  Scenario: Enhanced mix of config queries and direct queries with performance tracking
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I read query from config section "queries" key "complex_source_query"
    And I execute query on Oracle and store as source DataFrame
    And I execute direct query "SELECT report_id, report_name, created_by, created_date, status FROM reports" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "report_id" omitting columns "created_date"
    Then I generate data quality report
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @oracle @postgres @clob_xml_handling @enhanced
  Scenario: Enhanced CLOB and XML data handling with quality validation
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT doc_id, title, xml_content, clob_data, status FROM documents WHERE doc_type = 'XML'" on Oracle as source
    And I execute direct query "SELECT doc_id, title, xml_content, clob_data, status FROM documents WHERE doc_type = 'XML'" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "doc_id" omitting values "NULL,None,---"
    Then I generate data quality report
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  # Enhanced scenarios for different environments
  @database @oracle @dev_environment @enhanced
  Scenario: Enhanced development environment comparison with quality metrics
    Given I load configuration from "config.ini"
    And I connect to Oracle database using "DEV_ORACLE" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT test_id, test_name, test_result, execution_date FROM dev_tests" on Oracle as source
    And I validate data quality for source DataFrame
    Then source DataFrame should have no duplicate records
    And I print DataFrame info for source
    And I print performance metrics
    And I export source DataFrame to CSV "enhanced_dev_test_data.csv"

  @database @postgres @qa_environment @enhanced
  Scenario: Enhanced QA environment comparison with comprehensive validation
    Given I load configuration from "config.ini"
    And I connect to PostgreSQL database using "QA_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT qa_id, test_case, result, executed_by, execution_date FROM qa_results" on PostgreSQL as source
    And I validate data quality for source DataFrame
    Then source DataFrame should have no duplicate records
    And I print DataFrame info for source
    And I print performance metrics
    And I export source DataFrame to CSV "enhanced_qa_test_results.csv"

  # New advanced scenarios leveraging enhanced features
  @database @oracle @postgres @advanced_quality @performance
  Scenario: Advanced data quality analysis with performance benchmarking
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT transaction_id, customer_id, amount, currency, transaction_date, status, created_by FROM financial_transactions" on Oracle as source
    And I execute direct query "SELECT transaction_id, customer_id, amount, currency, transaction_date, status, created_by FROM financial_transactions" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Then source DataFrame should have no duplicate records
    And target DataFrame should have no duplicate records
    When I compare DataFrames using primary key "transaction_id" omitting columns "created_by,transaction_date" and values "NULL,None,---,PENDING,pending"
    Then I generate data quality report
    And data quality score should be above "96.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp
    And I save comparison results as JSON file "advanced_quality_analysis.json"

  @database @oracle @postgres @memory_optimization @large_scale
  Scenario: Large-scale comparison with memory optimization and chunked processing
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT * FROM (SELECT order_id, customer_id, product_id, quantity, price, order_date FROM order_details WHERE order_date >= SYSDATE - 90) WHERE ROWNUM <= 100000" on Oracle as source
    And I execute direct query "SELECT order_id, customer_id, product_id, quantity, price, order_date FROM order_details WHERE order_date >= CURRENT_DATE - INTERVAL '90 days' LIMIT 100000" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "order_id" omitting columns "order_date"
    Then I generate data quality report
    And data quality score should be above "85.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @oracle @postgres @comprehensive_audit @compliance
  Scenario: Comprehensive audit trail comparison for compliance reporting
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I load source data using table from config section "audit_settings" key "AUDIT_SOURCE_TABLE" on Oracle
    And I load target data using table from config section "audit_settings" key "AUDIT_TARGET_TABLE" on PostgreSQL
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key from config section "audit_settings" omitting columns "audit_timestamp,audit_user,last_modified" and values "SYSTEM,AUTO,NULL,None,---"
    Then I generate data quality report
    And data quality score should be above "99.0"
    And there should be no missing records in either DataFrame
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp
    And I save comparison results as JSON file "compliance_audit_report.json"

  @database @oracle @postgres @real_time_validation @monitoring
  Scenario: Real-time data validation with continuous monitoring capabilities
    Given I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration
    When I enable progress monitoring
    And I execute direct query "SELECT sync_id, table_name, record_count, checksum, sync_timestamp FROM sync_status WHERE sync_timestamp >= SYSDATE - 1/24" on Oracle as source
    And I execute direct query "SELECT sync_id, table_name, record_count, checksum, sync_timestamp FROM sync_status WHERE sync_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "sync_id" omitting columns "sync_timestamp"
    Then I generate data quality report
    And data quality score should be above "98.0"
    And there should be no missing records in either DataFrame
    And all fields should match between source and target DataFrames
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

Feature: Enhanced Database Comparison with Advanced Omit Options
  As a senior data engineer
  I want to perform sophisticated data comparisons with flexible omit capabilities
  So that I can conduct precise data validation while ignoring irrelevant differences

  Background:
    Given I load configuration from "config.ini"
    And I connect to Oracle database using "SAT_ORACLE" configuration
    And I connect to PostgreSQL database using "SAT_POSTGRES" configuration

  @database @enhanced_basic_comparison @performance
  Scenario: Enhanced basic comparison with performance monitoring
    When I enable progress monitoring
    And I execute direct query "SELECT emp_id, name, salary, department, created_date FROM employees" on Oracle as source
    And I execute direct query "SELECT emp_id, name, salary, department, created_date FROM employees" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "emp_id"
    Then I generate data quality report
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @enhanced_omit_columns @quality_focus
  Scenario: Enhanced comparison omitting audit and timestamp columns with quality focus
    When I enable progress monitoring
    And I execute direct query "SELECT emp_id, name, salary, department, created_date, modified_date, created_by, modified_by FROM employees" on Oracle as source
    And I execute direct query "SELECT emp_id, name, salary, department, created_date, modified_date, created_by, modified_by FROM employees" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Then source DataFrame should have no duplicate records
    And target DataFrame should have no duplicate records
    When I compare DataFrames using primary key "emp_id" omitting columns "created_date,modified_date,created_by,modified_by"
    Then I generate data quality report
    And data quality score should be above "95.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @enhanced_omit_values @null_handling
  Scenario: Enhanced NULL and empty value handling with comprehensive validation
    When I enable progress monitoring
    And I execute direct query "SELECT product_id, name, description, status, category FROM products" on Oracle as source
    And I execute direct query "SELECT product_id, name, description, status, category FROM products" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "product_id" omitting values "NaN,---,None,NULL,null,N/A,na,NA,empty,EMPTY"
    Then I generate data quality report
    And data quality score should be above "90.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @enhanced_comprehensive_omit @production_ready
  Scenario: Production-ready comprehensive comparison with multiple omit strategies
    When I enable progress monitoring
    And I execute direct query "SELECT customer_id, name, email, phone, status, created_date, last_login, notes, internal_notes FROM customers" on Oracle as source
    And I execute direct query "SELECT customer_id, name, email, phone, status, created_date, last_login, notes, internal_notes FROM customers" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Then source DataFrame should have no duplicate records
    And target DataFrame should have no duplicate records
    When I compare DataFrames using primary key "customer_id" omitting columns "created_date,last_login,internal_notes" and values "NaN,---,None,NULL,N/A,INACTIVE,inactive,PENDING,pending"
    Then I generate data quality report
    And data quality score should be above "93.0"
    And there should be no missing records in either DataFrame
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp
    And I save comparison results as JSON file "production_comprehensive_results.json"

  @database @enhanced_audit_comparison @compliance
  Scenario: Enterprise audit comparison with compliance-focused omissions
    When I enable progress monitoring
    And I execute direct query "SELECT order_id, customer_id, amount, status, created_by, created_date, modified_by, modified_date, audit_trail FROM orders" on Oracle as source
    And I execute direct query "SELECT order_id, customer_id, amount, status, created_by, created_date, modified_by, modified_date, audit_trail FROM orders" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "order_id" omitting columns "created_by,created_date,modified_by,modified_date,audit_trail" and values "SYSTEM,AUTO,BATCH,---,NULL,None"
    Then I generate data quality report
    And data quality score should be above "97.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp
    And I save comparison results as JSON file "enterprise_audit_compliance.json"

  @database @enhanced_financial_precision @accuracy_critical
  Scenario: Financial data comparison with precision handling and accuracy validation
    When I enable progress monitoring
    And I execute direct query "SELECT account_id, balance, interest_rate, status, currency, last_calculated FROM accounts" on Oracle as source
    And I execute direct query "SELECT account_id, balance, interest_rate, status, currency, last_calculated FROM accounts" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    And I compare DataFrames using primary key "account_id" omitting columns "last_calculated" and values "0.0,0,---,inactive,INACTIVE,CLOSED,closed"
    Then I generate data quality report
    And data quality score should be above "99.0"
    And field "balance" should have "0" delta records
    And field "interest_rate" should have "0" delta records
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp
    And I save comparison results as JSON file "financial_precision_validation.json"

  @database @context_based_omit @new_functionality
  Scenario: Context-based omit parameters with basic comparison steps
    When I enable progress monitoring
    And I execute direct query "SELECT emp_id, name, salary, department, created_date, modified_date, status FROM employees" on Oracle as source
    And I execute direct query "SELECT emp_id, name, salary, department, created_date, modified_date, status FROM employees" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Given I set omit columns to "created_date,modified_date"
    And I set omit values to "INACTIVE,inactive,NULL,None"
    When I compare DataFrames using primary key "emp_id"
    Then I generate data quality report
    And data quality score should be above "95.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @comprehensive_omit @advanced_functionality
  Scenario: Comprehensive DataFrame comparison with both omit columns and values
    When I enable progress monitoring
    And I execute direct query "SELECT product_id, name, description, price, status, created_date, updated_date, notes FROM products" on Oracle as source
    And I execute direct query "SELECT product_id, name, description, price, status, created_date, updated_date, notes FROM products" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    When I perform comprehensive DataFrame comparison using primary key "product_id" with omitted columns "created_date,updated_date,notes" and omitted values "DISCONTINUED,discontinued,NULL,None,N/A,---"
    Then I generate data quality report
    And data quality score should be above "92.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @optional_omit_columns @flexible_usage
  Scenario: Optional omit columns with flexible parameter handling
    When I enable progress monitoring
    And I execute direct query "SELECT customer_id, name, email, phone, address, created_date, status FROM customers" on Oracle as source
    And I execute direct query "SELECT customer_id, name, email, phone, address, created_date, status FROM customers" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    When I compare DataFrames using primary key "customer_id" with optional omit columns "created_date"
    Then I generate data quality report
    And data quality score should be above "90.0"
    And I print the comparison summary
    And I print performance metrics

  @database @optional_omit_values @null_flexibility
  Scenario: Optional omit values with NULL handling flexibility
    When I enable progress monitoring
    And I execute direct query "SELECT order_id, customer_id, amount, status, notes FROM orders" on Oracle as source
    And I execute direct query "SELECT order_id, customer_id, amount, status, notes FROM orders" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    When I compare DataFrames using primary key "order_id" with optional omit values "CANCELLED,cancelled,NULL,None,---"
    Then I generate data quality report
    And data quality score should be above "88.0"
    And I print the comparison summary
    And I print performance metrics

  @database @context_management @parameter_control
  Scenario: Context parameter management with clear functionality
    When I enable progress monitoring
    And I execute direct query "SELECT transaction_id, account_id, amount, type, status, created_date FROM transactions" on Oracle as source
    And I execute direct query "SELECT transaction_id, account_id, amount, type, status, created_date FROM transactions" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Given I set omit columns to "created_date"
    And I set omit values to "PENDING,pending,FAILED,failed"
    When I compare DataFrames using primary key "transaction_id"
    Then I generate data quality report
    And data quality score should be above "93.0"
    Given I clear omit parameters
    When I compare DataFrames using primary key "transaction_id"
    Then I generate data quality report
    And I print the comparison summary
    And I print performance metrics

  @database @numeric_precision @type_normalization
  Scenario: Numeric precision handling with int/float normalization
    When I enable progress monitoring
    And I execute direct query "SELECT account_id, balance, credit_limit, interest_rate FROM accounts WHERE account_type = 'SAVINGS'" on Oracle as source
    And I execute direct query "SELECT account_id, balance, credit_limit, interest_rate FROM accounts WHERE account_type = 'SAVINGS'" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    When I compare DataFrames using primary key "account_id"
    Then I generate data quality report
    And data quality score should be above "98.0"
    And field "balance" should have "0" delta records
    And field "credit_limit" should have "0" delta records
    And field "interest_rate" should have "0" delta records
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @mixed_omit_strategies @production_workflow
  Scenario: Production workflow with mixed omit strategies and context switching
    When I enable progress monitoring
    And I execute direct query "SELECT user_id, username, email, status, last_login, created_date, profile_data FROM users" on Oracle as source
    And I execute direct query "SELECT user_id, username, email, status, last_login, created_date, profile_data FROM users" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    # First comparison with context-based omit
    Given I set omit columns to "last_login,created_date"
    When I compare DataFrames using primary key "user_id"
    Then I generate data quality report
    And data quality score should be above "94.0"
    # Clear and use different strategy
    Given I clear omit parameters
    When I compare DataFrames using primary key "user_id" with optional omit values "SUSPENDED,suspended,INACTIVE,inactive"
    Then I generate data quality report
    And data quality score should be above "92.0"
    # Use comprehensive approach
    When I perform comprehensive DataFrame comparison using primary key "user_id" with omitted columns "profile_data" and omitted values "NULL,None,---"
    Then I generate data quality report
    And data quality score should be above "96.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @perfect_match_verification @new_step_definitions
  Scenario: Perfect match verification with enhanced delta analysis
    When I enable progress monitoring
    And I execute direct query "SELECT id, name, amount FROM perfect_match_test WHERE id <= 10" on Oracle as source
    And I execute direct query "SELECT id, name, amount FROM perfect_match_test WHERE id <= 10" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    When I compare DataFrames using primary key "id"
    Then the comparison should show a perfect match
    And field match percentage should be 100.0%
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @field_differences_count @delta_verification
  Scenario: Field differences count verification with precise delta analysis
    When I enable progress monitoring
    And I execute direct query "SELECT emp_id, name, salary, status FROM employees_test WHERE emp_id <= 100" on Oracle as source
    And I execute direct query "SELECT emp_id, name, salary, status FROM employees_test_modified WHERE emp_id <= 100" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    When I compare DataFrames using primary key "emp_id"
    Then the comparison should show 5 field differences
    And field match percentage should be 75.0%
    And I print the comparison summary
    And I print performance metrics

  @database @enhanced_delta_analysis @comprehensive_verification
  Scenario: Comprehensive delta analysis with multiple verification steps
    When I enable progress monitoring
    And I execute direct query "SELECT product_id, name, price, category, status FROM products_comparison WHERE product_id <= 50" on Oracle as source
    And I execute direct query "SELECT product_id, name, price, category, status FROM products_comparison WHERE product_id <= 50" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Given I set omit values to "DISCONTINUED,discontinued,INACTIVE,inactive"
    When I compare DataFrames using primary key "product_id"
    Then the comparison should show 0 field differences
    And the comparison should show a perfect match
    And field match percentage should be 100.0%
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @partial_match_analysis @field_statistics
  Scenario: Partial match analysis with detailed field statistics
    When I enable progress monitoring
    And I execute direct query "SELECT account_id, balance, status, last_updated, created_date FROM accounts_analysis" on Oracle as source
    And I execute direct query "SELECT account_id, balance, status, last_updated, created_date FROM accounts_analysis_target" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    Given I set omit columns to "last_updated,created_date"
    When I compare DataFrames using primary key "account_id"
    Then the comparison should show 3 field differences
    And field match percentage should be 66.67%
    And I generate data quality report
    And data quality score should be above "85.0"
    And I print the comparison summary
    And I print performance metrics

  @database @perfect_match_with_omit @advanced_verification
  Scenario: Perfect match verification with omit parameters demonstrating enhanced analysis
    When I enable progress monitoring
    And I execute direct query "SELECT customer_id, name, email, phone, status, created_date, notes FROM customers_perfect_match" on Oracle as source
    And I execute direct query "SELECT customer_id, name, email, phone, status, created_date, notes FROM customers_perfect_match" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    When I perform comprehensive DataFrame comparison using primary key "customer_id" with omitted columns "created_date,notes" and omitted values "NULL,None,---,INACTIVE,inactive"
    Then the comparison should show a perfect match
    And field match percentage should be 100.0%
    And I generate data quality report
    And data quality score should be above "99.0%"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp

  @database @delta_analysis_edge_cases @boundary_testing
  Scenario: Delta analysis with edge cases and boundary conditions
    When I enable progress monitoring
    And I execute direct query "SELECT test_id, numeric_field, text_field, boolean_field FROM delta_edge_cases" on Oracle as source
    And I execute direct query "SELECT test_id, numeric_field, text_field, boolean_field FROM delta_edge_cases_modified" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    When I compare DataFrames using primary key "test_id"
    Then the comparison should show 2 field differences
    And field match percentage should be 50.0%
    And field "numeric_field" should have "0" delta records
    And field "text_field" should have "1" delta records  
    And field "boolean_field" should have "1" delta records
    And I print the comparison summary
    And I print performance metrics

  @database @zero_differences_validation @perfect_match_edge_case
  Scenario: Zero differences validation ensuring perfect match detection works correctly
    When I enable progress monitoring
    And I execute direct query "SELECT order_id, customer_id, amount, status FROM orders_identical WHERE order_date >= CURRENT_DATE - 7" on Oracle as source
    And I execute direct query "SELECT order_id, customer_id, amount, status FROM orders_identical WHERE order_date >= CURRENT_DATE - 7" on PostgreSQL as target
    And I validate data quality for source DataFrame
    And I validate data quality for target DataFrame
    When I compare DataFrames using primary key "order_id"
    Then the comparison should show 0 field differences
    And the comparison should show a perfect match  
    And field match percentage should be 100.0%
    And there should be no missing records in either DataFrame
    And all fields should match between source and target DataFrames
    And I generate data quality report
    And data quality score should be above "100.0"
    And I print the comparison summary
    And I print performance metrics
    And I export all comparison results with timestamp