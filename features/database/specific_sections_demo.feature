Feature: Specific Database Sections Demo
  Using specific Oracle and PostgreSQL section names without environments
  
  Background:
    Given I load configuration from "config.ini"

  @database @oracle @S101
  Scenario: Connect to S101 Oracle database directly
    When I connect to Oracle database using "S101_ORACLE" configuration
    Then Oracle connection should be established successfully

  @database @oracle @S102  
  Scenario: Connect to S102 Oracle database directly
    When I connect to Oracle database using "S102_ORACLE" configuration
    Then Oracle connection should be established successfully

  @database @postgres @P101
  Scenario: Connect to P101 PostgreSQL database directly
    When I connect to PostgreSQL database using "P101_POSTGRES" configuration
    Then PostgreSQL connection should be established successfully

  @database @postgres @P102
  Scenario: Connect to P102 PostgreSQL database directly  
    When I connect to PostgreSQL database using "P102_POSTGRES" configuration
    Then PostgreSQL connection should be established successfully

  @database @comparison @cross_system
  Scenario: Compare data between S101 Oracle and P101 PostgreSQL
    Given I connect to Oracle database using "S101_ORACLE" configuration
    And I connect to PostgreSQL database using "P101_POSTGRES" configuration
    When I read query from config section "QUERIES" key "customer_report"
    And I execute query on Oracle and store as source DataFrame
    And I execute query on PostgreSQL and store as target DataFrame
    Then I perform DataFrame comparison with primary key "id"
    And DataFrames should match within 0% tolerance

  @database @comparison @oracle_to_oracle
  Scenario: Compare data between two Oracle systems S101 and S102
    Given I connect to Oracle database using "S101_ORACLE" configuration as source
    And I connect to Oracle database using "S102_ORACLE" configuration as target
    When I read query from config section "QUERIES" key "sales_summary"
    And I execute query on source database and store as source DataFrame
    And I execute query on target database and store as target DataFrame
    Then I perform DataFrame comparison with primary key "product_id"
    And DataFrames should match within 5% tolerance

  @database @comparison @postgres_to_postgres
  Scenario: Compare data between two PostgreSQL systems P101 and P102
    Given I connect to PostgreSQL database using "P101_POSTGRES" configuration as source
    And I connect to PostgreSQL database using "P102_POSTGRES" configuration as target
    When I read query from config section "QUERIES" key "inventory_check"
    And I execute query on source database and store as source DataFrame
    And I execute query on target database and store as target DataFrame
    Then I perform DataFrame comparison with primary key "id"
    And DataFrames should match exactly

  @database @table_based @S101
  Scenario: Load data from S101 Oracle using table configuration
    Given I connect to Oracle database using "S101_ORACLE" configuration
    When I load source data using table from config section "S101_TABLES" key "customer_table" on Oracle
    Then source DataFrame should contain data
    And source DataFrame should have more than 0 records

  @database @table_based @P101
  Scenario: Load data from P101 PostgreSQL using table configuration
    Given I connect to PostgreSQL database using "P101_POSTGRES" configuration
    When I load target data using table from config section "P101_TABLES" key "customer_table" on PostgreSQL
    Then target DataFrame should contain data
    And target DataFrame should have more than 0 records

  @database @multi_system @comprehensive
  Scenario: Multi-system comprehensive test across all databases
    Given I connect to Oracle database using "S101_ORACLE" configuration
    And I connect to Oracle database using "S102_ORACLE" configuration as secondary
    And I connect to PostgreSQL database using "P101_POSTGRES" configuration
    And I connect to PostgreSQL database using "P102_POSTGRES" configuration as secondary
    When I execute direct query "SELECT COUNT(*) as record_count FROM dual" on Oracle as source
    And I execute direct query "SELECT COUNT(*) as record_count FROM information_schema.tables" on PostgreSQL as target
    Then source DataFrame should contain data
    And target DataFrame should contain data
    And both databases should be accessible