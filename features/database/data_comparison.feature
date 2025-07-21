Feature: Database Data Comparison
  As a QA engineer
  I want to compare data between two databases
  So that I can identify discrepancies

  @smoke @database
  Scenario: Compare customer data between DEV and QA
    Given I connect to "DEV" Oracle database
    And I connect to "QA" Oracle database
    When I execute customer comparison query
    Then the data should match within acceptable thresholds
    And differences should be exported to Excel
    
  @regression @database
  Scenario Outline: Compare data with time windows
    Given I connect to "<source_env>" database
    And I connect to "<target_env>" database
    When I execute comparison query with window "<window_minutes>" minutes
    Then the results should be processed successfully
    And export should handle CLOB data properly
    
    Examples:
      | source_env | target_env | window_minutes |
      | DEV        | QA         | 10             |
      | QA         | PROD       | 5              |