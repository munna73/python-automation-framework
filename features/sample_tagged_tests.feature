Feature: Sample Tagged Tests
  Demonstrating multiple tag usage for command line execution

  @abctest @smoke @critical
  Scenario: ABC critical smoke test
    Given I have a test system configured
    When I execute ABC smoke tests
    Then all critical functions should work

  @abctest @regression @database
  Scenario: ABC database regression test
    Given I have ABC database configured  
    When I run ABC database regression tests
    Then all database operations should pass

  @abctest @performance @slow
  Scenario: ABC performance test
    Given I have ABC system under load
    When I measure ABC performance metrics
    Then performance should meet requirements

  @xyztest @smoke @fast
  Scenario: XYZ quick smoke test
    Given I have XYZ system ready
    When I run XYZ smoke tests
    Then XYZ should respond correctly

  @xyztest @regression @api
  Scenario: XYZ API regression test
    Given I have XYZ API configured
    When I test XYZ API endpoints
    Then all XYZ APIs should work properly

  @integration @abctest @xyztest
  Scenario: ABC and XYZ integration test
    Given I have both ABC and XYZ systems
    When I test ABC-XYZ integration
    Then integration should work seamlessly