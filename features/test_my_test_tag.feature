Feature: Test My Test Tag
  Testing that @my_test tag works with optimized configuration loading

  @my_test
  Scenario: Simple test that should not require API configuration
    Given I have a simple test setup
    When I run my test logic
    Then the test should pass without API_TOKEN validation

  @my_test @smoke  
  Scenario: Smoke test with my_test tag
    Given I have a basic test environment
    When I perform basic operations
    Then everything should work correctly