Feature: Demo HTML Report Testing
  As a test automation engineer
  I want to generate comprehensive HTML reports
  So that I can visualize test results effectively

  @demo @reporting
  Scenario: Successful test scenario
    Given I have a passing test step
    When I execute the test step
    Then the test should pass successfully

  @demo @reporting
  Scenario: Failed test scenario
    Given I have a failing test step
    When I execute the test step that will fail
    Then the test should fail with an error message

  @demo @reporting
  Scenario: Mixed test scenario
    Given I have multiple test steps
    When I execute a passing step
    And I execute another passing step
    Then all steps should complete
    But one step might have warnings