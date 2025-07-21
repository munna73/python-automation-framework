Feature: REST API Testing
  As a QA engineer
  I want to test REST API endpoints
  So that I can validate API functionality

  @smoke @api
  Scenario: Test GET endpoint
    Given API base URL is configured
    When I send GET request to "/customers" endpoint
    Then response status code should be 200
    And response should contain valid JSON
    
  @regression @api
  Scenario: Test POST endpoint with validation
    Given API base URL is configured
    And request payload is loaded from "customer.json"
    When I send POST request to "/customers" endpoint
    Then response status code should be 201
    And response should match expected schema