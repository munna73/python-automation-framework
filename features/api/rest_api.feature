Feature: REST API Testing
  As a QA engineer
  I want to test REST API endpoints
  So that I can validate API functionality and performance

  Background:
    Given API base URL is configured
    And API timeout is set to 30 seconds

  @smoke @api @get
  Scenario: Test GET endpoint - Retrieve all customers
    When I send GET request to "/customers" endpoint
    Then response status code should be 200
    And response should contain valid JSON
    And response time should be less than 2000 milliseconds
    And response header "Content-Type" should contain "application/json"

  @smoke @api @get
  Scenario: Test GET endpoint with ID - Retrieve specific customer
    When I send GET request to "/customers/123" endpoint
    Then response status code should be 200
    And response should contain valid JSON
    And response should have required fields:
      | field_name |
      | id         |
      | name       |
      | email      |
      | status     |

  @regression @api @post
  Scenario: Test POST endpoint - Create new customer
    Given request headers:
      | header_name   | header_value     |
      | Content-Type  | application/json |
      | Accept        | application/json |
    And request payload is loaded from "customer.json"
    When I send POST request to "/customers" endpoint
    Then response status code should be 201
    And response should match expected schema
    And response should contain field "id" with type "string"
    And response header "Location" should be present
    And I store response field "id" as "created_customer_id"

  @regression @api @put
  Scenario: Test PUT endpoint - Update existing customer
    Given request payload:
      """
      {
        "name": "Updated Customer Name",
        "email": "updated@example.com",
        "phone": "+1234567890",
        "status": "active"
      }
      """
    When I send PUT request to "/customers/{created_customer_id}" endpoint
    Then response status code should be 200
    And response field "name" should equal "Updated Customer Name"
    And response field "email" should equal "updated@example.com"

  @regression @api @patch
  Scenario: Test PATCH endpoint - Partial update
    Given request payload:
      """
      {
        "status": "inactive"
      }
      """
    When I send PATCH request to "/customers/{created_customer_id}" endpoint
    Then response status code should be 200
    And response field "status" should equal "inactive"
    And response field "name" should equal "Updated Customer Name"

  @regression @api @delete
  Scenario: Test DELETE endpoint - Remove customer
    When I send DELETE request to "/customers/{created_customer_id}" endpoint
    Then response status code should be 204
    And response body should be empty

  @regression @api @get @negative
  Scenario: Test GET non-existent resource
    When I send GET request to "/customers/99999999" endpoint
    Then response status code should be 404
    And response should contain field "error" with value "Customer not found"

  @regression @api @post @negative
  Scenario: Test POST with invalid payload
    Given request payload:
      """
      {
        "email": "invalid-email-format"
      }
      """
    When I send POST request to "/customers" endpoint
    Then response status code should be 400
    And response should contain field "error"
    And response field "error" should contain "validation failed"

  @regression @api @auth
  Scenario: Test endpoint with authentication
    Given API authentication token "Bearer test-token-12345"
    When I send GET request to "/customers/profile" endpoint
    Then response status code should be 200
    And request should have header "Authorization" with value "Bearer test-token-12345"

  @regression @api @query
  Scenario: Test GET with query parameters
    Given query parameters:
      | param_name | param_value |
      | status     | active      |
      | limit      | 10          |
      | offset     | 0           |
    When I send GET request to "/customers" endpoint
    Then response status code should be 200
    And response should be JSON array
    And response array should have maximum 10 items
    And each item in response should have field "status" with value "active"

  @regression @api @pagination
  Scenario Outline: Test pagination
    Given query parameters:
      | param_name | param_value |
      | page       | <page>      |
      | size       | <size>      |
    When I send GET request to "/customers" endpoint
    Then response status code should be 200
    And response should have pagination metadata
    And response field "page" should equal <page>
    And response field "size" should equal <size>

    Examples:
      | page | size |
      | 1    | 20   |
      | 2    | 50   |
      | 3    | 100  |

  @regression @api @headers
  Scenario: Test custom headers
    Given request headers:
      | header_name      | header_value    |
      | X-API-Version    | v2              |
      | X-Client-ID      | test-client     |
      | Accept-Language  | en-US           |
    When I send GET request to "/customers" endpoint
    Then response status code should be 200
    And response header "X-API-Version" should equal "v2"

  @performance @api @load
  Scenario: Test API performance under load
    Given concurrent users count is 10
    When I send 100 GET requests to "/customers" endpoint
    Then average response time should be less than 500 milliseconds
    And 95th percentile response time should be less than 1000 milliseconds
    And all responses should have status code 200

  @integration @api @chain
  Scenario: Test API request chaining
    # Create a customer
    Given request payload is loaded from "customer.json"
    When I send POST request to "/customers" endpoint
    Then response status code should be 201
    And I store response field "id" as "customer_id"
    
    # Create an order for the customer
    Given request payload:
      """
      {
        "customer_id": "{customer_id}",
        "items": [
          {"product_id": "prod-123", "quantity": 2},
          {"product_id": "prod-456", "quantity": 1}
        ],
        "total_amount": 150.00
      }
      """
    When I send POST request to "/orders" endpoint
    Then response status code should be 201
    And I store response field "order_id" as "order_id"
    
    # Verify the order
    When I send GET request to "/orders/{order_id}" endpoint
    Then response status code should be 200
    And response field "customer_id" should equal stored value "customer_id"

@regression @api @file
Scenario: Test file upload endpoint
  Given request headers:
    | header_name | header_value     |
    | Accept      | application/json |
  When I upload file "test_data/photo.jpg" to "/customers/{created_customer_id}/photo" with form data
    | field_name | field_value      |
    | title      | Profile Photo    |
  Then response status code should be 200
  And response should contain field "file_url"


  @regression @api @async
  Scenario: Test asynchronous API endpoint
    Given request payload is loaded from "large_import.json"
    When I send POST request to "/customers/import" endpoint
    Then response status code should be 202
    And response should contain field "job_id"
    And I store response field "job_id" as "import_job_id"
    When I poll GET request to "/jobs/{import_job_id}" until status is "completed" with timeout 60 seconds
    Then response field "status" should equal "completed"
    And response field "processed_count" should be greater than 0

  @security @api @injection
  Scenario: Test SQL injection prevention
    Given request payload:
      """
      {
        "name": "Test'; DROP TABLE customers; --",
        "email": "test@example.com"
      }
      """
    When I send POST request to "/customers" endpoint
    Then response status code should be 201
    And response field "name" should equal "Test'; DROP TABLE customers; --"

  @contract @api @schema
  Scenario: Test API contract with detailed schema validation
    When I send GET request to "/customers/123" endpoint
    Then response status code should be 200
    And response should match JSON schema:
      """
      {
        "type": "object",
        "required": ["id", "name", "email", "created_at"],
        "properties": {
          "id": {"type": "string", "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"},
          "name": {"type": "string", "minLength": 1, "maxLength": 100},
          "email": {"type": "string", "format": "email"},
          "phone": {"type": ["string", "null"]},
          "status": {"type": "string", "enum": ["active", "inactive", "pending"]},
          "created_at": {"type": "string", "format": "date-time"},
          "updated_at": {"type": "string", "format": "date-time"}
        }
      }
      """

  @regression @api @retry
  Scenario: Test API retry mechanism
    Given API retry configuration:
      | max_retries | retry_delay | retry_status_codes |
      | 3          | 2           | 502,503,504        |
    When I send GET request to "/unstable-endpoint" endpoint
    Then response status code should be 200
    And request should have been retried if needed

  @regression @api @graphql
  Scenario: Test GraphQL endpoint
    Given GraphQL query:
      """
      query GetCustomer($id: ID!) {
        customer(id: $id) {
          id
          name
          email
          orders {
            id
            total_amount
            status
          }
        }
      }
      """
    And GraphQL variables:
      """
      {
        "id": "123"
      }
      """
    When I send GraphQL request to "/graphql" endpoint
    Then response status code should be 200
    And response field "data.customer.id" should equal "123"
    And response should not contain field "errors"