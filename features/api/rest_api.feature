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

  @regression @api @security @auth
  Scenario: Test API endpoint with different authentication types
    # Test Bearer token authentication
    Given API authentication token "Bearer valid-token-123"
    When I send GET request to "/protected/profile" endpoint
    Then response status code should be 200
    And request should have header "Authorization" with value "Bearer valid-token-123"
    
    # Test API key authentication
    Given request headers:
      | header_name | header_value |
      | X-API-Key   | test-api-key |
    When I send GET request to "/protected/data" endpoint
    Then response status code should be 200

  @regression @api @error
  Scenario: Test comprehensive error handling
    # Test 400 Bad Request
    Given request payload:
      """
      {
        "invalid_field": "test"
      }
      """
    When I send POST request to "/customers" endpoint
    Then response status code should be 400
    And response should contain field "error"
    And response field "error" should contain "validation"
    
    # Test 401 Unauthorized
    When I send GET request to "/protected/admin" endpoint
    Then response status code should be 401
    And response should contain field "error"
    
    # Test 403 Forbidden
    Given API authentication token "Bearer limited-token"
    When I send DELETE request to "/admin/users/123" endpoint
    Then response status code should be 403
    
    # Test 404 Not Found
    When I send GET request to "/nonexistent/endpoint" endpoint
    Then response status code should be 404
    
    # Test 429 Rate Limit
    When I send 50 GET requests to "/rate-limited" endpoint
    Then response status code should be 429

  @regression @api @validation
  Scenario: Test comprehensive input validation
    # Test string length validation
    Given request payload:
      """
      {
        "name": "",
        "email": "test@example.com"
      }
      """
    When I send POST request to "/customers" endpoint
    Then response status code should be 400
    And response field "error" should contain "name"
    
    # Test email format validation
    Given request payload:
      """
      {
        "name": "Valid Name",
        "email": "invalid-email"
      }
      """
    When I send POST request to "/customers" endpoint
    Then response status code should be 400
    And response field "error" should contain "email"
    
    # Test required field validation
    Given request payload:
      """
      {
        "name": "Valid Name"
      }
      """
    When I send POST request to "/customers" endpoint
    Then response status code should be 400
    And response field "error" should contain "required"

  @performance @api @stress
  Scenario: Test API under stress conditions
    Given concurrent users count is 20
    When I send 200 GET requests to "/customers" endpoint
    Then average response time should be less than 1000 milliseconds
    And 95th percentile response time should be less than 2000 milliseconds
    And all responses should have status code 200

  @integration @api @data_flow
  Scenario: Test complete data flow through API
    # Create a customer
    Given request payload is loaded from "customer.json"
    When I send POST request to "/customers" endpoint
    Then response status code should be 201
    And I store response field "id" as "customer_id"
    
    # Update the customer
    Given request payload:
      """
      {
        "name": "Updated Name",
        "status": "premium"
      }
      """
    When I send PATCH request to "/customers/{customer_id}" endpoint
    Then response status code should be 200
    And response field "name" should equal "Updated Name"
    And response field "status" should equal "premium"
    
    # Create an order for the customer
    Given request payload:
      """
      {
        "customer_id": "{customer_id}",
        "items": [
          {"product_id": "prod-001", "quantity": 2, "price": 25.99},
          {"product_id": "prod-002", "quantity": 1, "price": 49.99}
        ],
        "total": 101.97
      }
      """
    When I send POST request to "/orders" endpoint
    Then response status code should be 201
    And I store response field "id" as "order_id"
    And response field "customer_id" should equal stored value "customer_id"
    
    # Get customer with orders
    When I send GET request to "/customers/{customer_id}/orders" endpoint
    Then response status code should be 200
    And response should be JSON array
    And response array should have maximum 10 items
    
    # Delete the order
    When I send DELETE request to "/orders/{order_id}" endpoint
    Then response status code should be 204
    
    # Delete the customer
    When I send DELETE request to "/customers/{customer_id}" endpoint
    Then response status code should be 204

  @regression @api @content_types
  Scenario: Test different content types
    # Test JSON content type
    Given request headers:
      | header_name  | header_value     |
      | Content-Type | application/json |
    And request payload:
      """
      {
        "name": "JSON Customer",
        "email": "json@example.com"
      }
      """
    When I send POST request to "/customers" endpoint
    Then response status code should be 201
    And response header "Content-Type" should contain "application/json"
    
    # Test XML content type (if supported)
    Given request headers:
      | header_name  | header_value    |
      | Content-Type | application/xml |
      | Accept       | application/xml |
    When I send GET request to "/customers" endpoint
    Then response status code should be 200
    And response header "Content-Type" should contain "xml"

  @regression @api @caching
  Scenario: Test API caching behavior
    # First request
    When I send GET request to "/customers/cached-data" endpoint
    Then response status code should be 200
    And response header "X-Cache-Status" should equal "MISS"
    
    # Second request should hit cache
    When I send GET request to "/customers/cached-data" endpoint
    Then response status code should be 200
    And response header "X-Cache-Status" should equal "HIT"
    And response time should be less than 100 milliseconds

  @regression @api @versioning
  Scenario: Test API versioning
    # Test v1 endpoint
    Given request headers:
      | header_name   | header_value |
      | Accept        | application/json |
      | API-Version   | v1           |
    When I send GET request to "/customers" endpoint
    Then response status code should be 200
    And response header "API-Version" should equal "v1"
    
    # Test v2 endpoint
    Given request headers:
      | header_name   | header_value |
      | Accept        | application/json |
      | API-Version   | v2           |
    When I send GET request to "/customers" endpoint
    Then response status code should be 200
    And response header "API-Version" should equal "v2"

  @regression @api @filtering_sorting
  Scenario: Test API filtering and sorting
    # Test filtering
    Given query parameters:
      | param_name | param_value |
      | status     | active      |
      | country    | USA         |
      | limit      | 5           |
    When I send GET request to "/customers" endpoint
    Then response status code should be 200
    And response should be JSON array
    And response array should have maximum 5 items
    And each item in response should have field "status" with value "active"
    
    # Test sorting
    Given query parameters:
      | param_name | param_value |
      | sort       | name        |
      | order      | desc        |
      | limit      | 10          |
    When I send GET request to "/customers" endpoint
    Then response status code should be 200
    And response should be JSON array
    And response array should have maximum 10 items

  @security @api @xss_prevention
  Scenario: Test XSS prevention
    Given request payload:
      """
      {
        "name": "<script>alert('xss')</script>",
        "email": "xss@example.com"
      }
      """
    When I send POST request to "/customers" endpoint
    Then response status code should be 201
    And response field "name" should not contain "<script>"
    And response field "name" should not contain "alert"

  @regression @api @bulk_operations
  Scenario: Test bulk operations
    # Bulk create
    Given request payload:
      """
      {
        "customers": [
          {"name": "Bulk Customer 1", "email": "bulk1@example.com"},
          {"name": "Bulk Customer 2", "email": "bulk2@example.com"},
          {"name": "Bulk Customer 3", "email": "bulk3@example.com"}
        ]
      }
      """
    When I send POST request to "/customers/bulk" endpoint
    Then response status code should be 201
    And response field "created_count" should equal "3"
    And response should contain field "customer_ids"
    
    # Bulk update
    Given request payload:
      """
      {
        "filter": {"status": "pending"},
        "update": {"status": "active"}
      }
      """
    When I send PATCH request to "/customers/bulk" endpoint
    Then response status code should be 200
    And response should contain field "updated_count"
    
    # Bulk delete
    Given request payload:
      """
      {
        "filter": {"email": {"contains": "@example.com"}}
      }
      """
    When I send DELETE request to "/customers/bulk" endpoint
    Then response status code should be 200
    And response should contain field "deleted_count"