# features/message_queue.feature
Feature: Message Queue Posting
  As a system developer,
  I want to verify that my application can reliably post messages to a message queue,
  so that I can ensure the data transfer layer is working correctly.

  Scenario: Post a single custom message
    Given MQ connection is configured
    When I post custom message "Hello, this is a test message."
    Then message should be posted successfully

  Scenario: Post a file as a single message
    Given MQ connection is configured
    When I post message from "test_data.txt" as single message
    Then message should be posted successfully

  Scenario: Post a file line by line and verify all lines are posted
    Given MQ connection is configured
    When I post message from "test_data.txt" line by line
    Then all lines should be posted successfully
    And success count should match file line count

  Scenario: Post a file and verify a specific success rate
    Given MQ connection is configured
    When I post message from "mixed_results.txt" line by line
    Then MQ posting should have 80% success rate

