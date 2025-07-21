Feature: IBM MQ Message Processing
  As a QA engineer
  I want to post messages to MQ queues
  So that I can test message processing

  @smoke @mq
  Scenario: Post single message to queue
    Given MQ connection is configured
    When I post message from "test_message.txt" as single message
    Then message should be posted successfully
    
  @regression @mq
  Scenario: Post file line by line
    Given MQ connection is configured
    When I post message from "test_data.txt" line by line
    Then all lines should be posted successfully
    And success count should match file line count