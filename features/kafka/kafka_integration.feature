Feature: Kafka Message Queue Integration
  As a test automation engineer
  I want to test Kafka message publishing and consuming
  So that I can validate message queue functionality

  Background:
    Given I have access to Kafka broker
    And I have configured Kafka connection parameters

  @kafka @smoke
  Scenario: Post single message to Kafka topic
    Given I have a message file "data/input/single_message.json"
    When I publish the message to Kafka topic "test-topic"
    Then the message should be successfully published
    And I should receive a confirmation with message offset

  @kafka @smoke
  Scenario: Post multiple messages to Kafka topic from file
    Given I have a message file "data/input/multiple_messages.json"
    When I publish all messages from file to Kafka topic "test-topic-batch"
    Then all messages should be successfully published
    And I should receive confirmations for all messages

  @kafka @consumer
  Scenario: Read messages from Kafka topic and write to file
    Given Kafka topic "test-topic" has messages available
    When I consume messages from Kafka topic "test-topic"
    And I write consumed messages to file "output/consumed_messages.txt"
    Then the output file should contain the consumed messages
    And each message should be on a separate line

  @kafka @roundtrip
  Scenario: Complete message roundtrip test
    Given I have a message file "data/input/roundtrip_messages.json"
    When I publish all messages from file to Kafka topic "roundtrip-topic"
    And I consume messages from Kafka topic "roundtrip-topic"
    And I write consumed messages to file "output/roundtrip_output.txt"
    Then the output file should contain all published messages
    And the message content should match the original input

  @kafka @consumer @timeout
  Scenario: Read messages with timeout
    Given Kafka topic "empty-topic" has no messages
    When I consume messages from Kafka topic "empty-topic" with timeout 5 seconds
    Then I should receive no messages within the timeout period
    And the consumer should handle the timeout gracefully

  @kafka @producer @error
  Scenario: Handle invalid topic publishing
    Given I have a message file "data/input/single_message.json"
    When I attempt to publish the message to invalid Kafka topic "invalid.topic.name"
    Then the publishing should fail with appropriate error
    And the error should be logged properly

  @kafka @consumer @error
  Scenario: Handle invalid topic consumption
    When I attempt to consume messages from invalid Kafka topic "invalid.topic.name"
    Then the consumption should fail with appropriate error
    And the error should be logged properly

  @kafka @configuration
  Scenario: Validate Kafka connection configuration
    When I validate Kafka broker connectivity
    Then the connection should be successful
    And I should be able to list available topics

  @kafka @performance
  Scenario: Publish large batch of messages
    Given I have a message file "data/input/large_batch_messages.json" with 1000 messages
    When I publish all messages from file to Kafka topic "performance-topic"
    Then all messages should be published within 30 seconds
    And I should track publishing performance metrics

  @kafka @serialization
  Scenario: Handle different message formats
    Given I have message files with different formats:
      | file_path                        | format |
      | data/input/json_messages.json    | json   |
      | data/input/text_messages.txt     | text   |
      | data/input/xml_messages.xml      | xml    |
    When I publish messages from each file to Kafka topic "format-topic"
    Then all messages should be published successfully
    And the message format should be preserved

  @kafka @partition
  Scenario: Publish messages to specific partition
    Given I have a message file "data/input/partitioned_messages.json"
    When I publish messages to Kafka topic "partitioned-topic" partition 2
    Then the messages should be published to the specified partition
    And I should verify the partition assignment

  @kafka @offset
  Scenario: Consume messages from specific offset
    Given Kafka topic "offset-topic" has messages at various offsets
    When I consume messages from Kafka topic "offset-topic" starting from offset 10
    Then I should receive messages from the specified offset onwards
    And earlier messages should be skipped

  @kafka @headers
  Scenario: Publish and consume messages with headers
    Given I have a message file "data/input/messages_with_headers.json"
    When I publish messages with custom headers to Kafka topic "headers-topic"
    And I consume messages from Kafka topic "headers-topic"
    Then the consumed messages should include the custom headers
    And header values should match the original headers

  @kafka @cleanup
  Scenario: Clean up test topics and messages
    Given I have created test topics during testing
    When I perform cleanup of test topics
    Then all test topics should be removed
    And test message files should be cleaned up