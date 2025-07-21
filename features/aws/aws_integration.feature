Feature: AWS Integration Testing
  As a QA engineer
  I want to test AWS SQS and S3 integration
  So that I can validate cloud messaging and file operations

  Background:
    Given AWS SQS connection is configured
    And AWS S3 connection is configured

  @smoke @aws @sqs
  Scenario: Test AWS SQS connection
    Given SQS queue URL is set to "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
    Then AWS SQS connection should be successful

  @smoke @aws @s3
  Scenario: Test AWS S3 connection
    Given S3 bucket is set to "test-automation-bucket"
    Then AWS S3 connection should be successful

  @regression @aws @sqs
  Scenario: Send message to SQS queue
    Given SQS queue URL is set to "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
    When I send message "Test message from automation framework" to SQS queue
    Then SQS message should be sent successfully

  @regression @aws @sqs @file
  Scenario: Send file to SQS line by line
    Given SQS queue URL is set to "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
    When I send file "test_data.txt" to SQS queue line by line
    Then SQS file should be sent with 10 successful messages

  @regression @aws @sqs @file
  Scenario: Send entire file as single SQS message
    Given SQS queue URL is set to "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
    When I send file "test_message.txt" to SQS queue as single message
    Then SQS file should be sent with 1 successful messages

  @regression @aws @sqs @fifo
  Scenario: Send message to FIFO queue with deduplication
    Given SQS queue URL is set to "https://sqs.us-east-1.amazonaws.com/123456789/test-queue.fifo"
    When I send message "FIFO test message with unique group ID" to SQS queue
    Then SQS message should be sent successfully

  @regression @aws @sqs @receive
  Scenario: Receive messages from SQS queue
    Given SQS queue URL is set to "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
    When I receive 5 messages from SQS queue
    Then SQS should receive 5 messages

  @integration @aws @s3 @download
  Scenario: Download single file from S3
    Given S3 bucket is set to "test-automation-bucket"
    When I download file "test-files/sample.txt" from S3 to "data/s3_downloads/sample.txt"
    Then S3 file download should be successful

  @integration @aws @s3 @download @directory
  Scenario: Download S3 directory to local directory
    Given S3 bucket is set to "test-automation-bucket"
    And S3 prefix is set to "incoming/"
    And local download directory is set to "data/s3_downloads/"
    When I download S3 directory to local directory
    Then S3 directory download should complete with 3 files

  @integration @aws @s3 @upload
  Scenario: Upload file to S3
    Given S3 bucket is set to "test-automation-bucket"
    When I upload file "data/test_data/customer.json" to S3 as "processed/customer_data.json"
    Then S3 upload should be successful

  @integration @aws @sql @sqs
  Scenario: Process SQS messages to SQL database
    Given AWS-SQL integration is configured
    And SQS queue URL is set to "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
    And message table "aws_sqs_messages" exists in "DEV" "POSTGRES" database
    When I process SQS queue to SQL database
    Then SQS messages should be saved to SQL successfully

  @integration @aws @sql @export
  Scenario: Export messages from SQL to file
    Given AWS-SQL integration is configured
    And message table "aws_sqs_messages" exists in "DEV" "POSTGRES" database
    When I export messages from SQL to file "output/exported_messages.txt"
    Then SQL message export should be successful
    And exported file should contain 5 messages

  @e2e @aws @sqs @sql @complete
  Scenario: Complete SQS to SQL to File workflow
    Given AWS-SQL integration is configured
    And SQS queue URL is set to "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
    And message table "aws_sqs_messages" exists in "DEV" "POSTGRES" database
    When I send file "test_data.txt" to SQS queue line by line
    And I process SQS queue to SQL database
    And I export messages from SQL to file "output/complete_workflow.txt"
    Then SQS file should be sent with 10 successful messages
    And SQS messages should be saved to SQL successfully
    And SQL message export should be successful

  @e2e @aws @s3 @sql @integration
  Scenario: S3 download and SQL processing workflow
    Given AWS-SQL integration is configured
    And S3 bucket is set to "test-automation-bucket"
    And S3 prefix is set to "incoming/"
    And local download directory is set to "data/s3_downloads/"
    And SQS queue URL is set to "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
    And message table "aws_sqs_messages" exists in "DEV" "POSTGRES" database
    When I download S3 directory to local directory
    And I send file "data/s3_downloads/messages.txt" to SQS queue line by line
    And I process SQS queue to SQL database
    Then S3 directory download should complete with 3 files
    And SQS messages should be saved to SQL successfully

  @performance @aws @sqs @bulk
  Scenario Outline: Bulk message processing performance
    Given SQS queue URL is set to "<queue_url>"
    And message table "aws_sqs_messages" exists in "DEV" "POSTGRES" database
    When I receive <message_count> messages from SQS queue
    And I save SQS messages to SQL database
    Then SQS should receive <message_count> messages
    And SQS messages should be saved to SQL successfully

    Examples:
      | queue_url                                                          | message_count |
      | https://sqs.us-east-1.amazonaws.com/123456789/test-queue          | 10           |
      | https://sqs.us-east-1.amazonaws.com/123456789/high-volume-queue   | 50           |
      | https://sqs.us-east-1.amazonaws.com/123456789/test-queue.fifo     | 20           |