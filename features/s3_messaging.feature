Feature: S3 Message-Style Operations
    As a test automation engineer
    I want to use S3 for message-style operations
    So that I can store, retrieve, and process data as messages in S3 objects

    Background:
        Given AWS S3 connection is configured
        And S3 bucket is set to "test-messaging-bucket"
        And S3 prefix is set to "messages/test"

    @s3 @messaging @line_by_line
    Scenario: Send file to S3 line by line and retrieve as messages
        When I send file "test_data/sample_messages.txt" to S3 prefix "messages/upload" line by line
        Then S3 file upload should be successful with 5 objects
        When I retrieve S3 objects from prefix "messages/upload" and write to file "output/retrieved_messages.txt" line by line
        Then S3 message retrieval should be successful
        And S3 should retrieve 5 messages to file

    @s3 @messaging @whole_file
    Scenario: Send file to S3 as whole file and retrieve as single message
        When I send file "test_data/sample_document.txt" to S3 prefix "messages/documents" as whole file
        Then S3 file upload should be successful with 1 objects
        When I retrieve S3 objects from prefix "messages/documents" and write to file "output/retrieved_document.txt" as whole file
        Then S3 message retrieval should be successful
        And S3 should retrieve 1 messages to file

    @s3 @messaging @single_message
    Scenario: Post and retrieve single message to/from S3
        When I post message "Hello S3 Message World!" to S3 as "test_message.txt"
        Then S3 message should be posted successfully
        When I get S3 object content from "test_message.txt"
        Then S3 object content should be retrieved successfully
        And S3 object content should contain "Hello S3 Message World!"

    @s3 @messaging @limited_retrieval
    Scenario: Retrieve limited number of S3 objects as messages
        # First populate S3 with some test messages
        When I post message "Message 1" to S3 as "batch/msg_001.txt"
        And I post message "Message 2" to S3 as "batch/msg_002.txt"
        And I post message "Message 3" to S3 as "batch/msg_003.txt"
        And I post message "Message 4" to S3 as "batch/msg_004.txt"
        And I post message "Message 5" to S3 as "batch/msg_005.txt"
        # Retrieve only 3 messages
        When I retrieve 3 S3 objects from prefix "batch" and write to file "output/limited_messages.txt" line by line
        Then S3 message retrieval should be successful
        And S3 should retrieve 3 messages to file

    @s3 @messaging @performance
    Scenario: Performance test for S3 message operations
        When I send file "test_data/large_message_file.txt" to S3 prefix "performance/test" line by line
        Then S3 file upload should be successful with 100 objects
        And S3 message processing should complete within 30 seconds
        When I retrieve S3 objects from prefix "performance/test" and write to file "output/performance_output.txt" line by line
        Then S3 message retrieval should be successful
        And S3 should retrieve 100 messages to file
        And S3 message processing should complete within 20 seconds

    @s3 @messaging @mixed_operations
    Scenario: Mixed S3 message operations workflow
        # Upload line by line
        When I send file "test_data/json_messages.txt" to S3 prefix "workflow/json" line by line
        Then S3 file upload should be successful with 10 objects
        
        # Upload as whole file
        When I send file "test_data/xml_document.xml" to S3 prefix "workflow/xml" as whole file
        Then S3 file upload should be successful with 1 objects
        
        # Post individual messages
        When I post message "{"type": "notification", "message": "Process completed"}" to S3 as "workflow/status/notification.json"
        Then S3 message should be posted successfully
        
        # Retrieve all as line-by-line
        When I retrieve S3 objects from prefix "workflow" and write to file "output/all_workflow_messages.txt" line by line
        Then S3 message retrieval should be successful
        
        # Verify specific content
        When I get S3 object content from "workflow/status/notification.json"
        Then S3 object content should contain "Process completed"

    @s3 @messaging @error_handling
    Scenario: Error handling for S3 message operations
        # Try to retrieve from non-existent prefix
        When I retrieve S3 objects from prefix "non/existent/prefix" and write to file "output/empty_result.txt" line by line
        Then S3 message retrieval should be successful
        And S3 should retrieve 0 messages to file
        
        # Try to get content from non-existent object
        When I get S3 object content from "non_existent_object.txt"
        Then S3 object content should be retrieved successfully

    @s3 @messaging @content_validation
    Scenario: Validate S3 message content integrity
        # Send structured data
        When I post message "Line 1: Important data" to S3 as "validation/line1.txt"
        And I post message "Line 2: More important data" to S3 as "validation/line2.txt"
        And I post message "Line 3: Final data" to S3 as "validation/line3.txt"
        
        # Retrieve and validate
        When I retrieve S3 objects from prefix "validation" and write to file "output/validated_messages.txt" line by line
        Then S3 message retrieval should be successful
        And S3 should retrieve 3 messages to file
        
        # Check individual content
        When I get S3 object content from "validation/line2.txt"
        Then S3 object content should contain "More important data"

    @s3 @messaging @cleanup
    Scenario: S3 message cleanup operations
        # Create test messages
        When I post message "Temporary message 1" to S3 as "cleanup/temp1.txt"
        And I post message "Temporary message 2" to S3 as "cleanup/temp2.txt"
        Then S3 message should be posted successfully
        
        # Verify they exist
        When I retrieve S3 objects from prefix "cleanup" and write to file "output/cleanup_before.txt" line by line
        Then S3 should retrieve 2 messages to file
        
        # Note: Actual cleanup would be handled by separate cleanup steps
        # This demonstrates the workflow for testing cleanup operations