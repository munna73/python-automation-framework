Feature: MQ Message-Style Operations
    As a test automation engineer
    I want to use IBM MQ for message-style operations
    So that I can send, retrieve, and process data as messages in MQ queues

    Background:
        Given MQ connection is configured

    @mq @messaging @line_by_line
    Scenario: Send file to MQ line by line and retrieve as messages
        When I send file "test_data/sample_messages.txt" to MQ line by line
        Then MQ file should be sent successfully with 5 messages
        When I retrieve MQ messages and write to file "output/retrieved_mq_messages.txt" line by line
        Then MQ message retrieval should be successful
        And MQ should retrieve 5 messages to file

    @mq @messaging @whole_file
    Scenario: Send file to MQ as whole file and retrieve as single message
        When I send file "test_data/sample_document.txt" to MQ as whole file
        Then MQ file should be sent successfully with 1 messages
        When I retrieve MQ messages and write to file "output/retrieved_mq_document.txt" as whole file
        Then MQ message retrieval should be successful
        And MQ should retrieve 1 messages to file

    @mq @messaging @single_message
    Scenario: Post and retrieve single message to/from MQ
        When I post custom MQ message "Hello MQ Message World!"
        Then MQ custom message should be posted successfully
        When I retrieve MQ messages and write to file "output/single_mq_message.txt" line by line
        Then MQ message retrieval should be successful
        And MQ should retrieve 1 messages to file

    @mq @messaging @limited_retrieval
    Scenario: Retrieve limited number of MQ messages
        # First populate MQ with some test messages
        When I post custom MQ message "Message 1"
        And I post custom MQ message "Message 2"
        And I post custom MQ message "Message 3"
        And I post custom MQ message "Message 4"
        And I post custom MQ message "Message 5"
        Then MQ custom message should be posted successfully
        # Retrieve only 3 messages
        When I retrieve 3 MQ messages and write to file "output/limited_mq_messages.txt" line by line
        Then MQ message retrieval should be successful
        And MQ should retrieve 3 messages to file

    @mq @messaging @export_formats
    Scenario: Export MQ messages in different formats
        # First populate MQ with test messages
        When I post custom MQ message "Test message 1"
        And I post custom MQ message "Test message 2"
        And I post custom MQ message "Test message 3"
        # Export in different formats
        When I export MQ messages to file "output/mq_messages.txt" in "txt" format
        Then MQ export should be successful with 3 messages
        When I export MQ messages to file "output/mq_messages.csv" in "csv" format
        Then MQ export should be successful with 3 messages
        When I export MQ messages to file "output/mq_messages.json" in "json" format
        Then MQ export should be successful with 3 messages
        When I export MQ messages to file "output/mq_messages.xml" in "xml" format
        Then MQ export should be successful with 3 messages

    @mq @messaging @queue_management
    Scenario: MQ queue management operations
        # Check initial queue depth
        When I get MQ queue depth
        Then MQ queue depth should be 0
        # Add some messages
        When I post custom MQ message "Queue management test 1"
        And I post custom MQ message "Queue management test 2"
        And I post custom MQ message "Queue management test 3"
        # Check queue depth after adding messages
        When I get MQ queue depth
        Then MQ queue depth should be 3
        # Drain the queue
        When I drain MQ queue to file "output/drained_mq_messages.txt"
        Then MQ queue should be drained successfully
        # Verify queue is empty
        When I get MQ queue depth
        Then MQ queue depth should be 0

    @mq @messaging @performance
    Scenario: Performance test for MQ message operations
        When I send file "test_data/large_message_file.txt" to MQ line by line
        Then MQ file should be sent successfully with 100 messages
        And MQ processing should complete within 30 seconds
        When I retrieve MQ messages and write to file "output/performance_mq_output.txt" line by line
        Then MQ message retrieval should be successful
        And MQ should retrieve 100 messages to file
        And MQ processing should complete within 20 seconds

    @mq @messaging @mixed_operations
    Scenario: Mixed MQ message operations workflow
        # Send file line by line
        When I send file "test_data/json_messages.txt" to MQ line by line
        Then MQ file should be sent successfully with 10 messages
        
        # Send file as whole
        When I send file "test_data/xml_document.xml" to MQ as whole file
        Then MQ file should be sent successfully with 1 messages
        
        # Post individual messages
        When I post custom MQ message "Individual message 1"
        And I post custom MQ message "Individual message 2"
        Then MQ custom message should be posted successfully
        
        # Check total queue depth
        When I get MQ queue depth
        Then MQ queue depth should be 13
        
        # Export all messages
        When I export MQ messages to file "output/all_mq_messages.json" in "json" format
        Then MQ export should be successful with 13 messages

    @mq @messaging @error_handling
    Scenario: Error handling for MQ message operations
        # Try to retrieve from empty queue
        When I retrieve MQ messages and write to file "output/empty_mq_result.txt" line by line
        Then MQ message retrieval should be successful
        And MQ should retrieve 0 messages to file
        
        # Verify queue depth is still 0
        When I get MQ queue depth
        Then MQ queue depth should be 0

    @mq @messaging @content_validation
    Scenario: Validate MQ message content integrity
        # Send structured data
        When I post custom MQ message "Line 1: Important MQ data"
        And I post custom MQ message "Line 2: More important MQ data"
        And I post custom MQ message "Line 3: Final MQ data"
        Then MQ custom message should be posted successfully
        
        # Retrieve and validate
        When I retrieve MQ messages and write to file "output/validated_mq_messages.txt" line by line
        Then MQ message retrieval should be successful
        And MQ should retrieve 3 messages to file

    @mq @messaging @batch_processing
    Scenario: MQ batch message processing
        # Send a batch of messages
        When I send file "test_data/batch_messages.txt" to MQ line by line
        Then MQ file should be sent successfully with 50 messages
        
        # Process in smaller batches
        When I retrieve 10 MQ messages and write to file "output/batch1_mq_messages.txt" line by line
        Then MQ should retrieve 10 messages to file
        
        When I retrieve 15 MQ messages and write to file "output/batch2_mq_messages.txt" line by line
        Then MQ should retrieve 15 messages to file
        
        # Check remaining messages
        When I get MQ queue depth
        Then MQ queue depth should be 25
        
        # Drain remaining messages
        When I drain MQ queue to file "output/remaining_mq_messages.txt"
        Then MQ queue should be drained successfully

    @mq @messaging @legacy_compatibility
    Scenario: Test legacy MQ step compatibility
        # Use existing legacy steps
        When I post message from "test_data/sample_file.txt" as single message
        Then message should be posted successfully
        
        When I post message from "test_data/multiline_file.txt" line by line
        Then all lines should be posted successfully
        
        # Verify with new steps
        When I get MQ queue depth
        Then MQ queue depth should be 6
        
        # Use new retrieval methods
        When I export MQ messages to file "output/legacy_compatibility.json" in "json" format
        Then MQ export should be successful with 6 messages

    @mq @messaging @comprehensive
    Scenario: Comprehensive MQ operations test
        # Test all major operations
        
        # 1. Basic message posting
        When I post custom MQ message "Comprehensive test start"
        Then MQ custom message should be posted successfully
        
        # 2. File operations
        When I send file "test_data/comprehensive_test.txt" to MQ line by line
        Then MQ file should be sent successfully with 20 messages
        
        When I send file "test_data/summary.txt" to MQ as whole file
        Then MQ file should be sent successfully with 1 messages
        
        # 3. Queue monitoring
        When I get MQ queue depth
        Then MQ queue depth should be 22
        
        # 4. Selective retrieval
        When I retrieve 5 MQ messages and write to file "output/comp_first_batch.txt" line by line
        Then MQ should retrieve 5 messages to file
        
        # 5. Format-specific exports
        When I export MQ messages to file "output/comp_remaining.csv" in "csv" format
        Then MQ export should be successful with 17 messages
        
        # 6. Complete drain
        When I drain MQ queue to file "output/comp_final_drain.txt"
        Then MQ queue should be drained successfully
        
        # 7. Verify empty
        When I get MQ queue depth
        Then MQ queue depth should be 0