Feature: Kafka Message-Style Operations
    As a test automation engineer
    I want to use Apache Kafka for message-style operations
    So that I can send, consume, and process data as messages in Kafka topics

    Background:
        Given Kafka connection is configured

    @kafka @messaging @line_by_line
    Scenario: Send file to Kafka line by line and consume as messages
        When I send file "test_data/sample_messages.txt" to Kafka topic "test-topic" line by line
        Then Kafka file should be sent successfully with 5 messages
        When I consume messages from Kafka topics "test-topic" and write to file "output/retrieved_kafka_messages.txt" line by line
        Then Kafka message consumption should be successful
        And Kafka should consume 5 messages to file

    @kafka @messaging @whole_file
    Scenario: Send file to Kafka as whole file and consume as single message
        When I send file "test_data/sample_document.txt" to Kafka topic "test-document" as whole file
        Then Kafka file should be sent successfully with 1 messages
        When I consume messages from Kafka topics "test-document" and write to file "output/retrieved_kafka_document.txt" as whole file
        Then Kafka message consumption should be successful
        And Kafka should consume 1 messages to file

    @kafka @messaging @single_message
    Scenario: Send and consume single message to/from Kafka
        When I send Kafka message "Hello Kafka Message World!" to topic "test-single"
        Then Kafka message should be sent successfully
        When I consume messages from Kafka topics "test-single" and write to file "output/single_kafka_message.txt" line by line
        Then Kafka message consumption should be successful
        And Kafka should consume 1 messages to file

    @kafka @messaging @limited_consumption
    Scenario: Consume limited number of Kafka messages
        # First populate Kafka with some test messages
        When I send Kafka message "Message 1" to topic "test-limited"
        And I send Kafka message "Message 2" to topic "test-limited"
        And I send Kafka message "Message 3" to topic "test-limited"
        And I send Kafka message "Message 4" to topic "test-limited"
        And I send Kafka message "Message 5" to topic "test-limited"
        Then Kafka message should be sent successfully
        # Consume only 3 messages
        When I consume 3 messages from Kafka topics "test-limited" and write to file "output/limited_kafka_messages.txt" line by line
        Then Kafka message consumption should be successful
        And Kafka should consume 3 messages to file

    @kafka @messaging @export_formats
    Scenario: Export Kafka messages in different formats
        # First populate Kafka with test messages
        When I send Kafka message "Test message 1" to topic "test-export"
        And I send Kafka message "Test message 2" to topic "test-export"
        And I send Kafka message "Test message 3" to topic "test-export"
        # Export in different formats
        When I export Kafka messages from topics "test-export" to file "output/kafka_messages.txt" in "txt" format
        Then Kafka export should be successful with 3 messages
        When I export Kafka messages from topics "test-export" to file "output/kafka_messages.csv" in "csv" format
        Then Kafka export should be successful with 3 messages
        When I export Kafka messages from topics "test-export" to file "output/kafka_messages.json" in "json" format
        Then Kafka export should be successful with 3 messages
        When I export Kafka messages from topics "test-export" to file "output/kafka_messages.xml" in "xml" format
        Then Kafka export should be successful with 3 messages

    @kafka @messaging @topic_metadata
    Scenario: Kafka topic metadata operations
        # Send some messages to create topic
        When I send Kafka message "Metadata test message 1" to topic "test-metadata"
        And I send Kafka message "Metadata test message 2" to topic "test-metadata"
        And I send Kafka message "Metadata test message 3" to topic "test-metadata"
        Then Kafka message should be sent successfully
        # Get topic metadata
        When I get topic metadata for Kafka topics "test-metadata"
        Then Kafka topic "test-metadata" should have 1 partitions
        And Kafka topic "test-metadata" should have at least 3 messages

    @kafka @messaging @multiple_topics
    Scenario: Multi-topic Kafka operations
        # Send messages to multiple topics
        When I send Kafka message "Multi-topic message 1" to topic "topic-a"
        And I send Kafka message "Multi-topic message 2" to topic "topic-b"
        And I send Kafka message "Multi-topic message 3" to topic "topic-a"
        Then Kafka message should be sent successfully
        # Consume from multiple topics
        When I consume messages from Kafka topics "topic-a,topic-b" and write to file "output/multi_topic_messages.txt" line by line
        Then Kafka message consumption should be successful
        And Kafka should consume 3 messages to file

    @kafka @messaging @batch_processing
    Scenario: Kafka batch message processing
        When I send batch Kafka messages to topic "test-batch"
            | message |
            | Batch message 1 |
            | Batch message 2 |
            | Batch message 3 |
            | Batch message 4 |
            | Batch message 5 |
        Then Kafka batch should be sent successfully with 5 messages
        # Consume in smaller batches
        When I consume 2 messages from Kafka topics "test-batch" and write to file "output/batch1_kafka_messages.txt" line by line
        Then Kafka should consume 2 messages to file
        When I consume 3 messages from Kafka topics "test-batch" and write to file "output/batch2_kafka_messages.txt" line by line
        Then Kafka should consume 3 messages to file

    @kafka @messaging @json_processing
    Scenario: Kafka JSON message processing
        When I send JSON messages to Kafka topic "test-json"
            | id | name | status |
            | 1 | John | active |
            | 2 | Jane | inactive |
            | 3 | Bob | active |
        Then Kafka JSON messages should be sent successfully with 3 messages
        # Export JSON messages
        When I export Kafka messages from topics "test-json" to file "output/json_kafka_messages.json" in "json" format
        Then Kafka export should be successful with 3 messages

    @kafka @messaging @performance
    Scenario: Performance test for Kafka message operations
        When I send file "test_data/large_message_file.txt" to Kafka topic "test-performance" line by line
        Then Kafka file should be sent successfully with 100 messages
        And Kafka processing should complete within 30 seconds
        When I consume messages from Kafka topics "test-performance" and write to file "output/performance_kafka_output.txt" line by line
        Then Kafka message consumption should be successful
        And Kafka should consume 100 messages to file
        And Kafka processing should complete within 20 seconds

    @kafka @messaging @seek_operations
    Scenario: Kafka seek operations
        # Send messages to topic
        When I send Kafka message "Seek test message 1" to topic "test-seek"
        And I send Kafka message "Seek test message 2" to topic "test-seek"
        And I send Kafka message "Seek test message 3" to topic "test-seek"
        Then Kafka message should be sent successfully
        
        # Seek to beginning and consume
        When I seek to beginning of Kafka topics "test-seek"
        And I consume messages from Kafka topics "test-seek" and write to file "output/seek_beginning_messages.txt" line by line
        Then Kafka message consumption should be successful
        And Kafka should consume 3 messages to file
        
        # Seek to end (no new messages to consume)
        When I seek to end of Kafka topics "test-seek"
        And I consume 1 messages from Kafka topics "test-seek" and write to file "output/seek_end_messages.txt" line by line
        Then Kafka message consumption should be successful
        And Kafka should consume 0 messages to file

    @kafka @messaging @mixed_operations
    Scenario: Mixed Kafka message operations workflow
        # Send file line by line
        When I send file "test_data/json_messages.txt" to Kafka topic "mixed-topic" line by line
        Then Kafka file should be sent successfully with 10 messages
        
        # Send file as whole
        When I send file "test_data/xml_document.xml" to Kafka topic "mixed-topic" as whole file
        Then Kafka file should be sent successfully with 1 messages
        
        # Send individual messages
        When I send Kafka message "Individual message 1" to topic "mixed-topic"
        And I send Kafka message "Individual message 2" to topic "mixed-topic"
        Then Kafka message should be sent successfully
        
        # Get topic metadata to verify total messages
        When I get topic metadata for Kafka topics "mixed-topic"
        Then Kafka topic "mixed-topic" should have at least 13 messages
        
        # Export all messages
        When I export Kafka messages from topics "mixed-topic" to file "output/all_kafka_messages.json" in "json" format
        Then Kafka export should be successful with 13 messages

    @kafka @messaging @error_handling
    Scenario: Error handling for Kafka message operations
        # Try to consume from non-existent topic (should handle gracefully)
        When I consume 1 messages from Kafka topics "non-existent-topic" and write to file "output/empty_kafka_result.txt" line by line
        Then Kafka message consumption should be successful
        And Kafka should consume 0 messages to file

    @kafka @messaging @content_validation
    Scenario: Validate Kafka message content integrity
        # Send structured data
        When I send Kafka message "Line 1: Important Kafka data" to topic "test-validation"
        And I send Kafka message "Line 2: More important Kafka data" to topic "test-validation"
        And I send Kafka message "Line 3: Final Kafka data" to topic "test-validation"
        Then Kafka message should be sent successfully
        
        # Consume and validate
        When I consume messages from Kafka topics "test-validation" and write to file "output/validated_kafka_messages.txt" line by line
        Then Kafka message consumption should be successful
        And Kafka should consume 3 messages to file

    @kafka @messaging @comprehensive
    Scenario: Comprehensive Kafka operations test
        # Test all major operations
        
        # 1. Basic message sending
        When I send Kafka message "Comprehensive test start" to topic "comprehensive-test"
        Then Kafka message should be sent successfully
        
        # 2. File operations
        When I send file "test_data/comprehensive_test.txt" to Kafka topic "comprehensive-test" line by line
        Then Kafka file should be sent successfully with 20 messages
        
        When I send file "test_data/summary.txt" to Kafka topic "comprehensive-test" as whole file
        Then Kafka file should be sent successfully with 1 messages
        
        # 3. Topic monitoring
        When I get topic metadata for Kafka topics "comprehensive-test"
        Then Kafka topic "comprehensive-test" should have at least 22 messages
        
        # 4. Selective consumption
        When I consume 5 messages from Kafka topics "comprehensive-test" and write to file "output/comp_first_batch.txt" line by line
        Then Kafka should consume 5 messages to file
        
        # 5. Format-specific exports
        When I export Kafka messages from topics "comprehensive-test" to file "output/comp_remaining.csv" in "csv" format
        Then Kafka export should be successful with 17 messages

    @kafka @messaging @cross_format
    Scenario: Cross-format Kafka message operations
        # Send messages in various formats
        When I send Kafka message '{"type": "json", "content": "JSON message"}' to topic "cross-format"
        And I send Kafka message "<xml><type>xml</type><content>XML message</content></xml>" to topic "cross-format"
        And I send Kafka message "Plain text message" to topic "cross-format"
        Then Kafka message should be sent successfully
        
        # Export in different formats to test parsing
        When I export Kafka messages from topics "cross-format" to file "output/cross_format.txt" in "txt" format
        Then Kafka export should be successful with 3 messages
        
        When I export Kafka messages from topics "cross-format" to file "output/cross_format.json" in "json" format
        Then Kafka export should be successful with 3 messages
        
        When I export Kafka messages from topics "cross-format" to file "output/cross_format.xml" in "xml" format
        Then Kafka export should be successful with 3 messages

    @kafka @messaging @high_throughput
    Scenario: High throughput Kafka operations
        # Send large batch of messages
        When I send file "test_data/high_throughput_data.txt" to Kafka topic "high-throughput" line by line
        Then Kafka file should be sent successfully with 1000 messages
        And Kafka processing should complete within 60 seconds
        
        # Consume in chunks
        When I consume 100 messages from Kafka topics "high-throughput" and write to file "output/throughput_chunk1.txt" line by line
        Then Kafka should consume 100 messages to file
        And Kafka processing should complete within 10 seconds
        
        When I consume 200 messages from Kafka topics "high-throughput" and write to file "output/throughput_chunk2.txt" line by line
        Then Kafka should consume 200 messages to file
        And Kafka processing should complete within 15 seconds
        
        # Export remaining in compressed format
        When I export Kafka messages from topics "high-throughput" to file "output/throughput_remaining.json" in "json" format
        Then Kafka export should be successful with 700 messages
        And Kafka processing should complete within 30 seconds