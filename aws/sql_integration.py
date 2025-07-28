"""
Integration utilities for managing AWS SQS messages in SQL databases.
"""
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from sqlalchemy import text
from db.database_connector import db_connector
from aws.sqs_connector import sqs_connector
from utils.logger import logger
from utils.export_utils import export_utils

class AWSSQLIntegration:
    """Integration between AWS SQS and SQL databases for message management."""
    
    def __init__(self):
        """Initialize AWS-SQL integration."""
        self.message_table_schema = {
            'message_id': 'VARCHAR(255) PRIMARY KEY',
            'queue_url': 'VARCHAR(500)',
            'message_body': 'TEXT',
            'receipt_handle': 'VARCHAR(1000)', 
            'message_attributes': 'TEXT',
            'received_timestamp': 'TIMESTAMP',
            'processed_timestamp': 'TIMESTAMP NULL',
            'status': 'VARCHAR(50)',
            'retry_count': 'INTEGER DEFAULT 0'
        }
    
    def create_message_table(self, 
                           environment: str, 
                           db_type: str, 
                           table_name: str = 'aws_sqs_messages') -> bool:
        """
        Create table for storing SQS messages.
        
        Args:
            environment: Database environment
            db_type: Database type (ORACLE, POSTGRES, MONGODB)
            table_name: Name of the table to create
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Creating SQS message table: {table_name}")
            
            if db_type.upper() == 'ORACLE':
                create_sql = f"""
                CREATE TABLE {table_name} (
                    message_id VARCHAR2(255) PRIMARY KEY,
                    queue_url VARCHAR2(500),
                    message_body CLOB,
                    receipt_handle VARCHAR2(1000),
                    message_attributes CLOB,
                    received_timestamp TIMESTAMP,
                    processed_timestamp TIMESTAMP,
                    status VARCHAR2(50),
                    retry_count NUMBER DEFAULT 0
                )
                """
            elif db_type.upper() == 'POSTGRES':
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    message_id VARCHAR(255) PRIMARY KEY,
                    queue_url VARCHAR(500),
                    message_body TEXT,
                    receipt_handle VARCHAR(1000),
                    message_attributes TEXT,
                    received_timestamp TIMESTAMP,
                    processed_timestamp TIMESTAMP,
                    status VARCHAR(50),
                    retry_count INTEGER DEFAULT 0
                )
                """
            elif db_type.upper() == 'MONGODB':
                return self.create_message_collection(environment, table_name)
            else:
                logger.error(f"Unsupported database type for table creation: {db_type}")
                return False
            
            # Execute create table
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            with engine.connect() as connection:
                connection.execute(text(create_sql))
                connection.commit()
            
            # Create indexes for better performance
            self._create_indexes(environment, db_type, table_name)
            
            logger.info(f"Successfully created table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create message table: {e}")
            return False
    
    def create_message_collection(self, environment: str, collection_name: str = 'aws_sqs_messages') -> bool:
        """Create MongoDB collection for messages."""
        try:
            db = db_connector.get_mongodb_connection(environment)
            
            # Create collection with indexes
            collection = db[collection_name]
            collection.create_index('message_id', unique=True)
            collection.create_index('queue_url')
            collection.create_index('status')
            collection.create_index('received_timestamp')
            collection.create_index([('status', 1), ('received_timestamp', -1)])
            
            logger.info(f"Created MongoDB collection: {collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create MongoDB collection: {e}")
            return False
    
    def _create_indexes(self, environment: str, db_type: str, table_name: str):
        """Create indexes for better query performance."""
        try:
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            
            indexes = [
                f"CREATE INDEX idx_{table_name}_status ON {table_name}(status)",
                f"CREATE INDEX idx_{table_name}_received ON {table_name}(received_timestamp)",
                f"CREATE INDEX idx_{table_name}_queue ON {table_name}(queue_url)",
                f"CREATE INDEX idx_{table_name}_status_received ON {table_name}(status, received_timestamp)"
            ]
            
            with engine.connect() as connection:
                for index_sql in indexes:
                    try:
                        connection.execute(text(index_sql))
                    except Exception as e:
                        logger.debug(f"Index might already exist: {e}")
                connection.commit()
                
        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")
    
    def save_messages_to_sql(self, 
                           messages: List[Dict[str, Any]], 
                           environment: str, 
                           db_type: str,
                           queue_url: str,
                           table_name: str = 'aws_sqs_messages') -> Dict[str, Any]:
        """
        Save SQS messages to SQL database with deduplication.
        
        Args:
            messages: List of SQS messages
            environment: Database environment
            db_type: Database type
            queue_url: SQS queue URL
            table_name: Table name for storing messages
            
        Returns:
            Save operation results
        """
        try:
            logger.info(f"Saving {len(messages)} SQS messages to SQL database")
            
            if not messages:
                return {'success_count': 0, 'error_count': 0, 'errors': [], 'duplicates': 0}
            
            # Check for existing messages to avoid duplicates
            message_ids = [msg.get('MessageId') for msg in messages if msg.get('MessageId')]
            duplicates = 0
            
            if message_ids and db_type.upper() != 'MONGODB':
                placeholders = ','.join(['?' if db_type.upper() == 'POSTGRES' else ':p' + str(i) 
                                       for i in range(len(message_ids))])
                existing_query = f"SELECT message_id FROM {table_name} WHERE message_id IN ({placeholders})"
                
                existing_df = db_connector.execute_query(environment, db_type, existing_query, message_ids)
                existing_ids = set(existing_df['message_id'].tolist()) if not existing_df.empty else set()
                duplicates = len(existing_ids)
            else:
                existing_ids = set()
            
            # Prepare data for insertion
            records = []
            for message in messages:
                msg_id = message.get('MessageId')
                if msg_id and msg_id not in existing_ids:
                    record = {
                        'message_id': msg_id,
                        'queue_url': queue_url,
                        'message_body': message.get('Body', ''),
                        'receipt_handle': message.get('ReceiptHandle', ''),
                        'message_attributes': json.dumps(message.get('MessageAttributes', {})),
                        'received_timestamp': datetime.now(),
                        'processed_timestamp': None,
                        'status': 'RECEIVED',
                        'retry_count': 0
                    }
                    records.append(record)
            
            if not records:
                logger.warning(f"All {len(messages)} messages were duplicates")
                return {
                    'success_count': 0,
                    'error_count': 0,
                    'errors': [],
                    'duplicates': duplicates,
                    'table_name': table_name
                }
            
            # Convert to DataFrame and save
            df = pd.DataFrame(records)
            
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            rows_inserted = df.to_sql(table_name, engine, if_exists='append', index=False)
            
            results = {
                'success_count': len(records),
                'error_count': 0,
                'errors': [],
                'duplicates': duplicates,
                'table_name': table_name,
                'total_records': len(messages)
            }
            
            logger.info(f"Successfully saved {len(records)} messages to {table_name} ({duplicates} duplicates skipped)")
            return results
            
        except Exception as e:
            logger.error(f"Failed to save messages to SQL: {e}")
            return {
                'success_count': 0,
                'error_count': len(messages),
                'errors': [str(e)],
                'duplicates': 0,
                'table_name': table_name,
                'total_records': len(messages)
            }
    
    def save_messages_batch(self, 
                           messages: List[Dict[str, Any]], 
                           environment: str, 
                           db_type: str,
                           queue_url: str,
                           batch_size: int = 100,
                           table_name: str = 'aws_sqs_messages') -> Dict[str, Any]:
        """Save messages in batches for better performance."""
        total_saved = 0
        total_errors = 0
        total_duplicates = 0
        all_errors = []
        
        try:
            for i in range(0, len(messages), batch_size):
                batch = messages[i:i + batch_size]
                result = self.save_messages_to_sql(
                    batch, environment, db_type, queue_url, table_name
                )
                
                total_saved += result['success_count']
                total_errors += result['error_count']
                total_duplicates += result.get('duplicates', 0)
                all_errors.extend(result['errors'])
                
                logger.info(f"Processed batch {i//batch_size + 1}: {result['success_count']} saved")
            
            return {
                'success_count': total_saved,
                'error_count': total_errors,
                'duplicates': total_duplicates,
                'errors': all_errors,
                'batches_processed': (len(messages) + batch_size - 1) // batch_size
            }
            
        except Exception as e:
            logger.error(f"Batch save failed: {e}")
            return {
                'success_count': total_saved,
                'error_count': total_errors + len(messages) - total_saved,
                'duplicates': total_duplicates,
                'errors': all_errors + [str(e)]
            }
    
    def download_messages_from_sql(self, 
                                 environment: str, 
                                 db_type: str,
                                 table_name: str = 'aws_sqs_messages',
                                 status_filter: str = None,
                                 limit: int = None) -> pd.DataFrame:
        """
        Download messages from SQL database.
        
        Args:
            environment: Database environment
            db_type: Database type
            table_name: Table name containing messages
            status_filter: Optional status filter
            limit: Optional limit on number of records
            
        Returns:
            DataFrame with messages
        """
        try:
            logger.info(f"Downloading messages from SQL table: {table_name}")
            
            # Build query with parameters
            base_query = f"SELECT * FROM {table_name}"
            params = {}
            
            where_conditions = []
            if status_filter:
                where_conditions.append("status = :status")
                params['status'] = status_filter
            
            if where_conditions:
                base_query += " WHERE " + " AND ".join(where_conditions)
            
            base_query += " ORDER BY received_timestamp DESC"
            
            if limit:
                if db_type.upper() == 'ORACLE':
                    base_query = f"SELECT * FROM ({base_query}) WHERE ROWNUM <= :limit"
                    params['limit'] = limit
                elif db_type.upper() == 'POSTGRES':
                    base_query += " LIMIT :limit"
                    params['limit'] = limit
            
            # Execute query
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            df = pd.read_sql_query(text(base_query), engine, params=params)
            
            logger.info(f"Downloaded {len(df)} messages from SQL database")
            return df
            
        except Exception as e:
            logger.error(f"Failed to download messages from SQL: {e}")
            return pd.DataFrame()
    
    def write_messages_to_file(self, 
                             df: pd.DataFrame, 
                             output_file_path: str,
                             one_message_per_line: bool = True,
                             output_format: str = None) -> bool:
        """
        Write messages from DataFrame to file with multiple format support.
        
        Args:
            df: DataFrame containing messages
            output_file_path: Output file path
            one_message_per_line: If True, write each message on separate line
            output_format: Output format (txt, json, csv)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Writing {len(df)} messages to file: {output_file_path}")
            
            # Ensure output directory exists
            output_path = Path(output_file_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Determine format from file extension if not specified
            if not output_format:
                if output_file_path.endswith('.csv'):
                    output_format = 'csv'
                elif output_file_path.endswith('.json'):
                    output_format = 'json'
                else:
                    output_format = 'txt'
            
            if output_format == 'csv':
                df.to_csv(output_path, index=False)
            elif output_format == 'json':
                # Convert datetime columns to string
                df_json = df.copy()
                for col in df_json.select_dtypes(include=['datetime64']).columns:
                    df_json[col] = df_json[col].astype(str)
                df_json.to_json(output_path, orient='records', indent=2)
            else:
                # Default text format
                if one_message_per_line:
                    # Write each message body as a separate line
                    with open(output_path, 'w', encoding='utf-8') as f:
                        for _, row in df.iterrows():
                            message_body = row.get('message_body', '')
                            f.write(f"{message_body}\n")
                else:
                    # Write all messages as JSON
                    messages_data = []
                    for _, row in df.iterrows():
                        message_data = {
                            'message_id': row.get('message_id'),
                            'queue_url': row.get('queue_url'),
                            'message_body': row.get('message_body'),
                            'received_timestamp': str(row.get('received_timestamp')),
                            'status': row.get('status')
                        }
                        messages_data.append(message_data)
                    
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(messages_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Successfully wrote messages to file: {output_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write messages to file: {e}")
            return False
    
    def process_queue_to_sql(self, 
                           queue_url: str, 
                           environment: str, 
                           db_type: str,
                           max_messages: int = 10,
                           delete_after_save: bool = False,
                           table_name: str = 'aws_sqs_messages') -> Dict[str, Any]:
        """
        Complete workflow: receive messages from SQS and save to SQL.
        
        Args:
            queue_url: SQS queue URL
            environment: Database environment
            db_type: Database type
            max_messages: Maximum messages to process
            delete_after_save: Whether to delete messages from SQS after saving
            table_name: SQL table name
            
        Returns:
            Processing results
        """
        try:
            logger.info(f"Processing SQS queue to SQL: {queue_url}")
            
            # Receive messages from SQS
            messages = sqs_connector.receive_messages(queue_url, max_messages)
            
            if not messages:
                logger.info("No messages received from SQS queue")
                return {
                    'messages_received': 0,
                    'messages_saved': 0,
                    'messages_deleted': 0,
                    'processed_count': 0,  # For step compatibility
                    'errors': []
                }
            
            # Save messages to SQL
            save_results = self.save_messages_to_sql(
                messages, environment, db_type, queue_url, table_name
            )
            
            deleted_count = 0
            delete_errors = []
            
            # Delete messages from SQS if requested and save was successful
            if delete_after_save and save_results['success_count'] > 0:
                for message in messages:
                    receipt_handle = message.get('ReceiptHandle')
                    if receipt_handle:
                        if sqs_connector.delete_message(queue_url, receipt_handle):
                            deleted_count += 1
                        else:
                            delete_errors.append(f"Failed to delete message: {message.get('MessageId')}")
            
            results = {
                'messages_received': len(messages),
                'messages_saved': save_results['success_count'],
                'messages_deleted': deleted_count,
                'processed_count': save_results['success_count'],  # For step compatibility
                'save_errors': save_results['errors'],
                'delete_errors': delete_errors,
                'duplicates': save_results.get('duplicates', 0),
                'table_name': table_name
            }
            
            logger.info(f"Queue processing completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to process queue to SQL: {e}")
            return {
                'messages_received': 0,
                'messages_saved': 0,
                'messages_deleted': 0,
                'processed_count': 0,
                'errors': [str(e)]
            }
    
    def export_messages_to_file_from_sql(self, 
                                       environment: str, 
                                       db_type: str,
                                       output_file_path: str,
                                       status_filter: str = None,
                                       limit: int = None,
                                       table_name: str = 'aws_sqs_messages',
                                       output_format: str = None) -> Dict[str, Any]:
        """
        Complete workflow: download messages from SQL and write to file.
        
        Args:
            environment: Database environment
            db_type: Database type
            output_file_path: Output file path
            status_filter: Optional status filter
            limit: Optional record limit
            table_name: SQL table name
            output_format: Output format (txt, json, csv)
            
        Returns:
            Export results
        """
        try:
            logger.info(f"Exporting messages from SQL to file: {output_file_path}")
            
            # Download messages from SQL
            df = self.download_messages_from_sql(
                environment, db_type, table_name, status_filter, limit
            )
            
            if df.empty:
                logger.info("No messages found in SQL database")
                return {
                    'messages_exported': 0,
                    'output_file': output_file_path,
                    'success': True
                }
            
            # Write to file
            success = self.write_messages_to_file(df, output_file_path, output_format=output_format)
            
            results = {
                'messages_exported': len(df),
                'output_file': output_file_path,
                'success': success,
                'table_name': table_name
            }
            
            logger.info(f"Message export completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to export messages from SQL: {e}")
            return {
                'messages_exported': 0,
                'output_file': output_file_path,
                'success': False,
                'error': str(e)
            }
    
    def update_message_status(self, 
                            environment: str, 
                            db_type: str,
                            message_id: str, 
                            new_status: str,
                            table_name: str = 'aws_sqs_messages') -> bool:
        """
        Update message status in SQL database.
        
        Args:
            environment: Database environment
            db_type: Database type
            message_id: Message ID to update
            new_status: New status value
            table_name: SQL table name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            update_sql = text(f"""
                UPDATE {table_name} 
                SET status = :status, 
                    processed_timestamp = :timestamp
                WHERE message_id = :message_id
            """)
            
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            with engine.connect() as connection:
                result = connection.execute(
                    update_sql,
                    status=new_status,
                    timestamp=datetime.now(),
                    message_id=message_id
                )
                connection.commit()
            
            logger.info(f"Updated message status: {message_id} -> {new_status}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update message status: {e}")
            return False
    
    def cleanup_test_messages(self, 
                            environment: str, 
                            db_type: str,
                            table_name: str = 'aws_sqs_messages',
                            days_to_keep: int = 7) -> bool:
        """
        Clean up old test messages from database.
        
        Args:
            environment: Database environment
            db_type: Database type
            table_name: Table name
            days_to_keep: Number of days to keep messages
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Cleaning up test messages older than {days_to_keep} days from {table_name}")
            
            if db_type.upper() == 'ORACLE':
                cleanup_sql = text(f"""
                    DELETE FROM {table_name} 
                    WHERE received_timestamp < SYSDATE - :days
                    AND status IN ('PROCESSED', 'FAILED')
                """)
            elif db_type.upper() == 'POSTGRES':
                cleanup_sql = text(f"""
                    DELETE FROM {table_name} 
                    WHERE received_timestamp < CURRENT_TIMESTAMP - INTERVAL '{days_to_keep} days'
                    AND status IN ('PROCESSED', 'FAILED')
                """)
            else:
                logger.error(f"Cleanup not implemented for {db_type}")
                return False
            
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            with engine.connect() as connection:
                if db_type.upper() == 'ORACLE':
                    result = connection.execute(cleanup_sql, days=days_to_keep)
                else:
                    result = connection.execute(cleanup_sql)
                connection.commit()
                
                deleted_count = result.rowcount
            
            logger.info(f"Cleaned up {deleted_count} messages")
            return True
            
        except Exception as e:
            logger.error(f"Failed to cleanup messages: {e}")
            return False
    
    def retry_failed_messages(self, 
                            environment: str, 
                            db_type: str,
                            queue_url: str,
                            max_retries: int = 3,
                            table_name: str = 'aws_sqs_messages') -> Dict[str, Any]:
        """
        Retry failed messages.
        
        Args:
            environment: Database environment
            db_type: Database type
            queue_url: SQS queue URL to resend messages
            max_retries: Maximum retry attempts
            table_name: Table name
            
        Returns:
            Retry results
        """
        try:
            logger.info(f"Retrying failed messages with retry count < {max_retries}")
            
            # Get failed messages
            failed_query = text(f"""
                SELECT * FROM {table_name} 
                WHERE status = 'FAILED' 
                AND retry_count < :max_retries
                ORDER BY received_timestamp
            """)
            
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            df = pd.read_sql_query(failed_query, engine, params={'max_retries': max_retries})
            
            if df.empty:
                logger.info("No failed messages to retry")
                return {'retry_success': 0, 'retry_failed': 0, 'total_attempted': 0}
            
            retry_success = 0
            retry_failed = 0
            
            for _, row in df.iterrows():
                try:
                    # Parse message attributes
                    message_attributes = json.loads(row.get('message_attributes', '{}'))
                    
                    # Resend to SQS
                    result = sqs_connector.send_message(
                        queue_url, 
                        row['message_body'],
                        message_attributes if message_attributes else None
                    )
                    
                    # Update status
                    self.update_message_status(
                        environment, db_type, 
                        row['message_id'], 
                        'RETRIED', 
                        table_name
                    )
                    
                    retry_success += 1
                    
                except Exception as e:
                    logger.error(f"Failed to retry message {row['message_id']}: {e}")
                    retry_failed += 1
                    
                    # Increment retry count
                    update_retry = text(f"""
                        UPDATE {table_name} 
                        SET retry_count = retry_count + 1
                        WHERE message_id = :message_id
                    """)
                    
                    with engine.connect() as connection:
                        connection.execute(update_retry, message_id=row['message_id'])
                        connection.commit()
            
            return {
                'retry_success': retry_success,
                'retry_failed': retry_failed,
                'total_attempted': len(df)
            }
            
        except Exception as e:
            logger.error(f"Failed to retry messages: {e}")
            return {'retry_success': 0, 'retry_failed': 0, 'error': str(e)}
    
    def archive_old_messages(self, 
                           environment: str, 
                           db_type: str,
                           days_to_keep: int = 30,
                           table_name: str = 'aws_sqs_messages',
                           archive_table: str = 'aws_sqs_messages_archive') -> Dict[str, Any]:
        """
        Archive old messages to separate table.
        
        Args:
            environment: Database environment
            db_type: Database type
            days_to_keep: Days to keep in main table
            table_name: Main table name
            archive_table: Archive table name
            
        Returns:
            Archive results
        """
        try:
            logger.info(f"Archiving messages older than {days_to_keep} days")
            
            # Create archive table if doesn't exist
            self.create_message_table(environment, db_type, archive_table)
            
            if db_type.upper() == 'ORACLE':
                archive_sql = text(f"""
                    INSERT INTO {archive_table}
                    SELECT * FROM {table_name}
                    WHERE received_timestamp < SYSDATE - :days
                    AND status IN ('PROCESSED', 'FAILED')
                """)
                
                delete_sql = text(f"""
                    DELETE FROM {table_name}
                    WHERE received_timestamp < SYSDATE - :days
                    AND status IN ('PROCESSED', 'FAILED')
                """)
            elif db_type.upper() == 'POSTGRES':
                archive_sql = text(f"""
                    INSERT INTO {archive_table}
                    SELECT * FROM {table_name}
                    WHERE received_timestamp < CURRENT_TIMESTAMP - INTERVAL '{days_to_keep} days'
                    AND status IN ('PROCESSED', 'FAILED')
                """)
                
                delete_sql = text(f"""
                    DELETE FROM {table_name}
                    WHERE received_timestamp < CURRENT_TIMESTAMP - INTERVAL '{days_to_keep} days'
                    AND status IN ('PROCESSED', 'FAILED')
                """)
            else:
                return {'archived': 0, 'error': f'Archive not implemented for {db_type}'}
            
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            with engine.connect() as connection:
                # Archive messages
                if db_type.upper() == 'ORACLE':
                    archive_result = connection.execute(archive_sql, days=days_to_keep)
                else:
                    archive_result = connection.execute(archive_sql)
                
                archived_count = archive_result.rowcount
                
                # Delete from main table
                if db_type.upper() == 'ORACLE':
                    delete_result = connection.execute(delete_sql, days=days_to_keep)
                else:
                    delete_result = connection.execute(delete_sql)
                    
                connection.commit()
            
            logger.info(f"Archived {archived_count} messages")
            return {'archived': archived_count, 'deleted': delete_result.rowcount}
            
        except Exception as e:
            logger.error(f"Failed to archive messages: {e}")
            return {'archived': 0, 'error': str(e)}
    
    def get_processing_metrics(self, 
                             environment: str, 
                             db_type: str,
                             table_name: str = 'aws_sqs_messages') -> Dict[str, Any]:
        """
        Get processing metrics for messages.
        
        Args:
            environment: Database environment
            db_type: Database type
            table_name: Table name
            
        Returns:
            Processing metrics
        """
        try:
            if db_type.upper() == 'ORACLE':
                metrics_query = text(f"""
                    SELECT 
                        status,
                        COUNT(*) as count,
                        MIN(received_timestamp) as oldest,
                        MAX(received_timestamp) as newest,
                        AVG(EXTRACT(DAY FROM (processed_timestamp - received_timestamp)) * 24 * 60 * 60 +
                            EXTRACT(HOUR FROM (processed_timestamp - received_timestamp)) * 60 * 60 +
                            EXTRACT(MINUTE FROM (processed_timestamp - received_timestamp)) * 60 +
                            EXTRACT(SECOND FROM (processed_timestamp - received_timestamp))) as avg_processing_time
                    FROM {table_name}
                    WHERE processed_timestamp IS NOT NULL
                    GROUP BY status
                """
            elif db_type.upper() == 'POSTGRES':
                metrics_query = text(f"""
                    SELECT 
                        status,
                        COUNT(*) as count,
                        MIN(received_timestamp) as oldest,
                        MAX(received_timestamp) as newest,
                        AVG(EXTRACT(EPOCH FROM (processed_timestamp - received_timestamp))) as avg_processing_time
                    FROM {table_name}
                    WHERE processed_timestamp IS NOT NULL
                    GROUP BY status
                """)
            else:
                return {'error': f'Metrics not implemented for {db_type}'}
            
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            df = pd.read_sql_query(metrics_query, engine)
            
            if df.empty:
                return {
                    'status_counts': {},
                    'total_messages': 0,
                    'oldest_message': None,
                    'newest_message': None,
                    'avg_processing_time_seconds': 0
                }
            
            # Get total count
            total_query = text(f"SELECT COUNT(*) as total FROM {table_name}")
            total_df = pd.read_sql_query(total_query, engine)
            total_count = total_df['total'].iloc[0]
            
            # Process results
            status_counts = df.set_index('status')['count'].to_dict()
            
            return {
                'status_counts': status_counts,
                'total_messages': total_count,
                'oldest_message': df['oldest'].min().isoformat() if pd.notna(df['oldest'].min()) else None,
                'newest_message': df['newest'].max().isoformat() if pd.notna(df['newest'].max()) else None,
                'avg_processing_time_seconds': float(df['avg_processing_time'].mean()) if pd.notna(df['avg_processing_time'].mean()) else 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            return {'error': str(e)}
    
    def get_queue_statistics(self, 
                           environment: str, 
                           db_type: str,
                           queue_url: str = None,
                           table_name: str = 'aws_sqs_messages') -> Dict[str, Any]:
        """
        Get statistics for specific queue or all queues.
        
        Args:
            environment: Database environment
            db_type: Database type
            queue_url: Optional specific queue URL
            table_name: Table name
            
        Returns:
            Queue statistics
        """
        try:
            if queue_url:
                stats_query = text(f"""
                    SELECT 
                        queue_url,
                        COUNT(*) as total_messages,
                        SUM(CASE WHEN status = 'RECEIVED' THEN 1 ELSE 0 END) as pending,
                        SUM(CASE WHEN status = 'PROCESSED' THEN 1 ELSE 0 END) as processed,
                        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                        SUM(CASE WHEN status = 'RETRIED' THEN 1 ELSE 0 END) as retried,
                        MAX(received_timestamp) as last_received
                    FROM {table_name}
                    WHERE queue_url = :queue_url
                    GROUP BY queue_url
                """)
                params = {'queue_url': queue_url}
            else:
                stats_query = text(f"""
                    SELECT 
                        queue_url,
                        COUNT(*) as total_messages,
                        SUM(CASE WHEN status = 'RECEIVED' THEN 1 ELSE 0 END) as pending,
                        SUM(CASE WHEN status = 'PROCESSED' THEN 1 ELSE 0 END) as processed,
                        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                        SUM(CASE WHEN status = 'RETRIED' THEN 1 ELSE 0 END) as retried,
                        MAX(received_timestamp) as last_received
                    FROM {table_name}
                    GROUP BY queue_url
                    ORDER BY total_messages DESC
                """)
                params = {}
            
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            df = pd.read_sql_query(stats_query, engine, params=params)
            
            if df.empty:
                return {'queues': [], 'total_queues': 0}
            
            # Convert to list of dictionaries
            queues = []
            for _, row in df.iterrows():
                queue_stats = {
                    'queue_url': row['queue_url'],
                    'total_messages': int(row['total_messages']),
                    'pending': int(row['pending']),
                    'processed': int(row['processed']),
                    'failed': int(row['failed']),
                    'retried': int(row['retried']),
                    'last_received': row['last_received'].isoformat() if pd.notna(row['last_received']) else None
                }
                queues.append(queue_stats)
            
            return {
                'queues': queues,
                'total_queues': len(queues)
            }
            
        except Exception as e:
            logger.error(f"Failed to get queue statistics: {e}")
            return {'error': str(e)}
    
    def process_messages_with_callback(self,
                                     environment: str,
                                     db_type: str,
                                     processor_func: callable,
                                     status_filter: str = 'RECEIVED',
                                     batch_size: int = 100,
                                     table_name: str = 'aws_sqs_messages') -> Dict[str, Any]:
        """
        Process messages with a custom callback function.
        
        Args:
            environment: Database environment
            db_type: Database type
            processor_func: Function to process each message
            status_filter: Status filter for messages to process
            batch_size: Number of messages to process in batch
            table_name: Table name
            
        Returns:
            Processing results
        """
        try:
            processed_count = 0
            failed_count = 0
            
            while True:
                # Get batch of messages
                df = self.download_messages_from_sql(
                    environment, db_type, table_name, 
                    status_filter=status_filter, 
                    limit=batch_size
                )
                
                if df.empty:
                    break
                
                for _, row in df.iterrows():
                    try:
                        # Process message
                        processor_func(row.to_dict())
                        
                        # Update status to processed
                        self.update_message_status(
                            environment, db_type,
                            row['message_id'],
                            'PROCESSED',
                            table_name
                        )
                        
                        processed_count += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to process message {row['message_id']}: {e}")
                        
                        # Update status to failed
                        self.update_message_status(
                            environment, db_type,
                            row['message_id'],
                            'FAILED',
                            table_name
                        )
                        
                        failed_count += 1
                
                logger.info(f"Processed batch: {processed_count} successful, {failed_count} failed")
                
                # Break if we processed less than batch size (no more messages)
                if len(df) < batch_size:
                    break
            
            return {
                'processed': processed_count,
                'failed': failed_count,
                'total': processed_count + failed_count
            }
            
        except Exception as e:
            logger.error(f"Failed to process messages with callback: {e}")
            return {
                'processed': processed_count,
                'failed': failed_count,
                'error': str(e)
            }

# Global AWS-SQL integration instance
aws_sql_integration = AWSSQLIntegration()