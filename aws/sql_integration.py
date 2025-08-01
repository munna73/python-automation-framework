"""
Integration utilities for managing AWS SQS messages in SQL databases.
"""
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from sqlalchemy import text, create_engine
from db.database_connector import db_connector
from aws.sqs_connector import sqs_connector
from utils.logger import logger
from utils.export_utils import export_utils

class AWSSQLIntegration:
    """Integration between AWS SQS and SQL databases for message management."""
    
    def __init__(self):
        """Initialize AWS-SQL integration."""
        # Define the schema for the message table.
        # This dictionary is used to dynamically create tables for different database types.
        self.message_table_schema = {
            'message_id': {'oracle': 'VARCHAR2(255) PRIMARY KEY', 'postgres': 'VARCHAR(255) PRIMARY KEY'},
            'queue_url': {'oracle': 'VARCHAR2(500)', 'postgres': 'VARCHAR(500)'},
            'message_body': {'oracle': 'CLOB', 'postgres': 'TEXT'},
            'receipt_handle': {'oracle': 'VARCHAR2(1000)', 'postgres': 'VARCHAR(1000)'},
            'message_attributes': {'oracle': 'CLOB', 'postgres': 'TEXT'},
            'received_timestamp': {'oracle': 'TIMESTAMP', 'postgres': 'TIMESTAMP'},
            'processed_timestamp': {'oracle': 'TIMESTAMP', 'postgres': 'TIMESTAMP NULL'},
            'status': {'oracle': 'VARCHAR2(50)', 'postgres': 'VARCHAR(50)'},
            'retry_count': {'oracle': 'NUMBER DEFAULT 0', 'postgres': 'INTEGER DEFAULT 0'}
        }
    
    def create_message_table(self, 
                           environment: str, 
                           db_type: str, 
                           table_name: str = 'aws_sqs_messages') -> bool:
        """
        Create table for storing SQS messages based on the schema dictionary.
        
        Args:
            environment: Database environment
            db_type: Database type (ORACLE, POSTGRES, MONGODB)
            table_name: Name of the table to create
            
        Returns:
            True if successful, False otherwise
        """
        db_type = db_type.upper()
        
        try:
            logger.info(f"Creating SQS message table: {table_name}")
            
            if db_type == 'MONGODB':
                return self.create_message_collection(environment, table_name)
            
            if db_type not in ['ORACLE', 'POSTGRES']:
                logger.error(f"Unsupported database type for table creation: {db_type}")
                return False

            columns = []
            for col_name, col_types in self.message_table_schema.items():
                col_definition = col_types.get(db_type.lower())
                if col_definition:
                    columns.append(f"{col_name} {col_definition}")
            
            # The 'IF NOT EXISTS' clause is supported by Postgres but not Oracle.
            create_sql_template = "CREATE TABLE {if_not_exists} {table_name} (\n" + ",\n".join(columns) + "\n)"
            if_not_exists = "IF NOT EXISTS" if db_type == 'POSTGRES' else ""
            
            create_sql = create_sql_template.format(
                if_not_exists=if_not_exists,
                table_name=table_name
            )
            
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
        """Create MongoDB collection for messages with indexes."""
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
                        logger.debug(f"Index might already exist or a creation error occurred: {e}")
                connection.commit()
                
        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")
    
    def _save_messages_to_mongodb(self,
                                 messages: List[Dict[str, Any]],
                                 environment: str,
                                 queue_url: str,
                                 collection_name: str) -> Dict[str, Any]:
        """
        Save SQS messages to MongoDB collection.
        
        Args:
            messages: List of SQS messages
            environment: Database environment
            queue_url: SQS queue URL
            collection_name: MongoDB collection name
            
        Returns:
            Save operation results
        """
        try:
            logger.info(f"Saving {len(messages)} SQS messages to MongoDB collection: {collection_name}")
            db = db_connector.get_mongodb_connection(environment)
            collection = db[collection_name]
            
            if not messages:
                return {'success_count': 0, 'error_count': 0, 'errors': [], 'duplicates': 0}
            
            # Check for existing messages to avoid duplicates
            message_ids = [msg.get('MessageId') for msg in messages if msg.get('MessageId')]
            existing_ids = set()
            if message_ids:
                existing_docs = list(collection.find({'message_id': {'$in': message_ids}}, {'message_id': 1}))
                existing_ids = {doc['message_id'] for doc in existing_docs}
            
            records_to_insert = []
            for message in messages:
                msg_id = message.get('MessageId')
                if msg_id and msg_id not in existing_ids:
                    record = {
                        'message_id': msg_id,
                        'queue_url': queue_url,
                        'message_body': message.get('Body', ''),
                        'receipt_handle': message.get('ReceiptHandle', ''),
                        'message_attributes': message.get('MessageAttributes', {}),
                        'received_timestamp': datetime.now(),
                        'processed_timestamp': None,
                        'status': 'RECEIVED',
                        'retry_count': 0
                    }
                    records_to_insert.append(record)

            if not records_to_insert:
                logger.warning(f"All {len(messages)} messages were duplicates")
                return {
                    'success_count': 0,
                    'error_count': 0,
                    'errors': [],
                    'duplicates': len(messages),
                    'table_name': collection_name,
                    'total_records': len(messages)
                }

            result = collection.insert_many(records_to_insert)
            
            results = {
                'success_count': len(result.inserted_ids),
                'error_count': 0,
                'errors': [],
                'duplicates': len(messages) - len(result.inserted_ids),
                'table_name': collection_name,
                'total_records': len(messages)
            }

            logger.info(f"Successfully saved {len(result.inserted_ids)} messages to {collection_name}")
            return results

        except Exception as e:
            logger.error(f"Failed to save messages to MongoDB: {e}")
            return {
                'success_count': 0,
                'error_count': len(messages),
                'errors': [str(e)],
                'duplicates': 0,
                'table_name': collection_name,
                'total_records': len(messages)
            }

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
        db_type = db_type.upper()
        if db_type == 'MONGODB':
            return self._save_messages_to_mongodb(messages, environment, queue_url, table_name)

        try:
            logger.info(f"Saving {len(messages)} SQS messages to SQL database")
            
            if not messages:
                return {'success_count': 0, 'error_count': 0, 'errors': [], 'duplicates': 0}
            
            # Check for existing messages to avoid duplicates
            message_ids = [msg.get('MessageId') for msg in messages if msg.get('MessageId')]
            existing_ids = set()
            duplicates = 0
            
            if message_ids:
                # Use parameterized query for safe execution
                placeholders = ', '.join([f":id{i}" for i in range(len(message_ids))])
                params = {f"id{i}": msg_id for i, msg_id in enumerate(message_ids)}
                
                existing_query = f"SELECT message_id FROM {table_name} WHERE message_id IN ({placeholders})"
                
                engine = db_connector.get_sqlalchemy_engine(environment, db_type)
                with engine.connect() as connection:
                    result = connection.execute(text(existing_query), **params)
                    existing_ids = {row[0] for row in result}

                duplicates = len(existing_ids)
            
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
                    'table_name': table_name,
                    'total_records': len(messages)
                }
            
            # Convert to DataFrame and save
            df = pd.DataFrame(records)
            
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            df.to_sql(table_name, engine, if_exists='append', index=False)
            
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
                connection.execute(
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
                params = {'days': days_to_keep}
            elif db_type.upper() == 'POSTGRES':
                cleanup_sql = text(f"""
                    DELETE FROM {table_name} 
                    WHERE received_timestamp < CURRENT_TIMESTAMP - INTERVAL '{days_to_keep} days'
                    AND status IN ('PROCESSED', 'FAILED')
                """)
                params = {}
            else:
                logger.error(f"Cleanup not implemented for {db_type}")
                return False
            
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            with engine.connect() as connection:
                result = connection.execute(cleanup_sql, **params)
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
            
            db_type = db_type.upper()
            if db_type == 'ORACLE':
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
                params = {'days': days_to_keep}
            elif db_type == 'POSTGRES':
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
                params = {}
            else:
                logger.error(f"Archiving not implemented for {db_type}")
                return {'archived_count': 0, 'deleted_count': 0, 'success': False}
            
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            with engine.connect() as connection:
                # First, insert old messages into the archive table
                archive_result = connection.execute(archive_sql, **params)
                archived_count = archive_result.rowcount
                logger.info(f"Archived {archived_count} messages.")
                
                # Then, delete those messages from the main table
                delete_result = connection.execute(delete_sql, **params)
                deleted_count = delete_result.rowcount
                logger.info(f"Deleted {deleted_count} messages from main table.")

                connection.commit()
            
            return {
                'archived_count': archived_count,
                'deleted_count': deleted_count,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Failed to archive messages: {e}")
            return {'archived_count': 0, 'deleted_count': 0, 'success': False, 'error': str(e)}

