"""
Integration utilities for managing AWS SQS messages in SQL databases.
"""
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
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
            else:
                logger.error(f"Unsupported database type for table creation: {db_type}")
                return False
            
            # Execute create table
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            with engine.connect() as connection:
                connection.execute(create_sql)
                connection.commit()
            
            logger.info(f"Successfully created table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create message table: {e}")
            return False
    
    def save_messages_to_sql(self, 
                           messages: List[Dict[str, Any]], 
                           environment: str, 
                           db_type: str,
                           queue_url: str,
                           table_name: str = 'aws_sqs_messages') -> Dict[str, Any]:
        """
        Save SQS messages to SQL database.
        
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
                return {'success_count': 0, 'error_count': 0, 'errors': []}
            
            # Prepare data for insertion
            records = []
            for message in messages:
                record = {
                    'message_id': message.get('MessageId'),
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
            
            # Convert to DataFrame and save
            df = pd.DataFrame(records)
            
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            rows_inserted = df.to_sql(table_name, engine, if_exists='append', index=False)
            
            results = {
                'success_count': len(records),
                'error_count': 0,
                'errors': [],
                'table_name': table_name,
                'total_records': len(records)
            }
            
            logger.info(f"Successfully saved {len(records)} messages to {table_name}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to save messages to SQL: {e}")
            return {
                'success_count': 0,
                'error_count': len(messages),
                'errors': [str(e)],
                'table_name': table_name,
                'total_records': len(messages)
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
            
            # Build query
            base_query = f"SELECT * FROM {table_name}"
            
            where_conditions = []
            if status_filter:
                where_conditions.append(f"status = '{status_filter}'")
            
            if where_conditions:
                base_query += " WHERE " + " AND ".join(where_conditions)
            
            base_query += " ORDER BY received_timestamp DESC"
            
            if limit:
                if db_type.upper() == 'ORACLE':
                    base_query = f"SELECT * FROM ({base_query}) WHERE ROWNUM <= {limit}"
                elif db_type.upper() == 'POSTGRES':
                    base_query += f" LIMIT {limit}"
            
            # Execute query
            df = db_connector.execute_query(environment, db_type, base_query)
            
            logger.info(f"Downloaded {len(df)} messages from SQL database")
            return df
            
        except Exception as e:
            logger.error(f"Failed to download messages from SQL: {e}")
            return pd.DataFrame()
    
    def write_messages_to_file(self, 
                             df: pd.DataFrame, 
                             output_file_path: str,
                             one_message_per_line: bool = True) -> bool:
        """
        Write messages from DataFrame to file.
        
        Args:
            df: DataFrame containing messages
            output_file_path: Output file path
            one_message_per_line: If True, write each message on separate line
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Writing {len(df)} messages to file: {output_file_path}")
            
            # Ensure output directory exists
            output_path = Path(output_file_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
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
                'save_errors': save_results['errors'],
                'delete_errors': delete_errors,
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
                'errors': [str(e)]
            }
    
    def export_messages_to_file_from_sql(self, 
                                       environment: str, 
                                       db_type: str,
                                       output_file_path: str,
                                       status_filter: str = None,
                                       limit: int = None,
                                       table_name: str = 'aws_sqs_messages') -> Dict[str, Any]:
        """
        Complete workflow: download messages from SQL and write to file.
        
        Args:
            environment: Database environment
            db_type: Database type
            output_file_path: Output file path
            status_filter: Optional status filter
            limit: Optional record limit
            table_name: SQL table name
            
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
            success = self.write_messages_to_file(df, output_file_path)
            
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
            update_sql = f"""
                UPDATE {table_name} 
                SET status = '{new_status}', 
                    processed_timestamp = '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'
                WHERE message_id = '{message_id}'
            """
            
            engine = db_connector.get_sqlalchemy_engine(environment, db_type)
            with engine.connect() as connection:
                result = connection.execute(update_sql)
                connection.commit()
            
            logger.info(f"Updated message status: {message_id} -> {new_status}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update message status: {e}")
            return False

# Global AWS-SQL integration instance
aws_sql_integration = AWSSQLIntegration()