#!/usr/bin/env python3
"""
Main runner script for the automation framework.
Enhanced with AWS SQS, S3, and SQL integration capabilities.
"""
import click
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import json

# Add parent directory to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database_connector import db_connector
from db.mongodb_connector import mongodb_connector
from utils.data_comparator import data_comparator
from api.rest_client import RestClient
from mq.mq_producer import mq_producer
from aws.sqs_connector import sqs_connector
from aws.s3_connector import S3Connector
from aws.sql_integration import AWSSQLIntegration
from utils.config_loader import ConfigLoader
from utils.logger import logger
from utils.export_utils import ExportUtils

export_utils = ExportUtils()
aws_sql_integration = AWSSQLIntegration()  # Create instance for AWS-SQL integration
api_client = RestClient()  # Create instance for API client
s3_connector = S3Connector()  # Create instance for S3 connector

@click.group()
@click.option('--env', default='DEV', help='Environment (DEV, QA, PROD)')
@click.option('--log-level', default='INFO', help='Log level')
@click.pass_context
def cli(ctx, env, log_level):
    """Python Automation Framework CLI with AWS, MongoDB, and SQL support."""
    ctx.ensure_object(dict)
    ctx.obj['ENV'] = env.upper()
    ctx.obj['LOG_LEVEL'] = log_level.upper()
    
    logger.info(f"Starting framework CLI - Environment: {env}, Log Level: {log_level}")

# AWS SQS Commands
@cli.command()
@click.option('--queue-url', required=True, help='SQS queue URL')
@click.option('--message', required=True, help='Message to send')
@click.option('--attributes', help='Message attributes (JSON format)')
@click.pass_context
def sqs_send(ctx, queue_url, message, attributes):
    """Send message to AWS SQS queue."""
    try:
        logger.info(f"Sending message to SQS queue: {queue_url}")
        
        message_attributes = None
        if attributes:
            try:
                message_attributes = json.loads(attributes)
            except json.JSONDecodeError:
                click.echo(f"Invalid JSON attributes: {attributes}", err=True)
                sys.exit(1)
        
        result = sqs_connector.send_message(queue_url, message, message_attributes)
        
        click.echo(f"\n{'='*50}")
        click.echo(f"SQS MESSAGE SENT")
        click.echo(f"{'='*50}")
        click.echo(f"Queue URL: {queue_url}")
        click.echo(f"Message ID: {result.get('MessageId')}")
        click.echo(f"MD5: {result.get('MD5OfBody')}")
        click.echo(f"{'='*50}")
        
    except Exception as e:
        logger.error(f"SQS send failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--queue-url', required=True, help='SQS queue URL')
@click.option('--file', required=True, help='File to send')
@click.option('--mode', default='line', help='Send mode: line (line-by-line) or file (entire file)')
@click.pass_context
def sqs_send_file(ctx, queue_url, file, mode):
    """Send file to AWS SQS queue."""
    try:
        logger.info(f"Sending file to SQS queue: {file} (mode: {mode})")
        
        line_by_line = mode == 'line'
        result = sqs_connector.send_file_as_messages(queue_url, file, line_by_line)
        
        click.echo(f"\n{'='*50}")
        click.echo(f"SQS FILE SEND RESULTS")
        click.echo(f"{'='*50}")
        click.echo(f"Queue URL: {queue_url}")
        click.echo(f"File: {result['file_path']}")
        click.echo(f"Mode: {result['mode']}")
        click.echo(f"Successful: {result['success_count']}")
        click.echo(f"Failed: {result['error_count']}")
        click.echo(f"Success Rate: {result['success_rate']:.2f}%")
        click.echo(f"{'='*50}")
        
        if result['errors']:
            click.echo("\nErrors:")
            for error in result['errors'][:5]:
                click.echo(f"  - {error}")
        
    except Exception as e:
        logger.error(f"SQS file send failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--queue-url', required=True, help='SQS queue URL')
@click.option('--max-messages', default=10, help='Maximum messages to receive')
@click.option('--export-file', help='Export messages to file')
@click.pass_context
def sqs_receive(ctx, queue_url, max_messages, export_file):
    """Receive messages from AWS SQS queue."""
    try:
        logger.info(f"Receiving messages from SQS queue: {queue_url}")
        
        messages = sqs_connector.receive_messages(queue_url, max_messages)
        
        click.echo(f"\n{'='*50}")
        click.echo(f"SQS MESSAGES RECEIVED")
        click.echo(f"{'='*50}")
        click.echo(f"Queue URL: {queue_url}")
        click.echo(f"Messages Received: {len(messages)}")
        click.echo(f"{'='*50}")
        
        for i, message in enumerate(messages[:5], 1):  # Show first 5
            click.echo(f"\nMessage {i}:")
            click.echo(f"  ID: {message.get('MessageId')}")
            click.echo(f"  Body: {message.get('Body', '')[:100]}{'...' if len(message.get('Body', '')) > 100 else ''}")
        
        if export_file and messages:
            # Export messages to file
            message_bodies = [msg.get('Body', '') for msg in messages]
            with open(export_file, 'w', encoding='utf-8') as f:
                for body in message_bodies:
                    f.write(f"{body}\n")
            click.echo(f"\nMessages exported to: {export_file}")
        
    except Exception as e:
        logger.error(f"SQS receive failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

# AWS S3 Commands
@cli.command()
@click.option('--bucket', required=True, help='S3 bucket name')
@click.option('--key', required=True, help='S3 object key')
@click.option('--local-file', required=True, help='Local file path to save')
@click.pass_context
def s3_download_file(ctx, bucket, key, local_file):
    """Download single file from AWS S3."""
    try:
        logger.info(f"Downloading S3 file: s3://{bucket}/{key}")
        
        success = s3_connector.download_file(bucket, key, local_file)
        
        click.echo(f"\n{'='*50}")
        click.echo(f"S3 FILE DOWNLOAD")
        click.echo(f"{'='*50}")
        click.echo(f"Bucket: {bucket}")
        click.echo(f"Key: {key}")
        click.echo(f"Local File: {local_file}")
        click.echo(f"Status: {'✓ Success' if success else '✗ Failed'}")
        click.echo(f"{'='*50}")
        
        if not success:
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"S3 download failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--bucket', required=True, help='S3 bucket name')
@click.option('--prefix', required=True, help='S3 prefix (directory)')
@click.option('--local-dir', required=True, help='Local directory to save files')
@click.option('--create-subdirs/--flatten', default=True, help='Create subdirectories or flatten structure')
@click.pass_context
def s3_download_directory(ctx, bucket, prefix, local_dir, create_subdirs):
    """Download directory from AWS S3."""
    try:
        logger.info(f"Downloading S3 directory: s3://{bucket}/{prefix}")
        
        result = s3_connector.download_directory(bucket, prefix, local_dir, create_subdirs)
        
        click.echo(f"\n{'='*50}")
        click.echo(f"S3 DIRECTORY DOWNLOAD")
        click.echo(f"{'='*50}")
        click.echo(f"Bucket: {bucket}")
        click.echo(f"Prefix: {prefix}")
        click.echo(f"Local Directory: {local_dir}")
        click.echo(f"Files Downloaded: {result['downloaded_count']}")
        click.echo(f"Failed Downloads: {result['failed_count']}")
        click.echo(f"Total Size: {result['total_size_bytes']:,} bytes")
        click.echo(f"Success Rate: {result['success_rate']:.2f}%")
        click.echo(f"{'='*50}")
        
        if result['failed_files']:
            click.echo("\nFailed Files:")
            for failed_file in result['failed_files'][:5]:
                click.echo(f"  - {failed_file}")
        
    except Exception as e:
        logger.error(f"S3 directory download failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--bucket', required=True, help='S3 bucket name')
@click.option('--local-file', required=True, help='Local file to upload')
@click.option('--key', help='S3 key (uses filename if not provided)')
@click.pass_context
def s3_upload(ctx, bucket, local_file, key):
    """Upload file to AWS S3."""
    try:
        logger.info(f"Uploading file to S3: {local_file}")
        
        success = s3_connector.upload_file(local_file, bucket, key)
        
        final_key = key or Path(local_file).name
        
        click.echo(f"\n{'='*50}")
        click.echo(f"S3 FILE UPLOAD")
        click.echo(f"{'='*50}")
        click.echo(f"Local File: {local_file}")
        click.echo(f"Bucket: {bucket}")
        click.echo(f"Key: {final_key}")
        click.echo(f"Status: {'✓ Success' if success else '✗ Failed'}")
        click.echo(f"{'='*50}")
        
        if not success:
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"S3 upload failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

# AWS-SQL Integration Commands
@cli.command()
@click.option('--queue-url', required=True, help='SQS queue URL')
@click.option('--env', required=True, help='Database environment')
@click.option('--db-type', required=True, help='Database type (ORACLE/POSTGRES/MONGODB)')
@click.option('--table-name', default='aws_sqs_messages', help='SQL table name')
@click.option('--max-messages', default=10, help='Maximum messages to process')
@click.option('--delete-after-save', is_flag=True, help='Delete messages from SQS after saving')
@click.pass_context
def sqs_to_sql(ctx, queue_url, env, db_type, table_name, max_messages, delete_after_save):
    """Process SQS messages to SQL database."""
    try:
        logger.info(f"Processing SQS queue to SQL: {queue_url} -> {env}.{table_name}")
        
        # Create table if it doesn't exist
        aws_sql_integration.create_message_table(env, db_type, table_name)
        
        # Process messages
        result = aws_sql_integration.process_queue_to_sql(
            queue_url, env, db_type, max_messages, delete_after_save, table_name
        )
        
        click.echo(f"\n{'='*50}")
        click.echo(f"SQS TO SQL PROCESSING")
        click.echo(f"{'='*50}")
        click.echo(f"Queue URL: {queue_url}")
        click.echo(f"Database: {env} {db_type}")
        click.echo(f"Table: {table_name}")
        click.echo(f"Messages Received: {result['messages_received']}")
        click.echo(f"Messages Saved: {result['messages_saved']}")
        click.echo(f"Messages Deleted: {result['messages_deleted']}")
        click.echo(f"{'='*50}")
        
        if result.get('save_errors'):
            click.echo("\nSave Errors:")
            for error in result['save_errors'][:3]:
                click.echo(f"  - {error}")
        
    except Exception as e:
        logger.error(f"SQS to SQL processing failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--env', required=True, help='Database environment')
@click.option('--db-type', required=True, help='Database type (ORACLE/POSTGRES/MONGODB)')
@click.option('--output-file', required=True, help='Output file path')
@click.option('--table-name', default='aws_sqs_messages', help='SQL table name')
@click.option('--status-filter', help='Filter by message status')
@click.option('--limit', type=int, help='Limit number of messages')
@click.pass_context
def sql_to_file(ctx, env, db_type, output_file, table_name, status_filter, limit):
    """Export messages from SQL database to file."""
    try:
        logger.info(f"Exporting messages from SQL to file: {env}.{table_name} -> {output_file}")
        
        result = aws_sql_integration.export_messages_to_file_from_sql(
            env, db_type, output_file, status_filter, limit, table_name
        )
        
        click.echo(f"\n{'='*50}")
        click.echo(f"SQL TO FILE EXPORT")
        click.echo(f"{'='*50}")
        click.echo(f"Database: {env} {db_type}")
        click.echo(f"Table: {table_name}")
        click.echo(f"Output File: {output_file}")
        click.echo(f"Messages Exported: {result['messages_exported']}")
        click.echo(f"Status: {'✓ Success' if result['success'] else '✗ Failed'}")
        click.echo(f"{'='*50}")
        
        if not result['success']:
            click.echo(f"Error: {result.get('error', 'Unknown error')}")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"SQL to file export failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

# Connection Test Commands
@cli.command()
@click.option('--queue-url', help='Optional specific queue URL to test')
@click.pass_context
def test_sqs(ctx, queue_url):
    """Test AWS SQS connection."""
    try:
        logger.info("Testing AWS SQS connection")
        
        success = sqs_connector.test_connection(queue_url)
        
        click.echo(f"\n{'='*50}")
        click.echo(f"AWS SQS CONNECTION TEST")
        click.echo(f"{'='*50}")
        click.echo(f"Queue URL: {queue_url or 'General SQS access'}")
        click.echo(f"Connection: {'✓ Success' if success else '✗ Failed'}")
        click.echo(f"{'='*50}")
        
        if not success:
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"SQS connection test failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--bucket', help='Optional specific bucket to test')
@click.pass_context
def test_s3(ctx, bucket):
    """Test AWS S3 connection."""
    try:
        logger.info("Testing AWS S3 connection")
        
        success = s3_connector.test_connection(bucket)
        
        click.echo(f"\n{'='*50}")
        click.echo(f"AWS S3 CONNECTION TEST")
        click.echo(f"{'='*50}")
        click.echo(f"Bucket: {bucket or 'General S3 access'}")
        click.echo(f"Connection: {'✓ Success' if success else '✗ Failed'}")
        click.echo(f"{'='*50}")
        
        if not success:
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"S3 connection test failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

# Keep all existing commands (compare, mongodb-query, api, mq, test-db, etc.)
# [Previous CLI commands remain unchanged...]

if __name__ == '__main__':
    cli()