#!/usr/bin/env python3
"""
Main runner script for the automation framework.
Provides CLI interface for running various framework operations including MongoDB.
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
from db.data_comparator import data_comparator
from web.api_client import api_client
from mq.mq_producer import mq_producer
from utils.config_loader import config_loader
from utils.logger import logger
from utils.export_utils import ExportUtils

export_utils = ExportUtils()

@click.group()
@click.option('--env', default='DEV', help='Environment (DEV, QA, PROD)')
@click.option('--log-level', default='INFO', help='Log level')
@click.pass_context
def cli(ctx, env, log_level):
    """Python Automation Framework CLI with MongoDB support."""
    ctx.ensure_object(dict)
    ctx.obj['ENV'] = env.upper()
    ctx.obj['LOG_LEVEL'] = log_level.upper()
    
    logger.info(f"Starting framework CLI - Environment: {env}, Log Level: {log_level}")

@cli.command()
@click.option('--source-env', default='DEV', help='Source environment')
@click.option('--target-env', default='QA', help='Target environment')
@click.option('--source-db', default='ORACLE', help='Source database type (ORACLE/POSTGRES)')
@click.option('--target-db', default='ORACLE', help='Target database type (ORACLE/POSTGRES)')
@click.option('--query-name', default='customer_comparison', help='Query name from config')
@click.option('--window-minutes', default=60, help='Time window in minutes for chunked queries')
@click.option('--primary-key', default='customer_id', help='Primary key column for comparison')
@click.option('--export-format', default='excel', help='Export format (excel/csv)')
@click.pass_context
def compare(ctx, source_env, target_env, source_db, target_db, query_name, window_minutes, primary_key, export_format):
    """Compare data between two databases."""
    try:
        logger.info(f"Starting database comparison: {source_env} {source_db} vs {target_env} {target_db}")
        
        # Get query from config
        query = config_loader.get_query(query_name)
        
        # Calculate date range for chunked query (last 24 hours by default)
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=24)
        
        # Execute queries on both environments
        logger.info("Executing query on source environment...")
        source_df = db_connector.execute_chunked_query(
            source_env, source_db, query, 'created_date',
            start_date, end_date, window_minutes
        )
        
        logger.info("Executing query on target environment...")
        target_df = db_connector.execute_chunked_query(
            target_env, target_db, query, 'created_date', 
            start_date, end_date, window_minutes
        )
        
        # Perform comparison
        comparison_name = f"{source_env}_{source_db}_vs_{target_env}_{target_db}_{query_name}"
        results = data_comparator.compare_dataframes(
            source_df, target_df, primary_key, comparison_name
        )
        
        # Export results
        exported_file = data_comparator.export_comparison_results(comparison_name, export_format)
        
        # Print summary
        click.echo(f"\n{'='*50}")
        click.echo(f"COMPARISON RESULTS: {comparison_name}")
        click.echo(f"{'='*50}")
        click.echo(f"Source records: {results['source_count']}")
        click.echo(f"Target records: {results['target_count']}")
        click.echo(f"Differences found: {results['differences_count']}")
        click.echo(f"Missing in target: {results['missing_in_target_count']}")
        click.echo(f"Missing in source: {results['missing_in_source_count']}")
        click.echo(f"Match percentage: {results['match_percentage']:.2f}%")
        click.echo(f"Results exported to: {exported_file}")
        click.echo(f"{'='*50}")
        
    except Exception as e:
        logger.error(f"Database comparison failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--env', required=True, help='Environment (DEV, QA, PROD)')
@click.option('--collection', required=True, help='MongoDB collection name')
@click.option('--query', help='MongoDB query filter (JSON format)')
@click.option('--limit', default=100, help='Maximum documents to return')
@click.option('--export-format', default='excel', help='Export format (excel/csv)')
@click.pass_context
def mongodb_query(ctx, env, collection, query, limit, export_format):
    """Query MongoDB collection and export results."""
    try:
        logger.info(f"Querying MongoDB: {env}.{collection}")
        
        # Parse query filter if provided
        query_filter = {}
        if query:
            try:
                query_filter = json.loads(query)
            except json.JSONDecodeError:
                click.echo(f"Invalid JSON query: {query}", err=True)
                sys.exit(1)
        
        # Execute MongoDB query
        df = mongodb_connector.execute_find_query(
            env, collection, query=query_filter, limit=limit
        )
        
        # Export results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if export_format == 'excel':
            filename = f"mongodb_{env}_{collection}_{timestamp}.xlsx"
            filepath = f"output/exports/{filename}"
            export_utils.write_single_dataframe_to_excel(df, filepath, f"{collection}_data")
        else:
            filename = f"mongodb_{env}_{collection}_{timestamp}.csv"
            filepath = f"output/exports/{filename}"
            export_utils.write_to_csv_safe(df, filepath)
        
        # Print summary
        click.echo(f"\n{'='*50}")
        click.echo(f"MONGODB QUERY RESULTS")
        click.echo(f"{'='*50}")
        click.echo(f"Environment: {env}")
        click.echo(f"Collection: {collection}")
        click.echo(f"Query Filter: {query_filter}")
        click.echo(f"Documents Found: {len(df)}")
        click.echo(f"Results exported to: {filepath}")
        click.echo(f"{'='*50}")
        
        if not df.empty:
            click.echo("\nFirst 5 documents:")
            click.echo(df.head().to_string())
        
    except Exception as e:
        logger.error(f"MongoDB query failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--env', required=True, help='Environment (DEV, QA, PROD)')
@click.option('--collection', required=True, help='MongoDB collection name')
@click.option('--pipeline', required=True, help='Aggregation pipeline (JSON format)')
@click.option('--export-format', default='excel', help='Export format (excel/csv)')
@click.pass_context
def mongodb_aggregate(ctx, env, collection, pipeline, export_format):
    """Execute MongoDB aggregation pipeline."""
    try:
        logger.info(f"Running MongoDB aggregation: {env}.{collection}")
        
        # Parse aggregation pipeline
        try:
            pipeline_stages = json.loads(pipeline)
        except json.JSONDecodeError:
            click.echo(f"Invalid JSON pipeline: {pipeline}", err=True)
            sys.exit(1)
        
        # Execute aggregation
        df = mongodb_connector.execute_aggregation_query(env, collection, pipeline_stages)
        
        # Export results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if export_format == 'excel':
            filename = f"mongodb_agg_{env}_{collection}_{timestamp}.xlsx"
            filepath = f"output/exports/{filename}"
            export_utils.write_single_dataframe_to_excel(df, filepath, f"{collection}_aggregation")
        else:
            filename = f"mongodb_agg_{env}_{collection}_{timestamp}.csv"
            filepath = f"output/exports/{filename}"
            export_utils.write_to_csv_safe(df, filepath)
        
        # Print summary
        click.echo(f"\n{'='*50}")
        click.echo(f"MONGODB AGGREGATION RESULTS")
        click.echo(f"{'='*50}")
        click.echo(f"Environment: {env}")
        click.echo(f"Collection: {collection}")
        click.echo(f"Documents Processed: {len(df)}")
        click.echo(f"Results exported to: {filepath}")
        click.echo(f"{'='*50}")
        
        if not df.empty:
            click.echo("\nAggregation results:")
            click.echo(df.to_string())
        
    except Exception as e:
        logger.error(f"MongoDB aggregation failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--env', required=True, help='Environment to test')
@click.option('--db-type', required=True, help='Database type (ORACLE/POSTGRES/MONGODB)')
@click.pass_context
def test_db(ctx, env, db_type):
    """Test database connection."""
    try:
        logger.info(f"Testing database connection: {env} {db_type}")
        
        if db_type.upper() == 'MONGODB':
            success = mongodb_connector.test_connection(env)
        else:
            success = db_connector.test_connection(env, db_type)
        
        click.echo(f"\n{'='*50}")
        click.echo(f"DATABASE CONNECTION TEST")
        click.echo(f"{'='*50}")
        click.echo(f"Environment: {env}")
        click.echo(f"Database Type: {db_type}")
        click.echo(f"Connection: {'✓ Success' if success else '✗ Failed'}")
        click.echo(f"{'='*50}")
        
        if not success:
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--env', required=True, help='Environment (DEV, QA, PROD)')
@click.option('--collection', required=True, help='MongoDB collection name')
@click.pass_context
def mongodb_stats(ctx, env, collection):
    """Get MongoDB collection statistics."""
    try:
        logger.info(f"Getting MongoDB collection statistics: {env}.{collection}")
        
        stats = mongodb_connector.get_collection_stats(env, collection)
        
        click.echo(f"\n{'='*50}")
        click.echo(f"MONGODB COLLECTION STATISTICS")
        click.echo(f"{'='*50}")
        click.echo(f"Environment: {env}")
        click.echo(f"Collection: {collection}")
        click.echo(f"Document Count: {stats['document_count']:,}")
        click.echo(f"Size (bytes): {stats['size_bytes']:,}")
        click.echo(f"Storage Size: {stats['storage_size']:,}")
        click.echo(f"Average Object Size: {stats['avg_obj_size']}")
        click.echo(f"Number of Indexes: {stats['indexes']}")
        click.echo(f"{'='*50}")
        
        if stats['index_sizes']:
            click.echo("\nIndex Sizes:")
            for index, size in stats['index_sizes'].items():
                click.echo(f"  {index}: {size:,} bytes")
        
    except Exception as e:
        logger.error(f"MongoDB stats retrieval failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

# Keep existing commands (api, mq, test_api, test_mq) unchanged...
# [Previous CLI commands remain the same]

if __name__ == '__main__':
    cli()