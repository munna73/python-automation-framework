"""
Enhanced step definitions for data comparison using config queries
"""
from behave import given, when, then
from db.database_connector import db_connector
from utils.config_loader import config_loader
from utils.logger import logger
from utils.data_comparator import data_comparator
import pandas as pd
import configparser
import os
from datetime import datetime, timedelta

# Store for query results
query_results = {}

@given('I execute comparison query "{query_key}" from config on source "{source_env}" "{source_db}" and target "{target_env}" "{target_db}"')
def step_execute_comparison_query(context, query_key, source_env, source_db, target_env, target_db):
    """
    Execute the same query from config on both source and target databases.
    
    This is a convenience step that combines reading query and executing on both databases.
    """
    try:
        # Read query from config
        config = configparser.ConfigParser()
        config_path = os.path.join('config', 'config.ini')
        config.read(config_path)
        
        # Default section for queries
        section = getattr(context, 'query_section', 'DATABASE_QUERIES')
        
        if section not in config:
            raise ValueError(f"Section '{section}' not found in config.ini")
        
        if query_key not in config[section]:
            raise ValueError(f"Query key '{query_key}' not found in section '{section}'")
        
        query = config[section][query_key]
        
        # Replace any placeholders in query
        query = replace_query_placeholders(query)
        
        logger.info(f"Executing comparison query '{query_key}' on source and target")
        
        # Execute on source
        source_df = db_connector.execute_query(source_env, source_db, query)
        query_results[f"source_{query_key}"] = source_df
        
        # Execute on target
        target_df = db_connector.execute_query(target_env, target_db, query)
        query_results[f"target_{query_key}"] = target_df
        
        # Store in context for immediate comparison
        context.source_df = source_df
        context.target_df = target_df
        context.comparison_query_key = query_key
        
        logger.info(f"Source rows: {len(source_df)}, Target rows: {len(target_df)}")
        
    except Exception as e:
        logger.error(f"Failed to execute comparison query: {e}")
        raise

@when('I compare the results with tolerance for numeric columns')
def step_compare_with_tolerance(context):
    """Compare results with tolerance for numeric differences."""
    if not hasattr(context, 'source_df') or not hasattr(context, 'target_df'):
        raise ValueError("No source/target dataframes found. Execute comparison query first")
    
    # Get tolerance from context or use default
    tolerance = float(getattr(context, 'numeric_tolerance', 0.01))
    
    # Get key columns
    key_columns = getattr(context, 'key_columns', None)
    if not key_columns:
        # Try to auto-detect key columns (columns with 'id' in name)
        potential_keys = [col for col in context.source_df.columns if 'id' in col.lower()]
        key_columns = potential_keys[:1] if potential_keys else None
    
    # Perform comparison with tolerance
    comparison_result = data_comparator.compare_dataframes_with_tolerance(
        context.source_df,
        context.target_df,
        key_columns=key_columns,
        numeric_tolerance=tolerance,
        exclude_columns=getattr(context, 'exclude_columns', [])
    )
    
    context.comparison_result = comparison_result
    
    # Log summary
    logger.info(f"Comparison with tolerance {tolerance}: {comparison_result['summary']['total_differences']} differences")

@then('I generate comparison report as "{report_format}" to "{report_path}"')
def step_generate_comparison_report(context, report_format, report_path):
    """Generate detailed comparison report."""
    if not hasattr(context, 'comparison_result'):
        raise ValueError("No comparison result found. Run comparison first")
    
    try:
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        if report_format.lower() == 'html':
            generate_html_comparison_report(context.comparison_result, report_path)
        elif report_format.lower() == 'excel':
            generate_excel_comparison_report(context.comparison_result, report_path)
        elif report_format.lower() == 'json':
            import json
            with open(report_path, 'w') as f:
                json.dump(context.comparison_result, f, indent=2, default=str)
        else:
            raise ValueError(f"Unsupported report format: {report_format}")
        
        logger.info(f"Generated comparison report: {report_path}")
        
    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        raise

@given('I set query section as "{section}"')
def step_set_query_section(context, section):
    """Set the config section to read queries from."""
    context.query_section = section
    logger.info(f"Set query section: {section}")

@given('I set numeric tolerance as {tolerance:f}')
def step_set_numeric_tolerance(context, tolerance):
    """Set tolerance for numeric comparisons."""
    context.numeric_tolerance = tolerance
    logger.info(f"Set numeric tolerance: {tolerance}")

def replace_query_placeholders(query):
    """Replace placeholders in query with actual values."""
    replacements = {
        '$$TODAY$$': datetime.now().strftime('%Y-%m-%d'),
        '$$YESTERDAY$$': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        '$$START_DATE$$': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
        '$$END_DATE$$': datetime.now().strftime('%Y-%m-%d'),
        '$$LAST_HOUR$$': (datetime.now() - timedelta(hours=1)).isoformat(),
        '$$CURRENT_MONTH$$': datetime.now().strftime('%Y-%m')
    }
    
    for placeholder, value in replacements.items():
        query = query.replace(placeholder, value)
    
    return query

def generate_html_comparison_report(comparison_result, report_path):
    """Generate HTML comparison report."""
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Data Comparison Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .summary {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; }}
            .differences {{ margin-top: 20px; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #4CAF50; color: white; }}
            tr:nth-child(even) {{ background-color: #f2f2f2; }}
            .added {{ background-color: #c8e6c9; }}
            .removed {{ background-color: #ffcdd2; }}
            .modified {{ background-color: #fff3cd; }}
        </style>
    </head>
    <body>
        <h1>Data Comparison Report</h1>
        <div class="summary">
            <h2>Summary</h2>
            <p>Total Differences: {comparison_result['summary']['total_differences']}</p>
            <p>Rows in Source Only: {comparison_result['summary']['source_only']}</p>
            <p>Rows in Target Only: {comparison_result['summary']['target_only']}</p>
            <p>Modified Rows: {comparison_result['summary']['modified']}</p>
            <p>Execution Time: {comparison_result['summary'].get('execution_time', 'N/A')}</p>
        </div>
        
        <div class="differences">
            <h2>Detailed Differences</h2>
    """
    
    # Add differences tables
    if comparison_result['differences']:
        for diff_type, records in comparison_result['differences'].items():
            if records:
                html_content += f"<h3>{diff_type.replace('_', ' ').title()}</h3>"
                html_content += "<table>"
                
                # Add headers
                if isinstance(records[0], dict):
                    html_content += "<tr>"
                    for key in records[0].keys():
                        html_content += f"<th>{key}</th>"
                    html_content += "</tr>"
                    
                    # Add rows
                    for record in records[:100]:  # Limit to 100 rows
                        html_content += f"<tr class='{diff_type}'>"
                        for value in record.values():
                            html_content += f"<td>{value}</td>"
                        html_content += "</tr>"
                
                html_content += "</table>"
    
    html_content += """
        </div>
    </body>
    </html>
    """
    
    with open(report_path, 'w') as f:
        f.write(html_content)

def generate_excel_comparison_report(comparison_result, report_path):
    """Generate Excel comparison report with multiple sheets."""
    
    
    with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
        # Summary sheet
        summary_df = pd.DataFrame([comparison_result['summary']])
        summary_df.to_excel(writer, sheet_name='Summary', index=False)