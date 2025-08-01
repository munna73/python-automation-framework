"""
Enhanced step definitions for data comparison using config queries
"""
from behave import given, when, then
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# Import your existing modules
try:
    from db.database_manager import DatabaseManager
    from utils.config_loader import config_loader
    from utils.logger import logger, db_logger
    # Import data comparator if it exists, otherwise create basic comparison
    try:
        from utils.data_comparator import data_comparator
    except ImportError:
        data_comparator = None
        logger.warning("data_comparator module not found, using basic comparison")
except ImportError as e:
    print(f"Import warning: {e}")
    print("Please adjust imports to match your existing module structure")


class EnhancedDataCompareSteps:
    """Enhanced data comparison step definitions."""
    
    def __init__(self, context):
        self.context = context
        self.query_results = {}  # Store for query results
        
        # Initialize database manager if not exists
        if not hasattr(context, 'db_manager'):
            context.db_manager = DatabaseManager()
        
        self.db_manager = context.db_manager
    
    def execute_query(self, environment: str, db_type: str, query: str) -> pd.DataFrame:
        """Execute query and return results as DataFrame."""
        try:
            results = self.db_manager.execute_sql_query(query, environment, db_type)
            df = pd.DataFrame(results)
            db_logger.info(f"Query executed on {environment} {db_type}: {len(df)} rows returned")
            return df
        except Exception as e:
            db_logger.error(f"Query execution failed: {e}")
            raise
    
    def basic_dataframe_comparison_with_tolerance(self, source_df: pd.DataFrame, target_df: pd.DataFrame,
                                                key_columns: Optional[List[str]] = None,
                                                numeric_tolerance: float = 0.01,
                                                exclude_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """Enhanced basic comparison with numeric tolerance."""
        exclude_columns = exclude_columns or []
        
        # Filter out excluded columns
        source_cols = [col for col in source_df.columns if col not in exclude_columns]
        target_cols = [col for col in target_df.columns if col not in exclude_columns]
        
        source_filtered = source_df[source_cols] if source_cols else source_df
        target_filtered = target_df[target_cols] if target_cols else target_df
        
        comparison_result = {
            'summary': {
                'source_rows': len(source_filtered),
                'target_rows': len(target_filtered),
                'source_columns': len(source_filtered.columns),
                'target_columns': len(target_filtered.columns),
                'total_differences': 0,
                'source_only': 0,
                'target_only': 0,
                'modified': 0,
                'numeric_tolerance': numeric_tolerance,
                'execution_time': datetime.now().isoformat()
            },
            'differences': {
                'source_only_rows': [],
                'target_only_rows': [],
                'modified_rows': [],
                'column_differences': []
            }
        }
        
        # Compare row counts
        row_diff = abs(len(source_filtered) - len(target_filtered))
        if row_diff > 0:
            comparison_result['summary']['total_differences'] += row_diff
            if len(source_filtered) > len(target_filtered):
                comparison_result['summary']['source_only'] = len(source_filtered) - len(target_filtered)
            else:
                comparison_result['summary']['target_only'] = len(target_filtered) - len(source_filtered)
        
        # Compare column names
        source_col_set = set(source_filtered.columns)
        target_col_set = set(target_filtered.columns)
        
        if source_col_set != target_col_set:
            missing_in_target = source_col_set - target_col_set
            missing_in_source = target_col_set - source_col_set
            
            if missing_in_target:
                comparison_result['differences']['column_differences'].append({
                    'type': 'columns_missing_in_target',
                    'columns': list(missing_in_target)
                })
                comparison_result['summary']['total_differences'] += len(missing_in_target)
            
            if missing_in_source:
                comparison_result['differences']['column_differences'].append({
                    'type': 'columns_missing_in_source',
                    'columns': list(missing_in_source)
                })
                comparison_result['summary']['total_differences'] += len(missing_in_source)
        
        # If we have key columns, perform row-by-row comparison
        if key_columns and all(col in source_filtered.columns for col in key_columns) and \
           all(col in target_filtered.columns for col in key_columns):
            
            # Set key columns as index for easier comparison
            source_indexed = source_filtered.set_index(key_columns)
            target_indexed = target_filtered.set_index(key_columns)
            
            # Find rows only in source
            source_only_idx = source_indexed.index.difference(target_indexed.index)
            if len(source_only_idx) > 0:
                comparison_result['differences']['source_only_rows'] = \
                    source_indexed.loc[source_only_idx].reset_index().to_dict('records')[:50]  # Limit to 50
                comparison_result['summary']['source_only'] += len(source_only_idx)
                comparison_result['summary']['total_differences'] += len(source_only_idx)
            
            # Find rows only in target
            target_only_idx = target_indexed.index.difference(source_indexed.index)
            if len(target_only_idx) > 0:
                comparison_result['differences']['target_only_rows'] = \
                    target_indexed.loc[target_only_idx].reset_index().to_dict('records')[:50]  # Limit to 50
                comparison_result['summary']['target_only'] += len(target_only_idx)
                comparison_result['summary']['total_differences'] += len(target_only_idx)
            
            # Find common rows and check for differences
            common_idx = source_indexed.index.intersection(target_indexed.index)
            if len(common_idx) > 0:
                for idx in common_idx[:100]:  # Limit comparison to 100 rows for performance
                    source_row = source_indexed.loc[idx]
                    target_row = target_indexed.loc[idx]
                    
                    row_differences = []
                    for col in source_row.index:
                        if col in target_row.index:
                            source_val = source_row[col]
                            target_val = target_row[col]
                            
                            # Handle numeric comparison with tolerance
                            if pd.api.types.is_numeric_dtype(type(source_val)) and \
                               pd.api.types.is_numeric_dtype(type(target_val)):
                                if abs(float(source_val) - float(target_val)) > numeric_tolerance:
                                    row_differences.append({
                                        'column': col,
                                        'source_value': source_val,
                                        'target_value': target_val,
                                        'difference': abs(float(source_val) - float(target_val))
                                    })
                            else:
                                # String comparison
                                if str(source_val) != str(target_val):
                                    row_differences.append({
                                        'column': col,
                                        'source_value': source_val,
                                        'target_value': target_val
                                    })
                    
                    if row_differences:
                        modified_row = {'key': idx, 'differences': row_differences}
                        comparison_result['differences']['modified_rows'].append(modified_row)
                        comparison_result['summary']['modified'] += 1
                        comparison_result['summary']['total_differences'] += len(row_differences)
        
        return comparison_result
    
    def replace_query_placeholders(self, query: str) -> str:
        """Replace placeholders in query with actual values."""
        replacements = {
            '$$TODAY$$': datetime.now().strftime('%Y-%m-%d'),
            '$$YESTERDAY$$': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
            '$$START_DATE$$': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
            '$$END_DATE$$': datetime.now().strftime('%Y-%m-%d'),
            '$$LAST_HOUR$$': (datetime.now() - timedelta(hours=1)).isoformat(),
            '$$CURRENT_MONTH$$': datetime.now().strftime('%Y-%m'),
            '$$CURRENT_YEAR$$': datetime.now().strftime('%Y'),
            '$$LAST_MONTH$$': (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m'),
            '$$FIRST_DAY_OF_MONTH$$': datetime.now().replace(day=1).strftime('%Y-%m-%d')
        }
        
        for placeholder, value in replacements.items():
            query = query.replace(placeholder, value)
        
        return query


# Global instance
enhanced_compare_instance = None


def get_enhanced_compare_steps(context):
    """Get or create EnhancedDataCompareSteps instance."""
    global enhanced_compare_instance
    if enhanced_compare_instance is None:
        enhanced_compare_instance = EnhancedDataCompareSteps(context)
    return enhanced_compare_instance


@given('I execute comparison query "{query_key}" from config on source "{source_env}" "{source_db}" and target "{target_env}" "{target_db}"')
def step_execute_comparison_query(context, query_key, source_env, source_db, target_env, target_db):
    """
    Execute the same query from config on both source and target databases.
    
    This is a convenience step that combines reading query and executing on both databases.
    """
    try:
        enhanced_steps = get_enhanced_compare_steps(context)
        
        # Default section for queries
        section = getattr(context, 'query_section', 'DATABASE_QUERIES')
        
        # Get query from config using enhanced config loader
        query = config_loader.get_custom_config(section, query_key)
        
        # Replace any placeholders in query
        query = enhanced_steps.replace_query_placeholders(query)
        
        logger.info(f"Executing comparison query '{query_key}' on source and target")
        db_logger.debug(f"Query: {query}")
        
        # Execute on source
        source_df = enhanced_steps.execute_query(source_env, source_db, query)
        enhanced_steps.query_results[f"source_{query_key}"] = source_df
        
        # Execute on target
        target_df = enhanced_steps.execute_query(target_env, target_db, query)
        enhanced_steps.query_results[f"target_{query_key}"] = target_df
        
        # Store in context for immediate comparison
        context.source_df = source_df
        context.target_df = target_df
        context.comparison_query_key = query_key
        context.source_env = source_env
        context.target_env = target_env
        
        logger.info(f"Source ({source_env}): {len(source_df)} rows, Target ({target_env}): {len(target_df)} rows")
        
    except Exception as e:
        logger.error(f"Failed to execute comparison query: {e}")
        raise


@when('I compare the results with tolerance for numeric columns')
def step_compare_with_tolerance(context):
    """Compare results with tolerance for numeric differences."""
    if not hasattr(context, 'source_df') or not hasattr(context, 'target_df'):
        raise ValueError("No source/target dataframes found. Execute comparison query first")
    
    enhanced_steps = get_enhanced_compare_steps(context)
    
    # Get tolerance from context or use default
    tolerance = float(getattr(context, 'numeric_tolerance', 0.01))
    
    # Get key columns
    key_columns = getattr(context, 'key_columns', None)
    if not key_columns:
        # Try to auto-detect key columns (columns with 'id' in name)
        potential_keys = [col for col in context.source_df.columns if 'id' in col.lower()]
        key_columns = potential_keys[:1] if potential_keys else None
        
        if key_columns:
            logger.info(f"Auto-detected key columns: {key_columns}")
    
    # Perform comparison with tolerance
    if data_comparator and hasattr(data_comparator, 'compare_dataframes_with_tolerance'):
        comparison_result = data_comparator.compare_dataframes_with_tolerance(
            context.source_df,
            context.target_df,
            key_columns=key_columns,
            numeric_tolerance=tolerance,
            exclude_columns=getattr(context, 'exclude_columns', [])
        )
    else:
        # Use basic comparison with tolerance
        comparison_result = enhanced_steps.basic_dataframe_comparison_with_tolerance(
            context.source_df,
            context.target_df,
            key_columns=key_columns,
            numeric_tolerance=tolerance,
            exclude_columns=getattr(context, 'exclude_columns', [])
        )
    
    context.comparison_result = comparison_result
    
    # Log summary
    total_diff = comparison_result['summary']['total_differences']
    logger.info(f"Comparison with tolerance {tolerance}: {total_diff} differences found")
    
    if total_diff > 0:
        logger.warning(f"Differences summary:")
        logger.warning(f"  Source only: {comparison_result['summary']['source_only']}")
        logger.warning(f"  Target only: {comparison_result['summary']['target_only']}")
        logger.warning(f"  Modified: {comparison_result['summary']['modified']}")


@when('I compare the results without tolerance')
def step_compare_without_tolerance(context):
    """Compare results with exact matching (no tolerance)."""
    # Set tolerance to 0 and use the tolerance comparison
    context.numeric_tolerance = 0.0
    step_compare_with_tolerance(context)


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
            with open(report_path, 'w') as f:
                json.dump(context.comparison_result, f, indent=2, default=str)
        elif report_format.lower() == 'csv':
            generate_csv_comparison_report(context.comparison_result, report_path)
        else:
            raise ValueError(f"Unsupported report format: {report_format}. Supported: html, excel, json, csv")
        
        logger.info(f"Generated {report_format.upper()} comparison report: {report_path}")
        
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


@given('I set comparison key columns as "{columns}"')
def step_set_comparison_key_columns(context, columns):
    """Set key columns for row-by-row comparison."""
    context.key_columns = [col.strip() for col in columns.split(',')]
    logger.info(f"Set comparison key columns: {context.key_columns}")


@given('I exclude columns "{columns}" from comparison')
def step_set_exclude_columns_for_comparison(context, columns):
    """Set columns to exclude from comparison."""
    context.exclude_columns = [col.strip() for col in columns.split(',')]
    logger.info(f"Set exclude columns: {context.exclude_columns}")


@then('the comparison should show no differences')
def step_verify_no_differences_in_comparison(context):
    """Verify that comparison found no differences."""
    if not hasattr(context, 'comparison_result'):
        raise ValueError("No comparison result found. Run comparison first")
    
    total_diff = context.comparison_result['summary']['total_differences']
    assert total_diff == 0, \
        f"Expected no differences, but found {total_diff} differences"
    
    logger.info("Comparison verification passed: No differences found")


@then('the comparison should show at most {max_differences:d} differences')
def step_verify_max_differences_in_comparison(context, max_differences):
    """Verify that differences don't exceed maximum threshold."""
    if not hasattr(context, 'comparison_result'):
        raise ValueError("No comparison result found. Run comparison first")
    
    total_diff = context.comparison_result['summary']['total_differences']
    assert total_diff <= max_differences, \
        f"Expected at most {max_differences} differences, but found {total_diff}"
    
    logger.info(f"Comparison verification passed: {total_diff} <= {max_differences} differences")


@then('the source should have {expected_count:d} rows')
def step_verify_source_row_count(context, expected_count):
    """Verify source dataframe row count."""
    if not hasattr(context, 'source_df'):
        raise ValueError("No source dataframe found")
    
    actual_count = len(context.source_df)
    assert actual_count == expected_count, \
        f"Source: expected {expected_count} rows, but got {actual_count}"
    
    logger.info(f"Source row count verification passed: {actual_count} rows")


@then('the target should have {expected_count:d} rows')
def step_verify_target_row_count(context, expected_count):
    """Verify target dataframe row count."""
    if not hasattr(context, 'target_df'):
        raise ValueError("No target dataframe found")
    
    actual_count = len(context.target_df)
    assert actual_count == expected_count, \
        f"Target: expected {expected_count} rows, but got {actual_count}"
    
    logger.info(f"Target row count verification passed: {actual_count} rows")


# Report generation functions
def generate_html_comparison_report(comparison_result: Dict[str, Any], report_path: str):
    """Generate HTML comparison report."""
    summary = comparison_result['summary']
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Enhanced Data Comparison Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .summary {{ background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
            .summary h2 {{ color: #495057; margin-top: 0; }}
            .metric {{ display: inline-block; margin: 10px 20px 10px 0; }}
            .metric-label {{ font-weight: bold; color: #6c757d; }}
            .metric-value {{ font-size: 1.2em; color: #495057; }}
            .differences {{ margin-top: 20px; }}
            table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
            th, td {{ border: 1px solid #dee2e6; padding: 12px; text-align: left; }}
            th {{ background-color: #e9ecef; color: #495057; font-weight: bold; }}
            tr:nth-child(even) {{ background-color: #f8f9fa; }}
            .source-only {{ background-color: #d1ecf1; }}
            .target-only {{ background-color: #f8d7da; }}
            .modified {{ background-color: #fff3cd; }}
            .section {{ margin: 30px 0; }}
            .section h3 {{ color: #495057; border-bottom: 2px solid #dee2e6; padding-bottom: 10px; }}
        </style>
    </head>
    <body>
        <h1>Enhanced Data Comparison Report</h1>
        
        <div class="summary">
            <h2>Comparison Summary</h2>
            <div class="metric">
                <span class="metric-label">Total Differences:</span>
                <span class="metric-value">{summary['total_differences']}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Source Rows:</span>
                <span class="metric-value">{summary['source_rows']}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Target Rows:</span>
                <span class="metric-value">{summary['target_rows']}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Source Only:</span>
                <span class="metric-value">{summary['source_only']}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Target Only:</span>
                <span class="metric-value">{summary['target_only']}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Modified Rows:</span>
                <span class="metric-value">{summary['modified']}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Numeric Tolerance:</span>
                <span class="metric-value">{summary.get('numeric_tolerance', 'N/A')}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Generated:</span>
                <span class="metric-value">{summary.get('execution_time', 'N/A')}</span>
            </div>
        </div>
    """
    
    # Add differences sections
    differences = comparison_result['differences']
    
    if differences.get('source_only_rows'):
        html_content += generate_html_table("Rows Only in Source", differences['source_only_rows'], 'source-only')
    
    if differences.get('target_only_rows'):
        html_content += generate_html_table("Rows Only in Target", differences['target_only_rows'], 'target-only')
    
    if differences.get('modified_rows'):
        html_content += generate_html_modified_table("Modified Rows", differences['modified_rows'])
    
    if differences.get('column_differences'):
        html_content += f"""
        <div class="section">
            <h3>Column Differences</h3>
            <ul>
        """
        for col_diff in differences['column_differences']:
            html_content += f"<li>{col_diff['type']}: {', '.join(col_diff['columns'])}</li>"
        html_content += "</ul></div>"
    
    html_content += """
        </div>
    </body>
    </html>
    """
    
    with open(report_path, 'w') as f:
        f.write(html_content)


def generate_html_table(title: str, records: List[Dict], css_class: str) -> str:
    """Generate HTML table for records."""
    if not records:
        return ""
    
    html = f"""
    <div class="section">
        <h3>{title} ({len(records)} records)</h3>
        <table>
    """
    
    # Add headers
    html += "<tr>"
    for key in records[0].keys():
        html += f"<th>{key}</th>"
    html += "</tr>"
    
    # Add rows (limit to 100 for performance)
    for record in records[:100]:
        html += f"<tr class='{css_class}'>"
        for value in record.values():
            html += f"<td>{value}</td>"
        html += "</tr>"
    
    if len(records) > 100:
        html += f"<tr><td colspan='{len(records[0])}' style='text-align: center; font-style: italic;'>... and {len(records) - 100} more rows</td></tr>"
    
    html += "</table></div>"
    return html


def generate_html_modified_table(title: str, modified_rows: List[Dict]) -> str:
    """Generate HTML table for modified rows."""
    if not modified_rows:
        return ""
    
    html = f"""
    <div class="section">
        <h3>{title} ({len(modified_rows)} records)</h3>
        <table>
            <tr>
                <th>Key</th>
                <th>Column</th>
                <th>Source Value</th>
                <th>Target Value</th>
                <th>Difference</th>
            </tr>
    """
    
    # Add rows (limit to 100)
    for row in modified_rows[:100]:
        key = row['key']
        for diff in row['differences']:
            html += f"""
            <tr class='modified'>
                <td>{key}</td>
                <td>{diff['column']}</td>
                <td>{diff['source_value']}</td>
                <td>{diff['target_value']}</td>
                <td>{diff.get('difference', 'N/A')}</td>
            </tr>
            """
    
    if len(modified_rows) > 100:
        html += f"<tr><td colspan='5' style='text-align: center; font-style: italic;'>... and {len(modified_rows) - 100} more rows</td></tr>"
    
    html += "</table></div>"
    return html


def generate_excel_comparison_report(comparison_result: Dict[str, Any], report_path: str):
    """Generate Excel comparison report with multiple sheets."""
    with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
        # Summary sheet
        summary_df = pd.DataFrame([comparison_result['summary']])
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        differences = comparison_result['differences']
        
        # Source only rows
        if differences.get('source_only_rows'):
            source_only_df = pd.DataFrame(differences['source_only_rows'])
            source_only_df.to_excel(writer, sheet_name='Source Only', index=False)
        
        # Target only rows
        if differences.get('target_only_rows'):
            target_only_df = pd.DataFrame(differences['target_only_rows'])
            target_only_df.to_excel(writer, sheet_name='Target Only', index=False)
        
        # Modified rows (flattened)
        if differences.get('modified_rows'):
            modified_data = []
            for row in differences['modified_rows']:
                key = row['key']
                for diff in row['differences']:
                    modified_data.append({
                        'Key': key,
                        'Column': diff['column'],
                        'Source Value': diff['source_value'],
                        'Target Value': diff['target_value'],
                        'Difference': diff.get('difference', 'N/A')
                    })
            
            if modified_data:
                modified_df = pd.DataFrame(modified_data)
                modified_df.to_excel(writer, sheet_name='Modified Rows', index=False)
        
        # Column differences
        if differences.get('column_differences'):
            col_diff_data = []
            for col_diff in differences['column_differences']:
                for column in col_diff['columns']:
                    col_diff_data.append({
                        'Type': col_diff['type'],
                        'Column': column
                    })
            
            if col_diff_data:
                col_diff_df = pd.DataFrame(col_diff_data)
                col_diff_df.to_excel(writer, sheet_name='Column Differences', index=False)


def generate_csv_comparison_report(comparison_result: Dict[str, Any], report_path: str):
    """Generate CSV summary report."""
    summary_df = pd.DataFrame([comparison_result['summary']])
    summary_df.to_csv(report_path, index=False)


# Cleanup function
def enhanced_data_compare_cleanup(context):
    """Cleanup function for enhanced data comparison steps."""
    global enhanced_compare_instance
    try:
        if enhanced_compare_instance:
            enhanced_compare_instance.query_results.clear()
        db_logger.debug("Enhanced data comparison cleanup completed")
    except Exception as e:
        db_logger.warning(f"Error during enhanced data comparison cleanup: {e}")