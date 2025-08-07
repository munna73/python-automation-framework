import pandas as pd
import cx_Oracle
import psycopg2
from sqlalchemy import create_engine, text
from behave import given, when, then
import os
import re
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any, Union
import openpyxl
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
import json
from pathlib import Path
from datetime import datetime

# Import your existing config loader
from utils.config_loader import ConfigLoader
from utils.custom_exceptions import ConfigurationError

# Import your existing logger
try:
    from utils.logger import logger, test_logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    test_logger = logger


class DatabaseComparisonManager:
    """Enhanced database comparison manager with data cleaning and export capabilities."""
    
    def __init__(self):
        self.config_loader: Optional[ConfigLoader] = None
        self.oracle_engine: Optional[Any] = None
        self.postgres_engine: Optional[Any] = None
        self.source_df: Optional[pd.DataFrame] = None
        self.target_df: Optional[pd.DataFrame] = None
        self.comparison_results: Dict[str, Any] = {}
        self.current_config: Dict[str, Any] = {}
        
        # Create output directory if it doesn't exist
        self.output_dir = Path("data/output")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory ensured: {self.output_dir}")
        
    def _get_output_path(self, filename: str) -> str:
        """Get full path for output file in data/output directory with timestamp"""
        # Extract file name and extension
        file_path = Path(filename)
        name_without_ext = file_path.stem
        extension = file_path.suffix
        
        # Generate timestamp in mmddyyyy_hhmmss format
        timestamp = datetime.now().strftime("%m%d%Y_%H%M%S")
        
        # Create new filename with timestamp
        timestamped_filename = f"{name_without_ext}_{timestamp}{extension}"
        
        return str(self.output_dir / timestamped_filename)
        
    def set_config_loader(self, config_loader: ConfigLoader) -> None:
        """Set the config loader instance"""
        self.config_loader = config_loader
        
    def get_oracle_connection(self, db_section: str) -> Any:
        """Create Oracle database connection using specified section"""
        if self.config_loader is None:
            raise ValueError("Config loader not initialized")
            
        try:
            # Get config section directly using get_custom_config
            db_config = self.config_loader.get_custom_config(db_section)
            
            # Build Oracle connection string
            username = db_config['username']
            password = db_config['password']
            host = db_config['host']
            port = db_config['port']
            service_name = db_config.get('service_name', db_config.get('database', ''))
            
            connection_string = f"{username}/{password}@{host}:{port}/{service_name}"
            self.oracle_engine = create_engine(f"oracle+cx_oracle://{connection_string}")
            logger.info(f"Connected to Oracle database using section: {db_section}")
            return self.oracle_engine
            
        except Exception as e:
            logger.error(f"Failed to connect to Oracle using section {db_section}: {str(e)}")
            raise ConfigurationError(f"Oracle connection failed: {str(e)}")
        
    def get_postgres_connection(self, db_section: str) -> Any:
        """Create PostgreSQL database connection using specified section"""
        if self.config_loader is None:
            raise ValueError("Config loader not initialized")
            
        try:
            # Get config section directly using get_custom_config
            db_config = self.config_loader.get_custom_config(db_section)
            
            # Build PostgreSQL connection string
            username = db_config['username']
            password = db_config['password']
            host = db_config['host']
            port = db_config['port']
            database = db_config['database']
            
            connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
            self.postgres_engine = create_engine(connection_string)
            logger.info(f"Connected to PostgreSQL database using section: {db_section}")
            return self.postgres_engine
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL using section {db_section}: {str(e)}")
            raise ConfigurationError(f"PostgreSQL connection failed: {str(e)}")
        
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame by handling XML, special characters, and CLOB data"""
        if df is None or df.empty:
            return df
            
        cleaned_df = df.copy()
        logger.debug(f"Cleaning data for DataFrame with {len(cleaned_df)} rows and {len(cleaned_df.columns)} columns")
        
        for column in cleaned_df.columns:
            if cleaned_df[column].dtype == 'object':
                logger.debug(f"Cleaning column: {column}")
                
                def clean_value(value):
                    """Clean individual cell value"""
                    if pd.isna(value) or value is None:
                        return ''
                    
                    # Handle CLOB data - convert to string first
                    if hasattr(value, 'read'):  # CLOB object
                        try:
                            value = value.read()
                        except:
                            value = str(value)
                    
                    value_str = str(value)
                    
                    # Handle None/NaN string representations
                    if value_str.lower() in ['none', 'nan', 'null', '<na>']:
                        return ''
                    
                    # Clean XML content - extract text content only
                    if value_str.strip().startswith('<') and ('>' in value_str):
                        try:
                            # Remove XML tags and extract text content
                            xml_cleaned = re.sub(r'<[^>]+>', '', value_str)
                            value_str = xml_cleaned
                        except Exception:
                            # If XML processing fails, continue with original value
                            pass
                    
                    # Remove special control characters but keep useful punctuation
                    # Keep: letters, numbers, spaces, common punctuation
                    value_str = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', value_str)
                    
                    # Replace multiple whitespaces with single space
                    value_str = re.sub(r'\s+', ' ', value_str)
                    
                    # Strip leading/trailing whitespace
                    value_str = value_str.strip()
                    
                    # Handle very long strings (truncate for comparison purposes)
                    if len(value_str) > 32767:  # Excel cell limit
                        value_str = value_str[:32760] + "..."
                        logger.warning(f"Truncated long value in column {column}")
                    
                    return value_str
                
                # Apply cleaning function to the column
                cleaned_df[column] = cleaned_df[column].apply(clean_value)
        
        logger.debug("Data cleaning completed")
        return cleaned_df
        
    def execute_query(self, engine: Any, query: str, connection_type: str = "unknown") -> pd.DataFrame:
        """Execute query and return cleaned DataFrame"""
        if engine is None:
            raise ValueError(f"Database engine is None for {connection_type}. Establish connection first.")
            
        try:
            logger.debug(f"Executing {connection_type} query: {query[:100]}{'...' if len(query) > 100 else ''}")
            
            # Test connection before executing main query
            if "oracle" in connection_type.lower():
                test_query = "SELECT 1 FROM DUAL"
            else:
                test_query = "SELECT 1"
                
            # Test connection
            try:
                test_result = pd.read_sql(text(test_query), engine)
                logger.debug(f"{connection_type} connection test successful")
            except Exception as conn_error:
                logger.error(f"{connection_type} connection test failed: {str(conn_error)}")
                raise RuntimeError(f"{connection_type} connection lost: {str(conn_error)}")
            
            # Execute actual query with proper CLOB handling
            df = pd.read_sql(text(query), engine)
            logger.info(f"{connection_type} query executed successfully. Retrieved {len(df)} rows, {len(df.columns)} columns")
            
            # Clean the data
            cleaned_df = self.clean_data(df)
            return cleaned_df
            
        except Exception as e:
            error_msg = f"Failed to execute {connection_type} query: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
    def compare_dataframes(self, primary_key: str) -> Dict[str, Any]:
        """Compare source and target DataFrames based on primary key"""
        if self.source_df is None:
            raise ValueError("Source DataFrame is None. Load source data first.")
        if self.target_df is None:
            raise ValueError("Target DataFrame is None. Load target data first.")
        if self.source_df.empty:
            raise ValueError("Source DataFrame is empty.")
        if self.target_df.empty:
            raise ValueError("Target DataFrame is empty.")
            
        logger.info(f"Starting comparison with primary key: {primary_key}")
        
        # Ensure primary key exists in both DataFrames
        if primary_key not in self.source_df.columns:
            available_cols = list(self.source_df.columns)
            raise ValueError(f"Primary key '{primary_key}' not found in source DataFrame. Available columns: {available_cols}")
        if primary_key not in self.target_df.columns:
            available_cols = list(self.target_df.columns)
            raise ValueError(f"Primary key '{primary_key}' not found in target DataFrame. Available columns: {available_cols}")
        
        # Convert primary key to string to handle mixed types
        self.source_df[primary_key] = self.source_df[primary_key].astype(str)
        self.target_df[primary_key] = self.target_df[primary_key].astype(str)
        
        # Set primary key as index for easier comparison
        source_indexed = self.source_df.set_index(primary_key)
        target_indexed = self.target_df.set_index(primary_key)
        
        # Find missing records
        source_keys = set(source_indexed.index)
        target_keys = set(target_indexed.index)
        
        missing_in_target = list(source_keys - target_keys)
        missing_in_source = list(target_keys - source_keys)
        common_keys = list(source_keys & target_keys)
        
        logger.info(f"Found {len(missing_in_target)} records missing in target")
        logger.info(f"Found {len(missing_in_source)} records missing in source")
        logger.info(f"Found {len(common_keys)} common records")
        
        # Field-level delta analysis for common records
        field_deltas = {}
        detailed_deltas = []
        
        if common_keys:
            common_source = source_indexed.loc[common_keys]
            common_target = target_indexed.loc[common_keys]
            
            # Compare each column
            for col in common_source.columns:
                if col in common_target.columns:
                    # Convert to string for comparison to handle mixed types
                    source_col = common_source[col].astype(str)
                    target_col = common_target[col].astype(str)
                    
                    # Find differences
                    different_mask = source_col != target_col
                    delta_keys = source_col[different_mask].index.tolist()
                    field_deltas[col] = delta_keys
                    
                    # Store detailed delta information
                    for key in delta_keys:
                        detailed_deltas.append({
                            'primary_key': key,
                            'field': col,
                            'source_value': source_col[key],
                            'target_value': target_col[key]
                        })
                        
                    if delta_keys:
                        logger.debug(f"Field '{col}' has {len(delta_keys)} differences")
        
        self.comparison_results = {
            'missing_in_target': missing_in_target,
            'missing_in_source': missing_in_source,
            'field_deltas': field_deltas,
            'detailed_deltas': detailed_deltas,
            'total_source_records': len(self.source_df),
            'total_target_records': len(self.target_df),
            'common_records': len(common_keys),
            'primary_key': primary_key
        }
        
        logger.info("Comparison completed successfully")
        return self.comparison_results
    
    def export_to_excel(self, filename: str) -> None:
        """Export DataFrames and comparison results to Excel with multiple tabs"""
        output_path = self._get_output_path(filename)
        logger.info(f"Exporting results to Excel file: {output_path}")
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Export source DataFrame
            if self.source_df is not None and not self.source_df.empty:
                self.source_df.to_excel(writer, sheet_name='Source_Data', index=False)
                logger.debug("Source data exported to 'Source_Data' sheet")
            
            # Export target DataFrame
            if self.target_df is not None and not self.target_df.empty:
                self.target_df.to_excel(writer, sheet_name='Target_Data', index=False)
                logger.debug("Target data exported to 'Target_Data' sheet")
            
            # Export comparison summary
            if self.comparison_results:
                self._create_comparison_summary_sheet(writer)
                logger.debug("Comparison summary exported")
                
                # Export detailed deltas if any exist
                if self.comparison_results.get('detailed_deltas'):
                    self._create_detailed_deltas_sheet(writer)
                    logger.debug("Detailed deltas exported")
                    
                # Export missing records details
                self._create_missing_records_sheet(writer)
                logger.debug("Missing records exported")
        
        logger.info(f"Excel export completed: {filename}")
    
    def _create_comparison_summary_sheet(self, writer: pd.ExcelWriter) -> None:
        """Create comparison summary sheet with formatting"""
        results = self.comparison_results
        
        # Create summary data
        summary_data = [
            ['Metric', 'Count'],
            ['Source Records', results['total_source_records']],
            ['Target Records', results['total_target_records']],
            ['Common Records', results['common_records']],
            ['Missing in Target', len(results['missing_in_target'])],
            ['Missing in Source', len(results['missing_in_source'])],
            ['Primary Key Used', results['primary_key']],
            ['', ''],  # Empty row
            ['Field', 'Delta Count']
        ]
        
        # Add field delta counts
        total_field_deltas = 0
        for field, deltas in results['field_deltas'].items():
            delta_count = len(deltas)
            total_field_deltas += delta_count
            summary_data.append([field, delta_count])
        
        # Add total
        summary_data.append(['Total Field Deltas', total_field_deltas])
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False, header=False)
    
    def _create_detailed_deltas_sheet(self, writer: pd.ExcelWriter) -> None:
        """Create detailed deltas sheet"""
        if not self.comparison_results.get('detailed_deltas'):
            return
            
        deltas_df = pd.DataFrame(self.comparison_results['detailed_deltas'])
        deltas_df.columns = ['Primary_Key', 'Field', 'Source_Value', 'Target_Value']
        deltas_df.to_excel(writer, sheet_name='Field_Deltas', index=False)
    
    def _create_missing_records_sheet(self, writer: pd.ExcelWriter) -> None:
        """Create missing records sheet"""
        missing_data = []
        
        # Add missing in target
        for missing_id in self.comparison_results['missing_in_target']:
            missing_data.append({
                'Primary_Key': missing_id,
                'Missing_In': 'Target',
                'Description': 'Record exists in source but not in target'
            })
        
        # Add missing in source
        for missing_id in self.comparison_results['missing_in_source']:
            missing_data.append({
                'Primary_Key': missing_id,
                'Missing_In': 'Source',
                'Description': 'Record exists in target but not in source'
            })
        
        if missing_data:
            missing_df = pd.DataFrame(missing_data)
            missing_df.to_excel(writer, sheet_name='Missing_Records', index=False)
    
    def export_to_csv(self, filename: str, export_type: str = 'summary') -> None:
        """Export results to CSV with different options"""
        logger.info(f"Exporting {export_type} to CSV: {filename}")
        
        if export_type == 'summary':
            self._export_summary_csv(filename)
        elif export_type == 'detailed':
            self._export_detailed_csv(filename)
        elif export_type == 'source':
            if self.source_df is not None:
                self.source_df.to_csv(filename, index=False)
            else:
                raise ValueError("Source DataFrame is None")
        elif export_type == 'target':
            if self.target_df is not None:
                self.target_df.to_csv(filename, index=False)
            else:
                raise ValueError("Target DataFrame is None")
        else:
            raise ValueError(f"Unknown export type: {export_type}")
    
    def _export_summary_csv(self, output_path: str) -> None:
        """Export comparison summary to CSV"""
        if not self.comparison_results:
            raise ValueError("No comparison results available")
            
        results = self.comparison_results
        summary_data = []
        
        # Add summary statistics
        summary_data.append(['Metric', 'Value'])
        summary_data.append(['Source Records', results['total_source_records']])
        summary_data.append(['Target Records', results['total_target_records']])
        summary_data.append(['Common Records', results['common_records']])
        summary_data.append(['Missing in Target', len(results['missing_in_target'])])
        summary_data.append(['Missing in Source', len(results['missing_in_source'])])
        summary_data.append(['Primary Key', results['primary_key']])
        
        # Add field delta summary
        for field, deltas in results['field_deltas'].items():
            summary_data.append([f'Field Deltas - {field}', len(deltas)])
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(output_path, index=False, header=False)
    
    def _export_detailed_csv(self, output_path: str) -> None:
        """Export detailed comparison results to CSV"""
        if not self.comparison_results:
            raise ValueError("No comparison results available")
            
        all_results = []
        results = self.comparison_results
        
        # Add missing records
        for missing_id in results['missing_in_target']:
            all_results.append({
                'Type': 'Missing in Target',
                'Primary_Key': missing_id,
                'Field': 'ALL',
                'Source_Value': 'EXISTS',
                'Target_Value': 'MISSING'
            })
        
        for missing_id in results['missing_in_source']:
            all_results.append({
                'Type': 'Missing in Source',
                'Primary_Key': missing_id,
                'Field': 'ALL',
                'Source_Value': 'MISSING',
                'Target_Value': 'EXISTS'
            })
        
        # Add field deltas
        for delta in results.get('detailed_deltas', []):
            all_results.append({
                'Type': 'Field Delta',
                'Primary_Key': delta['primary_key'],
                'Field': delta['field'],
                'Source_Value': str(delta['source_value'])[:1000],  # Limit length
                'Target_Value': str(delta['target_value'])[:1000]
            })
        
        if all_results:
            results_df = pd.DataFrame(all_results)
            results_df.to_csv(output_path, index=False)
        else:
            # Create empty file with headers
            pd.DataFrame(columns=['Type', 'Primary_Key', 'Field', 'Source_Value', 'Target_Value']).to_csv(output_path, index=False)
    
    def cleanup_connections(self) -> None:
        """Clean up database connections"""
        try:
            if self.oracle_engine:
                self.oracle_engine.dispose()
                self.oracle_engine = None
                logger.debug("Oracle connection cleaned up")
                
            if self.postgres_engine:
                self.postgres_engine.dispose()
                self.postgres_engine = None
                logger.debug("PostgreSQL connection cleaned up")
                
        except Exception as e:
            logger.warning(f"Error during connection cleanup: {e}")


# Global instance
db_comparison_manager = DatabaseComparisonManager()

# Step Definitions

@given('I load configuration from "{config_file}"')
def load_configuration_from_file(context, config_file):
    """Load configuration from specified config file in config directory"""
    try:
        # Initialize ConfigLoader with config directory
        config_loader = ConfigLoader(config_dir="config")
        config_loader.load_config_file(config_file)
        db_comparison_manager.set_config_loader(config_loader)
        context.config_loader = config_loader
        
        # Verify the config file path
        config_path = Path("config") / config_file
        logger.info(f"Configuration loaded from: {config_path}")
        
    except Exception as e:
        raise ConfigurationError(f"Failed to load configuration from 'config/{config_file}': {str(e)}")

@given('I connect to Oracle database using "{db_section}" configuration')
def connect_to_oracle(context, db_section):
    """Establish connection to Oracle database using specified config section"""
    try:
        oracle_engine = db_comparison_manager.get_oracle_connection(db_section)
        
        # Test the connection with a simple query
        test_df = pd.read_sql("SELECT 1 FROM DUAL", oracle_engine)
        
        # Store in both context and manager for reuse
        context.oracle_engine = oracle_engine
        context.oracle_section = db_section
        
        assert context.oracle_engine is not None, f"Failed to connect to Oracle database using section '{db_section}'"
        logger.info(f"Oracle connection validated successfully for section: {db_section}")
        
    except Exception as e:
        logger.error(f"Oracle connection failed: {str(e)}")
        raise

@given('I connect to PostgreSQL database using "{db_section}" configuration')
def connect_to_postgres(context, db_section):
    """Establish connection to PostgreSQL database using specified config section"""
    try:
        postgres_engine = db_comparison_manager.get_postgres_connection(db_section)
        
        # Test the connection with a simple query
        test_df = pd.read_sql("SELECT 1", postgres_engine)
        
        # Store in both context and manager for reuse
        context.postgres_engine = postgres_engine
        context.postgres_section = db_section
        
        assert context.postgres_engine is not None, f"Failed to connect to PostgreSQL database using section '{db_section}'"
        logger.info(f"PostgreSQL connection validated successfully for section: {db_section}")
        
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {str(e)}")
        raise

@when('I read query from config section "{section_name}" key "{query_key}"')
def read_query_from_config(context, section_name, query_key):
    """Read SQL query from configuration file"""
    try:
        if not hasattr(context, 'config_loader') or context.config_loader is None:
            raise ValueError("Configuration not loaded. Load config first.")
        
        query = context.config_loader.get_custom_config(section_name, query_key)
        context.current_query = query
        logger.debug(f"Query loaded from {section_name}.{query_key}")
        
    except Exception as e:
        logger.error(f"Failed to read query from config: {str(e)}")
        raise

@when('I execute query on Oracle and store as source DataFrame')
def execute_oracle_query_as_source(context):
    """Execute current query on Oracle and store as source DataFrame"""
    try:
        if not hasattr(context, 'current_query'):
            raise ValueError("No query loaded. Use 'read query from config' step first")
        if not hasattr(context, 'oracle_engine') or context.oracle_engine is None:
            raise ValueError("Oracle connection not established")
        
        # Use the oracle_engine from context instead of creating a new one
        db_comparison_manager.source_df = db_comparison_manager.execute_query(
            context.oracle_engine, context.current_query
        )
        
        if db_comparison_manager.source_df is not None:
            context.source_record_count = len(db_comparison_manager.source_df)
            logger.info(f"Source DataFrame loaded: {context.source_record_count} records")
        else:
            context.source_record_count = 0
            logger.warning("Source DataFrame is None")
            
    except Exception as e:
        logger.error(f"Failed to execute Oracle query as source: {str(e)}")
        raise

@when('I execute query on PostgreSQL and store as source DataFrame')
def execute_postgres_query_as_source(context):
    """Execute current query on PostgreSQL and store as source DataFrame"""
    try:
        if not hasattr(context, 'current_query'):
            raise ValueError("No query loaded. Use 'read query from config' step first")
        if not hasattr(context, 'postgres_engine') or context.postgres_engine is None:
            raise ValueError("PostgreSQL connection not established")
        
        # Use the postgres_engine from context instead of creating a new one
        db_comparison_manager.source_df = db_comparison_manager.execute_query(
            context.postgres_engine, context.current_query
        )
        
        if db_comparison_manager.source_df is not None:
            context.source_record_count = len(db_comparison_manager.source_df)
            logger.info(f"Source DataFrame loaded: {context.source_record_count} records")
        else:
            context.source_record_count = 0
            logger.warning("Source DataFrame is None")
            
    except Exception as e:
        logger.error(f"Failed to execute PostgreSQL query as source: {str(e)}")
        raise

@when('I execute query on Oracle and store as target DataFrame')
def execute_oracle_query_as_target(context):
    """Execute current query on Oracle and store as target DataFrame"""
    try:
        if not hasattr(context, 'current_query'):
            raise ValueError("No query loaded. Use 'read query from config' step first")
        if not hasattr(context, 'oracle_engine') or context.oracle_engine is None:
            raise ValueError("Oracle connection not established")
        
        # Use the oracle_engine from context instead of creating a new one
        db_comparison_manager.target_df = db_comparison_manager.execute_query(
            context.oracle_engine, context.current_query
        )
        
        if db_comparison_manager.target_df is not None:
            context.target_record_count = len(db_comparison_manager.target_df)
            logger.info(f"Target DataFrame loaded: {context.target_record_count} records")
        else:
            context.target_record_count = 0
            logger.warning("Target DataFrame is None")
            
    except Exception as e:
        logger.error(f"Failed to execute Oracle query as target: {str(e)}")
        raise

@when('I execute query on PostgreSQL and store as target DataFrame')
def execute_postgres_query_as_target(context):
    """Execute current query on PostgreSQL and store as target DataFrame"""
    try:
        if not hasattr(context, 'current_query'):
            raise ValueError("No query loaded. Use 'read query from config' step first")
        if not hasattr(context, 'postgres_engine') or context.postgres_engine is None:
            raise ValueError("PostgreSQL connection not established")
        
        # Use the postgres_engine from context instead of creating a new one
        db_comparison_manager.target_df = db_comparison_manager.execute_query(
            context.postgres_engine, context.current_query
        )
        
        if db_comparison_manager.target_df is not None:
            context.target_record_count = len(db_comparison_manager.target_df)
            logger.info(f"Target DataFrame loaded: {context.target_record_count} records")
        else:
            context.target_record_count = 0
            logger.warning("Target DataFrame is None")
            
    except Exception as e:
        logger.error(f"Failed to execute PostgreSQL query as target: {str(e)}")
        raise

@when('I execute direct query "{query_text}" on Oracle as source')
def execute_oracle_direct_query_source(context, query_text):
    """Execute direct query text on Oracle as source"""
    try:
        if not hasattr(context, 'oracle_engine') or context.oracle_engine is None:
            raise ValueError("Oracle connection not established")
        
        db_comparison_manager.source_df = db_comparison_manager.execute_query(
            context.oracle_engine, query_text
        )
        
        if db_comparison_manager.source_df is not None:
            context.source_record_count = len(db_comparison_manager.source_df)
            logger.info(f"Source DataFrame loaded: {context.source_record_count} records")
        else:
            context.source_record_count = 0
            
    except Exception as e:
        logger.error(f"Failed to execute Oracle direct query as source: {str(e)}")
        raise

@when('I execute direct query "{query_text}" on PostgreSQL as source')
def execute_postgres_direct_query_source(context, query_text):
    """Execute direct query text on PostgreSQL as source"""
    try:
        if not hasattr(context, 'postgres_engine') or context.postgres_engine is None:
            raise ValueError("PostgreSQL connection not established")
        
        db_comparison_manager.source_df = db_comparison_manager.execute_query(
            context.postgres_engine, query_text
        )
        
        if db_comparison_manager.source_df is not None:
            context.source_record_count = len(db_comparison_manager.source_df)
            logger.info(f"Source DataFrame loaded: {context.source_record_count} records")
        else:
            context.source_record_count = 0
            
    except Exception as e:
        logger.error(f"Failed to execute PostgreSQL direct query as source: {str(e)}")
        raise

@when('I execute direct query "{query_text}" on Oracle as target')
def execute_oracle_direct_query_target(context, query_text):
    """Execute direct query text on Oracle as target"""
    try:
        if not hasattr(context, 'oracle_engine') or context.oracle_engine is None:
            raise ValueError("Oracle connection not established")
        
        db_comparison_manager.target_df = db_comparison_manager.execute_query(
            context.oracle_engine, query_text
        )
        
        if db_comparison_manager.target_df is not None:
            context.target_record_count = len(db_comparison_manager.target_df)
            logger.info(f"Target DataFrame loaded: {context.target_record_count} records")
        else:
            context.target_record_count = 0
            
    except Exception as e:
        logger.error(f"Failed to execute Oracle direct query as target: {str(e)}")
        raise

@when('I execute direct query "{query_text}" on PostgreSQL as target')
def execute_postgres_direct_query_target(context, query_text):
    """Execute direct query text on PostgreSQL as target"""
    try:
        if not hasattr(context, 'postgres_engine') or context.postgres_engine is None:
            raise ValueError("PostgreSQL connection not established")
        
        db_comparison_manager.target_df = db_comparison_manager.execute_query(
            context.postgres_engine, query_text
        )
        
        if db_comparison_manager.target_df is not None:
            context.target_record_count = len(db_comparison_manager.target_df)
            logger.info(f"Target DataFrame loaded: {context.target_record_count} records")
        else:
            context.target_record_count = 0
            
    except Exception as e:
        logger.error(f"Failed to execute PostgreSQL direct query as target: {str(e)}")
        raise

@when('I load source data using table from config section "{settings_section}" key "SRCE_TABLE" on Oracle')
def load_source_from_oracle_config_table(context, settings_section):
    """Load source data from Oracle using SRCE_TABLE from specified settings section"""
    try:
        if not hasattr(context, 'config_loader') or context.config_loader is None:
            raise ValueError("Configuration not loaded")
        if not hasattr(context, 'oracle_engine') or context.oracle_engine is None:
            raise ValueError("Oracle connection not established")
            
        source_table = context.config_loader.get_custom_config(settings_section, 'SRCE_TABLE')
        query = f"SELECT * FROM {source_table}"
        
        db_comparison_manager.source_df = db_comparison_manager.execute_query(
            context.oracle_engine, query
        )
        
        if db_comparison_manager.source_df is not None:
            context.source_record_count = len(db_comparison_manager.source_df)
            logger.info(f"Source data loaded from {source_table}: {context.source_record_count} records")
        else:
            context.source_record_count = 0
            
    except Exception as e:
        logger.error(f"Failed to load source data from Oracle: {str(e)}")
        raise

@when('I load target data using table from config section "{settings_section}" key "TRGT_TABLE" on PostgreSQL')
def load_target_from_postgres_config_table(context, settings_section):
    """Load target data from PostgreSQL using TRGT_TABLE from specified settings section"""
    try:
        if not hasattr(context, 'config_loader') or context.config_loader is None:
            raise ValueError("Configuration not loaded")
        if not hasattr(context, 'postgres_engine') or context.postgres_engine is None:
            raise ValueError("PostgreSQL connection not established")
            
        target_table = context.config_loader.get_custom_config(settings_section, 'TRGT_TABLE')
        query = f"SELECT * FROM {target_table}"
        
        db_comparison_manager.target_df = db_comparison_manager.execute_query(
            context.postgres_engine, query
        )
        
        if db_comparison_manager.target_df is not None:
            context.target_record_count = len(db_comparison_manager.target_df)
            logger.info(f"Target data loaded from {target_table}: {context.target_record_count} records")
        else:
            context.target_record_count = 0
            
    except Exception as e:
        logger.error(f"Failed to load target data from PostgreSQL: {str(e)}")
        raise

@when('I load source data using table from config section "{settings_section}" key "SRCE_TABLE" on PostgreSQL')
def load_source_from_postgres_config_table(context, settings_section):
    """Load source data from PostgreSQL using SRCE_TABLE from specified settings section"""
    try:
        if not hasattr(context, 'config_loader') or context.config_loader is None:
            raise ValueError("Configuration not loaded")
        if not hasattr(context, 'postgres_engine') or context.postgres_engine is None:
            raise ValueError("PostgreSQL connection not established")
            
        source_table = context.config_loader.get_custom_config(settings_section, 'SRCE_TABLE')
        query = f"SELECT * FROM {source_table}"
        
        db_comparison_manager.source_df = db_comparison_manager.execute_query(
            context.postgres_engine, query
        )
        
        if db_comparison_manager.source_df is not None:
            context.source_record_count = len(db_comparison_manager.source_df)
            logger.info(f"Source data loaded from {source_table}: {context.source_record_count} records")
        else:
            context.source_record_count = 0
            
    except Exception as e:
        logger.error(f"Failed to load source data from PostgreSQL: {str(e)}")
        raise

@when('I load target data using table from config section "{settings_section}" key "TRGT_TABLE" on Oracle')
def load_target_from_oracle_config_table(context, settings_section):
    """Load target data from Oracle using TRGT_TABLE from specified settings section"""
    try:
        if not hasattr(context, 'config_loader') or context.config_loader is None:
            raise ValueError("Configuration not loaded")
        if not hasattr(context, 'oracle_engine') or context.oracle_engine is None:
            raise ValueError("Oracle connection not established")
            
        target_table = context.config_loader.get_custom_config(settings_section, 'TRGT_TABLE')
        query = f"SELECT * FROM {target_table}"
        
        db_comparison_manager.target_df = db_comparison_manager.execute_query(
            context.oracle_engine, query
        )
        
        if db_comparison_manager.target_df is not None:
            context.target_record_count = len(db_comparison_manager.target_df)
            logger.info(f"Target data loaded from {target_table}: {context.target_record_count} records")
        else:
            context.target_record_count = 0
            
    except Exception as e:
        logger.error(f"Failed to load target data from Oracle: {str(e)}")
        raise

@when('I compare DataFrames using primary key from config section "{settings_section}"')
def compare_dataframes_with_config_primary_key(context, settings_section):
    """Compare DataFrames using primary key from specified settings section"""
    try:
        if not hasattr(context, 'config_loader') or context.config_loader is None:
            raise ValueError("Configuration not loaded")
            
        primary_key = context.config_loader.get_custom_config(settings_section, 'primary_key')
        context.comparison_results = db_comparison_manager.compare_dataframes(primary_key)
        logger.info(f"Comparison completed using primary key: {primary_key}")
        
    except Exception as e:
        logger.error(f"Failed to compare DataFrames: {str(e)}")
        raise

@when('I compare DataFrames using primary key "{primary_key}"')
def compare_dataframes_with_specified_primary_key(context, primary_key):
    """Compare DataFrames using specified primary key"""
    try:
        context.comparison_results = db_comparison_manager.compare_dataframes(primary_key)
        logger.info(f"Comparison completed using primary key: {primary_key}")
        
    except Exception as e:
        logger.error(f"Failed to compare DataFrames: {str(e)}")
        raise

# Verification Steps

@then('the source DataFrame should have "{expected_count:d}" records')
def verify_source_record_count(context, expected_count):
    """Verify source DataFrame record count"""
    if db_comparison_manager.source_df is None:
        raise ValueError("Source DataFrame is None")
    actual_count = len(db_comparison_manager.source_df)
    assert actual_count == expected_count, f"Expected {expected_count} records in source, but got {actual_count}"

@then('the target DataFrame should have "{expected_count:d}" records')
def verify_target_record_count(context, expected_count):
    """Verify target DataFrame record count"""
    if db_comparison_manager.target_df is None:
        raise ValueError("Target DataFrame is None")
    actual_count = len(db_comparison_manager.target_df)
    assert actual_count == expected_count, f"Expected {expected_count} records in target, but got {actual_count}"

@then('there should be "{expected_count:d}" records missing in target')
def verify_missing_in_target_count(context, expected_count):
    """Verify count of records missing in target"""
    if not hasattr(context, 'comparison_results'):
        raise ValueError("No comparison results available")
    actual_count = len(context.comparison_results['missing_in_target'])
    assert actual_count == expected_count, f"Expected {expected_count} records missing in target, but got {actual_count}"

@then('there should be "{expected_count:d}" records missing in source')
def verify_missing_in_source_count(context, expected_count):
    """Verify count of records missing in source"""
    if not hasattr(context, 'comparison_results'):
        raise ValueError("No comparison results available")
    actual_count = len(context.comparison_results['missing_in_source'])
    assert actual_count == expected_count, f"Expected {expected_count} records missing in source, but got {actual_count}"

@then('field "{field_name}" should have "{expected_count:d}" delta records')
def verify_field_delta_count(context, field_name, expected_count):
    """Verify count of delta records for specific field"""
    if not hasattr(context, 'comparison_results'):
        raise ValueError("No comparison results available")
    actual_count = len(context.comparison_results['field_deltas'].get(field_name, []))
    assert actual_count == expected_count, f"Expected {expected_count} delta records for field '{field_name}', but got {actual_count}"

@then('there should be no missing records in either DataFrame')
def verify_no_missing_records(context):
    """Verify no records are missing in either DataFrame"""
    if not hasattr(context, 'comparison_results'):
        raise ValueError("No comparison results available")
    missing_in_target = len(context.comparison_results['missing_in_target'])
    missing_in_source = len(context.comparison_results['missing_in_source'])
    assert missing_in_target == 0, f"Found {missing_in_target} records missing in target"
    assert missing_in_source == 0, f"Found {missing_in_source} records missing in source"

@then('all fields should match between source and target DataFrames')
def verify_all_fields_match(context):
    """Verify all fields have no deltas between DataFrames"""
    if not hasattr(context, 'comparison_results'):
        raise ValueError("No comparison results available")
    field_deltas = context.comparison_results['field_deltas']
    total_deltas = sum(len(deltas) for deltas in field_deltas.values())
    assert total_deltas == 0, f"Found {total_deltas} total field deltas across all columns"

# Export Steps

@then('I export comparison results to CSV file "{filename}"')
def export_comparison_results_to_csv(context, filename):
    """Export detailed comparison results to CSV file"""
    try:
        if not hasattr(context, 'comparison_results'):
            raise ValueError("No comparison results available. Run comparison first.")
        
        db_comparison_manager.export_to_csv(filename, 'detailed')
        logger.info(f"Comparison results exported to CSV: {filename}")
        
    except Exception as e:
        logger.error(f"Failed to export comparison results to CSV: {str(e)}")
        raise

@then('I export comparison summary to CSV file "{filename}"')
def export_comparison_summary_to_csv(context, filename):
    """Export comparison summary to CSV file"""
    try:
        if not hasattr(context, 'comparison_results'):
            raise ValueError("No comparison results available. Run comparison first.")
        
        db_comparison_manager.export_to_csv(filename, 'summary')
        logger.info(f"Comparison summary exported to CSV: {filename}")
        
    except Exception as e:
        logger.error(f"Failed to export comparison summary to CSV: {str(e)}")
        raise

@then('I export all results to Excel file "{filename}"')
def export_all_results_to_excel(context, filename):
    """Export DataFrames and comparison results to Excel with multiple tabs"""
    try:
        if not hasattr(context, 'comparison_results'):
            raise ValueError("No comparison results available. Run comparison first.")
        
        db_comparison_manager.export_to_excel(filename)
        logger.info(f"All results exported to Excel: {filename}")
        
    except Exception as e:
        logger.error(f"Failed to export results to Excel: {str(e)}")
        raise

@then('I export source DataFrame to CSV "{filename}"')
def export_source_to_csv(context, filename):
    """Export source DataFrame to CSV file"""
    try:
        db_comparison_manager.export_to_csv(filename, 'source')
        logger.info(f"Source DataFrame exported to CSV: {filename}")
        
    except Exception as e:
        logger.error(f"Failed to export source DataFrame to CSV: {str(e)}")
        raise

@then('I export target DataFrame to CSV "{filename}"')
def export_target_to_csv(context, filename):
    """Export target DataFrame to CSV file"""
    try:
        db_comparison_manager.export_to_csv(filename, 'target')
        logger.info(f"Target DataFrame exported to CSV: {filename}")
        
    except Exception as e:
        logger.error(f"Failed to export target DataFrame to CSV: {str(e)}")
        raise

@then('I print the comparison summary')
def print_comparison_summary(context):
    """Print detailed comparison summary"""
    try:
        if not hasattr(context, 'comparison_results'):
            raise ValueError("No comparison results available")
            
        results = context.comparison_results
        print("\n" + "="*80)
        print("DATA COMPARISON SUMMARY")
        print("="*80)
        print(f"Primary Key Used: {results['primary_key']}")
        print(f"Source Records: {results['total_source_records']:,}")
        print(f"Target Records: {results['total_target_records']:,}")
        print(f"Common Records: {results['common_records']:,}")
        print(f"Missing in Target: {len(results['missing_in_target']):,}")
        print(f"Missing in Source: {len(results['missing_in_source']):,}")
        
        if results['missing_in_target']:
            display_count = min(10, len(results['missing_in_target']))
            print(f"\nMissing in Target (showing first {display_count}):")
            for i, missing_id in enumerate(results['missing_in_target'][:display_count]):
                print(f"  {i+1}. {missing_id}")
            if len(results['missing_in_target']) > 10:
                print(f"  ... and {len(results['missing_in_target']) - 10} more")
                
        if results['missing_in_source']:
            display_count = min(10, len(results['missing_in_source']))
            print(f"\nMissing in Source (showing first {display_count}):")
            for i, missing_id in enumerate(results['missing_in_source'][:display_count]):
                print(f"  {i+1}. {missing_id}")
            if len(results['missing_in_source']) > 10:
                print(f"  ... and {len(results['missing_in_source']) - 10} more")
        
        print("\nField-Level Deltas:")
        total_field_deltas = 0
        for field, delta_ids in results['field_deltas'].items():
            delta_count = len(delta_ids)
            total_field_deltas += delta_count
            if delta_count > 0:
                print(f"  {field}: {delta_count:,} differences")
                if delta_count <= 5:
                    print(f"    Primary Keys: {delta_ids}")
                else:
                    print(f"    Primary Keys (first 5): {delta_ids[:5]}...")
            else:
                print(f"  {field}: No differences")
        
        print(f"\nTotal Field Deltas: {total_field_deltas:,}")
        
        # Calculate data quality metrics
        if results['total_source_records'] > 0:
            match_rate = (results['common_records'] / results['total_source_records']) * 100
            print(f"Data Match Rate: {match_rate:.2f}%")
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"Failed to print comparison summary: {str(e)}")
        raise

@then('I print DataFrame info for source')
def print_source_dataframe_info(context):
    """Print information about source DataFrame"""
    try:
        if db_comparison_manager.source_df is None:
            print("Source DataFrame is None")
            return
            
        df = db_comparison_manager.source_df
        print(f"\n--- Source DataFrame Info ---")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"Data Types:\n{df.dtypes}")
        print(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        if not df.empty:
            print(f"\nFirst 3 rows:")
            print(df.head(3).to_string())
            
        print("----------------------------")
        
    except Exception as e:
        logger.error(f"Failed to print source DataFrame info: {str(e)}")
        raise

@then('I print DataFrame info for target')
def print_target_dataframe_info(context):
    """Print information about target DataFrame"""
    try:
        if db_comparison_manager.target_df is None:
            print("Target DataFrame is None")
            return
            
        df = db_comparison_manager.target_df
        print(f"\n--- Target DataFrame Info ---")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"Data Types:\n{df.dtypes}")
        print(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        if not df.empty:
            print(f"\nFirst 3 rows:")
            print(df.head(3).to_string())
            
        print("----------------------------")
        
    except Exception as e:
        logger.error(f"Failed to print target DataFrame info: {str(e)}")
        raise

# Additional debugging and connection management steps

@when('I verify Oracle connection is active')
def verify_oracle_connection(context):
    """Verify Oracle connection is still active"""
    try:
        if not hasattr(context, 'oracle_engine') or context.oracle_engine is None:
            raise ValueError("Oracle connection not established")
        
        # Test connection
        test_df = pd.read_sql("SELECT 1 FROM DUAL", context.oracle_engine)
        logger.info("Oracle connection verified successfully")
        
    except Exception as e:
        logger.error(f"Oracle connection verification failed: {str(e)}")
        # Try to reconnect
        if hasattr(context, 'oracle_section'):
            logger.info(f"Attempting to reconnect to Oracle using section: {context.oracle_section}")
            try:
                context.oracle_engine = db_comparison_manager.get_oracle_connection(context.oracle_section)
                test_df = pd.read_sql("SELECT 1 FROM DUAL", context.oracle_engine)
                logger.info("Oracle reconnection successful")
            except Exception as reconnect_error:
                logger.error(f"Oracle reconnection failed: {str(reconnect_error)}")
                raise
        else:
            raise

@when('I verify PostgreSQL connection is active')
def verify_postgres_connection(context):
    """Verify PostgreSQL connection is still active"""
    try:
        if not hasattr(context, 'postgres_engine') or context.postgres_engine is None:
            raise ValueError("PostgreSQL connection not established")
        
        # Test connection
        test_df = pd.read_sql("SELECT 1", context.postgres_engine)
        logger.info("PostgreSQL connection verified successfully")
        
    except Exception as e:
        logger.error(f"PostgreSQL connection verification failed: {str(e)}")
        # Try to reconnect
        if hasattr(context, 'postgres_section'):
            logger.info(f"Attempting to reconnect to PostgreSQL using section: {context.postgres_section}")
            try:
                context.postgres_engine = db_comparison_manager.get_postgres_connection(context.postgres_section)
                test_df = pd.read_sql("SELECT 1", context.postgres_engine)
                logger.info("PostgreSQL reconnection successful")
            except Exception as reconnect_error:
                logger.error(f"PostgreSQL reconnection failed: {str(reconnect_error)}")
                raise
        else:
            raise
def after_scenario(context, scenario):
    """Cleanup after scenario - called from environment.py"""
    try:
        db_comparison_manager.cleanup_connections()
        
        # Reset DataFrames
        db_comparison_manager.source_df = None
        db_comparison_manager.target_df = None
        db_comparison_manager.comparison_results = {}
        
        logger.debug("Database comparison manager cleaned up")
        
    except Exception as e:
        logger.warning(f"Error during database comparison cleanup: {e}")

# Additional utility steps

@when('I validate data quality for source DataFrame')
def validate_source_data_quality(context):
    """Validate data quality metrics for source DataFrame"""
    try:
        if db_comparison_manager.source_df is None:
            raise ValueError("Source DataFrame is None")
            
        df = db_comparison_manager.source_df
        quality_metrics = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_records': df.duplicated().sum(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
        }
        
        context.source_quality_metrics = quality_metrics
        logger.info(f"Source data quality validated. Duplicates: {quality_metrics['duplicate_records']}")
        
    except Exception as e:
        logger.error(f"Failed to validate source data quality: {str(e)}")
        raise

@when('I validate data quality for target DataFrame')
def validate_target_data_quality(context):
    """Validate data quality metrics for target DataFrame"""
    try:
        if db_comparison_manager.target_df is None:
            raise ValueError("Target DataFrame is None")
            
        df = db_comparison_manager.target_df
        quality_metrics = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_records': df.duplicated().sum(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
        }
        
        context.target_quality_metrics = quality_metrics
        logger.info(f"Target data quality validated. Duplicates: {quality_metrics['duplicate_records']}")
        
    except Exception as e:
        logger.error(f"Failed to validate target data quality: {str(e)}")
        raise

@then('source DataFrame should have no duplicate records')
def verify_source_no_duplicates(context):
    """Verify source DataFrame has no duplicate records"""
    if not hasattr(context, 'source_quality_metrics'):
        raise ValueError("Source data quality not validated. Run validation step first.")
    
    duplicate_count = context.source_quality_metrics['duplicate_records']
    assert duplicate_count == 0, f"Found {duplicate_count} duplicate records in source DataFrame"

@then('target DataFrame should have no duplicate records')
def verify_target_no_duplicates(context):
    """Verify target DataFrame has no duplicate records"""
    if not hasattr(context, 'target_quality_metrics'):
        raise ValueError("Target data quality not validated. Run validation step first.")
    
    duplicate_count = context.target_quality_metrics['duplicate_records']
    assert duplicate_count == 0, f"Found {duplicate_count} duplicate records in target DataFrame"

@then('I save comparison results as JSON file "{filename}"')
def save_comparison_results_as_json(context, filename):
    """Save comparison results as JSON file for further analysis"""
    try:
        if not hasattr(context, 'comparison_results'):
            raise ValueError("No comparison results available")
        
        output_path = db_comparison_manager._get_output_path(filename)
        
        # Convert results to JSON-serializable format
        json_results = {
            'summary': {
                'primary_key': context.comparison_results['primary_key'],
                'total_source_records': context.comparison_results['total_source_records'],
                'total_target_records': context.comparison_results['total_target_records'],
                'common_records': context.comparison_results['common_records'],
                'missing_in_target_count': len(context.comparison_results['missing_in_target']),
                'missing_in_source_count': len(context.comparison_results['missing_in_source'])
            },
            'missing_in_target': context.comparison_results['missing_in_target'],
            'missing_in_source': context.comparison_results['missing_in_source'],
            'field_delta_counts': {field: len(deltas) for field, deltas in context.comparison_results['field_deltas'].items()},
            'field_deltas': context.comparison_results['field_deltas']
        }
        
        with open(output_path, 'w') as f:
            json.dump(json_results, f, indent=2, default=str)
        
        logger.info(f"Comparison results saved as JSON: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to save comparison results as JSON: {str(e)}")
        raise
    