import pandas as pd
import cx_Oracle
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from behave import given, when, then
import os
import re
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any, Union, List
import openpyxl
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
import json
from pathlib import Path
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
import threading
from tqdm import tqdm
import hashlib
import warnings

# Import your existing config loader
from utils.config_loader import ConfigLoader, config_loader
from utils.custom_exceptions import ConfigurationError
# Import on-demand configuration helpers
from utils.config_helper import get_config_helper, load_db_config_when_needed, load_config_value_when_needed

# Import your existing logger
try:
    from utils.logger import logger, test_logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    test_logger = logger


class PerformanceMonitor:
    """Performance monitoring utility"""
    
    def __init__(self):
        self.timings = {}
        self.start_times = {}
    
    def start_timer(self, operation: str):
        """Start timing an operation"""
        self.start_times[operation] = time.time()
    
    def end_timer(self, operation: str):
        """End timing an operation and store result"""
        if operation in self.start_times:
            elapsed = time.time() - self.start_times[operation]
            self.timings[operation] = elapsed
            logger.info(f"Operation '{operation}' completed in {elapsed:.2f} seconds")
            return elapsed
        return None
    
    def get_timing(self, operation: str) -> Optional[float]:
        """Get timing for specific operation"""
        return self.timings.get(operation)
    
    def get_all_timings(self) -> Dict[str, float]:
        """Get all recorded timings"""
        return self.timings.copy()


class ConnectionPoolManager:
    """Enhanced connection pool manager"""
    
    def __init__(self):
        self.oracle_pools = {}
        self.postgres_pools = {}
        self._lock = threading.Lock()
    
    def get_oracle_pool(self, connection_config: Dict[str, Any], pool_size: int = 5) -> cx_Oracle.SessionPool:
        """Get or create Oracle connection pool"""
        pool_key = self._generate_pool_key(connection_config)
        
        with self._lock:
            if pool_key not in self.oracle_pools:
                try:
                    dsn = cx_Oracle.makedsn(
                        connection_config['host'],
                        int(connection_config['port']),
                        service_name=connection_config.get('service_name', connection_config.get('database', ''))
                    )
                    
                    self.oracle_pools[pool_key] = cx_Oracle.SessionPool(
                        user=connection_config['username'],
                        password=connection_config['password'],
                        dsn=dsn,
                        min=1,
                        max=pool_size,
                        increment=1,
                        encoding="UTF-8"
                    )
                    logger.info(f"Created Oracle connection pool with {pool_size} connections")
                except Exception as e:
                    logger.error(f"Failed to create Oracle connection pool: {str(e)}")
                    raise
            
            return self.oracle_pools[pool_key]
    
    def get_postgres_engine(self, connection_config: Dict[str, Any], pool_size: int = 5):
        """Get or create PostgreSQL SQLAlchemy engine with connection pooling"""
        pool_key = self._generate_pool_key(connection_config)
        
        with self._lock:
            if pool_key not in self.postgres_pools:
                try:
                    connection_string = (
                        f"postgresql://{connection_config['username']}:"
                        f"{connection_config['password']}@{connection_config['host']}:"
                        f"{connection_config['port']}/{connection_config['database']}"
                    )
                    
                    self.postgres_pools[pool_key] = create_engine(
                        connection_string,
                        poolclass=QueuePool,
                        pool_size=pool_size,
                        max_overflow=10,
                        pool_pre_ping=True,
                        pool_recycle=3600
                    )
                    logger.info(f"Created PostgreSQL connection pool with {pool_size} connections")
                except Exception as e:
                    logger.error(f"Failed to create PostgreSQL connection pool: {str(e)}")
                    raise
            
            return self.postgres_pools[pool_key]
    
    def _generate_pool_key(self, config: Dict[str, Any]) -> str:
        """Generate unique key for connection pool"""
        key_parts = [
            config['host'],
            str(config['port']),
            config['username'],
            config.get('database', config.get('service_name', ''))
        ]
        return hashlib.md5('|'.join(key_parts).encode()).hexdigest()
    
    def cleanup_all_pools(self):
        """Cleanup all connection pools"""
        with self._lock:
            # Cleanup Oracle pools
            for pool_key, pool in self.oracle_pools.items():
                try:
                    pool.close()
                    logger.debug(f"Closed Oracle pool: {pool_key}")
                except Exception as e:
                    logger.warning(f"Error closing Oracle pool {pool_key}: {e}")
            
            # Cleanup PostgreSQL pools
            for pool_key, engine in self.postgres_pools.items():
                try:
                    engine.dispose()
                    logger.debug(f"Disposed PostgreSQL engine: {pool_key}")
                except Exception as e:
                    logger.warning(f"Error disposing PostgreSQL engine {pool_key}: {e}")
            
            self.oracle_pools.clear()
            self.postgres_pools.clear()


class EnhancedDatabaseComparisonManager:
    """Enhanced database comparison manager with improved performance and robustness."""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_loader: Optional[ConfigLoader] = None
        self.oracle_engine: Optional[Any] = None
        self.postgres_engine: Optional[Any] = None
        self.source_df: Optional[pd.DataFrame] = None
        self.target_df: Optional[pd.DataFrame] = None
        self.comparison_results: Dict[str, Any] = {}
        self.current_config: Dict[str, Any] = {}
        
        # Performance monitoring
        self.performance_monitor = PerformanceMonitor()
        self.connection_pool_manager = ConnectionPoolManager()
        
        # Configuration
        self._load_default_config(config_file)
        
        # Compiled regex patterns for performance
        self._regex_patterns = {
            'trailing_zeros': re.compile(r'\.0+$'),
            'xml_tags': re.compile(r'<[^>]+>'),
            'control_chars': re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]'),
            'multiple_spaces': re.compile(r'\s+')
        }
        
        # Create output directory
        self.output_dir = self._get_output_directory()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory ensured: {self.output_dir}")
        
    def _load_default_config(self, config_file: Optional[str] = None):
        """Load default configuration"""
        try:
            # Initialize config_loader with the global instance
            from utils.config_loader import config_loader as global_config_loader
            self.config_loader = global_config_loader
            logger.info("✅ DatabaseComparisonManager config_loader initialized")
            
            if config_file:
                config_path = Path("config") / config_file
                if config_path.exists():
                    logger.info(f"Using config file: {config_path}")
                else:
                    logger.warning(f"Config file not found: {config_path}")
        except Exception as e:
            logger.error(f"Failed to load default config: {e}")
            # Fallback: try to create new config loader instance
            try:
                from utils.config_loader import ConfigLoader
                self.config_loader = ConfigLoader()
                logger.info("✅ DatabaseComparisonManager config_loader initialized via fallback")
            except Exception as fallback_error:
                logger.error(f"Config loader fallback also failed: {fallback_error}")
                self.config_loader = None
    
    def _get_output_directory(self) -> Path:
        """Get configurable output directory"""
        # Try to get from config first, fallback to default
        try:
            if self.config_loader:
                output_dir = self.config_loader.get_custom_config('settings', 'output_directory', default='data/output') # type: ignore
                return Path(output_dir)
        except:
            pass
        return Path("data/output")
        
    def _get_output_path(self, filename: str) -> str:
        """Get full path for output file with timestamp"""
        file_path = Path(filename)
        name_without_ext = file_path.stem
        extension = file_path.suffix
        
        # Generate timestamp in mmddyyyy_hhmmss format
        timestamp = datetime.now().strftime("%m%d%Y_%H%M%S")
        timestamped_filename = f"{name_without_ext}_{timestamp}{extension}"
        
        return str(self.output_dir / timestamped_filename)
        
    def set_config_loader(self, config_loader: ConfigLoader) -> None:
        """Set the config loader instance"""
        self.config_loader = config_loader
        
    def get_oracle_connection(self, db_section: str) -> Any:
        """Create Oracle database connection using connection pooling"""
        if self.config_loader is None:
            raise ValueError("Config loader not initialized")
            
        try:
            self.performance_monitor.start_timer('oracle_connection')
            
            # Get config section
            db_config = self.config_loader.get_custom_config(db_section)
            
            # Get connection from pool
            pool = self.connection_pool_manager.get_oracle_pool(db_config)
            connection = pool.acquire()
            
            # Test the connection
            cursor = connection.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            cursor.close()
            
            self.oracle_engine = connection
            self.performance_monitor.end_timer('oracle_connection')
            logger.info(f"Connected to Oracle database using section: {db_section}")
            return self.oracle_engine
            
        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Oracle database error (Code: {error_obj.code}): {error_obj.message}")
            raise ConfigurationError(f"Oracle connection failed: {error_obj.message}")
        except Exception as e:
            logger.error(f"Failed to connect to Oracle using section {db_section}: {str(e)}")
            raise ConfigurationError(f"Oracle connection failed: {str(e)}")
        
    def get_postgres_connection(self, db_section: str) -> Any:
        """Create PostgreSQL database connection using connection pooling"""
        if self.config_loader is None:
            raise ValueError("Config loader not initialized")
            
        try:
            self.performance_monitor.start_timer('postgres_connection')
            
            # Get config section
            db_config = self.config_loader.get_custom_config(db_section)
            
            # Get engine from pool
            self.postgres_engine = self.connection_pool_manager.get_postgres_engine(db_config)
            
            # Test connection
            with self.postgres_engine.connect() as conn: # type: ignore
                conn.execute(text("SELECT 1"))
            
            self.performance_monitor.end_timer('postgres_connection')
            logger.info(f"Connected to PostgreSQL database using section: {db_section}")
            return self.postgres_engine
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL using section {db_section}: {str(e)}")
            raise ConfigurationError(f"PostgreSQL connection failed: {str(e)}")
        
    def clean_data(self, df: pd.DataFrame, chunk_size: int = 10000) -> pd.DataFrame:
        """Enhanced data cleaning with chunked processing for large datasets"""
        if df is None or df.empty:
            return df
            
        self.performance_monitor.start_timer('data_cleaning')
        logger.info(f"Cleaning data for DataFrame with {len(df)} rows and {len(df.columns)} columns")
        
        # Process in chunks if dataset is large
        if len(df) > chunk_size:
            return self._clean_data_chunked(df, chunk_size)
        else:
            return self._clean_data_single(df)
    
    def _clean_data_chunked(self, df: pd.DataFrame, chunk_size: int) -> pd.DataFrame:
        """Clean data using chunked processing"""
        cleaned_chunks = []
        
        with tqdm(total=len(df), desc="Cleaning data") as pbar:
            for start_idx in range(0, len(df), chunk_size):
                end_idx = min(start_idx + chunk_size, len(df))
                chunk = df.iloc[start_idx:end_idx].copy()
                cleaned_chunk = self._clean_data_single(chunk) # type: ignore
                cleaned_chunks.append(cleaned_chunk)
                pbar.update(len(chunk))
        
        result = pd.concat(cleaned_chunks, ignore_index=True)
        self.performance_monitor.end_timer('data_cleaning')
        return result
    
    def _clean_data_single(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean single DataFrame or chunk"""
        cleaned_df = df.copy()
        
        for column in cleaned_df.columns:
            if cleaned_df[column].dtype == 'object':
                cleaned_df[column] = cleaned_df[column].apply(self._clean_value)
        
        return cleaned_df
    
    def _clean_value(self, value):
        """Enhanced value cleaning with compiled regex patterns"""
        if pd.isna(value) or value is None:
            return ''
        
        # Handle CLOB data
        if hasattr(value, 'read'):
            try:
                value = value.read()
            except:
                value = str(value)
        
        value_str = str(value)
        
        # Handle None/NaN string representations
        if value_str.lower() in ['none', 'nan', 'null', '<na>']:
            return ''
        
        # Clean XML content using compiled regex
        if value_str.strip().startswith('<') and ('>' in value_str):
            try:
                value_str = self._regex_patterns['xml_tags'].sub('', value_str)
            except Exception:
                pass
        
        # Remove control characters using compiled regex
        value_str = self._regex_patterns['control_chars'].sub('', value_str)
        
        # Replace multiple whitespaces using compiled regex
        value_str = self._regex_patterns['multiple_spaces'].sub(' ', value_str)
        
        # Strip whitespace
        value_str = value_str.strip()
        
        # Handle very long strings
        if len(value_str) > 32767:
            value_str = value_str[:32760] + "..."
            logger.warning("Truncated long value in data cleaning")
        
        return value_str
        
    def execute_query(self, engine: Any, query: str, connection_type: str = "unknown", query_key: Optional[str] = None) -> pd.DataFrame:
        """Execute query with enhanced error handling and performance monitoring"""
        if engine is None:
            raise ValueError(f"Database engine is None for {connection_type}. Establish connection first.")
            
        self.performance_monitor.start_timer(f'{connection_type}_query_execution')
        
        try:
            # Enhanced logging with query key information
            query_info = f"query '{query_key}'" if query_key else "query"
            logger.info(f"Executing {connection_type} {query_info}: {query[:100]}{'...' if len(query) > 100 else ''}")
            
            # Enhanced connection handling
            if isinstance(engine, cx_Oracle.Connection):
                df = self._execute_oracle_query(engine, query, connection_type)
            else:
                df = self._execute_postgres_query(engine, query, connection_type)
            
            # Enhanced success logging with query key
            success_msg = f"{connection_type} query"
            if query_key:
                success_msg = f"{connection_type} query '{query_key}'"
            logger.info(f"{success_msg} executed successfully. Retrieved {len(df)} rows, {len(df.columns)} columns")
            
            # Clean the data
            cleaned_df = self.clean_data(df)
            
            self.performance_monitor.end_timer(f'{connection_type}_query_execution')
            return cleaned_df
            
        except Exception as e:
            error_msg = f"Failed to execute {connection_type} query: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
    
    def _execute_oracle_query(self, engine: cx_Oracle.Connection, query: str, connection_type: str) -> pd.DataFrame:
        """Execute Oracle query with specific error handling"""
        try:
            # Test connection
            test_cursor = engine.cursor()
            test_cursor.execute("SELECT 1 FROM DUAL")
            test_cursor.fetchone()
            test_cursor.close()
            
            # Execute main query
            df = pd.read_sql(query, engine)
            return df
            
        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Oracle query error (Code: {error_obj.code}): {error_obj.message}")
            raise RuntimeError(f"Oracle query failed: {error_obj.message}")
    
    def _execute_postgres_query(self, engine, query: str, connection_type: str) -> pd.DataFrame:
        """Execute PostgreSQL query with specific error handling"""
        try:
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            # Execute main query
            df = pd.read_sql(text(query), engine)
            return df
            
        except Exception as e:
            logger.error(f"PostgreSQL query error: {str(e)}")
            raise RuntimeError(f"PostgreSQL query failed: {str(e)}")
   
    def validate_primary_key(self, df: pd.DataFrame, primary_key: str, df_name: str) -> bool:
        """Validate primary key uniqueness and existence"""
        try:
            primary_key_lower = primary_key.lower()
            
            # Check if primary key column exists
            if primary_key_lower not in df.columns.str.lower():
                available_cols = list(df.columns)
                raise ValueError(f"Primary key '{primary_key}' not found in {df_name} DataFrame. Available columns: {available_cols}")
            
            # Get the actual column name (preserve original case)
            actual_col = None
            for col in df.columns:
                if col.lower() == primary_key_lower:
                    actual_col = col
                    break
            
            # Check for duplicates
            duplicate_count = df[actual_col].duplicated().sum()
            if duplicate_count > 0:
                logger.warning(f"Found {duplicate_count} duplicate values in primary key '{actual_col}' for {df_name}")
                return False
            
            # Check for null values
            null_count = df[actual_col].isnull().sum()
            if null_count > 0:
                logger.warning(f"Found {null_count} null values in primary key '{actual_col}' for {df_name}")
                return False
            
            logger.info(f"Primary key '{actual_col}' validation passed for {df_name}")
            return True
            
        except Exception as e:
            logger.error(f"Primary key validation failed for {df_name}: {str(e)}")
            return False

    def compare_dataframes(self, primary_key: str, omit_columns: Optional[List[str]] = None, 
                          omit_values: Optional[List[str]] = None, chunk_size: int = 50000) -> Dict[str, Any]:
        """
        Enhanced DataFrame comparison with performance optimization and validation
        """
        # Input validation
        if self.source_df is None:
            raise ValueError("Source DataFrame is None. Load source data first.")
        if self.target_df is None:
            raise ValueError("Target DataFrame is None. Load target data first.")
        if self.source_df.empty:
            raise ValueError("Source DataFrame is empty.")
        if self.target_df.empty:
            raise ValueError("Target DataFrame is empty.")
        
        self.performance_monitor.start_timer('dataframe_comparison')
        
        logger.info(f"Starting enhanced comparison with primary key: {primary_key}")
        if omit_columns:
            logger.info(f"Omitting columns from comparison: {omit_columns}")
        if omit_values:
            logger.info(f"Treating these values as equal: {omit_values}")
        
        # Validate primary keys
        if not self.validate_primary_key(self.source_df, primary_key, "source"):
            logger.warning("Source primary key validation failed - comparison may be unreliable")
        if not self.validate_primary_key(self.target_df, primary_key, "target"):
            logger.warning("Target primary key validation failed - comparison may be unreliable")
        
        # Normalize column names to lowercase for comparison
        source_df_normalized = self.source_df.copy()
        target_df_normalized = self.target_df.copy()
        
        # Convert all column names to lowercase
        source_df_normalized.columns = source_df_normalized.columns.str.lower()
        target_df_normalized.columns = target_df_normalized.columns.str.lower()
        
        # Normalize primary key name to lowercase
        primary_key_lower = primary_key.lower()
        
        logger.info(f"Normalized column names - Source: {list(source_df_normalized.columns)}")
        logger.info(f"Normalized column names - Target: {list(target_df_normalized.columns)}")
        
        # Validate that both DataFrames have common columns
        source_columns = set(source_df_normalized.columns)
        target_columns = set(target_df_normalized.columns)
        common_columns = source_columns & target_columns
        
        if not common_columns:
            raise ValueError(f"No common columns found between source and target DataFrames.\n"
                           f"Source columns: {list(source_columns)}\n"
                           f"Target columns: {list(target_columns)}")
            
        # Filter to common columns only
        source_df_filtered = source_df_normalized[list(common_columns)].copy()
        target_df_filtered = target_df_normalized[list(common_columns)].copy()
        
        # Ensure primary key exists in both DataFrames
        if primary_key_lower not in source_df_filtered.columns:
            available_cols = list(source_df_filtered.columns)
            raise ValueError(f"Primary key '{primary_key_lower}' not found in source DataFrame. Available columns: {available_cols}")
        if primary_key_lower not in target_df_filtered.columns:
            available_cols = list(target_df_filtered.columns)
            raise ValueError(f"Primary key '{primary_key_lower}' not found in target DataFrame. Available columns: {available_cols}")
        
        # Enhanced type-aware comparison
        source_df_processed, target_df_processed = self._prepare_dataframes_for_comparison(
            source_df_filtered, target_df_filtered # type: ignore
        )
        
        # Find missing records using primary key
        source_keys = set(source_df_processed[primary_key_lower])
        target_keys = set(target_df_processed[primary_key_lower])
        
        missing_in_target = list(source_keys - target_keys)
        missing_in_source = list(target_keys - source_keys)
        common_keys = list(source_keys & target_keys)
        
        logger.info(f"Found {len(missing_in_target)} records missing in target")
        logger.info(f"Found {len(missing_in_source)} records missing in source")
        logger.info(f"Found {len(common_keys)} common records")
        
        # Set primary key as index for easier comparison
        source_indexed = source_df_processed.set_index(primary_key_lower)
        target_indexed = target_df_processed.set_index(primary_key_lower)
        
        # Determine columns to compare (exclude primary key and omit_columns)
        columns_to_compare = [col for col in common_columns if col != primary_key_lower]
        
        # Handle omit_columns (convert to lowercase and filter out)
        if omit_columns is not None:
            omit_columns_lower = {col.lower() for col in omit_columns}
            columns_to_compare = [col for col in columns_to_compare if col not in omit_columns_lower]
            logger.info(f"After omitting columns, comparing: {columns_to_compare}")
        
        # Prepare omit_values for case-insensitive comparison
        omit_values_lower = set()
        if omit_values is not None:
            omit_values_lower = {str(val).lower() for val in omit_values}
        
        # Enhanced field-level delta analysis
        field_deltas, detailed_deltas = self._perform_field_comparison(
            source_indexed, target_indexed, common_keys, columns_to_compare, 
            omit_values_lower, chunk_size
        )
        
        # Analyze delta results and provide comprehensive feedback
        total_deltas = sum(len(deltas) for deltas in field_deltas.values())
        fields_with_deltas = sum(1 for deltas in field_deltas.values() if len(deltas) > 0)
        fields_without_deltas = len(field_deltas) - fields_with_deltas
        
        # Log delta analysis results
        if total_deltas == 0:
            logger.info(f"🎉 PERFECT MATCH: No field-level differences found across {len(columns_to_compare)} columns and {len(common_keys)} records")
            logger.info(f"All {len(columns_to_compare)} compared fields are identical between source and target")
        else:
            logger.info(f"Field-level delta analysis: {total_deltas} total differences found")
            logger.info(f"Fields with differences: {fields_with_deltas}/{len(columns_to_compare)}")
            logger.info(f"Fields with perfect match: {fields_without_deltas}/{len(columns_to_compare)}")
            
            # Log details about fields with differences
            for field, deltas in field_deltas.items():
                if deltas:
                    logger.info(f"  - Field '{field}': {len(deltas)} records differ")
        
        # Create missing records details for export
        missing_records_details = []
        
        # Add missing in target
        for missing_id in missing_in_target:
            missing_records_details.append({
                'primary_key': missing_id,
                'missing_in': 'Target',
                'table_name': 'target_table'
            })
        
        # Add missing in source  
        for missing_id in missing_in_source:
            missing_records_details.append({
                'primary_key': missing_id,
                'missing_in': 'Source',
                'table_name': 'source_table'
            })
        
        # Store comprehensive comparison results
        self.comparison_results = {
            'missing_in_target': missing_in_target,
            'missing_in_source': missing_in_source,
            'missing_records_details': missing_records_details,
            'field_deltas': field_deltas,
            'detailed_deltas': detailed_deltas,
            'total_source_records': len(self.source_df),
            'total_target_records': len(self.target_df),
            'common_records': len(common_keys),
            'primary_key': primary_key_lower,
            'common_columns': list(common_columns),
            'columns_compared': columns_to_compare,
            'omitted_columns': omit_columns or [],
            'omitted_values': omit_values or [],
            'source_only_columns': list(source_columns - target_columns),
            'target_only_columns': list(target_columns - source_columns),
            # Enhanced delta analysis metadata
            'delta_summary': {
                'total_field_differences': total_deltas,
                'fields_with_differences': fields_with_deltas,
                'fields_without_differences': fields_without_deltas,
                'fields_compared_count': len(columns_to_compare),
                'perfect_match': total_deltas == 0 and len(missing_in_target) == 0 and len(missing_in_source) == 0,
                'field_match_percentage': round((fields_without_deltas / len(columns_to_compare) * 100), 2) if columns_to_compare else 100,
                'record_match_percentage': round((len(common_keys) / max(len(self.source_df), len(self.target_df)) * 100), 2) if max(len(self.source_df), len(self.target_df)) > 0 else 100
            },
            'performance_metrics': self.performance_monitor.get_all_timings(),
            'comparison_timestamp': datetime.now().isoformat()
        }
        
        # Final comparison status logging
        if self.comparison_results['delta_summary']['perfect_match']:
            logger.info("🎉 COMPARISON RESULT: PERFECT MATCH - All records and fields are identical!")
        else:
            missing_total = len(missing_in_target) + len(missing_in_source)
            logger.info(f"📊 COMPARISON RESULT: {total_deltas} field differences, {missing_total} missing records")
            logger.info(f"   Field match rate: {self.comparison_results['delta_summary']['field_match_percentage']}%")
            logger.info(f"   Record match rate: {self.comparison_results['delta_summary']['record_match_percentage']}%")
        
        self.performance_monitor.end_timer('dataframe_comparison')
        logger.info("Enhanced comparison completed successfully")
        return self.comparison_results
    
    def _prepare_dataframes_for_comparison(self, source_df: pd.DataFrame, target_df: pd.DataFrame) -> tuple:
        """Prepare DataFrames for comparison with numeric type normalization"""
        self.performance_monitor.start_timer('dataframe_preparation')
        
        # Enhanced type-aware conversion with numeric normalization
        source_processed = source_df.copy()
        target_processed = target_df.copy()
        
        for col in source_processed.columns:
            if col in target_processed.columns:
                source_dtype = source_processed[col].dtype
                target_dtype = target_processed[col].dtype
                
                # Handle numeric columns with potential int/float differences (1234 vs 1234.0)
                if (source_dtype in ['int64', 'float64', 'int32', 'float32'] or 
                    target_dtype in ['int64', 'float64', 'int32', 'float32']):
                    
                    logger.debug(f"Normalizing numeric column '{col}': source_dtype={source_dtype}, target_dtype={target_dtype}")
                    
                    # Convert both to string and normalize numeric representation
                    source_processed[col] = self._normalize_numeric_values(source_processed[col])
                    target_processed[col] = self._normalize_numeric_values(target_processed[col])
                    
                else:
                    # Convert to string for consistent comparison
                    source_processed[col] = source_processed[col].astype(str)
                    target_processed[col] = target_processed[col].astype(str)
                    
                    # Handle non-numeric precision issues using compiled regex
                    source_processed[col] = source_processed[col].apply(
                        lambda x: self._regex_patterns['trailing_zeros'].sub('', x)
                    )
                    target_processed[col] = target_processed[col].apply(
                        lambda x: self._regex_patterns['trailing_zeros'].sub('', x)
                    )
        
        self.performance_monitor.end_timer('dataframe_preparation')
        return source_processed, target_processed
    
    def _normalize_numeric_values(self, series: pd.Series) -> pd.Series:
        """Normalize numeric values to handle int/float representation differences"""
        try:
            # Convert to string first
            str_series = series.astype(str)
            
            # For each value, try to normalize numeric representation
            normalized_series = str_series.apply(self._normalize_single_numeric_value)
            
            logger.debug(f"Normalized {len(series)} numeric values in column")
            return normalized_series
            
        except Exception as e:
            logger.warning(f"Failed to normalize numeric series: {e}, falling back to string conversion")
            return series.astype(str)
    
    def _normalize_single_numeric_value(self, value: str) -> str:
        """Normalize a single numeric value string"""
        try:
            # Handle NaN, None, empty values
            if value in ['nan', 'None', '', 'NaN', 'null', 'NULL']:
                return value
            
            # Try to parse as float and convert back to remove trailing zeros
            try:
                # Parse as float to handle both int and float representations
                numeric_val = float(value)
                
                # Check if it's actually an integer value (no fractional part)
                if numeric_val.is_integer():
                    # Return as integer string to avoid .0 suffix
                    return str(int(numeric_val))
                else:
                    # Return as float but remove trailing zeros
                    formatted = f"{numeric_val:g}"  # %g format removes trailing zeros
                    return formatted
                    
            except (ValueError, OverflowError):
                # If parsing fails, apply regex to remove trailing zeros
                return self._regex_patterns['trailing_zeros'].sub('', value)
                
        except Exception as e:
            # If all else fails, return original value
            logger.debug(f"Could not normalize numeric value '{value}': {e}")
            return value
    
    def _perform_field_comparison(self, source_indexed: pd.DataFrame, target_indexed: pd.DataFrame,
                                 common_keys: List, columns_to_compare: List, omit_values_lower: set,
                                 chunk_size: int) -> tuple:
        """Perform field-level comparison with chunked processing"""
        self.performance_monitor.start_timer('field_comparison')
        
        field_deltas = {}
        detailed_deltas = []
        
        if not common_keys or not columns_to_compare:
            self.performance_monitor.end_timer('field_comparison')
            return field_deltas, detailed_deltas
        
        logger.info(f"Comparing {len(columns_to_compare)} columns across {len(common_keys)} records")
        
        # Process in chunks for large datasets
        if len(common_keys) > chunk_size:
            return self._perform_chunked_field_comparison(
                source_indexed, target_indexed, common_keys, columns_to_compare, 
                omit_values_lower, chunk_size
            )
        
        # Process all at once for smaller datasets
        common_source = source_indexed.loc[common_keys]
        common_target = target_indexed.loc[common_keys]
        
        with tqdm(total=len(columns_to_compare), desc="Comparing fields") as pbar:
            for col in columns_to_compare:
                delta_keys = []
                
                for key in common_keys:
                    source_val = str(common_source.loc[key, col])
                    target_val = str(common_target.loc[key, col])
                    
                    if not self._values_are_equal(source_val, target_val, omit_values_lower):
                        delta_keys.append(key)
                        detailed_deltas.append({
                            'primary_key': key,
                            'field': col,
                            'source_value': source_val,
                            'target_value': target_val
                        })
                
                field_deltas[col] = delta_keys
                if delta_keys:
                    logger.debug(f"Field '{col}' has {len(delta_keys)} differences")
                else:
                    logger.debug(f"Field '{col}' is identical across all {len(common_keys)} records")
                
                pbar.update(1)
        
        # Summary logging for field comparison results
        total_differences = sum(len(deltas) for deltas in field_deltas.values())
        fields_with_differences = sum(1 for deltas in field_deltas.values() if deltas)
        
        if total_differences == 0:
            logger.info(f"✅ Field comparison complete: All {len(columns_to_compare)} fields match perfectly")
        else:
            logger.info(f"Field comparison complete: {fields_with_differences} fields have differences, {len(columns_to_compare) - fields_with_differences} fields match perfectly")
        
        self.performance_monitor.end_timer('field_comparison')
        return field_deltas, detailed_deltas
    
    def _perform_chunked_field_comparison(self, source_indexed: pd.DataFrame, target_indexed: pd.DataFrame,
                                        common_keys: List, columns_to_compare: List, omit_values_lower: set,
                                        chunk_size: int) -> tuple:
        """Perform field comparison using chunked processing"""
        field_deltas = {col: [] for col in columns_to_compare}
        detailed_deltas = []
        
        # Process keys in chunks
        key_chunks = [common_keys[i:i + chunk_size] for i in range(0, len(common_keys), chunk_size)]
        
        with tqdm(total=len(key_chunks) * len(columns_to_compare), desc="Comparing fields (chunked)") as pbar:
            for chunk_keys in key_chunks:
                chunk_source = source_indexed.loc[chunk_keys]
                chunk_target = target_indexed.loc[chunk_keys]
                
                for col in columns_to_compare:
                    chunk_deltas = []
                    
                    for key in chunk_keys:
                        source_val = str(chunk_source.loc[key, col])
                        target_val = str(chunk_target.loc[key, col])
                        
                        if not self._values_are_equal(source_val, target_val, omit_values_lower):
                            chunk_deltas.append(key)
                            detailed_deltas.append({
                                'primary_key': key,
                                'field': col,
                                'source_value': source_val,
                                'target_value': target_val
                            })
                    
                    field_deltas[col].extend(chunk_deltas)
                    pbar.update(1)
        
        # Summary logging for chunked field comparison results
        total_differences = sum(len(deltas) for deltas in field_deltas.values())
        fields_with_differences = sum(1 for deltas in field_deltas.values() if deltas)
        
        if total_differences == 0:
            logger.info(f"✅ Chunked field comparison complete: All {len(columns_to_compare)} fields match perfectly across {len(common_keys)} records")
        else:
            logger.info(f"Chunked field comparison complete: {fields_with_differences} fields have differences, {len(columns_to_compare) - fields_with_differences} fields match perfectly")
        
        return field_deltas, detailed_deltas
    
    def _values_are_equal(self, source_val: str, target_val: str, omit_values_lower: set) -> bool:
        """Check if two values are equal considering omit_values"""
        # Normalize for omit_values comparison (case-insensitive)
        source_val_norm = source_val.lower()
        target_val_norm = target_val.lower()
        
        # Check if both values are in omit_values (treat as equal)
        is_omitted_pair = (source_val_norm in omit_values_lower and 
                          target_val_norm in omit_values_lower)
        
        # Compare values
        return source_val == target_val or is_omitted_pair
    
    def export_to_excel(self, filename: str) -> None:
        """Enhanced Excel export with performance tracking"""
        self.performance_monitor.start_timer('excel_export')
        output_path = self._get_output_path(filename)
        logger.info(f"Exporting results to Excel file: {output_path}")
        
        try:
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
                    
                    # Export performance metrics
                    self._create_performance_metrics_sheet(writer)
                    logger.debug("Performance metrics exported")
        
            self.performance_monitor.end_timer('excel_export')
            logger.info(f"Excel export completed: {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to export to Excel: {str(e)}")
            raise
    
    def _create_comparison_summary_sheet(self, writer: pd.ExcelWriter) -> None:
        """Create enhanced comparison summary sheet with formatting"""
        results = self.comparison_results
        
        # Create summary data
        summary_data = [
            ['Metric', 'Value'],
            ['Comparison Timestamp', results.get('comparison_timestamp', 'N/A')],
            ['Source Records', results['total_source_records']],
            ['Target Records', results['total_target_records']],
            ['Common Records', results['common_records']],
            ['Missing in Target', len(results['missing_in_target'])],
            ['Missing in Source', len(results['missing_in_source'])],
            ['Primary Key Used', results['primary_key']],
            ['Common Columns', ', '.join(results['common_columns'])],
            ['Columns Compared', ', '.join(results['columns_compared'])],
            ['Omitted Columns', ', '.join(results['omitted_columns'])],
            ['Omitted Values', ', '.join(results['omitted_values'])],
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
        
        # Calculate data quality metrics
        if results['total_source_records'] > 0:
            match_rate = (results['common_records'] / results['total_source_records']) * 100
            summary_data.append(['Data Match Rate (%)', f"{match_rate:.2f}"])
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False, header=False)
    
    def _create_detailed_deltas_sheet(self, writer: pd.ExcelWriter) -> None:
        """Create enhanced detailed deltas sheet"""
        if not self.comparison_results.get('detailed_deltas'):
            return
            
        deltas_df = pd.DataFrame(self.comparison_results['detailed_deltas'])
        deltas_df.columns = ['Primary_Key', 'Field', 'Source_Value', 'Target_Value']
        
        # Add delta type classification
        deltas_df['Delta_Type'] = deltas_df.apply(self._classify_delta_type, axis=1)
        
        # Sort for better organization
        deltas_df = deltas_df.sort_values(['Field', 'Primary_Key'])
        
        deltas_df.to_excel(writer, sheet_name='Field_Deltas', index=False)
    
    def _classify_delta_type(self, row) -> str:
        """Classify the type of delta for better analysis"""
        source_val = str(row['Source_Value']).strip()
        target_val = str(row['Target_Value']).strip()
        
        if source_val == '' and target_val != '':
            return 'Source_Empty'
        elif source_val != '' and target_val == '':
            return 'Target_Empty'
        elif source_val.lower() in ['null', 'none', 'nan'] or target_val.lower() in ['null', 'none', 'nan']:
            return 'Null_Difference'
        elif source_val.isdigit() and target_val.isdigit():
            return 'Numeric_Difference'
        elif len(source_val) != len(target_val):
            return 'Length_Difference'
        else:
            return 'Value_Difference'
    
    def _create_missing_records_sheet(self, writer: pd.ExcelWriter) -> None:
        """Create enhanced missing records sheet"""
        missing_data = []
        
        # Add missing in target
        for missing_id in self.comparison_results['missing_in_target']:
            missing_data.append({
                'Primary_Key': missing_id,
                'Missing_In': 'Target',
                'Impact': 'Source has extra record',
                'Recommendation': 'Investigate why record exists in source but not target'
            })
        
        # Add missing in source
        for missing_id in self.comparison_results['missing_in_source']:
            missing_data.append({
                'Primary_Key': missing_id,
                'Missing_In': 'Source',
                'Impact': 'Target has extra record',
                'Recommendation': 'Investigate why record exists in target but not source'
            })
        
        if missing_data:
            missing_df = pd.DataFrame(missing_data)
            missing_df.to_excel(writer, sheet_name='Missing_Records', index=False)
    
    def _create_performance_metrics_sheet(self, writer: pd.ExcelWriter) -> None:
        """Create performance metrics sheet"""
        if not self.comparison_results.get('performance_metrics'):
            return
        
        metrics_data = []
        for operation, duration in self.comparison_results['performance_metrics'].items():
            metrics_data.append({
                'Operation': operation,
                'Duration_Seconds': round(duration, 3),
                'Duration_Minutes': round(duration / 60, 3)
            })
        
        if metrics_data:
            metrics_df = pd.DataFrame(metrics_data)
            metrics_df = metrics_df.sort_values('Duration_Seconds', ascending=False)
            metrics_df.to_excel(writer, sheet_name='Performance_Metrics', index=False)
    
    def export_to_csv(self, filename: str, export_type: str = 'summary') -> None:
        """Enhanced CSV export with performance tracking"""
        self.performance_monitor.start_timer(f'csv_export_{export_type}')
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create full path with timestamp
        if not filename.endswith('.csv'):
            filename += '.csv'
        full_path = self._get_output_path(filename)
        
        logger.info(f"Exporting {export_type} to CSV: {full_path}")
        
        try:
            if export_type == 'summary':
                self._export_summary_csv(str(full_path))
            elif export_type == 'detailed':
                self._export_detailed_csv(str(full_path))
            elif export_type == 'source':
                if self.source_df is not None:
                    self.source_df.to_csv(str(full_path), index=False)
                    logger.info(f"Source data exported to: {full_path}")
                else:
                    raise ValueError("Source DataFrame is None")
            elif export_type == 'target':
                if self.target_df is not None:
                    self.target_df.to_csv(str(full_path), index=False)
                    logger.info(f"Target data exported to: {full_path}")
                else:
                    raise ValueError("Target DataFrame is None")
            elif export_type == 'performance':
                self._export_performance_csv(str(full_path))
            else:
                raise ValueError(f"Unknown export type: {export_type}")
                
            self.performance_monitor.end_timer(f'csv_export_{export_type}')
            
        except Exception as e:
            logger.error(f"Failed to export {export_type} to {full_path}: {str(e)}")
            raise

    def _export_summary_csv(self, output_path: str) -> None:
        """Export enhanced comparison summary to CSV"""
        try:
            if not self.comparison_results:
                raise ValueError("No comparison results available")
                
            results = self.comparison_results
            summary_data = []
            
            # Add metadata
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            summary_data.append(['Export Timestamp', timestamp])
            summary_data.append(['Comparison Timestamp', results.get('comparison_timestamp', 'N/A')])
            summary_data.append(['', ''])  # Separator
            
            # Add basic metrics
            summary_data.extend([
                ['Metric', 'Value'],
                ['Source Records', results['total_source_records']],
                ['Target Records', results['total_target_records']],
                ['Common Records', results['common_records']],
                ['Missing in Target', len(results['missing_in_target'])],
                ['Missing in Source', len(results['missing_in_source'])],
                ['Primary Key', results['primary_key']],
                ['Common Columns', ', '.join(results['common_columns'])],
                ['Columns Compared', ', '.join(results['columns_compared'])],
                ['Omitted Columns', ', '.join(results['omitted_columns'])],
                ['Omitted Values', ', '.join(results['omitted_values'])],
                ['', '']  # Separator
            ])
            
            # Add field delta summary
            summary_data.append(['Field', 'Delta Count'])
            total_field_deltas = 0
            for field, deltas in results['field_deltas'].items():
                delta_count = len(deltas)
                total_field_deltas += delta_count
                summary_data.append([field, delta_count])
            
            summary_data.append(['Total Field Deltas', total_field_deltas])
            
            # Add quality metrics
            if results['total_source_records'] > 0:
                match_rate = (results['common_records'] / results['total_source_records']) * 100
                summary_data.append(['Data Match Rate (%)', f"{match_rate:.2f}"])
            
            # Add performance summary if available
            if results.get('performance_metrics'):
                summary_data.append(['', ''])  # Separator
                summary_data.append(['Performance Summary', ''])
                total_time = sum(results['performance_metrics'].values())
                summary_data.append(['Total Processing Time (seconds)', f"{total_time:.2f}"])
            
            # Create DataFrame and export
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_csv(output_path, index=False, header=False)
            logger.info(f"Enhanced summary exported successfully to: {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to export summary: {str(e)}")
            raise
    
    def _export_detailed_csv(self, output_path: str) -> None:
        """Export enhanced detailed comparison results to CSV"""
        try:
            if not self.comparison_results:
                raise ValueError("No comparison results available")
                
            all_results = []
            results = self.comparison_results
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Add missing records in target
            for missing_id in results['missing_in_target']:
                all_results.append({
                    'Timestamp': timestamp,
                    'Type': 'Missing in Target',
                    'Primary_Key': str(missing_id),
                    'Field': 'ALL',
                    'Source_Value': 'EXISTS',
                    'Target_Value': 'MISSING',
                    'Delta_Type': 'Missing_Record',
                    'Description': f"Record with key '{missing_id}' exists in source but missing in target"
                })
            
            # Add missing records in source
            for missing_id in results['missing_in_source']:
                all_results.append({
                    'Timestamp': timestamp,
                    'Type': 'Missing in Source',
                    'Primary_Key': str(missing_id),
                    'Field': 'ALL',
                    'Source_Value': 'MISSING',
                    'Target_Value': 'EXISTS',
                    'Delta_Type': 'Missing_Record',
                    'Description': f"Record with key '{missing_id}' exists in target but missing in source"
                })
            
            # Add field deltas with enhanced classification
            for delta in results.get('detailed_deltas', []):
                delta_type = self._classify_delta_type(delta)
                all_results.append({
                    'Timestamp': timestamp,
                    'Type': 'Field Delta',
                    'Primary_Key': str(delta['primary_key']),
                    'Field': str(delta['field']),
                    'Source_Value': str(delta['source_value'])[:1000],  # Limit length
                    'Target_Value': str(delta['target_value'])[:1000],
                    'Delta_Type': delta_type,
                    'Description': f"Field '{delta['field']}' differs between source and target"
                })
            
            if all_results:
                results_df = pd.DataFrame(all_results)
                # Sort by Type, Field, and Primary_Key for better organization
                results_df = results_df.sort_values(['Type', 'Field', 'Primary_Key'])
                results_df.to_csv(output_path, index=False)
                logger.info(f"Enhanced detailed results exported successfully to: {output_path} ({len(all_results)} records)")
            else:
                # Create empty file with headers
                empty_df = pd.DataFrame(columns=[
                    'Timestamp', 'Type', 'Primary_Key', 'Field', 
                    'Source_Value', 'Target_Value', 'Delta_Type', 'Description'
                ])
                empty_df.to_csv(output_path, index=False)
                logger.info(f"No differences found. Empty detailed results file created: {output_path}")
                
        except Exception as e:
            logger.error(f"Failed to export detailed results: {str(e)}")
            raise
    
    def _export_performance_csv(self, output_path: str) -> None:
        """Export performance metrics to CSV"""
        try:
            if not self.comparison_results.get('performance_metrics'):
                logger.warning("No performance metrics available to export")
                return
            
            performance_data = []
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            for operation, duration in self.comparison_results['performance_metrics'].items():
                performance_data.append({
                    'Timestamp': timestamp,
                    'Operation': operation,
                    'Duration_Seconds': round(duration, 3),
                    'Duration_Minutes': round(duration / 60, 3),
                    'Performance_Category': self._categorize_operation(operation)
                })
            
            if performance_data:
                perf_df = pd.DataFrame(performance_data)
                perf_df = perf_df.sort_values('Duration_Seconds', ascending=False)
                perf_df.to_csv(output_path, index=False)
                logger.info(f"Performance metrics exported to: {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to export performance metrics: {str(e)}")
            raise
    
    def _categorize_operation(self, operation: str) -> str:
        """Categorize operations for performance analysis"""
        if 'connection' in operation.lower():
            return 'Database_Connection'
        elif 'query' in operation.lower():
            return 'Query_Execution'
        elif 'cleaning' in operation.lower():
            return 'Data_Processing'
        elif 'comparison' in operation.lower():
            return 'Data_Comparison'
        elif 'export' in operation.lower():
            return 'Data_Export'
        else:
            return 'Other'

    def export_missing_records_csv(self, filename: str) -> None:
        """Export enhanced missing records to a separate CSV"""
        try:
            if not self.comparison_results:
                raise ValueError("No comparison results available")
            
            output_path = self._get_output_path(filename)
            results = self.comparison_results
            missing_records = []
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Add missing records details with enhanced information
            for record in results.get('missing_records_details', []):
                missing_records.append({
                    'Timestamp': timestamp,
                    'Primary_Key': str(record['primary_key']),
                    'Missing_In': record['missing_in'],
                    'Table_Name': record['table_name'],
                    'Status': 'Missing',
                    'Impact_Level': 'High' if record['missing_in'] == 'Target' else 'Medium',
                    'Recommendation': f"Investigate missing record in {record['missing_in'].lower()}"
                })
            
            if missing_records:
                missing_df = pd.DataFrame(missing_records)
                missing_df.to_csv(output_path, index=False)
                logger.info(f"Enhanced missing records exported to: {output_path} ({len(missing_records)} records)")
            else:
                # Create empty file with headers
                empty_df = pd.DataFrame(columns=[
                    'Timestamp', 'Primary_Key', 'Missing_In', 'Table_Name', 
                    'Status', 'Impact_Level', 'Recommendation'
                ])
                empty_df.to_csv(output_path, index=False)
                logger.info(f"No missing records found. Empty file created: {output_path}")
                
        except Exception as e:
            logger.error(f"Failed to export missing records: {str(e)}")
            raise
        
    def cleanup_connections(self) -> None:
        """Enhanced connection cleanup with performance tracking"""
        self.performance_monitor.start_timer('connection_cleanup')
        
        try:
            # Use connection pool manager for cleanup
            self.connection_pool_manager.cleanup_all_pools()
            
            # Reset engines
            self.oracle_engine = None
            self.postgres_engine = None
            
            logger.info("All database connections cleaned up successfully")
            
        except Exception as e:
            logger.warning(f"Error during connection cleanup: {e}")
        finally:
            self.performance_monitor.end_timer('connection_cleanup')
    
    def get_data_quality_report(self) -> Dict[str, Any]:
        """Generate comprehensive data quality report"""
        if not self.comparison_results:
            raise ValueError("No comparison results available")
        
        results = self.comparison_results
        
        # Calculate quality metrics
        total_records = max(results['total_source_records'], results['total_target_records'])
        common_records = results['common_records']
        missing_records = len(results['missing_in_target']) + len(results['missing_in_source'])
        field_deltas_count = sum(len(deltas) for deltas in results['field_deltas'].values())
        
        # Calculate percentages
        completeness = (common_records / total_records * 100) if total_records > 0 else 0
        accuracy = ((common_records - field_deltas_count) / common_records * 100) if common_records > 0 else 0
        consistency = ((total_records - missing_records) / total_records * 100) if total_records > 0 else 0
        
        quality_report = {
            'overall_score': (completeness + accuracy + consistency) / 3,
            'completeness_percentage': completeness,
            'accuracy_percentage': accuracy,
            'consistency_percentage': consistency,
            'total_issues': missing_records + field_deltas_count,
            'critical_issues': len(results['missing_in_target']) + len(results['missing_in_source']),
            'field_level_issues': field_deltas_count,
            'recommendations': self._generate_quality_recommendations(results)
        }
        
        return quality_report
    
    def _generate_quality_recommendations(self, results: Dict[str, Any]) -> List[str]:
        """Generate data quality recommendations based on comparison results"""
        recommendations = []
        
        missing_target = len(results['missing_in_target'])
        missing_source = len(results['missing_in_source'])
        field_deltas = sum(len(deltas) for deltas in results['field_deltas'].values())
        
        if missing_target > 0:
            recommendations.append(f"Investigate {missing_target} records missing in target - potential data sync issues")
        
        if missing_source > 0:
            recommendations.append(f"Investigate {missing_source} records missing in source - potential data completeness issues")
        
        if field_deltas > 0:
            recommendations.append(f"Review {field_deltas} field-level differences for data accuracy")
        
        # Field-specific recommendations
        for field, deltas in results['field_deltas'].items():
            if len(deltas) > 0:
                percentage = (len(deltas) / results['common_records'] * 100) if results['common_records'] > 0 else 0
                if percentage > 10:  # More than 10% differences
                    recommendations.append(f"High difference rate ({percentage:.1f}%) in field '{field}' - review data transformation logic")
        
        if not recommendations:
            recommendations.append("Data quality looks good - no critical issues identified")
        
        return recommendations


# Global instance with enhanced capabilities
db_comparison_manager = EnhancedDatabaseComparisonManager()

# Enhanced Step Definitions

@given('I load configuration from "{config_file}"')
def load_configuration_from_file(context, config_file):
    """Initialize configuration loader for on-demand loading"""
    try:
        # Initialize ConfigLoader for on-demand use
        if not hasattr(context, 'config_loader') or context.config_loader is None:
            # Use the global config_loader instance for consistency
            from utils.config_loader import config_loader as global_config_loader
            context.config_loader = global_config_loader
            logger.info(f"✅ Configuration loader initialized for: {config_file}")
        
        # Skip config helper initialization to avoid circular references
        # The config_loader is sufficient for database operations
        logger.info(f"✅ Configuration ready for use")
        
        # Verify the config file exists
        config_path = Path("config") / config_file
        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        
        logger.info(f"Configuration file available: {config_path}")
        
    except Exception as e:
        raise ConfigurationError(f"Failed to initialize configuration for '{config_file}': {str(e)}")

@given('I connect to Oracle database using "{db_section}" configuration')
def connect_to_oracle(context, db_section):
    """Establish connection to Oracle database using on-demand config loading"""
    try:
        logger.info(f"🔄 Loading Oracle configuration for section: {db_section}")
        
        # Load database configuration on-demand
        db_config = load_db_config_when_needed(context, db_section)
        logger.info(f"✅ Oracle config loaded: {db_config.host}:{db_config.port}/{db_config.database}")
        
        # Create connection using the loaded config
        oracle_engine = db_comparison_manager.get_oracle_connection(db_section)
        
        # Store in both context and manager for reuse
        context.oracle_engine = oracle_engine
        context.oracle_section = db_section
        
        assert context.oracle_engine is not None, f"Failed to connect to Oracle database using section '{db_section}'"
        logger.info(f"✅ Oracle connection established for section: {db_section}")
        
    except Exception as e:
        logger.error(f"❌ Oracle connection failed for {db_section}: {str(e)}")
        
        # Provide helpful hints based on error type
        error_msg = str(e).lower()
        if 'environment variable' in error_msg:
            logger.error(f"💡 Hint: Set environment variable for {db_section}")
            logger.error(f"   Example: export {db_section}_PWD=your_password")
        elif 'listener' in error_msg or 'connection' in error_msg:
            logger.error(f"💡 Hint: Check if Oracle database is running and accessible")
        
        raise

@given('I connect to PostgreSQL database using "{db_section}" configuration')
def connect_to_postgres(context, db_section):
    """Establish connection to PostgreSQL database using on-demand config loading"""
    try:
        logger.info(f"🔄 Loading PostgreSQL configuration for section: {db_section}")
        
        # Load database configuration on-demand
        db_config = load_db_config_when_needed(context, db_section)
        logger.info(f"✅ PostgreSQL config loaded: {db_config.host}:{db_config.port}/{db_config.database}")
        
        # Create connection using the loaded config
        postgres_engine = db_comparison_manager.get_postgres_connection(db_section)
        
        # Store in both context and manager for reuse
        context.postgres_engine = postgres_engine
        context.postgres_section = db_section
        
        assert context.postgres_engine is not None, f"Failed to connect to PostgreSQL database using section '{db_section}'"
        logger.info(f"✅ PostgreSQL connection established for section: {db_section}")
        
    except Exception as e:
        logger.error(f"❌ PostgreSQL connection failed for {db_section}: {str(e)}")
        
        # Provide helpful hints based on error type
        error_msg = str(e).lower()
        if 'environment variable' in error_msg:
            logger.error(f"💡 Hint: Set environment variable for {db_section}")
            logger.error(f"   Example: export {db_section}_PWD=your_password")
        elif 'connection' in error_msg or 'refused' in error_msg:
            logger.error(f"💡 Hint: Check if PostgreSQL database is running and accessible")
        
        raise

# Enhanced execution steps with better error handling
@when('I execute query on Oracle and store as source DataFrame')
def execute_oracle_query_as_source(context):
    """Execute current query on Oracle and store as source DataFrame"""
    try:
        if not hasattr(context, 'current_query'):
            raise ValueError("No query loaded. Use 'read query from config' step first")
        if not hasattr(context, 'oracle_engine') or context.oracle_engine is None:
            raise ValueError("Oracle connection not established")
        
        # Get query key for better logging
        query_key = getattr(context, 'current_query_key', None)
        
        db_comparison_manager.source_df = db_comparison_manager.execute_query(
            context.oracle_engine, context.current_query, "Oracle", query_key
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

# Enhanced comparison step with validation - removed duplicate

@when('I compare DataFrames using primary key "{primary_key}"')
def compare_dataframes_basic(context, primary_key):
    """Compare DataFrames using specified primary key (basic version with optional omit support)"""
    try:
        # Validate inputs before comparison
        if db_comparison_manager.source_df is None:
            raise ValueError("Source DataFrame is None")
        if db_comparison_manager.target_df is None:
            raise ValueError("Target DataFrame is None")
        
        # Check if context has omit parameters from previous steps
        omit_columns = getattr(context, 'omit_columns', None)
        omit_values = getattr(context, 'omit_values', None)
        
        context.comparison_results = db_comparison_manager.compare_dataframes(
            primary_key, omit_columns=omit_columns, omit_values=omit_values
        )
        
        if omit_columns or omit_values:
            logger.info(f"Basic comparison completed using primary key: {primary_key}, with omit_columns: {omit_columns}, omit_values: {omit_values}")
        else:
            logger.info(f"Basic comparison completed using primary key: {primary_key}")
        
    except Exception as e:
        logger.error(f"Failed to compare DataFrames: {str(e)}")
        raise

@when('I compare DataFrames using primary key from config section "{section_name}"')
def compare_dataframes_from_config(context, section_name):
    """Compare DataFrames using primary key from config section (with optional omit support)"""
    try:
        # Load primary key configuration on-demand
        logger.info(f"🔄 Loading primary key from config section: {section_name}")
        primary_key = load_config_value_when_needed(context, section_name, 'primary_key')
        logger.info(f"✅ Primary key loaded: {primary_key}")
        
        # Validate inputs before comparison
        if db_comparison_manager.source_df is None:
            raise ValueError("Source DataFrame is None")
        if db_comparison_manager.target_df is None:
            raise ValueError("Target DataFrame is None")
        
        # Check if context has omit parameters from previous steps
        omit_columns = getattr(context, 'omit_columns', None)
        omit_values = getattr(context, 'omit_values', None)
        
        context.comparison_results = db_comparison_manager.compare_dataframes(
            primary_key, omit_columns=omit_columns, omit_values=omit_values
        )
        
        if omit_columns or omit_values:
            logger.info(f"Config-driven comparison completed using primary key: {primary_key}, with omit_columns: {omit_columns}, omit_values: {omit_values}")
        else:
            logger.info(f"Config-driven comparison completed using primary key: {primary_key}")
        
    except Exception as e:
        logger.error(f"Failed to compare DataFrames from config: {str(e)}")
        raise



@when('I load source data using table from config section "{section_name}" key "{table_key}" on Oracle')
def load_source_data_from_config_oracle(context, section_name, table_key):
    """Load source data from Oracle using table name from config"""
    try:
        if not hasattr(context, 'config_loader') or context.config_loader is None:
            raise ValueError("Configuration not loaded. Load config first.")
        if not hasattr(context, 'oracle_engine') or context.oracle_engine is None:
            raise ValueError("Oracle connection not established")
        
        table_name = context.config_loader.get_custom_config(section_name, table_key)
        query = f"SELECT * FROM {table_name}"
        query_key = f"{section_name}.{table_key}"
        
        db_comparison_manager.source_df = db_comparison_manager.execute_query(
            context.oracle_engine, query, "Oracle", query_key
        )
        
        if db_comparison_manager.source_df is not None:
            context.source_record_count = len(db_comparison_manager.source_df)
            logger.info(f"Source DataFrame loaded from {table_name}: {context.source_record_count} records")
        else:
            context.source_record_count = 0
            logger.warning("Source DataFrame is None")
            
    except Exception as e:
        logger.error(f"Failed to load source data from config: {str(e)}")
        raise

@when('I load target data using table from config section "{section_name}" key "{table_key}" on PostgreSQL')
def load_target_data_from_config_postgres(context, section_name, table_key):
    """Load target data from PostgreSQL using table name from config"""
    try:
        if not hasattr(context, 'config_loader') or context.config_loader is None:
            raise ValueError("Configuration not loaded. Load config first.")
        if not hasattr(context, 'postgres_engine') or context.postgres_engine is None:
            raise ValueError("PostgreSQL connection not established")
        
        table_name = context.config_loader.get_custom_config(section_name, table_key)
        query = f"SELECT * FROM {table_name}"
        query_key = f"{section_name}.{table_key}"
        
        db_comparison_manager.target_df = db_comparison_manager.execute_query(
            context.postgres_engine, query, "PostgreSQL", query_key
        )
        
        if db_comparison_manager.target_df is not None:
            context.target_record_count = len(db_comparison_manager.target_df)
            logger.info(f"Target DataFrame loaded from {table_name}: {context.target_record_count} records")
        else:
            context.target_record_count = 0
            logger.warning("Target DataFrame is None")
            
    except Exception as e:
        logger.error(f"Failed to load target data from config: {str(e)}")
        raise

@when('I execute direct query "{query}" on Oracle as source')
def execute_direct_oracle_query_as_source(context, query):
    """Execute direct Oracle query and store as source DataFrame"""
    try:
        if not hasattr(context, 'oracle_engine') or context.oracle_engine is None:
            raise ValueError("Oracle connection not established")
        
        db_comparison_manager.source_df = db_comparison_manager.execute_query(
            context.oracle_engine, query, "Oracle", "direct_query"
        )
        
        if db_comparison_manager.source_df is not None:
            context.source_record_count = len(db_comparison_manager.source_df)
            logger.info(f"Source DataFrame loaded: {context.source_record_count} records")
        else:
            context.source_record_count = 0
            logger.warning("Source DataFrame is None")
            
    except Exception as e:
        logger.error(f"Failed to execute Oracle direct query as source: {str(e)}")
        raise

@when('I execute direct query "{query}" on PostgreSQL as target')
def execute_direct_postgres_query_as_target(context, query):
    """Execute direct PostgreSQL query and store as target DataFrame"""
    try:
        if not hasattr(context, 'postgres_engine') or context.postgres_engine is None:
            raise ValueError("PostgreSQL connection not established")
        
        db_comparison_manager.target_df = db_comparison_manager.execute_query(
            context.postgres_engine, query, "PostgreSQL", "direct_query"
        )
        
        if db_comparison_manager.target_df is not None:
            context.target_record_count = len(db_comparison_manager.target_df)
            logger.info(f"Target DataFrame loaded: {context.target_record_count} records")
        else:
            context.target_record_count = 0
            logger.warning("Target DataFrame is None")
            
    except Exception as e:
        logger.error(f"Failed to execute PostgreSQL direct query as target: {str(e)}")
        raise

@when('I execute direct query "{query}" on PostgreSQL as source')
def execute_direct_postgres_query_as_source(context, query):
    """Execute direct PostgreSQL query and store as source DataFrame"""
    try:
        if not hasattr(context, 'postgres_engine') or context.postgres_engine is None:
            raise ValueError("PostgreSQL connection not established")
        
        db_comparison_manager.source_df = db_comparison_manager.execute_query(
            context.postgres_engine, query, "PostgreSQL", "direct_query"
        )
        
        if db_comparison_manager.source_df is not None:
            context.source_record_count = len(db_comparison_manager.source_df)
            logger.info(f"Source DataFrame loaded: {context.source_record_count} records")
        else:
            context.source_record_count = 0
            logger.warning("Source DataFrame is None")
            
    except Exception as e:
        logger.error(f"Failed to execute PostgreSQL direct query as source: {str(e)}")
        raise

@when('I execute direct query "{query}" on Oracle as target')
def execute_direct_oracle_query_as_target(context, query):
    """Execute direct Oracle query and store as target DataFrame"""
    try:
        if not hasattr(context, 'oracle_engine') or context.oracle_engine is None:
            raise ValueError("Oracle connection not established")
        
        db_comparison_manager.target_df = db_comparison_manager.execute_query(
            context.oracle_engine, query, "Oracle", "direct_query"
        )
        
        if db_comparison_manager.target_df is not None:
            context.target_record_count = len(db_comparison_manager.target_df)
            logger.info(f"Target DataFrame loaded: {context.target_record_count} records")
        else:
            context.target_record_count = 0
            logger.warning("Target DataFrame is None")
            
    except Exception as e:
        logger.error(f"Failed to execute Oracle direct query as target: {str(e)}")
        raise

@then('source DataFrame should have no duplicate records')
def verify_source_no_duplicates(context):
    """Verify source DataFrame has no duplicate records"""
    if db_comparison_manager.source_df is None:
        raise ValueError("Source DataFrame is None - no data was loaded")
    
    duplicate_count = db_comparison_manager.source_df.duplicated().sum()
    assert duplicate_count == 0, (
        f"Source DataFrame has {duplicate_count} duplicate records"
    )
    logger.info("Source DataFrame duplicate check passed: no duplicates found")

@then('target DataFrame should have no duplicate records')
def verify_target_no_duplicates(context):
    """Verify target DataFrame has no duplicate records"""
    if db_comparison_manager.target_df is None:
        raise ValueError("Target DataFrame is None - no data was loaded")
    
    duplicate_count = db_comparison_manager.target_df.duplicated().sum()
    assert duplicate_count == 0, (
        f"Target DataFrame has {duplicate_count} duplicate records"
    )
    logger.info("Target DataFrame duplicate check passed: no duplicates found")

@then('there should be no missing records in either DataFrame')
def verify_no_missing_records(context):
    """Verify there are no missing records between source and target"""
    if not hasattr(context, 'comparison_results'):
        raise ValueError("No comparison results available")
    
    missing_in_target = len(context.comparison_results['missing_in_target'])
    missing_in_source = len(context.comparison_results['missing_in_source'])
    
    assert missing_in_target == 0, f"Found {missing_in_target} records missing in target"
    assert missing_in_source == 0, f"Found {missing_in_source} records missing in source"
    
    logger.info("Missing records check passed: no missing records found")

@then('all fields should match between source and target DataFrames')
def verify_all_fields_match(context):
    """Verify all fields match between source and target DataFrames"""
    if not hasattr(context, 'comparison_results'):
        raise ValueError("No comparison results available")
    
    field_deltas = context.comparison_results['field_deltas']
    total_deltas = sum(len(deltas) for deltas in field_deltas.values())
    
    assert total_deltas == 0, f"Found {total_deltas} field differences"
    logger.info("Field comparison check passed: all fields match")

@then('there should be "{expected_count:d}" records missing in target')
def verify_missing_in_target_count(context, expected_count):
    """Verify expected count of records missing in target"""
    if not hasattr(context, 'comparison_results'):
        raise ValueError("No comparison results available")
    
    actual_count = len(context.comparison_results['missing_in_target'])
    assert actual_count == expected_count, (
        f"Expected {expected_count} records missing in target, found {actual_count}"
    )
    logger.info(f"Missing in target verification passed: {actual_count} records")

@then('there should be "{expected_count:d}" records missing in source')
def verify_missing_in_source_count(context, expected_count):
    """Verify expected count of records missing in source"""
    if not hasattr(context, 'comparison_results'):
        raise ValueError("No comparison results available")
    
    actual_count = len(context.comparison_results['missing_in_source'])
    assert actual_count == expected_count, (
        f"Expected {expected_count} records missing in source, found {actual_count}"
    )
    logger.info(f"Missing in source verification passed: {actual_count} records")

@then('field "{field_name}" should have "{expected_count:d}" delta records')
def verify_field_delta_count(context, field_name, expected_count):
    """Verify expected count of delta records for specific field"""
    if not hasattr(context, 'comparison_results'):
        raise ValueError("No comparison results available")
    
    field_deltas = context.comparison_results['field_deltas']
    if field_name not in field_deltas:
        actual_count = 0
    else:
        actual_count = len(field_deltas[field_name])
    
    assert actual_count == expected_count, (
        f"Expected {expected_count} delta records for field '{field_name}', found {actual_count}"
    )
    logger.info(f"Field delta verification passed for '{field_name}': {actual_count} deltas")

@then('I print the comparison summary')
def print_comparison_summary(context):
    """Print detailed comparison summary"""
    if not hasattr(context, 'comparison_results'):
        raise ValueError("No comparison results available")
    
    results = context.comparison_results
    print("\n" + "="*60)
    print("COMPARISON SUMMARY")
    print("="*60)
    print(f"Source Records: {results['total_source_records']:,}")
    print(f"Target Records: {results['total_target_records']:,}")
    print(f"Common Records: {results['common_records']:,}")
    print(f"Missing in Target: {len(results['missing_in_target']):,}")
    print(f"Missing in Source: {len(results['missing_in_source']):,}")
    print(f"Primary Key: {results['primary_key']}")
    print(f"Common Columns: {len(results['common_columns'])}")
    print(f"Columns Compared: {len(results['columns_compared'])}")
    
    if results['field_deltas']:
        print("\nField Differences:")
        total_deltas = 0
        for field, deltas in results['field_deltas'].items():
            delta_count = len(deltas)
            total_deltas += delta_count
            if delta_count > 0:
                print(f"  {field}: {delta_count:,} differences")
        print(f"Total Field Differences: {total_deltas:,}")
    
    print("="*60)
    logger.info("Comparison summary displayed")

@then('I print DataFrame info for source')
def print_source_dataframe_info(context):
    """Print detailed information about source DataFrame"""
    if db_comparison_manager.source_df is None:
        print("Source DataFrame: None")
        return
    
    df = db_comparison_manager.source_df
    print("\n" + "="*50)
    print("SOURCE DATAFRAME INFO")
    print("="*50)
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"Data Types:\n{df.dtypes}")
    print(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    print(f"Null Values:\n{df.isnull().sum()}")
    print("="*50)
    logger.info("Source DataFrame info displayed")

@then('I print DataFrame info for target')
def print_target_dataframe_info(context):
    """Print detailed information about target DataFrame"""
    if db_comparison_manager.target_df is None:
        print("Target DataFrame: None")
        return
    
    df = db_comparison_manager.target_df
    print("\n" + "="*50)
    print("TARGET DATAFRAME INFO")
    print("="*50)
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"Data Types:\n{df.dtypes}")
    print(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    print(f"Null Values:\n{df.isnull().sum()}")
    print("="*50)
    logger.info("Target DataFrame info displayed")

@then('I export source DataFrame to CSV "{filename}"')
def export_source_dataframe_to_csv(context, filename):
    """Export source DataFrame to CSV file"""
    try:
        if db_comparison_manager.source_df is None:
            raise ValueError("Source DataFrame is None")
        
        db_comparison_manager.export_to_csv(filename, 'source')
        logger.info(f"Source DataFrame exported to CSV: {filename}")
        
    except Exception as e:
        logger.error(f"Failed to export source DataFrame to CSV: {str(e)}")
        raise

# Enhanced export step with comprehensive output
@then('I export comparison results to CSV file "{filename}"')
def export_comparison_results_to_csv(context, filename):
    """Export detailed comparison results to CSV file with enhanced features"""
    try:
        if not hasattr(context, 'comparison_results'):
            raise ValueError("No comparison results available. Run comparison first.")
        
        # Create timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = filename.replace('.csv', '')
        
        # Export all types of files with enhanced content
        export_tasks = [
            (f"{base_name}_{timestamp}.csv", 'detailed'),
            (f"summary_{timestamp}.csv", 'summary'),
            (f"source_{timestamp}.csv", 'source'),
            (f"target_{timestamp}.csv", 'target'),
            (f"performance_{timestamp}.csv", 'performance')
        ]
        
        for export_filename, export_type in export_tasks:
            try:
                db_comparison_manager.export_to_csv(export_filename, export_type)
                logger.info(f"Successfully exported {export_type}: {export_filename}")
            except Exception as e:
                logger.warning(f"Failed to export {export_type}: {str(e)}")
        
        # Export missing records separately
        try:
            missing_filename = f"missing_records_{timestamp}.csv"
            db_comparison_manager.export_missing_records_csv(missing_filename)
        except Exception as e:
            logger.warning(f"Failed to export missing records: {str(e)}")
            
        logger.info(f"All enhanced comparison results exported with timestamp: {timestamp}")
        
    except Exception as e:
        logger.error(f"Failed to export comparison results to CSV: {str(e)}")
        raise

# New enhanced step definitions
@then('I generate data quality report')
def generate_data_quality_report(context):
    """Generate comprehensive data quality report"""
    try:
        if not hasattr(context, 'comparison_results'):
            raise ValueError("No comparison results available. Run comparison first.")
        
        quality_report = db_comparison_manager.get_data_quality_report()
        context.quality_report = quality_report
        
        # Print summary
        print("\n" + "="*80)
        print("DATA QUALITY REPORT")
        print("="*80)
        print(f"Overall Quality Score: {quality_report['overall_score']:.1f}%")
        print(f"Completeness: {quality_report['completeness_percentage']:.1f}%")
        print(f"Accuracy: {quality_report['accuracy_percentage']:.1f}%")
        print(f"Consistency: {quality_report['consistency_percentage']:.1f}%")
        print(f"Total Issues: {quality_report['total_issues']}")
        print(f"Critical Issues: {quality_report['critical_issues']}")
        print(f"Field-Level Issues: {quality_report['field_level_issues']}")
        print("\nRecommendations:")
        for i, recommendation in enumerate(quality_report['recommendations'], 1):
            print(f"  {i}. {recommendation}")
        print("="*80)
        
        logger.info("Data quality report generated successfully")
        
    except Exception as e:
        logger.error(f"Failed to generate data quality report: {str(e)}")
        raise

@then('I print performance metrics')
def print_performance_metrics(context):
    """Print detailed performance metrics"""
    try:
        if not hasattr(context, 'comparison_results'):
            raise ValueError("No comparison results available")
        
        results = context.comparison_results
        if not results.get('performance_metrics'):
            print("No performance metrics available")
            return
        
        print("\n" + "="*60)
        print("PERFORMANCE METRICS")
        print("="*60)
        
        metrics = results['performance_metrics']
        total_time = sum(metrics.values())
        
        print(f"Total Processing Time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        print("\nDetailed Breakdown:")
        
        # Sort by duration (longest first)
        sorted_metrics = sorted(metrics.items(), key=lambda x: x[1], reverse=True)
        
        for operation, duration in sorted_metrics:
            percentage = (duration / total_time * 100) if total_time > 0 else 0
            print(f"  {operation:<25}: {duration:>8.2f}s ({percentage:>5.1f}%)")
        
        print("="*60)
        logger.info("Performance metrics displayed successfully")
        
    except Exception as e:
        logger.error(f"Failed to print performance metrics: {str(e)}")
        raise

@when('I read query from config section "{section_name}" key "{query_key}"')
def read_query_from_config(context, section_name, query_key):
    """Read SQL query from configuration file with validation"""
    try:
        if not hasattr(context, 'config_loader') or context.config_loader is None:
            raise ValueError("Configuration not loaded. Load config first.")
        
        query = context.config_loader.get_custom_config(section_name, query_key)
        
        # Basic query validation
        if not query or not query.strip():
            raise ValueError(f"Empty query found in {section_name}.{query_key}")
        
        # Check for potential SQL injection patterns (basic validation)
        dangerous_patterns = ['DROP ', 'DELETE ', 'TRUNCATE ', 'ALTER ', 'CREATE ', 'EXEC']
        query_upper = query.upper()
        for pattern in dangerous_patterns:
            if pattern in query_upper:
                logger.warning(f"Potentially dangerous SQL pattern '{pattern}' found in query")
        
        context.current_query = query
        context.current_query_key = f"{section_name}.{query_key}"  # Store the query key for logging
        logger.debug(f"Query loaded from {section_name}.{query_key} (length: {len(query)} characters)")
        
    except Exception as e:
        logger.error(f"Failed to read query from config: {str(e)}")
        raise

# Enhanced data loading steps with better error handling
@when('I execute query on PostgreSQL and store as source DataFrame')
def execute_postgres_query_as_source(context):
    """Execute current query on PostgreSQL and store as source DataFrame"""
    try:
        if not hasattr(context, 'current_query'):
            raise ValueError("No query loaded. Use 'read query from config' step first")
        if not hasattr(context, 'postgres_engine') or context.postgres_engine is None:
            raise ValueError("PostgreSQL connection not established")
        
        # Get query key for better logging
        query_key = getattr(context, 'current_query_key', None)
        
        db_comparison_manager.source_df = db_comparison_manager.execute_query(
            context.postgres_engine, context.current_query, "PostgreSQL", query_key
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
        
        # Get query key for better logging
        query_key = getattr(context, 'current_query_key', None)
        
        db_comparison_manager.target_df = db_comparison_manager.execute_query(
            context.oracle_engine, context.current_query, "Oracle", query_key
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
        
        # Get query key for better logging
        query_key = getattr(context, 'current_query_key', None)
        
        db_comparison_manager.target_df = db_comparison_manager.execute_query(
            context.postgres_engine, context.current_query, "PostgreSQL", query_key
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

# Enhanced comparison steps with omit options - using distinct patterns
@when('I perform DataFrame comparison using primary key "{primary_key}" with omitted columns "{omit_columns}"')
def compare_dataframes_with_omit_columns(context, primary_key, omit_columns):
    """Compare DataFrames using specified primary key and omitting specified columns"""
    try:
        # Parse comma-separated column names
        omit_columns_list = [col.strip() for col in omit_columns.split(',') if col.strip()]
        
        context.comparison_results = db_comparison_manager.compare_dataframes(
            primary_key, omit_columns=omit_columns_list, omit_values=None
        )
        logger.info(f"Enhanced comparison completed using primary key: {primary_key}, omitting columns: {omit_columns_list}")
        
    except Exception as e:
        logger.error(f"Failed to compare DataFrames: {str(e)}")
        raise

@when('I perform DataFrame comparison using primary key "{primary_key}" with omitted values "{omit_values}"')
def compare_dataframes_with_omit_values(context, primary_key, omit_values):
    """Compare DataFrames using specified primary key and treating specified values as equal"""
    try:
        # Parse comma-separated values
        omit_values_list = [val.strip() for val in omit_values.split(',') if val.strip()]
        
        context.comparison_results = db_comparison_manager.compare_dataframes(
            primary_key, omit_columns=None, omit_values=omit_values_list
        )
        logger.info(f"Enhanced comparison completed using primary key: {primary_key}, treating as equal: {omit_values_list}")
        
    except Exception as e:
        logger.error(f"Failed to compare DataFrames: {str(e)}")
        raise

# Comprehensive step definition supporting both omit_columns and omit_values
@when('I perform comprehensive DataFrame comparison using primary key "{primary_key}" with omitted columns "{omit_columns}" and omitted values "{omit_values}"')
def compare_dataframes_with_omit_columns_and_values(context, primary_key, omit_columns, omit_values):
    """Compare DataFrames using specified primary key with both omitted columns and values"""
    try:
        # Parse comma-separated column names and values
        omit_columns_list = [col.strip() for col in omit_columns.split(',') if col.strip()] if omit_columns else None
        omit_values_list = [val.strip() for val in omit_values.split(',') if val.strip()] if omit_values else None
        
        context.comparison_results = db_comparison_manager.compare_dataframes(
            primary_key, omit_columns=omit_columns_list, omit_values=omit_values_list
        )
        logger.info(f"Comprehensive comparison completed using primary key: {primary_key}, omitting columns: {omit_columns_list}, treating as equal: {omit_values_list}")
        
    except Exception as e:
        logger.error(f"Failed to compare DataFrames: {str(e)}")
        raise

# Helper steps to set omit parameters for use with basic comparison steps
@given('I set omit columns to "{omit_columns}"')
def set_omit_columns_in_context(context, omit_columns):
    """Set omit columns in context for use with basic comparison steps"""
    if omit_columns and omit_columns.lower() not in ['none', 'null', '']:
        context.omit_columns = [col.strip() for col in omit_columns.split(',') if col.strip()]
    else:
        context.omit_columns = None
    logger.info(f"Set omit columns in context: {context.omit_columns}")

@given('I set omit values to "{omit_values}"')
def set_omit_values_in_context(context, omit_values):
    """Set omit values in context for use with basic comparison steps"""
    if omit_values and omit_values.lower() not in ['none', 'null', '']:
        context.omit_values = [val.strip() for val in omit_values.split(',') if val.strip()]
    else:
        context.omit_values = None
    logger.info(f"Set omit values in context: {context.omit_values}")

@given('I clear omit parameters')
def clear_omit_parameters(context):
    """Clear any previously set omit parameters"""
    context.omit_columns = None
    context.omit_values = None
    logger.info("Cleared omit parameters from context")

# Optional parameter step definitions for flexible usage
@when('I perform DataFrame comparison with primary key "{primary_key}" and optional omit columns "{omit_columns}"')
def compare_dataframes_with_optional_omit_columns(context, primary_key, omit_columns):
    """Compare DataFrames with optional omit columns (use 'none' or empty to skip)"""
    try:
        # Handle 'none' or empty string as no omission
        omit_columns_list = None
        if omit_columns and omit_columns.lower() not in ['none', 'null', '']:
            omit_columns_list = [col.strip() for col in omit_columns.split(',') if col.strip()]
        
        context.comparison_results = db_comparison_manager.compare_dataframes(
            primary_key, omit_columns=omit_columns_list, omit_values=None
        )
        logger.info(f"Optional omit columns comparison completed using primary key: {primary_key}, omitting columns: {omit_columns_list}")
        
    except Exception as e:
        logger.error(f"Failed to compare DataFrames: {str(e)}")
        raise

@when('I perform DataFrame comparison with primary key "{primary_key}" and optional omit values "{omit_values}"')
def compare_dataframes_with_optional_omit_values(context, primary_key, omit_values):
    """Compare DataFrames with optional omit values (use 'none' or empty to skip)"""
    try:
        # Handle 'none' or empty string as no omission
        omit_values_list = None
        if omit_values and omit_values.lower() not in ['none', 'null', '']:
            omit_values_list = [val.strip() for val in omit_values.split(',') if val.strip()]
        
        context.comparison_results = db_comparison_manager.compare_dataframes(
            primary_key, omit_columns=None, omit_values=omit_values_list
        )
        logger.info(f"Optional omit values comparison completed using primary key: {primary_key}, treating as equal: {omit_values_list}")
        
    except Exception as e:
        logger.error(f"Failed to compare DataFrames: {str(e)}")
        raise

# Perfect match verification step
@then('the comparison should show a perfect match')
def verify_perfect_match(context):
    """Verify that the comparison shows a perfect match (no differences at all)"""
    assert hasattr(context, 'comparison_results'), "No comparison results available"
    
    delta_summary = context.comparison_results.get('delta_summary', {})
    perfect_match = delta_summary.get('perfect_match', False)
    
    if not perfect_match:
        total_deltas = delta_summary.get('total_field_differences', 0)
        missing_in_target = len(context.comparison_results.get('missing_in_target', []))
        missing_in_source = len(context.comparison_results.get('missing_in_source', []))
        
        error_msg = f"Expected perfect match but found differences:\n"
        error_msg += f"  - Field differences: {total_deltas}\n"
        error_msg += f"  - Missing in target: {missing_in_target}\n" 
        error_msg += f"  - Missing in source: {missing_in_source}"
        
        assert False, error_msg
    
    logger.info("✅ Perfect match verification passed")

@then('the comparison should show {expected_field_differences:d} field differences')
def verify_field_differences_count(context, expected_field_differences):
    """Verify the exact number of field differences"""
    assert hasattr(context, 'comparison_results'), "No comparison results available"
    
    delta_summary = context.comparison_results.get('delta_summary', {})
    actual_deltas = delta_summary.get('total_field_differences', 0)
    
    assert actual_deltas == expected_field_differences, (
        f"Expected {expected_field_differences} field differences, but found {actual_deltas}"
    )
    
    logger.info(f"✅ Field differences count verification passed: {actual_deltas}")

@then('field match percentage should be {expected_percentage:f}%')
def verify_field_match_percentage(context, expected_percentage):
    """Verify the field match percentage"""
    assert hasattr(context, 'comparison_results'), "No comparison results available"
    
    delta_summary = context.comparison_results.get('delta_summary', {})
    actual_percentage = delta_summary.get('field_match_percentage', 0)
    
    assert actual_percentage >= expected_percentage, (
        f"Field match percentage {actual_percentage}% is below expected {expected_percentage}%"
    )
    
    logger.info(f"✅ Field match percentage verification passed: {actual_percentage}%")

# Enhanced verification steps with better error messages
@then('the source DataFrame should have "{expected_count:d}" records')
def verify_source_record_count(context, expected_count):
    """Verify source DataFrame record count with enhanced error message"""
    if db_comparison_manager.source_df is None:
        raise ValueError("Source DataFrame is None - no data was loaded")
    
    actual_count = len(db_comparison_manager.source_df)
    assert actual_count == expected_count, (
        f"Source record count mismatch:\n"
        f"  Expected: {expected_count:,}\n"
        f"  Actual:   {actual_count:,}\n"
        f"  Difference: {actual_count - expected_count:,}"
    )
    logger.info(f"Source record count verification passed: {actual_count:,} records")

@then('the target DataFrame should have "{expected_count:d}" records')
def verify_target_record_count(context, expected_count):
    """Verify target DataFrame record count with enhanced error message"""
    if db_comparison_manager.target_df is None:
        raise ValueError("Target DataFrame is None - no data was loaded")
    
    actual_count = len(db_comparison_manager.target_df)
    assert actual_count == expected_count, (
        f"Target record count mismatch:\n"
        f"  Expected: {expected_count:,}\n"
        f"  Actual:   {actual_count:,}\n"
        f"  Difference: {actual_count - expected_count:,}"
    )
    logger.info(f"Target record count verification passed: {actual_count:,} records")

# Enhanced export steps
@then('I export all results to Excel file "{filename}"')
def export_all_results_to_excel(context, filename):
    """Export DataFrames and comparison results to Excel with enhanced features"""
    try:
        if not hasattr(context, 'comparison_results'):
            raise ValueError("No comparison results available. Run comparison first.")
        
        db_comparison_manager.export_to_excel(filename)
        logger.info(f"Enhanced Excel export completed: {filename}")
        
    except Exception as e:
        logger.error(f"Failed to export results to Excel: {str(e)}")
        raise

@then('I export all comparison results with timestamp')
def export_all_comparison_results(context):
    """Export all types of comparison results with enhanced timestamp and metadata"""
    try:
        if not hasattr(context, 'comparison_results'):
            raise ValueError("No comparison results available. Run comparison first.")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Export all file types with enhanced features
        exports = [
            (f"detailed_comparison_{timestamp}.csv", 'detailed'),
            (f"summary_comparison_{timestamp}.csv", 'summary'),
            (f"source_data_{timestamp}.csv", 'source'),
            (f"target_data_{timestamp}.csv", 'target'),
            (f"performance_metrics_{timestamp}.csv", 'performance'),
            (f"enhanced_excel_report_{timestamp}.xlsx", 'excel')
        ]
        
        successful_exports = []
        failed_exports = []
        
        for filename, export_type in exports:
            try:
                if export_type == 'excel':
                    db_comparison_manager.export_to_excel(filename)
                else:
                    db_comparison_manager.export_to_csv(filename, export_type)
                successful_exports.append((filename, export_type))
                logger.info(f"Successfully exported {export_type}: {filename}")
            except Exception as e:
                failed_exports.append((filename, export_type, str(e)))
                logger.warning(f"Failed to export {export_type}: {str(e)}")
        
        # Export missing records separately
        try:
            missing_filename = f"missing_records_{timestamp}.csv"
            db_comparison_manager.export_missing_records_csv(missing_filename)
            successful_exports.append((missing_filename, 'missing_records'))
        except Exception as e:
            failed_exports.append((missing_filename, 'missing_records', str(e)))
            logger.warning(f"Failed to export missing records: {str(e)}")
        
        # Print summary
        print(f"\nExport Summary (Timestamp: {timestamp}):")
        print(f"  Successful: {len(successful_exports)}")
        print(f"  Failed: {len(failed_exports)}")
        
        if failed_exports:
            print("  Failed exports:")
            for filename, export_type, error in failed_exports:
                print(f"    - {export_type}: {error}")
            
        logger.info(f"Enhanced export completed with timestamp: {timestamp}")
        
    except Exception as e:
        logger.error(f"Failed to export all comparison results: {str(e)}")
        raise

# Enhanced debugging and validation steps
@when('I validate data quality for source DataFrame')
def validate_source_data_quality(context):
    """Validate data quality metrics for source DataFrame with enhanced checks"""
    try:
        if db_comparison_manager.source_df is None:
            raise ValueError("Source DataFrame is None")
            
        df = db_comparison_manager.source_df
        
        # Enhanced quality metrics
        quality_metrics = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_records': df.duplicated().sum(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'empty_string_counts': {},
            'data_type_distribution': df.dtypes.value_counts().to_dict(),
            'numeric_columns': list(df.select_dtypes(include=['number']).columns),
            'text_columns': list(df.select_dtypes(include=['object']).columns)
        }
        
        # Check for empty strings in text columns
        for col in quality_metrics['text_columns']:
            empty_count = (df[col].astype(str).str.strip() == '').sum()
            quality_metrics['empty_string_counts'][col] = empty_count
        
        context.source_quality_metrics = quality_metrics
        logger.info(f"Enhanced source data quality validated. Duplicates: {quality_metrics['duplicate_records']}, Memory: {quality_metrics['memory_usage_mb']:.2f} MB")
        
    except Exception as e:
        logger.error(f"Failed to validate source data quality: {str(e)}")
        raise

@when('I validate data quality for target DataFrame')
def validate_target_data_quality(context):
    """Validate data quality metrics for target DataFrame with enhanced checks"""
    try:
        if db_comparison_manager.target_df is None:
            raise ValueError("Target DataFrame is None")
            
        df = db_comparison_manager.target_df
        
        # Enhanced quality metrics
        quality_metrics = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_records': df.duplicated().sum(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'empty_string_counts': {},
            'data_type_distribution': df.dtypes.value_counts().to_dict(),
            'numeric_columns': list(df.select_dtypes(include=['number']).columns),
            'text_columns': list(df.select_dtypes(include=['object']).columns)
        }
        
        # Check for empty strings in text columns
        for col in quality_metrics['text_columns']:
            empty_count = (df[col].astype(str).str.strip() == '').sum()
            quality_metrics['empty_string_counts'][col] = empty_count
        
        context.target_quality_metrics = quality_metrics
        logger.info(f"Enhanced target data quality validated. Duplicates: {quality_metrics['duplicate_records']}, Memory: {quality_metrics['memory_usage_mb']:.2f} MB")
        
    except Exception as e:
        logger.error(f"Failed to validate target data quality: {str(e)}")
        raise

# Enhanced cleanup function
def after_scenario(context, scenario):
    """Enhanced cleanup after scenario with performance tracking"""
    try:
        # Cleanup connections
        db_comparison_manager.cleanup_connections()
        
        # Reset DataFrames
        db_comparison_manager.source_df = None
        db_comparison_manager.target_df = None
        db_comparison_manager.comparison_results = {}
        
        # Log final performance summary if available
        if hasattr(db_comparison_manager, 'performance_monitor'):
            timings = db_comparison_manager.performance_monitor.get_all_timings()
            if timings:
                total_time = sum(timings.values())
                logger.info(f"Scenario completed in {total_time:.2f} seconds")
        
        logger.debug("Enhanced database comparison manager cleaned up")
        
    except Exception as e:
        logger.warning(f"Error during enhanced database comparison cleanup: {e}")

# New utility steps for enhanced functionality
@then('I save comparison results as JSON file "{filename}"')
def save_comparison_results_as_json(context, filename):
    """Save enhanced comparison results as JSON file"""
    try:
        if not hasattr(context, 'comparison_results'):
            raise ValueError("No comparison results available")
        
        output_path = db_comparison_manager._get_output_path(filename)
        
        # Convert results to JSON-serializable format with enhanced data
        json_results = {
            'metadata': {
                'export_timestamp': datetime.now().isoformat(),
                'comparison_timestamp': context.comparison_results.get('comparison_timestamp'),
                'primary_key': context.comparison_results['primary_key'],
                'version': '2.0'
            },
            'summary': {
                'total_source_records': context.comparison_results['total_source_records'],
                'total_target_records': context.comparison_results['total_target_records'],
                'common_records': context.comparison_results['common_records'],
                'missing_in_target_count': len(context.comparison_results['missing_in_target']),
                'missing_in_source_count': len(context.comparison_results['missing_in_source']),
                'total_field_deltas': sum(len(deltas) for deltas in context.comparison_results['field_deltas'].values())
            },
            'detailed_results': {
                'missing_in_target': context.comparison_results['missing_in_target'],
                'missing_in_source': context.comparison_results['missing_in_source'],
                'field_delta_counts': {field: len(deltas) for field, deltas in context.comparison_results['field_deltas'].items()},
                'field_deltas': context.comparison_results['field_deltas']
            },
            'configuration': {
                'omitted_columns': context.comparison_results['omitted_columns'],
                'omitted_values': context.comparison_results['omitted_values'],
                'columns_compared': context.comparison_results['columns_compared']
            },
            'performance_metrics': context.comparison_results.get('performance_metrics', {}),
            'data_quality': db_comparison_manager.get_data_quality_report() if context.comparison_results else {}
        }
        
        with open(output_path, 'w') as f:
            json.dump(json_results, f, indent=2, default=str)
        
        logger.info(f"Enhanced comparison results saved as JSON: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to save comparison results as JSON: {str(e)}")
        raise

@then('data quality score should be above "{threshold:f}"')
def verify_data_quality_score(context, threshold):
    """Verify that data quality score meets minimum threshold"""
    try:
        quality_report = db_comparison_manager.get_data_quality_report()
        actual_score = quality_report['overall_score']
        
        assert actual_score >= threshold, (
            f"Data quality score below threshold:\n"
            f"  Required: {threshold:.1f}%\n"
            f"  Actual:   {actual_score:.1f}%\n"
            f"  Gap:      {threshold - actual_score:.1f}%"
        )
        
        logger.info(f"Data quality score verification passed: {actual_score:.1f}% >= {threshold:.1f}%")
        
    except Exception as e:
        logger.error(f"Failed to verify data quality score: {str(e)}")
        raise

@when('I enable progress monitoring')
def enable_progress_monitoring(context):
    """Enable detailed progress monitoring for operations"""
    try:
        # This could be expanded to configure specific monitoring settings
        context.progress_monitoring = True
        logger.info("Progress monitoring enabled")
        
    except Exception as e:
        logger.error(f"Failed to enable progress monitoring: {str(e)}")
        raise


# Additional step definitions for multiple specific database sections

@given('I connect to Oracle database using "{db_section}" configuration as source')
def connect_to_oracle_as_source(context, db_section):
    """Connect to Oracle database as source for comparison"""
    connect_to_oracle(context, db_section)
    context.source_engine = context.oracle_engine
    context.source_section = db_section
    context.source_db_type = "Oracle"
    logger.info(f"✅ Oracle source database connected: {db_section}")


@given('I connect to Oracle database using "{db_section}" configuration as target')
def connect_to_oracle_as_target(context, db_section):
    """Connect to Oracle database as target for comparison"""
    # Create a separate connection for target
    try:
        db_config = load_db_config_when_needed(context, db_section)
        connection_string = f"oracle+cx_oracle://{db_config.username}:{db_config.password}@{db_config.host}:{db_config.port}/?service_name={db_config.database}"
        context.target_oracle_engine = create_engine(connection_string, echo=False)
        context.target_section = db_section
        context.target_db_type = "Oracle"
        logger.info(f"✅ Oracle target database connected: {db_section}")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Oracle target database '{db_section}': {str(e)}")
        raise


@given('I connect to PostgreSQL database using "{db_section}" configuration as source')
def connect_to_postgres_as_source(context, db_section):
    """Connect to PostgreSQL database as source for comparison"""
    connect_to_postgres(context, db_section)
    context.source_engine = context.postgres_engine
    context.source_section = db_section
    context.source_db_type = "PostgreSQL"
    logger.info(f"✅ PostgreSQL source database connected: {db_section}")


@given('I connect to PostgreSQL database using "{db_section}" configuration as target')
def connect_to_postgres_as_target(context, db_section):
    """Connect to PostgreSQL database as target for comparison"""
    # Create a separate connection for target
    try:
        db_config = load_db_config_when_needed(context, db_section)
        connection_string = f"postgresql://{db_config.username}:{db_config.password}@{db_config.host}:{db_config.port}/{db_config.database}"
        context.target_postgres_engine = create_engine(connection_string, echo=False)
        context.target_section = db_section
        context.target_db_type = "PostgreSQL"
        logger.info(f"✅ PostgreSQL target database connected: {db_section}")
    except Exception as e:
        logger.error(f"❌ Failed to connect to PostgreSQL target database '{db_section}': {str(e)}")
        raise


@given('I connect to Oracle database using "{db_section}" configuration as secondary')
def connect_to_oracle_as_secondary(context, db_section):
    """Connect to Oracle database as secondary connection"""
    try:
        db_config = load_db_config_when_needed(context, db_section)
        connection_string = f"oracle+cx_oracle://{db_config.username}:{db_config.password}@{db_config.host}:{db_config.port}/?service_name={db_config.database}"
        context.secondary_oracle_engine = create_engine(connection_string, echo=False)
        context.secondary_oracle_section = db_section
        logger.info(f"✅ Oracle secondary database connected: {db_section}")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Oracle secondary database '{db_section}': {str(e)}")
        raise


@given('I connect to PostgreSQL database using "{db_section}" configuration as secondary')
def connect_to_postgres_as_secondary(context, db_section):
    """Connect to PostgreSQL database as secondary connection"""
    try:
        db_config = load_db_config_when_needed(context, db_section)
        connection_string = f"postgresql://{db_config.username}:{db_config.password}@{db_config.host}:{db_config.port}/{db_config.database}"
        context.secondary_postgres_engine = create_engine(connection_string, echo=False)
        context.secondary_postgres_section = db_section
        logger.info(f"✅ PostgreSQL secondary database connected: {db_section}")
    except Exception as e:
        logger.error(f"❌ Failed to connect to PostgreSQL secondary database '{db_section}': {str(e)}")
        raise


@when('I execute query on source database and store as source DataFrame')
def execute_query_on_source_database(context):
    """Execute current query on source database"""
    try:
        if not hasattr(context, 'source_engine') or context.source_engine is None:
            raise ValueError("Source database connection not established")
        if not hasattr(context, 'current_query'):
            raise ValueError("No query loaded. Use 'read query from config' step first")
        
        # Get query key for better logging
        query_key = getattr(context, 'current_query_key', None)
        
        db_comparison_manager.source_df = db_comparison_manager.execute_query(
            context.source_engine, context.current_query, context.source_db_type, query_key
        )
        
        if db_comparison_manager.source_df is not None:
            context.source_record_count = len(db_comparison_manager.source_df)
            logger.info(f"Source DataFrame loaded from {context.source_db_type}: {context.source_record_count} records")
        else:
            context.source_record_count = 0
            logger.warning("Source DataFrame is None")
            
    except Exception as e:
        logger.error(f"Failed to execute query on source {context.source_db_type}: {str(e)}")
        raise


@when('I execute query on target database and store as target DataFrame')
def execute_query_on_target_database(context):
    """Execute current query on target database"""
    try:
        # Determine target engine
        target_engine = None
        if hasattr(context, 'target_oracle_engine') and context.target_oracle_engine:
            target_engine = context.target_oracle_engine
        elif hasattr(context, 'target_postgres_engine') and context.target_postgres_engine:
            target_engine = context.target_postgres_engine
        
        if target_engine is None:
            raise ValueError("Target database connection not established")
        if not hasattr(context, 'current_query'):
            raise ValueError("No query loaded. Use 'read query from config' step first")
        
        # Get query key for better logging
        query_key = getattr(context, 'current_query_key', None)
        
        db_comparison_manager.target_df = db_comparison_manager.execute_query(
            target_engine, context.current_query, context.target_db_type, query_key
        )
        
        if db_comparison_manager.target_df is not None:
            context.target_record_count = len(db_comparison_manager.target_df)
            logger.info(f"Target DataFrame loaded from {context.target_db_type}: {context.target_record_count} records")
        else:
            context.target_record_count = 0
            logger.warning("Target DataFrame is None")
            
    except Exception as e:
        logger.error(f"Failed to execute query on target {context.target_db_type}: {str(e)}")
        raise


@then('both databases should be accessible')
def verify_both_databases_accessible(context):
    """Verify that both primary and secondary databases are accessible"""
    try:
        connections_verified = 0
        
        # Check Oracle connections
        if hasattr(context, 'oracle_engine') and context.oracle_engine:
            with context.oracle_engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM DUAL"))
            logger.info("✅ Primary Oracle database accessible")
            connections_verified += 1
            
        if hasattr(context, 'secondary_oracle_engine') and context.secondary_oracle_engine:
            with context.secondary_oracle_engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM DUAL"))
            logger.info("✅ Secondary Oracle database accessible")
            connections_verified += 1
        
        # Check PostgreSQL connections
        if hasattr(context, 'postgres_engine') and context.postgres_engine:
            with context.postgres_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Primary PostgreSQL database accessible")
            connections_verified += 1
            
        if hasattr(context, 'secondary_postgres_engine') and context.secondary_postgres_engine:
            with context.secondary_postgres_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Secondary PostgreSQL database accessible")
            connections_verified += 1
        
        assert connections_verified >= 2, f"Expected at least 2 database connections, verified {connections_verified}"
        logger.info(f"All {connections_verified} database connections are accessible")
        
    except Exception as e:
        logger.error(f"Database accessibility verification failed: {str(e)}")
        raise