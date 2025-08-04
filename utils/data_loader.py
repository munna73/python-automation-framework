"""
Data Loader Module for Test Automation Framework. 

This module provides comprehensive data loading capabilities for test automation,
including support for various data formats (CSV, Excel, JSON, XML, YAML),
database connections, API data loading, and data transformation.

Author: Test Automation Framework
Version: 1.0.0
"""

import pandas as pd
import numpy as np
import json
import yaml
import xml.etree.ElementTree as ET
import csv
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Callable, Generator
from datetime import datetime, date
import sqlite3
import requests
from urllib.parse import urljoin
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from functools import lru_cache
import pickle
import gzip
import zipfile
import tarfile
from io import StringIO, BytesIO

# Import custom modules
try:
    # Corrected imports:
    # 1. Importing specific exception classes.
    # 2. Importing the 'logger' instance (assuming it's a singleton or initialized globally).
    # 3. Importing the 'config_loader' singleton instance.
    from utils.custom_exceptions import DataLoadError, ConfigurationError, ValidationError
    from utils.logger import logger
    from utils.config_loader import config_loader 
    from db.database_connector import DatabaseConnector
    from api.rest_client import RestClient
except ImportError as e:
    # Fallback for standalone operation or initial debugging.
    # This block ensures the DataLoader can still be instantiated even if
    # some core dependencies are not yet fully set up, though functionality will be limited.
    print(f"Warning: Could not import custom modules: {e}")
    class DataLoadError(Exception):
        pass
    class ConfigurationError(Exception):
        pass
    class ValidationError(Exception):
        pass
    # Define mock objects for critical dependencies if imports fail,
    # to prevent NameError and allow the program to proceed for debugging.
    class MockConfigLoader:
        def load_config_file(self, filename): return {} # Mock method for load_config_file
    config_loader = MockConfigLoader()
    class MockDatabaseConnector:
        def __init__(self, *args, **kwargs): pass
        def execute_query(self, query): return pd.DataFrame()
        def close(self): pass
    DatabaseConnector = MockDatabaseConnector
    class MockRestClient:
        def __init__(self, *args, **kwargs): pass
        def get(self, *args, **kwargs): return requests.Response()
        def post(self, *args, **kwargs): return requests.Response()
        def close(self): pass
    RestClient = MockRestClient
    # Fallback for logger if it couldn't be imported
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)


class DataLoader:
    """
    Comprehensive data loader for test automation framework.
    
    Supports loading data from various sources including files, databases,
    APIs, and web services with data transformation and validation capabilities.
    """
    
    def __init__(self, config_path: Optional[str] = None, cache_enabled: bool = True):
        """
        Initialize DataLoader with configuration.
        
        Args:
            config_path: Path to configuration file. This is now primarily for
                         loading DataLoader's *internal* settings, not the main
                         application configuration (which is handled by config_loader).
            cache_enabled: Enable/disable data caching.
        """
        self.config_path = config_path
        self.cache_enabled = cache_enabled
        self.data_cache = {}
        self.transformations = {}
        self.validators = {}
        self.logger = self._setup_logger()
        
        # Load configuration for DataLoader's internal settings
        # This uses the global config_loader instance to load the specific file
        if config_path and os.path.exists(config_path):
            self.config = config_loader.load_config_file(os.path.basename(config_path))
        else:
            self.config = self._get_default_config()
            
        # Initialize connectors
        self.db_connector = None
        self.api_client = None
        
        # Data format handlers
        self.format_handlers = {
            '.csv': self._load_csv,
            '.json': self._load_json,
            '.yaml': self._load_yaml,
            '.yml': self._load_yaml,
            '.xml': self._load_xml,
            '.xlsx': self._load_excel,
            '.xls': self._load_excel,
            '.parquet': self._load_parquet,
            '.pickle': self._load_pickle,
            '.pkl': self._load_pickle,
            '.txt': self._load_text,
            '.tsv': self._load_tsv
        }

    def _setup_logger(self) -> logging.Logger:
        """Set up logging for data loader."""
        try:
            # Use the imported 'logger' instance directly
            return logger 
        except NameError: 
            # Fallback to standard logging if 'logger' could not be imported
            logging.basicConfig(level=logging.INFO)
            return logging.getLogger(__name__)

    def _load_configuration(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from file using the global config_loader instance.
        This is for DataLoader's internal configuration, not the main app config.
        """
        try:
            # Use the global config_loader instance to load the specific file
            return config_loader.load_config_file(os.path.basename(config_path))
        except Exception as e:
            self.logger.warning(f"Could not load config from {config_path}: {e}")
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration for DataLoader's internal settings."""
        return {
            'data_sources': {
                'csv': {'encoding': 'utf-8', 'delimiter': ','},
                'json': {'encoding': 'utf-8'},
                'excel': {'engine': 'openpyxl'},
                'xml': {'encoding': 'utf-8'},
                'yaml': {'safe_load': True}
            },
            'cache': {
                'enabled': True,
                'ttl': 3600,  # 1 hour
                'max_size': 100
            },
            'transformations': {
                'auto_convert_types': True,
                'handle_nulls': True,
                'strip_whitespace': True
            },
            'database': {
                'connection_timeout': 30,
                'query_timeout': 300
            },
            'api': {
                'timeout': 30,
                'retries': 3,
                'backoff_factor': 0.3
            }
        }

    def load_data(self, source: Union[str, Dict, List], 
                  data_type: Optional[str] = None,
                  **kwargs) -> Union[pd.DataFrame, Dict, List]:
        """
        Load data from various sources.
        
        Args:
            source: Data source (file path, URL, database query, etc.)
            data_type: Type of data source ('file', 'database', 'api', 'inline')
            **kwargs: Additional parameters for data loading
            
        Returns:
            Loaded data as DataFrame, dict, or list
        """
        try:
            # Auto-detect data type if not specified
            if data_type is None:
                data_type = self._detect_data_type(source)
            
            # Check cache first
            cache_key = self._generate_cache_key(source, data_type, kwargs)
            if self.cache_enabled and cache_key in self.data_cache:
                self.logger.info(f"Loading data from cache: {cache_key}")
                return self.data_cache[cache_key]['data']
            
            # Load data based on type
            if data_type == 'file':
                data = self._load_from_file(source, **kwargs)
            elif data_type == 'database':
                data = self._load_from_database(source, **kwargs)
            elif data_type == 'api':
                data = self._load_from_api(source, **kwargs)
            elif data_type == 'inline':
                data = self._load_inline_data(source, **kwargs)
            else:
                raise DataLoadError(f"Unsupported data type: {data_type}")
            
            # Apply transformations
            if kwargs.get('transform', True):
                data = self._apply_transformations(data, **kwargs)
            
            # Validate data
            if kwargs.get('validate', True):
                self._validate_data(data, **kwargs)
            
            # Cache the result
            if self.cache_enabled:
                self._cache_data(cache_key, data)
            
            self.logger.info(f"Successfully loaded data from {source}")
            return data
            
        except Exception as e:
            self.logger.error(f"Error loading data from {source}: {str(e)}")
            raise DataLoadError(f"Failed to load data: {str(e)}")

    def _detect_data_type(self, source: Union[str, Dict, List]) -> str:
        """Auto-detect the type of data source."""
        if isinstance(source, (dict, list)):
            return 'inline'
        elif isinstance(source, str):
            if source.startswith(('http://', 'https://')):
                return 'api'
            elif source.startswith(('SELECT', 'INSERT', 'UPDATE', 'DELETE')):
                return 'database'
            elif os.path.exists(source) or '.' in source:
                return 'file'
        return 'inline'

    def _load_from_file(self, file_path: str, **kwargs) -> Union[pd.DataFrame, Dict, List]:
        """Load data from file based on extension."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        file_extension = Path(file_path).suffix.lower()
        
        if file_extension in self.format_handlers:
            return self.format_handlers[file_extension](file_path, **kwargs)
        else:
            raise DataLoadError(f"Unsupported file format: {file_extension}")

    def _load_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Load CSV file."""
        csv_config = self.config['data_sources']['csv']
        
        params = {
            'encoding': kwargs.get('encoding', csv_config.get('encoding', 'utf-8')),
            'delimiter': kwargs.get('delimiter', csv_config.get('delimiter', ',')),
            'header': kwargs.get('header', 0),
            'index_col': kwargs.get('index_col', None),
            'skiprows': kwargs.get('skiprows', None),
            'nrows': kwargs.get('nrows', None),
            'na_values': kwargs.get('na_values', None),
            'dtype': kwargs.get('dtype', None),
            'parse_dates': kwargs.get('parse_dates', None)
        }
        
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}
        
        return pd.read_csv(file_path, **params)

    def _load_json(self, file_path: str, **kwargs) -> Union[Dict, List, pd.DataFrame]:
        """Load JSON file."""
        json_config = self.config['data_sources']['json']
        encoding = kwargs.get('encoding', json_config.get('encoding', 'utf-8'))
        
        with open(file_path, 'r', encoding=encoding) as f:
            data = json.load(f)
        
        # Convert to DataFrame if requested
        if kwargs.get('as_dataframe', False):
            if isinstance(data, list):
                return pd.DataFrame(data)
            elif isinstance(data, dict) and 'data' in data:
                return pd.DataFrame(data['data'])
            else:
                return pd.json_normalize(data)
        
        return data

    def _load_yaml(self, file_path: str, **kwargs) -> Union[Dict, List, pd.DataFrame]:
        """Load YAML file."""
        yaml_config = self.config['data_sources']['yaml']
        encoding = kwargs.get('encoding', 'utf-8')
        safe_load = kwargs.get('safe_load', yaml_config.get('safe_load', True))
        
        with open(file_path, 'r', encoding=encoding) as f:
            if safe_load:
                data = yaml.safe_load(f)
            else:
                data = yaml.load(f, Loader=yaml.FullLoader)
        
        # Convert to DataFrame if requested
        if kwargs.get('as_dataframe', False):
            if isinstance(data, list):
                return pd.DataFrame(data)
            else:
                return pd.json_normalize(data)
        
        return data

    def _load_xml(self, file_path: str, **kwargs) -> Union[Dict, pd.DataFrame]:
        """Load XML file."""
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Convert XML to dictionary
        data = self._xml_to_dict(root)
        
        # Convert to DataFrame if requested
        if kwargs.get('as_dataframe', False):
            # Try to find array-like structures to convert
            for key, value in data.items():
                if isinstance(value, list):
                    return pd.DataFrame(value)
            
            # If no arrays found, normalize the structure
            return pd.json_normalize(data)
        
        return data

    def _load_excel(self, file_path: str, **kwargs) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """Load Excel file."""
        excel_config = self.config['data_sources']['excel']
        
        params = {
            'engine': kwargs.get('engine', excel_config.get('engine', 'openpyxl')),
            'sheet_name': kwargs.get('sheet_name', 0),
            'header': kwargs.get('header', 0),
            'index_col': kwargs.get('index_col', None),
            'skiprows': kwargs.get('skiprows', None),
            'nrows': kwargs.get('nrows', None),
            'dtype': kwargs.get('dtype', None)
        }
        
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}
        
        return pd.read_excel(file_path, **params)

    def _load_parquet(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Load Parquet file."""
        return pd.read_parquet(file_path, **kwargs)

    def _load_pickle(self, file_path: str, **kwargs) -> Any:
        """Load pickled data."""
        with open(file_path, 'rb') as f:
            return pickle.load(f)

    def _load_text(self, file_path: str, **kwargs) -> Union[str, List[str], pd.DataFrame]:
        """Load text file."""
        encoding = kwargs.get('encoding', 'utf-8')
        
        with open(file_path, 'r', encoding=encoding) as f:
            if kwargs.get('as_lines', False):
                return f.readlines()
            else:
                content = f.read()
                
        if kwargs.get('as_dataframe', False):
            # Try to parse as structured text
            lines = content.strip().split('\n')
            if kwargs.get('delimiter'):
                data = [line.split(kwargs['delimiter']) for line in lines]
                return pd.DataFrame(data[1:], columns=data[0])
        
        return content

    def _load_tsv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Load TSV (Tab-Separated Values) file."""
        kwargs['delimiter'] = '\t'
        return self._load_csv(file_path, **kwargs)

    def _load_from_database(self, query: str, **kwargs) -> pd.DataFrame:
        """Load data from database."""
        if self.db_connector is None:
            self.db_connector = self._create_db_connector(**kwargs)
        
        try:
            return self.db_connector.execute_query(query)
        except Exception as e:
            raise DataLoadError(f"Database query failed: {str(e)}")

    def _load_from_api(self, url: str, **kwargs) -> Union[Dict, List, pd.DataFrame]:
        """Load data from API endpoint."""
        if self.api_client is None:
            self.api_client = self._create_api_client(**kwargs)
        
        try:
            method = kwargs.get('method', 'GET').upper()
            headers = kwargs.get('headers', {})
            params = kwargs.get('params', {})
            data = kwargs.get('data', None)
            
            if method == 'GET':
                response = self.api_client.get(url, headers=headers, params=params)
            elif method == 'POST':
                response = self.api_client.post(url, headers=headers, data=data, params=params)
            else:
                raise DataLoadError(f"Unsupported HTTP method: {method}")
            
            # Parse response based on content type
            content_type = response.headers.get('content-type', '').lower()
            
            if 'application/json' in content_type:
                api_data = response.json()
            elif 'text/csv' in content_type:
                api_data = pd.read_csv(StringIO(response.text))
            elif 'application/xml' in content_type or 'text/xml' in content_type:
                root = ET.fromstring(response.text)
                api_data = self._xml_to_dict(root)
            else:
                api_data = response.text
            
            # Convert to DataFrame if requested
            if kwargs.get('as_dataframe', False) and isinstance(api_data, (dict, list)):
                if isinstance(api_data, list):
                    return pd.DataFrame(api_data)
                elif 'data' in api_data:
                    return pd.DataFrame(api_data['data'])
                else:
                    return pd.json_normalize(api_data)
            
            return api_data
            
        except Exception as e:
            raise DataLoadError(f"API request failed: {str(e)}")

    def _load_inline_data(self, data: Union[Dict, List], **kwargs) -> Union[pd.DataFrame, Dict, List]:
        """Process inline data."""
        if kwargs.get('as_dataframe', False) and isinstance(data, (list, dict)):
            if isinstance(data, list):
                return pd.DataFrame(data)
            else:
                return pd.json_normalize(data)
        
        return data

    def load_multiple_sources(self, sources: List[Dict[str, Any]], 
                            merge_strategy: str = 'concat') -> pd.DataFrame:
        """
        Load data from multiple sources and combine them.
        
        Args:
            sources: List of source configurations
            merge_strategy: How to combine data ('concat', 'merge', 'join')
            
        Returns:
            Combined DataFrame
        """
        dataframes = []
        
        for source_config in sources:
            source = source_config.get('source')
            data_type = source_config.get('type')
            options = source_config.get('options', {})
            
            data = self.load_data(source, data_type, **options)
            
            # Ensure data is a DataFrame
            if not isinstance(data, pd.DataFrame):
                if isinstance(data, (list, dict)):
                    data = pd.json_normalize(data) if isinstance(data, dict) else pd.DataFrame(data)
                else:
                    raise DataLoadError(f"Cannot convert data to DataFrame: {type(data)}")
            
            dataframes.append(data)
        
        # Combine DataFrames based on strategy
        if merge_strategy == 'concat':
            return pd.concat(dataframes, ignore_index=True)
        elif merge_strategy == 'merge' and len(dataframes) >= 2:
            result = dataframes[0]
            for df in dataframes[1:]:
                result = pd.merge(result, df, how='outer')
            return result
        elif merge_strategy == 'join' and len(dataframes) >= 2:
            result = dataframes[0]
            for df in dataframes[1:]:
                result = result.join(df, rsuffix='_right')
            return result
        else:
            return dataframes[0] if dataframes else pd.DataFrame()

    def load_data_async(self, sources: List[Union[str, Dict]], 
                       max_workers: int = 5) -> List[Any]:
        """
        Load data from multiple sources asynchronously.
        
        Args:
            sources: List of data sources
            max_workers: Maximum number of concurrent workers
            
        Returns:
            List of loaded data
        """
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all loading tasks
            future_to_source = {}
            for source in sources:
                if isinstance(source, dict):
                    future = executor.submit(
                        self.load_data, 
                        source.get('source'),
                        source.get('type'),
                        **source.get('options', {})
                    )
                else:
                    future = executor.submit(self.load_data, source)
                future_to_source[future] = source
            
            # Collect results
            for future in as_completed(future_to_source): # Corrected from `futures` to `future_to_source`
                source = future_to_source[future]
                try:
                    data = future.result()
                    results.append({'source': source, 'data': data, 'success': True})
                except Exception as e:
                    results.append({'source': source, 'error': str(e), 'success': False})
        
        return results

    def load_streaming_data(self, source: str, chunk_size: int = 1000, 
                          **kwargs) -> Generator[pd.DataFrame, None, None]:
        """
        Load data in streaming fashion for large datasets.
        
        Args:
            source: Data source
            chunk_size: Size of each chunk
            **kwargs: Additional parameters
            
        Yields:
            DataFrame chunks
        """
        if source.endswith('.csv'):
            for chunk in pd.read_csv(source, chunksize=chunk_size, **kwargs):
                yield self._apply_transformations(chunk, **kwargs)
        
        elif source.startswith('SELECT'):
            # For database streaming, we need to implement pagination
            offset = 0
            while True:
                paginated_query = f"{source} LIMIT {chunk_size} OFFSET {offset}"
                chunk = self._load_from_database(paginated_query, **kwargs)
                
                if chunk.empty:
                    break
                    
                yield self._apply_transformations(chunk, **kwargs)
                offset += chunk_size
        
        else:
            # For other sources, load all data and chunk it
            data = self.load_data(source, **kwargs)
            if isinstance(data, pd.DataFrame):
                for i in range(0, len(data), chunk_size):
                    yield data.iloc[i:i+chunk_size]

    def register_transformation(self, name: str, func: Callable):
        """Register a custom data transformation function."""
        self.transformations[name] = func

    def register_validator(self, name: str, func: Callable):
        """Register a custom data validation function."""
        self.validators[name] = func

    def _apply_transformations(self, data: Any, **kwargs) -> Any:
        """Apply registered transformations to data."""
        if not isinstance(data, pd.DataFrame):
            return data
        
        # Apply built-in transformations
        if self.config['transformations']['strip_whitespace']:
            data = self._strip_whitespace(data)
        
        if self.config['transformations']['auto_convert_types']:
            data = self._auto_convert_types(data)
        
        if self.config['transformations']['handle_nulls']:
            data = self._handle_nulls(data)
        
        # Apply custom transformations
        transformations = kwargs.get('transformations', [])
        for transform_name in transformations:
            if transform_name in self.transformations:
                data = self.transformations[transform_name](data)
        
        return data

    def _validate_data(self, data: Any, **kwargs):
        """Validate loaded data."""
        validators = kwargs.get('validators', [])
        
        for validator_name in validators:
            if validator_name in self.validators:
                is_valid, message = self.validators[validator_name](data)
                if not is_valid:
                    raise ValidationError(f"Data validation failed: {message}")

    def _strip_whitespace(self, data: pd.DataFrame) -> pd.DataFrame:
        """Remove leading/trailing whitespace from string columns."""
        string_columns = data.select_dtypes(include=['object']).columns
        data[string_columns] = data[string_columns].apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        return data

    def _auto_convert_types(self, data: pd.DataFrame) -> pd.DataFrame:
        """Automatically convert data types."""
        for column in data.columns:
            # Try to convert to numeric
            if data[column].dtype == 'object':
                try:
                    data[column] = pd.to_numeric(data[column], errors='ignore')
                except:
                    pass
                
                # Try to convert to datetime
                if data[column].dtype == 'object':
                    try:
                        data[column] = pd.to_datetime(data[column], errors='ignore', infer_datetime_format=True)
                    except:
                        pass
        
        return data

    def _handle_nulls(self, data: pd.DataFrame) -> pd.DataFrame:
        """Handle null values in data."""
        # Replace common null representations
        null_values = ['', 'NULL', 'null', 'None', 'none', 'N/A', 'n/a', '#N/A']
        data = data.replace(null_values, np.nan)
        return data

    def _xml_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """Convert XML element to dictionary."""
        result = {}
        
        # Add attributes
        if element.attrib:
            result.update(element.attrib)
        
        # Add text content
        if element.text and element.text.strip():
            if len(element) == 0:
                return element.text.strip()
            else:
                result['text'] = element.text.strip()
        
        # Add child elements
        for child in element:
            child_data = self._xml_to_dict(child)
            
            if child.tag in result:
                # Convert to list if multiple elements with same tag
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        
        return result

    def _create_db_connector(self, **kwargs) -> 'DatabaseConnector':
        """Create database connector."""
        try:
            return DatabaseConnector(**kwargs)
        except NameError: # If DatabaseConnector import failed
            # Fallback to simple SQLite connector
            return self._create_simple_db_connector(**kwargs)

    def _create_simple_db_connector(self, **kwargs):
        """Create simple database connector for SQLite."""
        class SimpleDBConnector:
            def __init__(self, db_path: str = ':memory:'):
                self.connection = sqlite3.connect(db_path)
            
            def execute_query(self, query: str) -> pd.DataFrame:
                return pd.read_sql_query(query, self.connection)
            
            def close(self):
                self.connection.close()
        
        return SimpleDBConnector(kwargs.get('db_path', ':memory:'))

    def _create_api_client(self, **kwargs) -> 'RestClient':
        """Create API client."""
        try:
            return RestClient(**kwargs)
        except NameError: # If RestClient import failed
            # Fallback to simple requests-based client
            return self._create_simple_api_client(**kwargs)

    def _create_simple_api_client(self, **kwargs):
        """Create simple API client using requests."""
        class SimpleAPIClient:
            def __init__(self, base_url: str = '', timeout: int = 30):
                self.base_url = base_url
                self.timeout = timeout
                self.session = requests.Session()
            
            def get(self, url: str, headers: Dict = None, params: Dict = None):
                full_url = urljoin(self.base_url, url)
                return self.session.get(full_url, headers=headers, params=params, timeout=self.timeout)
            
            def post(self, url: str, headers: Dict = None, data: Any = None, params: Dict = None):
                full_url = urljoin(self.base_url, url)
                return self.session.post(full_url, headers=headers, json=data, params=params, timeout=self.timeout)
        
        return SimpleAPIClient(
            kwargs.get('base_url', ''),
            kwargs.get('timeout', 30)
        )

    def _generate_cache_key(self, source: Any, data_type: str, kwargs: Dict) -> str:
        """Generate cache key for data."""
        import hashlib
        key_string = f"{source}_{data_type}_{sorted(kwargs.items())}"
        return hashlib.md5(key_string.encode()).hexdigest()

    def _cache_data(self, cache_key: str, data: Any):
        """Cache loaded data."""
        if len(self.data_cache) >= self.config['cache']['max_size']:
            # Remove oldest entry
            oldest_key = next(iter(self.data_cache))
            del self.data_cache[oldest_key]
        
        self.data_cache[cache_key] = {
            'data': data,
            'timestamp': time.time()
        }

    def clear_cache(self):
        """Clear data cache."""
        self.data_cache.clear()
        self.logger.info("Data cache cleared")

    def get_cache_info(self) -> Dict[str, Any]:
        """Get cache information."""
        return {
            'cache_size': len(self.data_cache),
            'max_size': self.config['cache']['max_size'],
            'keys': list(self.data_cache.keys())
        }

    def save_data(self, data: Union[pd.DataFrame, Dict, List], 
                  output_path: str, format_type: Optional[str] = None, **kwargs):
        """
        Save data to file.
        
        Args:
            data: Data to save
            output_path: Output file path
            format_type: Output format (auto-detected if None)
            **kwargs: Additional parameters
        """
        if format_type is None:
            format_type = Path(output_path).suffix.lower()
        
        if format_type in ['.csv']:
            if isinstance(data, pd.DataFrame):
                data.to_csv(output_path, index=False, **kwargs)
            else:
                raise DataLoadError("CSV format requires DataFrame")
        
        elif format_type in ['.json']:
            with open(output_path, 'w') as f:
                if isinstance(data, pd.DataFrame):
                    data.to_json(f, orient='records', **kwargs)
                else:
                    json.dump(data, f, **kwargs)
        
        elif format_type in ['.xlsx', '.xls']:
            if isinstance(data, pd.DataFrame):
                data.to_excel(output_path, index=False, **kwargs)
            else:
                raise DataLoadError("Excel format requires DataFrame")
        
        elif format_type in ['.parquet']:
            if isinstance(data, pd.DataFrame):
                data.to_parquet(output_path, **kwargs)
            else:
                raise DataLoadError("Parquet format requires DataFrame")
        
        elif format_type in ['.pickle', '.pkl']:
            with open(output_path, 'wb') as f:
                pickle.dump(data, f)
        
        else:
            raise DataLoadError(f"Unsupported output format: {format_type}")
        
        self.logger.info(f"Data saved to {output_path}")

    def get_data_info(self, data: Any) -> Dict[str, Any]:
        """Get information about loaded data."""
        info = {
            'type': type(data).__name__,
            'size': sys.getsizeof(data)
        }
        
        if isinstance(data, pd.DataFrame):
            info.update({
                'shape': data.shape,
                'columns': list(data.columns),
                'dtypes': data.dtypes.to_dict(),
                'memory_usage': data.memory_usage(deep=True).sum(),
                'null_counts': data.isnull().sum().to_dict()
            })
        
        elif isinstance(data, (list, dict)):
            info['length'] = len(data)
            if isinstance(data, list) and data:
                info['sample_item_type'] = type(data[0]).__name__
            elif isinstance(data, dict):
                info['keys'] = list(data.keys())
        
        return info

    def create_data_pipeline(self, steps: List[Dict[str, Any]]) -> 'DataPipeline':
        """
        Create a data processing pipeline.
        
        Args:
            steps: List of pipeline steps
            
        Returns:
            DataPipeline object
        """
        return DataPipeline(self, steps)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.db_connector and hasattr(self.db_connector, 'close'):
            self.db_connector.close()
        
        if self.api_client and hasattr(self.api_client, 'close'):
            self.api_client.close()


class DataPipeline:
    """
    Data processing pipeline for chaining operations.
    """
    
    def __init__(self, data_loader: DataLoader, steps: List[Dict[str, Any]]):
        """
        Initialize data pipeline.
        
        Args:
            data_loader: DataLoader instance
            steps: List of pipeline steps
        """
        self.data_loader = data_loader
        self.steps = steps
        self.logger = data_loader.logger # Use the same logger as DataLoader
        
    def run(self, initial_data: Optional[Any] = None) -> Any:
        """
        Execute the data pipeline.
        
        Args:
            initial_data: Optional initial data to start the pipeline with.
            
        Returns:
            The final processed data.
        """
        current_data = initial_data
        
        for i, step_config in enumerate(self.steps):
            step_type = step_config.get('type')
            step_args = step_config.get('args', {})
            
            self.logger.info(f"Executing pipeline step {i+1}: {step_type}")
            
            try:
                if step_type == 'load_data':
                    current_data = self.data_loader.load_data(**step_args)
                elif step_type == 'transform':
                    transform_name = step_args.get('name')
                    if transform_name in self.data_loader.transformations:
                        current_data = self.data_loader.transformations[transform_name](current_data, **step_args)
                    else:
                        raise DataLoadError(f"Unknown transformation: {transform_name}")
                elif step_type == 'validate':
                    validator_name = step_args.get('name')
                    if validator_name in self.data_loader.validators:
                        is_valid, message = self.data_loader.validators[validator_name](current_data, **step_args)
                        if not is_valid:
                            raise ValidationError(f"Pipeline validation failed: {message}")
                    else:
                        raise DataLoadError(f"Unknown validator: {validator_name}")
                elif step_type == 'save_data':
                    self.data_loader.save_data(current_data, **step_args)
                else:
                    raise DataLoadError(f"Unsupported pipeline step type: {step_type}")
            except Exception as e:
                self.logger.error(f"Error in pipeline step {i+1} ({step_type}): {e}")
                raise DataLoadError(f"Pipeline execution failed at step {i+1}")
                
        return current_data
