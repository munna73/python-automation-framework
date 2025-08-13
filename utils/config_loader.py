"""Simplified configuration loader without active environment dependency."""
import os
import configparser
from typing import Dict, Any, Optional, Union, List, Tuple
from functools import lru_cache
from pathlib import Path
import json
import yaml
import base64
from dataclasses import dataclass, field
import re
from datetime import datetime, timedelta
import threading

from utils.custom_exceptions import ConfigurationError


@dataclass
class DatabaseConfig:
    """Enhanced database configuration data class."""
    host: str
    port: int
    database: str
    username: str
    password: str
    ssl_enabled: bool = False
    pool_size: int = 5
    timeout: int = 30
    max_overflow: int = 10
    pool_pre_ping: bool = True
    pool_recycle: int = 3600
    connect_args: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.host:
            raise ConfigurationError("Database host cannot be empty")
        if not (1 <= self.port <= 65535):
            raise ConfigurationError(f"Invalid port number: {self.port}")
        if not self.username:
            raise ConfigurationError("Database username cannot be empty")
        if not self.password:
            raise ConfigurationError("Database password cannot be empty")
    
    def to_connection_string(self, db_type: str, include_credentials: bool = True) -> str:
        """Convert config to connection string with optional credential masking."""
        if not include_credentials:
            username = "***"
            password = "***"
        else:
            username = self.username
            password = self.password
            
        if db_type.upper() == 'ORACLE':
            return f"{username}/{password}@{self.host}:{self.port}/{self.database}"
        elif db_type.upper() == 'POSTGRES':
            return f"postgresql://{username}:{password}@{self.host}:{self.port}/{self.database}"
        elif db_type.upper() == 'MONGODB':
            return f"mongodb://{username}:{password}@{self.host}:{self.port}/{self.database}"
        elif db_type.upper() == 'MYSQL':
            return f"mysql://{username}:{password}@{self.host}:{self.port}/{self.database}"
        else:
            raise ConfigurationError(f"Unknown database type: {db_type}")
    
    def to_dict(self, include_credentials: bool = False) -> Dict[str, Any]:
        """Convert to dictionary with optional credential masking."""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'username': self.username if include_credentials else "***",
            'password': self.password if include_credentials else "***",
            'ssl_enabled': self.ssl_enabled,
            'pool_size': self.pool_size,
            'timeout': self.timeout,
            'max_overflow': self.max_overflow,
            'pool_pre_ping': self.pool_pre_ping,
            'pool_recycle': self.pool_recycle,
            'connect_args': self.connect_args
        }


@dataclass
class ComparisonConfig:
    """Configuration for database comparison settings."""
    source_table: str
    target_table: str
    primary_key: str
    omit_columns: List[str] = field(default_factory=list)
    omit_values: List[str] = field(default_factory=list)
    chunk_size: int = 50000
    enable_performance_monitoring: bool = True
    data_quality_threshold: float = 90.0
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.primary_key:
            raise ConfigurationError("Primary key cannot be empty")
        if not (0.0 <= self.data_quality_threshold <= 100.0):
            raise ConfigurationError("Data quality threshold must be between 0 and 100")


class ConfigLoader:
    """Simplified configuration loader without active environment dependency."""
    
    # Define which fields should be resolved from environment variables
    SENSITIVE_FIELDS = {
        'username', 'password', 'pwd', 'token', 'key', 'secret',
        'aws_access_key', 'aws_secret_key', 'access_key_id', 'secret_access_key'
    }
    
    # Configuration validation rules
    VALIDATION_RULES = {
        'port': lambda x: 1 <= int(x) <= 65535,
        'timeout': lambda x: int(x) > 0,
        'pool_size': lambda x: int(x) > 0,
        'retry_count': lambda x: int(x) >= 0,
    }
    
    def __init__(self, config_dir: Optional[str] = None, cache_timeout: int = 300):
        """
        Initialize the ConfigLoader.
        
        Args:
            config_dir: The directory where configuration files are located
            cache_timeout: Cache timeout in seconds
        """
        self.config_dir = Path(config_dir or "config")
        self.cache_timeout = cache_timeout
        
        # Thread-safe caching
        self._config_cache: Dict[str, Tuple[Any, datetime]] = {}
        self._cache_lock = threading.RLock()
        
        # Configuration file watchers
        self._file_timestamps: Dict[str, float] = {}
        
        # Initialize logging
        try:
            from utils.logger import get_logger
            self.logger = get_logger("config_loader")
        except ImportError:
            import logging
            self.logger = logging.getLogger(__name__)
    
    def _is_cache_valid(self, cache_time: datetime) -> bool:
        """Check if cache entry is still valid."""
        return datetime.now() - cache_time < timedelta(seconds=self.cache_timeout)
    
    def _should_resolve_from_env(self, key: str, value: str) -> bool:
        """Check if a configuration value should be resolved from environment variables."""
        key_lower = key.lower()
        if any(sensitive in key_lower for sensitive in self.SENSITIVE_FIELDS):
            # Check if the value looks like an environment variable reference
            if re.match(r'^[A-Z][A-Z0-9_]*$', value):
                return True
        return False
    
    def _resolve_value(self, key: str, value: str, context: str = "") -> str:
        """Resolve a configuration value from environment variables if needed."""
        # Handle environment variable resolution
        if self._should_resolve_from_env(key, value):
            env_value = os.getenv(value)
            if env_value:
                return env_value
            else:
                raise ConfigurationError(
                    f"Environment variable '{value}' not found. "
                    f"Please set it as a system environment variable. "
                    f"Context: {context}",
                    config_key=value
                )
        
        return value
    
    def _validate_value(self, key: str, value: str, context: str = "") -> str:
        """Validate configuration value according to rules."""
        key_lower = key.lower()
        for rule_key, rule_func in self.VALIDATION_RULES.items():
            if rule_key in key_lower:
                try:
                    if not rule_func(value):
                        raise ConfigurationError(f"Validation failed for {context}: {key}={value}")
                except (ValueError, TypeError) as e:
                    raise ConfigurationError(f"Invalid value for {context}: {key}={value} ({str(e)})")
        return value
    
    def _is_file_modified(self, filename: str) -> bool:
        """Check if file has been modified since last load."""
        file_path = self.config_dir / filename
        if not file_path.exists():
            return False
        
        current_mtime = file_path.stat().st_mtime
        cached_mtime = self._file_timestamps.get(filename, 0)
        
        if current_mtime > cached_mtime:
            self._file_timestamps[filename] = current_mtime
            return True
        return False
    
    @lru_cache(maxsize=32)
    def load_config_file(self, filename: str, force_reload: bool = False) -> Dict[str, Any]:
        """
        Load configuration from file with caching.
        
        NOTE: This loads the ENTIRE config file regardless of which sections exist.
        All sections in the file become available for use.
        """
        cache_key = f"file_{filename}"
        
        with self._cache_lock:
            # Check cache first
            if not force_reload and cache_key in self._config_cache:
                cached_data, cache_time = self._config_cache[cache_key]
                if self._is_cache_valid(cache_time) and not self._is_file_modified(filename):
                    self.logger.debug(f"Using cached config for {filename}")
                    return cached_data
            
            # Load from file
            file_path = self.config_dir / filename
            
            if not file_path.exists():
                raise ConfigurationError(f"Configuration file not found: {file_path}",
                                       config_file=str(file_path))
            
            try:
                if filename.endswith('.ini'):
                    data = self._load_ini_config(file_path)
                elif filename.endswith('.json'):
                    data = self._load_json_config(file_path)
                elif filename.endswith(('.yml', '.yaml')):
                    data = self._load_yaml_config(file_path)
                else:
                    raise ConfigurationError(f"Unsupported config format: {filename}",
                                           config_file=filename)
                
                # Cache the result
                self._config_cache[cache_key] = (data, datetime.now())
                self.logger.debug(f"Loaded and cached config from {filename}")
                return data
                
            except Exception as e:
                if isinstance(e, ConfigurationError):
                    raise
                raise ConfigurationError(f"Failed to load config file: {str(e)}",
                                       config_file=str(file_path))
    
    def _load_ini_config(self, file_path: Path) -> Dict[str, Any]:
        """Load INI configuration file with enhanced processing."""
        config = configparser.ConfigParser(interpolation=None)
        config.read(file_path, encoding='utf-8')
        
        result = {}
        for section in config.sections():
            result[section] = {}
            for key, value in config[section].items():
                context = f"{section}.{key}"
                try:
                    # Resolve and validate value
                    resolved_value = self._resolve_value(key, value, context)
                    validated_value = self._validate_value(key, resolved_value, context)
                    result[section][key] = validated_value
                except Exception as e:
                    self.logger.error(f"Error processing {context}: {str(e)}")
                    raise
        
        return result
    
    def _load_json_config(self, file_path: Path) -> Dict[str, Any]:
        """Load JSON configuration file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return self._resolve_dict_values(data)
    
    def _load_yaml_config(self, file_path: Path) -> Dict[str, Any]:
        """Load YAML configuration file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return self._resolve_dict_values(data)
    
    def _resolve_dict_values(self, data: Any, context: str = "") -> Any:
        """Recursively resolve and validate values in dictionary."""
        if isinstance(data, dict):
            result = {}
            for k, v in data.items():
                new_context = f"{context}.{k}" if context else k
                if isinstance(v, str):
                    try:
                        resolved_value = self._resolve_value(k, v, new_context)
                        result[k] = self._validate_value(k, resolved_value, new_context)
                    except Exception as e:
                        self.logger.error(f"Error processing {new_context}: {str(e)}")
                        raise
                else:
                    result[k] = self._resolve_dict_values(v, new_context)
            return result
        elif isinstance(data, list):
            return [self._resolve_dict_values(item, f"{context}[{i}]") 
                    for i, item in enumerate(data)]
        else:
            return data
    
    def get_database_config(self, section_name: str) -> DatabaseConfig:
        """
        Get database configuration from ANY section name.
        
        Args:
            section_name: The exact section name in config.ini (e.g., "SAT_ORACLE", "SAT_POSTGRES")
        
        Returns:
            DatabaseConfig object
        """
        config = self.load_config_file("config.ini")
        
        if section_name not in config:
            available_sections = list(config.keys())
            raise ConfigurationError(
                f"Configuration section '{section_name}' not found. "
                f"Available sections: {available_sections}",
                config_key=section_name
            )
        
        db_config = config[section_name]
        
        try:
            return DatabaseConfig(
                host=db_config['host'],
                port=int(db_config['port']),
                database=db_config.get('database', db_config.get('service_name', '')),
                username=db_config['username'],
                password=db_config['password'],
                ssl_enabled=db_config.get('ssl_enabled', 'false').lower() == 'true',
                pool_size=int(db_config.get('pool_size', 5)),
                timeout=int(db_config.get('timeout', 30)),
                max_overflow=int(db_config.get('max_overflow', 10)),
                pool_pre_ping=db_config.get('pool_pre_ping', 'true').lower() == 'true',
                pool_recycle=int(db_config.get('pool_recycle', 3600)),
                connect_args=json.loads(db_config.get('connect_args', '{}'))
            )
            
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            raise ConfigurationError(f"Invalid configuration for {section_name}: {str(e)}",
                                   config_key=section_name)
    
    def get_comparison_config(self, section_name: str = "comparison_settings") -> ComparisonConfig:
        """
        Get database comparison configuration from specified section.
        
        Args:
            section_name: Section name in config file (default: "comparison_settings")
        """
        config = self.load_config_file("config.ini")
        
        if section_name not in config:
            available_sections = list(config.keys())
            raise ConfigurationError(
                f"Comparison configuration section '{section_name}' not found. "
                f"Available sections: {available_sections}",
                config_key=section_name
            )
        
        comp_config = config[section_name]
        
        try:
            return ComparisonConfig(
                source_table=comp_config.get('SRCE_TABLE', comp_config.get('source_table', '')),
                target_table=comp_config.get('TRGT_TABLE', comp_config.get('target_table', '')),
                primary_key=comp_config['primary_key'],
                omit_columns=[col.strip() for col in comp_config.get('omit_columns', '').split(',') if col.strip()],
                omit_values=[val.strip() for val in comp_config.get('omit_values', '').split(',') if val.strip()],
                chunk_size=int(comp_config.get('chunk_size', 50000)),
                enable_performance_monitoring=comp_config.get('enable_performance_monitoring', 'true').lower() == 'true',
                data_quality_threshold=float(comp_config.get('data_quality_threshold', 90.0))
            )
        except (KeyError, ValueError) as e:
            raise ConfigurationError(f"Invalid comparison configuration in section '{section_name}': {str(e)}",
                                   config_key=section_name)
    
    def get_custom_config(self, section: str, key: Optional[str] = None, default: Any = None) -> Any:
        """
        Get custom configuration from any section in config.ini.
        
        Args:
            section: Section name in config file
            key: Optional specific key within section
            default: Default value if key/section not found
        
        Returns:
            Configuration value, section dict, or default
        """
        try:
            config = self.load_config_file("config.ini")
            
            if section not in config:
                if default is not None:
                    return default
                available_sections = list(config.keys())
                raise ConfigurationError(
                    f"Section '{section}' not found in config. "
                    f"Available sections: {available_sections}",
                    config_key=section
                )
            
            if key:
                if key not in config[section]:
                    if default is not None:
                        return default
                    available_keys = list(config[section].keys())
                    raise ConfigurationError(
                        f"Key '{key}' not found in section '{section}'. "
                        f"Available keys: {available_keys}",
                        config_key=f"{section}.{key}"
                    )
                return config[section][key]
            
            return config[section]
            
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(f"Failed to get custom config: {str(e)}",
                                   config_key=f"{section}.{key if key else ''}")
    
    def list_available_sections(self) -> List[str]:
        """List all available configuration sections."""
        try:
            config = self.load_config_file("config.ini")
            return list(config.keys())
        except Exception as e:
            self.logger.error(f"Failed to list sections: {str(e)}")
            return []
    
    def section_exists(self, section_name: str) -> bool:
        """Check if a configuration section exists."""
        try:
            config = self.load_config_file("config.ini")
            return section_name in config
        except Exception:
            return False
    
    def validate_specific_sections(self, section_names: List[str]) -> Dict[str, bool]:
        """
        Validate specific configuration sections.
        
        Args:
            section_names: List of section names to validate
        
        Returns:
            Dictionary mapping section names to validation results
        """
        validation_results = {}
        
        for section_name in section_names:
            try:
                # Just try to access the section
                config = self.load_config_file("config.ini")
                if section_name in config:
                    # Check if it has minimum required fields
                    section_data = config[section_name]
                    if 'host' in section_data and 'port' in section_data:
                        validation_results[section_name] = True
                    else:
                        self.logger.warning(f"Section '{section_name}' missing required fields")
                        validation_results[section_name] = False
                else:
                    self.logger.error(f"Section '{section_name}' not found")
                    validation_results[section_name] = False
                    
            except Exception as e:
                self.logger.error(f"Validation failed for section '{section_name}': {str(e)}")
                validation_results[section_name] = False
        
        # Summary
        total_sections = len(validation_results)
        passed_sections = sum(validation_results.values())
        
        if passed_sections == total_sections:
            self.logger.info(f"✓ All {total_sections} specified sections validated successfully")
        else:
            self.logger.error(f"✗ {passed_sections}/{total_sections} sections passed validation")
            for section, result in validation_results.items():
                status = "✓" if result else "✗"
                self.logger.info(f"  {status} {section}")
        
        return validation_results
    
    def print_environment_status(self) -> None:
        """Print status of required environment variables for debugging."""
        self.logger.info("=== Environment Variables Status ===")
        
        patterns = [
            r'.*_ORACLE_USERNAME$',
            r'.*_ORACLE_PASSWORD$', 
            r'.*_POSTGRES_USERNAME$',
            r'.*_POSTGRES_PASSWORD$',
            r'.*_USERNAME$',
            r'.*_PASSWORD$',
            r'.*_TOKEN$'
        ]
        
        found_vars = []
        
        for pattern in patterns:
            for var in os.environ.keys():
                if re.match(pattern, var):
                    found_vars.append(var)
        
        if found_vars:
            self.logger.info("Found environment variables:")
            for var in sorted(set(found_vars)):
                self.logger.info(f"  ✓ {var}: SET")
        else:
            self.logger.warning("No credential environment variables found")
        
        self.logger.info("=" * 40)
    
    def reload_config(self) -> None:
        """Clear cache and reload configuration."""
        with self._cache_lock:
            self._config_cache.clear()
            self._file_timestamps.clear()
        
        # Clear LRU cache  
        self.load_config_file.cache_clear()
        self.logger.info("Configuration cache cleared and reloaded")
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get a summary of current configuration."""
        summary = {
            'config_directory': str(self.config_dir),
            'cache_entries': len(self._config_cache),
            'cache_timeout': self.cache_timeout,
            'available_sections': self.list_available_sections(),
            'config_files': []
        }
        
        # List available config files
        for ext in ['*.ini', '*.json', '*.yml', '*.yaml']:
            summary['config_files'].extend([
                f.name for f in self.config_dir.glob(ext)
            ])
        
        return summary


# Example usage for your specific use case
if __name__ == "__main__":
    # Your usage pattern
    loader = ConfigLoader(config_dir="config")
    
    # This is how you'll use it in your feature files
    try:
        # Load specific database configurations by section name
        oracle_config = loader.get_database_config("SAT_ORACLE")
        postgres_config = loader.get_database_config("SAT_POSTGRES")
        
        print(f"Oracle: {oracle_config.host}:{oracle_config.port}")
        print(f"Postgres: {postgres_config.host}:{postgres_config.port}")
        
        # Load comparison settings
        comparison_config = loader.get_comparison_config("comparison_settings")
        print(f"Primary key: {comparison_config.primary_key}")
        
        # Load custom config sections
        custom_section = loader.get_custom_config("custom_section")
        specific_value = loader.get_custom_config("custom_section", "specific_key")
        
        # List what's available
        print("Available sections:", loader.list_available_sections())
        
        # Validate specific sections you care about
        validation_results = loader.validate_specific_sections([
            "SAT_ORACLE", "SAT_POSTGRES", "comparison_settings"
        ])
        print("Validation results:", validation_results)
        
    except ConfigurationError as e:
        print(f"Configuration error: {e}")


#         @given('I connect to Oracle database using "{db_section}" configuration')
# def connect_to_oracle(context, db_section):
#     """Establish connection using ANY section name."""
#     try:
#         # Direct section access - no active environment needed
#         db_config = context.config_loader.get_database_config(db_section)
        
#         # Rest of your connection logic unchanged
#         oracle_engine = db_comparison_manager.get_oracle_connection(db_section)
#         context.oracle_engine = oracle_engine
        
#     except Exception as e:
#         logger.error(f"Oracle connection failed for section '{db_section}': {str(e)}")
#         raise