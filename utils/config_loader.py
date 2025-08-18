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
    """Tag-aware configuration loader with lazy loading support."""
    
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
    
    # Tag to section mapping - defines which sections are needed for each tag
    TAG_TO_SECTIONS = {
        'oracle': ['*_ORACLE', 'comparison_settings', 'QUERIES'],
        'postgres': ['*_POSTGRES', 'comparison_settings', 'QUERIES'], 
        'mongodb': ['*_MONGODB'],
        'kafka': ['*_KAFKA'],
        'aws': ['*_SQS', '*_S3'],
        'sqs': ['*_SQS'],
        's3': ['*_S3'],
        'mq': ['*_MQ', '*_MQ_FIFO'],
        'database': ['*_ORACLE', '*_POSTGRES', 'comparison_settings', 'QUERIES'],
        'comparison': ['comparison_settings'],
        'api': ['API', '*_API'],
        # Environment-specific tags
        'dev': ['DEFAULT', 'QUERIES'],
        'qa': ['DEFAULT', 'QUERIES'],
        'staging': ['DEFAULT', 'QUERIES'],
        'prod': ['DEFAULT', 'QUERIES'],
        # Specific system tags
        'S101': ['S101_*'],
        'S102': ['S102_*'],
        'S103': ['S103_*'],
        'P101': ['P101_*'],
        'P102': ['P102_*'],
        'P103': ['P103_*'],
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
        
        # Tag-based loading state
        self._active_tags: List[str] = []
        self._required_sections: set = set()
        self._loaded_sections: Dict[str, Any] = {}
        self._lazy_loading_enabled: bool = True
        
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
            # More precise matching to avoid false positives
            if key_lower == rule_key or key_lower.endswith('_' + rule_key):
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
    
    def _auto_detect_active_tags(self) -> List[str]:
        """
        Auto-detect active tags from the current execution context.
        This works by inspecting the call stack to find Behave context.
        
        Returns:
            List of active tags
        """
        import inspect
        
        # Look for Behave context in the call stack
        for frame_info in inspect.stack():
            frame_locals = frame_info.frame.f_locals
            
            # Look for 'context' with scenario
            if 'context' in frame_locals:
                context = frame_locals['context']
                if hasattr(context, 'scenario') and context.scenario:
                    tags = []
                    # Get tags from scenario
                    if hasattr(context.scenario, 'tags'):
                        tags.extend(context.scenario.tags)
                    # Get tags from feature  
                    if hasattr(context.scenario, 'feature') and hasattr(context.scenario.feature, 'tags'):
                        tags.extend(context.scenario.feature.tags)
                    
                    if tags:
                        self.logger.debug(f"Auto-detected tags from context: {tags}")
                        return tags
        
        self.logger.debug("No tags auto-detected, lazy loading disabled")
        return []
    
    def set_active_tags(self, tags: List[str]) -> None:
        """
        Set the active tags for this test run to enable lazy loading.
        
        Args:
            tags: List of active tags from the test execution
        """
        self._active_tags = [tag.lstrip('@') for tag in tags]  # Remove @ prefix if present
        self._required_sections = self._determine_required_sections()
        self.logger.info(f"Active tags set: {self._active_tags}")
        self.logger.info(f"Required sections determined: {sorted(self._required_sections)}")
    
    def _determine_required_sections(self) -> set:
        """
        Determine which configuration sections are required based on active tags.
        
        Returns:
            Set of section names/patterns that need to be loaded
        """
        required_sections = set()
        
        # Always include DEFAULT section
        required_sections.add('DEFAULT')
        
        # Map tags to sections
        for tag in self._active_tags:
            if tag in self.TAG_TO_SECTIONS:
                required_sections.update(self.TAG_TO_SECTIONS[tag])
        
        # If no specific tags found, load everything (fallback)
        if len(required_sections) == 1:  # Only DEFAULT
            self.logger.warning(f"No matching sections found for tags {self._active_tags}, loading all sections")
            self._lazy_loading_enabled = False
        
        return required_sections
    
    def _section_matches_pattern(self, section_name: str, pattern: str) -> bool:
        """
        Check if a section name matches a pattern.
        
        Args:
            section_name: Actual section name
            pattern: Pattern (can contain wildcards like '*_ORACLE')
        
        Returns:
            True if section matches pattern
        """
        if pattern == section_name:
            return True
        
        if '*' in pattern:
            # Convert pattern to regex
            regex_pattern = pattern.replace('*', '.*')
            return re.match(f'^{regex_pattern}$', section_name) is not None
        
        return False
    
    def _should_load_section(self, section_name: str) -> bool:
        """
        Determine if a section should be loaded based on active tags.
        
        Args:
            section_name: Name of the section to check
        
        Returns:
            True if section should be loaded
        """
        if not self._lazy_loading_enabled:
            return True
        
        if not self._required_sections:
            return True
        
        # Check if section matches any required pattern
        for required_pattern in self._required_sections:
            if self._section_matches_pattern(section_name, required_pattern):
                return True
        
        return False
    
    @lru_cache(maxsize=32)
    def load_config_file(self, filename: str, force_reload: bool = False) -> Dict[str, Any]:
        """
        Load configuration from file with tag-aware lazy loading.
        
        NOTE: With lazy loading enabled, this only loads sections needed by active tags.
        """
        if self._lazy_loading_enabled and self._loaded_sections:
            # Return cached lazy-loaded sections
            cache_key = f"lazy_{filename}_{hash(frozenset(self._active_tags))}"
            
            with self._cache_lock:
                if cache_key in self._config_cache:
                    cached_data, cache_time = self._config_cache[cache_key]
                    if self._is_cache_valid(cache_time) and not self._is_file_modified(filename):
                        self.logger.debug(f"Using cached lazy-loaded config for {filename}")
                        return cached_data
        
        return self._load_config_with_lazy_loading(filename, force_reload)
    
    def _load_config_with_lazy_loading(self, filename: str, force_reload: bool = False) -> Dict[str, Any]:
        """
        Load configuration with lazy loading based on active tags.
        Auto-detects tags if not already set.
        """
        cache_key = f"file_{filename}"
        
        # Auto-detect tags if not already set
        if not self._active_tags and self._lazy_loading_enabled:
            detected_tags = self._auto_detect_active_tags()
            if detected_tags:
                self.set_active_tags(detected_tags)
            else:
                self._lazy_loading_enabled = False
        
        with self._cache_lock:
            # Load full config first
            file_path = self.config_dir / filename
            
            if not file_path.exists():
                raise ConfigurationError(f"Configuration file not found: {file_path}",
                                       config_file=str(file_path))
            
            try:
                if filename.endswith('.ini'):
                    full_data = self._load_ini_config_lazy(file_path)
                elif filename.endswith('.json'):
                    full_data = self._load_json_config(file_path)
                elif filename.endswith(('.yml', '.yaml')):
                    full_data = self._load_yaml_config(file_path)
                else:
                    raise ConfigurationError(f"Unsupported config format: {filename}",
                                           config_file=filename)
                
                # Cache the result
                if self._lazy_loading_enabled:
                    lazy_cache_key = f"lazy_{filename}_{hash(frozenset(self._active_tags))}"
                    self._config_cache[lazy_cache_key] = (full_data, datetime.now())
                    
                    loaded_section_names = list(full_data.keys())
                    skipped_sections = []
                    
                    # Log what was loaded vs skipped
                    all_sections = self._get_all_section_names(file_path)
                    for section in all_sections:
                        if section not in loaded_section_names:
                            skipped_sections.append(section)
                    
                    self.logger.info(f"Tag-based loading for {self._active_tags}: loaded {len(loaded_section_names)} sections, skipped {len(skipped_sections)} sections")
                    self.logger.debug(f"Loaded sections: {loaded_section_names}")
                    if skipped_sections:
                        self.logger.debug(f"Skipped sections: {skipped_sections}")
                else:
                    self._config_cache[cache_key] = (full_data, datetime.now())
                    self.logger.debug(f"Loaded full config from {filename}")
                
                return full_data
                
            except Exception as e:
                if isinstance(e, ConfigurationError):
                    raise
                raise ConfigurationError(f"Failed to load config file: {str(e)}",
                                       config_file=str(file_path))
    
    def _get_all_section_names(self, file_path: Path) -> List[str]:
        """
        Get all section names from config file without loading values.
        """
        if file_path.suffix == '.ini':
            config = configparser.ConfigParser(interpolation=None)
            config.read(file_path, encoding='utf-8')
            return list(config.sections())
        # For other formats, would need similar lightweight parsing
        return []
    
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
    
    def _load_ini_config_lazy(self, file_path: Path) -> Dict[str, Any]:
        """
        Load INI configuration file with lazy loading - only specified sections.
        """
        config = configparser.ConfigParser(interpolation=None)
        config.read(file_path, encoding='utf-8')
        
        result = {}
        loaded_count = 0
        skipped_count = 0
        
        for section in config.sections():
            if self._should_load_section(section):
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
                loaded_count += 1
            else:
                skipped_count += 1
                self.logger.debug(f"Skipping section '{section}' - not required by active tags")
        
        self.logger.info(f"Tag-based loading: {loaded_count} sections loaded, {skipped_count} sections skipped")
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
    
    def load_specific_section(self, filename: str, section_name: str) -> Dict[str, Any]:
        """
        Load a specific section from config file without loading others.
        
        Args:
            filename: Config file name
            section_name: Section name to load
        
        Returns:
            Section data as dictionary
        """
        file_path = self.config_dir / filename
        
        if not file_path.exists():
            raise ConfigurationError(f"Configuration file not found: {file_path}",
                                   config_file=str(file_path))
        
        if filename.endswith('.ini'):
            config = configparser.ConfigParser(interpolation=None)
            config.read(file_path, encoding='utf-8')
            
            if section_name not in config.sections():
                available_sections = list(config.sections())
                raise ConfigurationError(
                    f"Section '{section_name}' not found in {filename}. "
                    f"Available sections: {available_sections}",
                    config_key=section_name
                )
            
            result = {}
            for key, value in config[section_name].items():
                context = f"{section_name}.{key}"
                try:
                    resolved_value = self._resolve_value(key, value, context)
                    validated_value = self._validate_value(key, resolved_value, context)
                    result[key] = validated_value
                except Exception as e:
                    self.logger.error(f"Error processing {context}: {str(e)}")
                    raise
            
            self.logger.debug(f"Loaded specific section '{section_name}' from {filename}")
            return result
        else:
            # For non-INI files, load full file and extract section
            full_config = self.load_config_file(filename)
            if section_name in full_config:
                return full_config[section_name]
            else:
                raise ConfigurationError(
                    f"Section '{section_name}' not found in {filename}",
                    config_key=section_name
                )
    
    def get_database_config(self, section_name: str) -> DatabaseConfig:
        """
        Get database configuration with automatic tag-aware lazy loading.
        
        Args:
            section_name: The exact section name in config.ini (e.g., "S101_ORACLE", "P101_POSTGRES")
        
        Returns:
            DatabaseConfig object
        """
        # Load config with automatic tag detection and lazy loading
        config = self.load_config_file("config.ini")
        
        if section_name not in config:
            available_sections = list(config.keys())
            raise ConfigurationError(
                f"Configuration section '{section_name}' not found. "
                f"Available sections: {available_sections}",
                config_key=section_name
            )
        
        section_data = config[section_name]
        
        try:
            return DatabaseConfig(
                host=section_data['host'],
                port=int(section_data['port']),
                database=section_data.get('database', section_data.get('service_name', '')),
                username=section_data['username'],
                password=section_data['password'],
                ssl_enabled=section_data.get('ssl_enabled', 'false').lower() == 'true',
                pool_size=int(section_data.get('pool_size', 5)),
                timeout=int(section_data.get('timeout', 30)),
                max_overflow=int(section_data.get('max_overflow', 10)),
                pool_pre_ping=section_data.get('pool_pre_ping', 'true').lower() == 'true',
                pool_recycle=int(section_data.get('pool_recycle', 3600)),
                connect_args=json.loads(section_data.get('connect_args', '{}'))
            )
            
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            raise ConfigurationError(f"Invalid configuration for {section_name}: {str(e)}",
                                   config_key=section_name)
    
    def get_comparison_config(self, section_name: str = "comparison_settings") -> ComparisonConfig:
        """
        Get database comparison configuration with automatic tag-aware lazy loading.
        
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
    
    def get_kafka_config(self, section_name: str = "S101_KAFKA") -> Dict[str, Any]:
        """
        Get Kafka configuration with automatic tag-aware lazy loading.
        
        Args:
            section_name: Kafka section name in config file (default: "S101_KAFKA")
        
        Returns:
            Dictionary with Kafka configuration
        """
        config = self.load_config_file("config.ini")
        
        if section_name not in config:
            available_sections = [s for s in config.keys() if 'KAFKA' in s]
            raise ConfigurationError(
                f"Kafka configuration section '{section_name}' not found. "
                f"Available Kafka sections: {available_sections}",
                config_key=section_name
            )
        
        kafka_config = config[section_name]
        
        try:
            # Return the kafka configuration as-is, with type conversions for numeric values
            return {
                'bootstrap_servers': kafka_config.get('bootstrap_servers', 'localhost:9092'),
                'client_id': kafka_config.get('client_id', 'test-automation'),
                'group_id': kafka_config.get('group_id', 'test-group'),
                'value_serializer': kafka_config.get('value_serializer', 'string'),
                'key_serializer': kafka_config.get('key_serializer', 'string'),
                'value_deserializer': kafka_config.get('value_deserializer', 'string'),
                'key_deserializer': kafka_config.get('key_deserializer', 'string'),
                'auto_offset_reset': kafka_config.get('auto_offset_reset', 'latest'),
                'enable_auto_commit': kafka_config.get('enable_auto_commit', 'true'),
                'auto_commit_interval_ms': kafka_config.get('auto_commit_interval_ms', '5000'),
                'session_timeout_ms': kafka_config.get('session_timeout_ms', '30000'),
                'heartbeat_interval_ms': kafka_config.get('heartbeat_interval_ms', '3000'),
                'max_poll_records': kafka_config.get('max_poll_records', '500'),
                'fetch_min_bytes': kafka_config.get('fetch_min_bytes', '1'),
                'fetch_max_wait_ms': kafka_config.get('fetch_max_wait_ms', '500'),
                'consumer_timeout_ms': kafka_config.get('consumer_timeout_ms', '1000'),
                'compression_type': kafka_config.get('compression_type', 'none'),
                'acks': kafka_config.get('acks', 'all'),
                'retries': kafka_config.get('retries', '3'),
                'batch_size': kafka_config.get('batch_size', '16384'),
                'linger_ms': kafka_config.get('linger_ms', '10'),
                'buffer_memory': kafka_config.get('buffer_memory', '33554432'),
                'max_block_ms': kafka_config.get('max_block_ms', '60000'),
                'request_timeout_ms': kafka_config.get('request_timeout_ms', '30000'),
                'security_protocol': kafka_config.get('security_protocol'),
                'sasl_mechanism': kafka_config.get('sasl_mechanism'),
                'sasl_username': kafka_config.get('sasl_username'),
                'sasl_password': kafka_config.get('sasl_password'),
                'ssl_cafile': kafka_config.get('ssl_cafile'),
                'ssl_certfile': kafka_config.get('ssl_certfile'),
                'ssl_keyfile': kafka_config.get('ssl_keyfile'),
                'test_topic': kafka_config.get('test_topic', 'connection-test')
            }
            
        except Exception as e:
            raise ConfigurationError(f"Invalid Kafka configuration in section '{section_name}': {str(e)}",
                                   config_key=section_name)
    
    def get_api_config(self, section_name: str = "API") -> Dict[str, Any]:
        """
        Get API configuration with automatic tag-aware lazy loading.
        
        Args:
            section_name: API section name in config file (default: "API")
        
        Returns:
            Dictionary with API configuration
        """
        config = self.load_config_file("config.ini")
        
        if section_name not in config:
            available_sections = [s for s in config.keys() if 'API' in s.upper()]
            raise ConfigurationError(
                f"API configuration section '{section_name}' not found. "
                f"Available API sections: {available_sections}",
                config_key=section_name
            )
        
        api_config = config[section_name]
        
        try:
            # Return the API configuration with defaults
            return {
                'base_url': api_config.get('base_url', ''),
                'timeout': int(api_config.get('timeout', 30)),
                'verify_ssl': api_config.get('verify_ssl', 'true').lower() == 'true',
                'token': api_config.get('token'),
                'auth_type': api_config.get('auth_type', 'bearer'),
                'max_retries': int(api_config.get('max_retries', 3)),
                'retry_delay': int(api_config.get('retry_delay', 1)),
                'retry_status_codes': [int(code.strip()) for code in api_config.get('retry_status_codes', '500,502,503,504,429').split(',')],
                'headers': json.loads(api_config.get('headers', '{}')) if api_config.get('headers') else {}
            }
            
        except Exception as e:
            raise ConfigurationError(f"Invalid API configuration in section '{section_name}': {str(e)}",
                                   config_key=section_name)
    
    def get_custom_config(self, section: str, key: Optional[str] = None, default: Any = None) -> Any:
        """
        Get custom configuration with automatic tag-aware lazy loading.
        
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
        """List all available configuration sections with automatic tag-aware loading."""
        try:
            config = self.load_config_file("config.ini")
            return list(config.keys())
        except Exception as e:
            self.logger.error(f"Failed to list sections: {str(e)}")
            return []
    
    def section_exists(self, section_name: str) -> bool:
        """Check if a configuration section exists with automatic tag-aware loading."""
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
    
    def enable_lazy_loading(self, enabled: bool = True) -> None:
        """
        Enable or disable lazy loading.
        
        Args:
            enabled: Whether to enable lazy loading
        """
        self._lazy_loading_enabled = enabled
        if enabled:
            self.logger.info("Lazy loading enabled - will load only sections required by active tags")
        else:
            self.logger.info("Lazy loading disabled - will load all sections")
    
    def get_loading_stats(self) -> Dict[str, Any]:
        """
        Get statistics about configuration loading.
        
        Returns:
            Dictionary with loading statistics
        """
        return {
            'lazy_loading_enabled': self._lazy_loading_enabled,
            'active_tags': self._active_tags,
            'required_sections': sorted(self._required_sections),
            'loaded_sections_count': len(self._loaded_sections),
            'cache_entries': len(self._config_cache)
        }
    
    def reload_config(self) -> None:
        """Clear cache and reload configuration."""
        with self._cache_lock:
            self._config_cache.clear()
            self._file_timestamps.clear()
            self._loaded_sections.clear()
        
        # Clear LRU cache  
        self.load_config_file.cache_clear()
        self.logger.info("Configuration cache cleared and reloaded")
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get a summary of current configuration."""
        summary = {
            'config_directory': str(self.config_dir),
            'cache_entries': len(self._config_cache),
            'cache_timeout': self.cache_timeout,
            'lazy_loading_enabled': self._lazy_loading_enabled,
            'active_tags': self._active_tags,
            'required_sections': sorted(self._required_sections),
            'available_sections': self.list_available_sections() if not self._lazy_loading_enabled else [],
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


# Create global instance with automatic tag detection
config_loader = ConfigLoader()

# Global config_loader will automatically detect tags when used
# No manual setup required - just use config_loader.get_database_config() as before