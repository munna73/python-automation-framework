"""Enhanced configuration loader with validation and caching."""
import os
import configparser
from typing import Dict, Any, Optional, Union, List
from functools import lru_cache
from pathlib import Path
import json
import yaml
from dataclasses import dataclass
import re

from utils.custom_exceptions import ConfigurationError


@dataclass
class DatabaseConfig:
    """Database configuration data class."""
    host: str
    port: int
    database: str
    username: str
    password: str
    ssl_enabled: bool = False
    pool_size: int = 5
    timeout: int = 30
    
    def to_connection_string(self, db_type: str) -> str:
        """Convert config to connection string."""
        if db_type.upper() == 'ORACLE':
            return f"{self.username}/{self.password}@{self.host}:{self.port}/{self.database}"
        elif db_type.upper() == 'POSTGRES':
            return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif db_type.upper() == 'MONGODB':
            return f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            raise ConfigurationError(f"Unknown database type: {db_type}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'username': self.username,
            'password': self.password,
            'ssl_enabled': self.ssl_enabled,
            'pool_size': self.pool_size,
            'timeout': self.timeout
        }


@dataclass
class APIConfig:
    """API configuration data class."""
    base_url: str
    token: str
    timeout: int = 30
    retry_count: int = 3
    verify_ssl: bool = True
    
    def get_headers(self) -> Dict[str, str]:
        """Get API headers with authentication."""
        return {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }


@dataclass
class MQConfig:
    """Message Queue configuration data class."""
    host: str
    port: int
    queue_manager: str
    channel: str
    queue: str
    username: str
    password: str
    ssl_cipher: Optional[str] = None
    timeout: int = 30


@dataclass
class AWSConfig:
    """AWS configuration data class."""
    access_key_id: str
    secret_access_key: str
    region: str = 'us-east-1'
    s3_bucket: Optional[str] = None
    
    def to_boto_config(self) -> Dict[str, str]:
        """Convert to boto3 configuration."""
        return {
            'aws_access_key_id': self.access_key_id,
            'aws_secret_access_key': self.secret_access_key,
            'region_name': self.region
        }


class ConfigLoader:
    """Configuration loader with support for multiple formats."""
    
    # Define which fields should be resolved from environment variables
    SENSITIVE_FIELDS = {
        'username', 'password', 'pwd', 
        'aws_access_key', 'aws_secret_key', 
        'access_key_id', 'secret_access_key'
    }
    
    def __init__(self, config_dir: Optional[str] = None):
        self.config_dir = Path(config_dir or "config")
        self._config_cache: Dict[str, Any] = {}
        
    def _should_resolve_from_env(self, key: str, value: str) -> bool:
        """
        Check if a configuration value should be resolved from environment variables.
        
        Args:
            key: The configuration key
            value: The configuration value
            
        Returns:
            True if it should be resolved from environment
        """
        # Check if the key is a sensitive field
        key_lower = key.lower()
        if any(sensitive in key_lower for sensitive in self.SENSITIVE_FIELDS):
            # Check if the value looks like an environment variable reference
            if re.match(r'^[A-Z][A-Z0-9_]*$', value):
                return True
        return False
        
    def _resolve_value(self, key: str, value: str, context: str = "") -> str:
        """
        Resolve a configuration value, checking environment variables for sensitive fields.
        
        Args:
            key: The configuration key
            value: The value from config file
            context: Context for better error messages
            
        Returns:
            Resolved value
        """
        if self._should_resolve_from_env(key, value):
            # Try to get from system environment
            env_value = os.getenv(value)
            if env_value:
                return env_value
            else:
                # For sensitive fields, raise error if env var not found
                raise ConfigurationError(
                    f"Environment variable '{value}' not found. "
                    f"Please set it as a system environment variable. "
                    f"Context: {context}",
                    config_key=value
                )
        
        # Return original value for non-sensitive fields
        return value
                        
    @lru_cache(maxsize=32)
    def load_config_file(self, filename: str) -> Dict[str, Any]:
        """Load configuration from file with caching."""
        file_path = self.config_dir / filename
        
        if not file_path.exists():
            raise ConfigurationError(f"Configuration file not found: {file_path}",
                                   config_file=str(file_path))
            
        try:
            if filename.endswith('.ini'):
                return self._load_ini_config(file_path)
            elif filename.endswith('.json'):
                return self._load_json_config(file_path)
            elif filename.endswith(('.yml', '.yaml')):
                return self._load_yaml_config(file_path)
            else:
                raise ConfigurationError(f"Unsupported config format: {filename}",
                                       config_file=filename)
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(f"Failed to load config file: {str(e)}",
                                   config_file=str(file_path))
            
    def _load_ini_config(self, file_path: Path) -> Dict[str, Any]:
        """Load INI configuration file."""
        config = configparser.ConfigParser()
        config.read(file_path)
        
        # Convert to dictionary and resolve sensitive values from environment
        result = {}
        for section in config.sections():
            result[section] = {}
            for key, value in config[section].items():
                context = f"{section}.{key}"
                result[section][key] = self._resolve_value(key, value, context)
        
        return result
        
    def _load_json_config(self, file_path: Path) -> Dict[str, Any]:
        """Load JSON configuration file."""
        with open(file_path) as f:
            data = json.load(f)
        
        # Recursively resolve sensitive values in JSON
        return self._resolve_dict_values(data)
    
    def _load_yaml_config(self, file_path: Path) -> Dict[str, Any]:
        """Load YAML configuration file."""
        with open(file_path) as f:
            data = yaml.safe_load(f)
        
        # Recursively resolve sensitive values in YAML
        return self._resolve_dict_values(data)
    
    def _resolve_dict_values(self, data: Any, context: str = "") -> Any:
        """Recursively resolve sensitive values in dictionary."""
        if isinstance(data, dict):
            result = {}
            for k, v in data.items():
                new_context = f"{context}.{k}" if context else k
                if isinstance(v, str):
                    result[k] = self._resolve_value(k, v, new_context)
                else:
                    result[k] = self._resolve_dict_values(v, new_context)
            return result
        elif isinstance(data, list):
            return [self._resolve_dict_values(item, f"{context}[{i}]") 
                    for i, item in enumerate(data)]
        else:
            return data
            
    def get_database_config(self, env: str, db_type: str) -> DatabaseConfig:
        """
        Get database configuration for specific environment and type.
        
        Args:
            env: Environment name (DEV, QA, PROD)
            db_type: Database type (ORACLE, POSTGRES, MONGODB)
            
        Returns:
            DatabaseConfig object
        """
        config_key = f"{env}_{db_type}"
        
        if config_key in self._config_cache:
            return self._config_cache[config_key]
            
        config = self.load_config_file("config.ini")
        
        if config_key not in config:
            raise ConfigurationError(f"Configuration not found for {config_key}",
                                   config_key=config_key)
            
        db_config = config[config_key]
        
        # Username and password are already resolved from environment variables
        # by _resolve_value in _load_ini_config (only if they look like env vars)
        username = db_config.get('username')
        password = db_config.get('password')
        
        # Validate that credentials exist
        if not username:
            raise ConfigurationError(
                f"Username not found for {config_key}.",
                config_key=f"{config_key}.username"
            )
        
        if not password:
            raise ConfigurationError(
                f"Password not found for {config_key}.",
                config_key=f"{config_key}.password"
            )
        
        # Handle Oracle service_name vs database
        database = db_config.get('database', db_config.get('service_name', ''))
        
        try:
            db_config_obj = DatabaseConfig(
                host=db_config['host'],
                port=int(db_config['port']),
                database=database,
                username=username,
                password=password,
                ssl_enabled=db_config.get('ssl_enabled', 'false').lower() == 'true',
                pool_size=int(db_config.get('pool_size', 5)),
                timeout=int(db_config.get('timeout', 30))
            )
        except (KeyError, ValueError) as e:
            raise ConfigurationError(f"Invalid configuration for {config_key}: {str(e)}",
                                   config_key=config_key)
        
        self._config_cache[config_key] = db_config_obj
        return db_config_obj
    
    def get_aws_config(self) -> AWSConfig:
        """Get AWS configuration."""
        try:
            config = self.load_config_file("config.ini")
            aws_config = config.get('AWS', {})
            
            # These will be resolved from environment if they match the pattern
            access_key = aws_config.get('access_key_id', '')
            secret_key = aws_config.get('secret_access_key', '')
            
            if not access_key or not secret_key:
                raise ConfigurationError(
                    "AWS credentials not found in configuration.",
                    config_key="AWS"
                )
            
            return AWSConfig(
                access_key_id=access_key,
                secret_access_key=secret_key,
                region=aws_config.get('region', 'us-east-1'),
                s3_bucket=aws_config.get('s3_bucket')
            )
        except KeyError as e:
            raise ConfigurationError(f"Invalid AWS configuration: {str(e)}",
                                   config_key="AWS")
        
    def get_api_config(self) -> APIConfig:
        """Get API configuration."""
        try:
            config = self.load_config_file("config.ini")
            api_config = config.get('API', {})
            
            return APIConfig(
                base_url=api_config.get('base_url', 'http://localhost:8080'),
                token=api_config.get('token', ''),  # Can be direct value or env var
                timeout=int(api_config.get('timeout', '30')),
                retry_count=int(api_config.get('retry_count', '3')),
                verify_ssl=api_config.get('verify_ssl', 'true').lower() == 'true'
            )
        except ValueError as e:
            raise ConfigurationError(f"Invalid API configuration: {str(e)}",
                                   config_key="API")
        
    def get_mq_config(self) -> MQConfig:
        """Get MQ configuration."""
        try:
            config = self.load_config_file("config.ini")
            mq_config = config.get('MQ', {})
            
            return MQConfig(
                host=mq_config.get('host', 'localhost'),
                port=int(mq_config.get('port', '1414')),
                queue_manager=mq_config.get('queue_manager', ''),
                channel=mq_config.get('channel', ''),
                queue=mq_config.get('queue', ''),
                username=mq_config.get('username', ''),
                password=mq_config.get('password', ''),  # Will resolve from env if uppercase
                ssl_cipher=mq_config.get('ssl_cipher'),
                timeout=int(mq_config.get('timeout', '30'))
            )
        except ValueError as e:
            raise ConfigurationError(f"Invalid MQ configuration: {str(e)}",
                                   config_key="MQ")
    
    def print_environment_status(self) -> None:
        """Print status of required environment variables for debugging."""
        print("\n=== Environment Variables Status ===")
        
        # Check for database and AWS credentials
        patterns = [
            r'.*_ORACLE_USERNAME$',
            r'.*_ORACLE_PASSWORD$',
            r'.*_POSTGRES_USERNAME$',
            r'.*_POSTGRES_PASSWORD$',
            r'.*_MONGODB_USERNAME$',
            r'.*_MONGODB_PASSWORD$',
            r'AWS_ACCESS_KEY_ID$',
            r'AWS_SECRET_ACCESS_KEY$'
        ]
        
        print("Checking for credential environment variables:")
        for pattern in patterns:
            found = False
            for var in os.environ.keys():
                if re.match(pattern, var):
                    print(f"  ✓ {var}: SET")
                    found = True
            if not found:
                # Show example of what we're looking for
                example = pattern.replace('.*', 'QA').replace('$', '')
                print(f"  ✗ No variable matching pattern: {example}")
        
        print("===================================\n")
    
    def get_test_data(self, data_file: str, key: Optional[str] = None) -> Any:
        """
        Load test data from file.
        
        Args:
            data_file: Test data file name
            key: Optional key to extract specific data
            
        Returns:
            Test data
        """
        test_data_dir = self.config_dir.parent / "test_data"
        file_path = test_data_dir / data_file
        
        if not file_path.exists():
            raise ConfigurationError(f"Test data file not found: {file_path}",
                                   config_file=str(file_path))
        
        if data_file.endswith('.json'):
            with open(file_path) as f:
                data = json.load(f)
        elif data_file.endswith(('.yml', '.yaml')):
            with open(file_path) as f:
                data = yaml.safe_load(f)
        else:
            raise ConfigurationError(f"Unsupported test data format: {data_file}",
                                   config_file=data_file)
        
        if key:
            if key not in data:
                raise ConfigurationError(f"Key '{key}' not found in test data",
                                       config_key=key,
                                       config_file=data_file)
            return data[key]
        
        return data
        
    def validate_config(self) -> bool:
        """Validate all configuration settings."""
        print("Validating configuration...")
        
        # Print environment status for debugging
        self.print_environment_status()
        
        try:
            config = self.load_config_file("config.ini")
            
            # Test loading some configs
            environments = self.get_all_environments()
            print(f"Found environments: {environments}")
            
            return True
            
        except Exception as e:
            print(f"Configuration validation failed: {e}")
            return False
    
    def get_custom_config(self, section: str, key: Optional[str] = None) -> Any:
        """
        Get custom configuration from config.ini.
        
        Args:
            section: Section name in config file
            key: Optional specific key within section
            
        Returns:
            Configuration value or section dict
        """
        config = self.load_config_file("config.ini")
        
        if section not in config:
            raise ConfigurationError(f"Section '{section}' not found in config",
                                   config_key=section)
        
        if key:
            if key not in config[section]:
                raise ConfigurationError(f"Key '{key}' not found in section '{section}'",
                                       config_key=f"{section}.{key}")
            return config[section][key]
        
        return config[section]
    
    def reload_config(self) -> None:
        """Clear cache and reload configuration."""
        self._config_cache.clear()
        self.load_config_file.cache_clear()
    
    def get_all_environments(self) -> List[str]:
        """Get all available environments from config."""
        config = self.load_config_file("config.ini")
        environments = set()
        
        # Extract environment names from section names (e.g., DEV_ORACLE -> DEV)
        pattern = re.compile(r'^([A-Z]+)_[A-Z]+$')
        for section in config.keys():
            match = pattern.match(section)
            if match:
                environments.add(match.group(1))
        
        return sorted(list(environments))
    
    def get_all_database_types(self, env: str) -> List[str]:
        """Get all database types available for an environment."""
        config = self.load_config_file("config.ini")
        db_types = []
        
        for section in config.keys():
            if section.startswith(f"{env}_"):
                db_type = section.replace(f"{env}_", "")
                if db_type in ['ORACLE', 'POSTGRES', 'POSTGRESQL', 'MONGODB']:
                    db_types.append(db_type)
        
        return sorted(db_types)


# Singleton instance
config_loader = ConfigLoader()