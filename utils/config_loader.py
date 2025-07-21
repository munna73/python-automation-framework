"""
Configuration loader utility for handling application settings including AWS.
"""
import configparser
import os
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv

class ConfigLoader:
    """Handles loading and managing configuration from various sources."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the config loader.
        
        Args:
            config_path: Path to the config.ini file. If None, uses default location.
        """
        # Load environment variables
        load_dotenv()
        
        # Set config path
        if config_path is None:
            self.config_path = Path(__file__).parent.parent / "config" / "config.ini"
        else:
            self.config_path = Path(config_path)
        
        # Initialize config parser
        self.config = configparser.ConfigParser()
        self.config.read(self.config_path)
        
    def get_database_config(self, environment: str, db_type: str) -> Dict[str, Any]:
        """
        Get database configuration for specified environment and type.
        
        Args:
            environment: Environment name (DEV, QA, PROD)
            db_type: Database type (ORACLE, POSTGRES, MONGODB)
            
        Returns:
            Database configuration dictionary
        """
        section_name = f"{environment.upper()}_{db_type.upper()}"
        
        if section_name not in self.config:
            raise ValueError(f"Configuration section '{section_name}' not found")
        
        config_dict = dict(self.config[section_name])
        
        # Add password from environment variable
        pwd_env_var = f"{section_name}_PWD"
        config_dict['password'] = os.getenv(pwd_env_var)
        
        if not config_dict['password']:
            raise ValueError(f"Password not found in environment variable '{pwd_env_var}'")
        
        return config_dict
    
    def get_api_config(self) -> Dict[str, Any]:
        """Get API configuration."""
        config_dict = dict(self.config['API_ENDPOINTS'])
        
        # Add token from environment variable
        config_dict['token'] = os.getenv('API_TOKEN')
        
        return config_dict
    
    def get_mq_config(self) -> Dict[str, Any]:
        """Get MQ configuration."""
        config_dict = dict(self.config['MQ_CONFIG'])
        
        # Add password from environment variable
        config_dict['password'] = os.getenv('MQ_PWD')
        
        return config_dict
    
    def get_aws_config(self) -> Dict[str, Any]:
        """
        Get AWS configuration from environment variables and config file.
        
        Returns:
            AWS configuration dictionary
        """
        aws_config = {
            'access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'region': os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        }
        
        # Add config file settings if AWS_CONFIG section exists
        if 'AWS_CONFIG' in self.config:
            aws_file_config = dict(self.config['AWS_CONFIG'])
            
            # Override region from config file if not set in environment
            if not aws_config['region'] and 'region' in aws_file_config:
                aws_config['region'] = aws_file_config['region']
            
            # Add additional AWS settings from config
            aws_config.update({
                'sqs_queue_url': aws_file_config.get('sqs_queue_url'),
                's3_bucket_name': aws_file_config.get('s3_bucket_name'),
                's3_download_prefix': aws_file_config.get('s3_download_prefix'),
                's3_upload_prefix': aws_file_config.get('s3_upload_prefix')
            })
        
        # Validate required credentials
        if not aws_config['access_key_id'] or not aws_config['secret_access_key']:
            raise ValueError("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
        
        return aws_config
    
    def get_aws_sqs_config(self) -> Dict[str, Any]:
        """Get AWS SQS specific configuration."""
        sqs_config = {
            'max_messages': 10,
            'wait_time_seconds': 20,
            'visibility_timeout': 30,
            'delete_after_processing': True
        }
        
        if 'AWS_SQS_SETTINGS' in self.config:
            sqs_file_config = dict(self.config['AWS_SQS_SETTINGS'])
            
            # Convert string values to appropriate types
            sqs_config.update({
                'max_messages': int(sqs_file_config.get('max_messages', 10)),
                'wait_time_seconds': int(sqs_file_config.get('wait_time_seconds', 20)),
                'visibility_timeout': int(sqs_file_config.get('visibility_timeout', 30)),
                'delete_after_processing': sqs_file_config.get('delete_after_processing', 'true').lower() == 'true'
            })
        
        return sqs_config
    
    def get_aws_s3_config(self) -> Dict[str, Any]:
        """Get AWS S3 specific configuration."""
        s3_config = {
            'download_directory': 'data/s3_downloads',
            'create_subdirs': True,
            'max_file_size_mb': 100
        }
        
        if 'AWS_S3_SETTINGS' in self.config:
            s3_file_config = dict(self.config['AWS_S3_SETTINGS'])
            
            # Convert string values to appropriate types
            s3_config.update({
                'download_directory': s3_file_config.get('download_directory', 'data/s3_downloads'),
                'create_subdirs': s3_file_config.get('create_subdirs', 'true').lower() == 'true',
                'max_file_size_mb': int(s3_file_config.get('max_file_size_mb', 100))
            })
        
        return s3_config
    
    def get_sql_message_table_config(self) -> Dict[str, Any]:
        """Get SQL message table configuration."""
        table_config = {
            'table_name': 'aws_sqs_messages',
            'auto_create_table': True,
            'cleanup_processed_after_days': 30
        }
        
        if 'SQL_MESSAGE_TABLE' in self.config:
            table_file_config = dict(self.config['SQL_MESSAGE_TABLE'])
            
            table_config.update({
                'table_name': table_file_config.get('table_name', 'aws_sqs_messages'),
                'auto_create_table': table_file_config.get('auto_create_table', 'true').lower() == 'true',
                'cleanup_processed_after_days': int(table_file_config.get('cleanup_processed_after_days', 30))
            })
        
        return table_config
    
    def get_query(self, query_name: str) -> str:
        """
        Get SQL query by name.
        
        Args:
            query_name: Name of the query in the DATABASE_QUERIES section
            
        Returns:
            SQL query string
        """
        if 'DATABASE_QUERIES' not in self.config:
            raise ValueError("DATABASE_QUERIES section not found in config")
        
        query = self.config.get('DATABASE_QUERIES', query_name, fallback=None)
        
        if not query:
            raise ValueError(f"Query '{query_name}' not found in DATABASE_QUERIES section")
        
        return query
    
    def get_mongodb_query(self, query_name: str) -> str:
        """
        Get MongoDB query by name.
        
        Args:
            query_name: Name of the query in the MONGODB_QUERIES section
            
        Returns:
            MongoDB query string
        """
        if 'MONGODB_QUERIES' not in self.config:
            raise ValueError("MONGODB_QUERIES section not found in config")
        
        query = self.config.get('MONGODB_QUERIES', query_name, fallback=None)
        
        if not query:
            raise ValueError(f"MongoDB query '{query_name}' not found in MONGODB_QUERIES section")
        
        return query
    
    def get_export_settings(self) -> Dict[str, Any]:
        """Get export configuration settings."""
        if 'EXPORT_SETTINGS' not in self.config:
            return {
                'excel_max_rows': 1000000,
                'csv_encoding': 'utf-8-sig',
                'date_format': '%Y-%m-%d %H:%M:%S',
                'decimal_places': 2
            }
        
        settings = dict(self.config['EXPORT_SETTINGS'])
        
        # Convert numeric settings
        settings['excel_max_rows'] = int(settings.get('excel_max_rows', 1000000))
        settings['decimal_places'] = int(settings.get('decimal_places', 2))
        
        return settings
    
    def get_setting(self, section: str, key: str, fallback: Any = None) -> Any:
        """
        Get a specific setting value.
        
        Args:
            section: Configuration section name
            key: Setting key name
            fallback: Default value if setting not found
            
        Returns:
            Setting value or fallback
        """
        return self.config.get(section, key, fallback=fallback)
    
    def get_window_minutes(self) -> int:
        """Get time window in minutes for chunked queries."""
        return int(self.get_setting('DEFAULT', 'window_minutes', 60))
    
    def get_chunk_size(self) -> int:
        """Get chunk size for large data processing."""
        return int(self.get_setting('DEFAULT', 'chunk_size', 10000))

# Global config loader instance
config_loader = ConfigLoader()