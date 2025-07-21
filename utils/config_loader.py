"""
Configuration loader utility for handling application settings.
"""
import configparser
import os
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv

class ConfigLoader:
    """Handles loading and managing configuration from various sources."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the config loader."""
        load_dotenv()
        
        if config_path is None:
            self.config_path = Path(__file__).parent.parent / "config" / "config.ini"
        else:
            self.config_path = Path(config_path)
        
        self.config = configparser.ConfigParser()
        self.config.read(self.config_path)
        
    def get_database_config(self, environment: str, db_type: str) -> Dict[str, Any]:
        """Get database configuration for specified environment and type."""
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
        config_dict['token'] = os.getenv('API_TOKEN')
        return config_dict
    
    def get_mq_config(self) -> Dict[str, Any]:
        """Get MQ configuration."""
        config_dict = dict(self.config['MQ_CONFIG'])
        config_dict['password'] = os.getenv('MQ_PWD')
        return config_dict
    
    def get_query(self, query_name: str) -> str:
        """Get SQL query by name."""
        if 'DATABASE_QUERIES' not in self.config:
            raise ValueError("DATABASE_QUERIES section not found in config")
        
        query = self.config.get('DATABASE_QUERIES', query_name, fallback=None)
        
        if not query:
            raise ValueError(f"Query '{query_name}' not found in DATABASE_QUERIES section")
        
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
        settings['excel_max_rows'] = int(settings.get('excel_max_rows', 1000000))
        settings['decimal_places'] = int(settings.get('decimal_places', 2))
        return settings

# Global config loader instance
config_loader = ConfigLoader()