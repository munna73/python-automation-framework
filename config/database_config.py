# config/database_config.py

from typing import Dict, Any, Optional
import os
from dataclasses import dataclass
from utils.config_loader import config_loader
from utils.logger import logger


@dataclass
class DatabaseConfig:
    """Database configuration data class."""
    host: str
    port: int
    database: str
    username: str
    password: str
    connection_timeout: int = 30
    query_timeout: int = 300
    pool_size: int = 5
    max_overflow: int = 10
    
    def get_connection_string(self, db_type: str) -> str:
        """Generate connection string for the database type."""
        if db_type.upper() == 'ORACLE':
            return f"oracle+cx_oracle://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif db_type.upper() in ['POSTGRES', 'POSTGRESQL']:
            return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")


class DatabaseConfigManager:
    """Manage database configurations for different environments."""
    
    def __init__(self):
        self.configs = {}
        self._load_configurations()
    
    def _load_configurations(self):
        """Load all database configurations from config files and environment variables."""
        
        # Load from config.ini
        environments = ['DEV', 'QA', 'STAGING', 'PROD']
        db_types = ['ORACLE', 'POSTGRES', 'MONGODB']
        
        for env in environments:
            for db_type in db_types:
                try:
                    config_section = f"{env}_{db_type}"
                    
                    if db_type == 'MONGODB':
                        config = self._load_mongodb_config(env)
                    else:
                        config = self._load_sql_config(env, db_type)
                    
                    if config:
                        self.configs[config_section] = config
                        logger.debug(f"Loaded configuration for {config_section}")
                        
                except Exception as e:
                    logger.warning(f"Failed to load config for {config_section}: {e}")
    
    def _load_sql_config(self, env: str, db_type: str) -> Optional[DatabaseConfig]:
        """Load SQL database configuration."""
        section_name = f"{env}_{db_type}"
        
        try:
            host = config_loader.config.get(section_name, 'host')
            port = config_loader.config.getint(section_name, 'port')
            
            # Handle different database types
            if db_type.upper() == 'ORACLE':
                database = config_loader.config.get(section_name, 'service_name')
            else:  # PostgreSQL
                database = config_loader.config.get(section_name, 'database')
            
            username = config_loader.config.get(section_name, 'username')
            
            # Get password from environment variable
            password_env_var = f"{env}_{db_type}_PWD"
            password = os.getenv(password_env_var)
            
            if not password:
                logger.warning(f"Password not found in environment variable: {password_env_var}")
                return None
            
            # Optional configuration parameters
            connection_timeout = config_loader.config.getint(section_name, 'connection_timeout', fallback=30)
            query_timeout = config_loader.config.getint(section_name, 'query_timeout', fallback=300)
            pool_size = config_loader.config.getint(section_name, 'pool_size', fallback=5)
            max_overflow = config_loader.config.getint(section_name, 'max_overflow', fallback=10)
            
            return DatabaseConfig(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password,
                connection_timeout=connection_timeout,
                query_timeout=query_timeout,
                pool_size=pool_size,
                max_overflow=max_overflow
            )
            
        except Exception as e:
            logger.error(f"Error loading SQL config for {section_name}: {e}")
            return None
    
    def _load_mongodb_config(self, env: str) -> Optional[Dict]:
        """Load MongoDB configuration."""
        section_name = f"{env}_MONGODB"
        
        try:
            host = config_loader.config.get(section_name, 'host')
            port = config_loader.config.getint(section_name, 'port')
            database = config_loader.config.get(section_name, 'database')
            username = config_loader.config.get(section_name, 'username', fallback=None)
            
            # Get password from environment variable
            password_env_var = f"{env}_MONGODB_PWD"
            password = os.getenv(password_env_var)
            
            # Optional configuration parameters
            connection_timeout = config_loader.config.getint(section_name, 'connection_timeout', fallback=30)
            max_pool_size = config_loader.config.getint(section_name, 'max_pool_size', fallback=100)
            
            config = {
                'host': host,
                'port': port,
                'database': database,
                'connection_timeout': connection_timeout,
                'max_pool_size': max_pool_size
            }
            
            # Add authentication if provided
            if username and password:
                config['username'] = username
                config['password'] = password
            
            return config
            
        except Exception as e:
            logger.error(f"Error loading MongoDB config for {section_name}: {e}")
            return None
    
    def get_config(self, env: str, db_type: str) -> Optional[Any]:
        """Get configuration for specific environment and database type."""
        config_key = f"{env}_{db_type}".upper()
        return self.configs.get(config_key)
    
    def get_all_configs(self) -> Dict[str, Any]:
        """Get all loaded configurations."""
        return self.configs.copy()
    
    def validate_config(self, env: str, db_type: str) -> bool:
        """Validate that configuration exists and is complete."""
        config = self.get_config(env, db_type)
        
        if not config:
            logger.error(f"No configuration found for {env}_{db_type}")
            return False
        
        if db_type.upper() == 'MONGODB':
            required_fields = ['host', 'port', 'database']
        else:
            required_fields = ['host', 'port', 'database', 'username', 'password']
        
        for field in required_fields:
            if not config.get(field) if isinstance(config, dict) else not getattr(config, field, None):
                logger.error(f"Missing required field '{field}' in {env}_{db_type} configuration")
                return False
        
        logger.info(f"Configuration validated successfully for {env}_{db_type}")
        return True
    
    def list_available_configs(self) -> List[str]:
        """List all available configuration keys."""
        return list(self.configs.keys())


# Global instance
db_config_manager = DatabaseConfigManager()


