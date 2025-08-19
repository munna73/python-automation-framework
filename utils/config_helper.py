"""
Robust configuration loading helper for test scenarios.
Provides reliable config loading with fallback mechanisms.
"""
from typing import Optional, Dict, Any, Union
from utils.config_loader import ConfigLoader, DatabaseConfig, ConfigurationError
from utils.logger import logger
import os
import configparser
from pathlib import Path


class TestConfigHelper:
    """Helper class for loading configuration on-demand during test execution."""
    
    def __init__(self, context):
        """Initialize with Behave context."""
        self.context = context
        self._ensure_config_loader()
    
    def _ensure_config_loader(self):
        """Ensure config loader is available in context."""
        if not hasattr(self.context, 'config_loader') or self.context.config_loader is None:
            try:
                # First try to use the global config_loader instance
                from utils.config_loader import config_loader
                self.context.config_loader = config_loader
                logger.debug("Using global config_loader instance")
            except Exception as e:
                logger.error(f"Failed to use global config_loader: {e}")
                # Fallback: create new instance
                self.context.config_loader = ConfigLoader(config_dir="config" if os.path.exists("config") else None)
                logger.debug("ConfigLoader initialized on-demand")
    
    def load_database_config(self, section_name: str, required_env_vars: Optional[Dict[str, str]] = None) -> DatabaseConfig:
        """
        Load database configuration for a specific section when needed.
        
        Args:
            section_name: Config section name (e.g., 'S101_ORACLE', 'P101_POSTGRES')
            required_env_vars: Optional dict of env vars to set before loading
            
        Returns:
            DatabaseConfig object
            
        Raises:
            ConfigurationError: If config cannot be loaded
        """
        cache_key = f"db_config_{section_name}"
        
        # Check cache first
        if hasattr(self.context, 'config_cache') and cache_key in self.context.config_cache:
            logger.debug(f"Using cached config for {section_name}")
            return self.context.config_cache[cache_key]
        
        try:
            # Set environment variables if provided
            if required_env_vars:
                for env_var, value in required_env_vars.items():
                    os.environ[env_var] = value
                    logger.debug(f"Set environment variable {env_var} for {section_name}")
            
            # Try the standard config loader method first
            try:
                db_config = self.context.config_loader.get_database_config(section_name)
                logger.info(f"✅ Database config loaded via config_loader: {section_name}")
                
                # Cache it
                if not hasattr(self.context, 'config_cache'):
                    self.context.config_cache = {}
                self.context.config_cache[cache_key] = db_config
                
                return db_config
                
            except ConfigurationError as config_error:
                # If standard loading fails, use direct section loading fallback
                logger.warning(f"Standard config loading failed for {section_name}, using direct fallback: {config_error}")
                return self._load_database_config_direct(section_name, cache_key)
            
        except Exception as e:
            logger.error(f"❌ Failed to load database config for {section_name}: {e}")
            raise ConfigurationError(f"Failed to load database configuration for '{section_name}': {str(e)}")
    
    def _load_database_config_direct(self, section_name: str, cache_key: str) -> DatabaseConfig:
        """Direct config loading fallback that bypasses tag-based validation."""
        config_path = Path('config/config.ini')
        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        
        parser = configparser.ConfigParser()
        parser.read(config_path)
        
        if section_name not in parser:
            available = [s for s in parser.sections() if any(db_type in s for db_type in ['_ORACLE', '_POSTGRES', '_MONGODB'])]
            raise ConfigurationError(f"Section '{section_name}' not found. Available database sections: {available}")
        
        section_data = parser[section_name]
        
        # Resolve password environment variable
        password_key = section_data.get('password', '')
        resolved_password = password_key
        if password_key and password_key.isupper() and '_' in password_key:
            resolved_password = os.getenv(password_key, password_key)
            if resolved_password == password_key:
                logger.warning(f"Environment variable '{password_key}' not set, using literal value")
        
        db_config = DatabaseConfig(
            host=section_data.get('host', ''),
            port=int(section_data.get('port', 5432)),
            database=section_data.get('database', section_data.get('service_name', '')),
            username=section_data.get('username', ''),
            password=resolved_password
        )
        
        # Cache the result
        if not hasattr(self.context, 'config_cache'):
            self.context.config_cache = {}
        self.context.config_cache[cache_key] = db_config
        
        logger.info(f"✅ Database config loaded directly: {section_name} -> {db_config.host}:{db_config.port}")
        return db_config
    
    def load_api_config(self, section_name: str = 'API') -> Dict[str, Any]:
        """
        Load API configuration when needed.
        
        Args:
            section_name: Config section name (default: 'API')
            
        Returns:
            API configuration dictionary
        """
        cache_key = f"api_config_{section_name}"
        
        # Check cache first
        if hasattr(self.context, 'config_cache') and cache_key in self.context.config_cache:
            logger.debug(f"Using cached API config for {section_name}")
            return self.context.config_cache[cache_key]
        
        try:
            logger.info(f"Loading API configuration for {section_name}")
            api_config = self.context.config_loader.get_custom_config(section_name)
            
            # Cache it
            if not hasattr(self.context, 'config_cache'):
                self.context.config_cache = {}
            self.context.config_cache[cache_key] = api_config
            
            logger.info(f"✅ API config loaded: {section_name}")
            return api_config
            
        except Exception as e:
            logger.error(f"❌ Failed to load API config for {section_name}: {e}")
            raise

    def load_custom_config(self, section_name: str, key: str) -> Any:
        """
        Load a specific configuration value when needed.
        
        Args:
            section_name: Config section name
            key: Configuration key
            
        Returns:
            Configuration value
        """
        cache_key = f"custom_config_{section_name}_{key}"
        
        # Check cache first
        if hasattr(self.context, 'config_cache') and cache_key in self.context.config_cache:
            logger.debug(f"Using cached config for {section_name}.{key}")
            return self.context.config_cache[cache_key]
            
        try:
            # Load the value from the config loader
            logger.debug(f"Loading custom config for section {section_name} and key {key}")
            value = self.context.config_loader.get_custom_config(section_name, key)
            
            # Cache it
            if not hasattr(self.context, 'config_cache'):
                self.context.config_cache = {}
            self.context.config_cache[cache_key] = value
            
            logger.debug(f"✅ Config loaded: {section_name}.{key} = {value}")
            return value
        except Exception as e:
            logger.error(f"❌ Failed to load config {section_name}.{key}: {e}")
            raise


# Convenience functions for use in step definitions
def get_config_helper(context) -> TestConfigHelper:
    """Get or create a TestConfigHelper for the current context."""
    if not hasattr(context, '_config_helper'):
        try:
            context._config_helper = TestConfigHelper(context)
        except Exception as e:
            logger.error(f"Failed to create TestConfigHelper: {e}")
            # Return a minimal config helper that uses the global config_loader
            class MinimalConfigHelper:
                def __init__(self, context):
                    self.context = context
                    # Ensure context has config_loader
                    if not hasattr(context, 'config_loader') or context.config_loader is None:
                        from utils.config_loader import config_loader
                        context.config_loader = config_loader
                
                def load_database_config(self, section_name: str, required_env_vars=None):
                    return load_db_config_when_needed(context, section_name, required_env_vars)
                
                def load_api_config(self, section_name: str = 'API'):
                    try:
                        return self.context.config_loader.get_api_config(section_name)
                    except Exception as e:
                        raise ConfigurationError(f"Failed to load API config for '{section_name}': {e}")
                
                def load_custom_config(self, section_name: str, key: str):
                    return load_config_value_when_needed(context, section_name, key)
            
            context._config_helper = MinimalConfigHelper(context)
            logger.info("Using minimal config helper as fallback")
    
    return context._config_helper


def load_db_config_when_needed(context, section_name: str, env_vars: Optional[Dict[str, str]] = None) -> DatabaseConfig:
    """
    Robust convenience function to load database config on-demand.
    Uses direct config loading to ensure reliability.
    
    Args:
        context: Behave context
        section_name: Database section name (e.g., 'S101_ORACLE', 'P101_POSTGRES')
        env_vars: Optional environment variables to set
        
    Returns:
        DatabaseConfig object
    """
    # Set environment variables if provided
    if env_vars:
        for env_var, value in env_vars.items():
            os.environ[env_var] = value
            logger.debug(f"Set environment variable {env_var} for {section_name}")
    
    try:
        logger.info(f"Loading database configuration for {section_name}")
        
        # Use direct section loading for maximum reliability
        config_path = Path('config/config.ini')
        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        
        parser = configparser.ConfigParser()
        parser.read(config_path)
        
        if section_name not in parser:
            available = [s for s in parser.sections() if any(db_type in s for db_type in ['_ORACLE', '_POSTGRES', '_MONGODB'])]
            raise ConfigurationError(f"Section '{section_name}' not found. Available database sections: {available}")
        
        section_data = parser[section_name]
        
        # Create DatabaseConfig with environment variable resolution
        password_key = section_data.get('password', '')
        resolved_password = password_key
        if password_key and password_key.isupper() and '_' in password_key:
            resolved_password = os.getenv(password_key, password_key)
            if resolved_password == password_key:
                logger.warning(f"Environment variable '{password_key}' not set, using literal value")
        
        db_config = DatabaseConfig(
            host=section_data.get('host', ''),
            port=int(section_data.get('port', 5432)),
            database=section_data.get('database', section_data.get('service_name', '')),
            username=section_data.get('username', ''),
            password=resolved_password
        )
        
        logger.info(f"✅ Database config loaded: {section_name} -> {db_config.host}:{db_config.port}")
        return db_config
                
    except Exception as e:
        logger.error(f"❌ Failed to load database config for {section_name}: {e}")
        raise ConfigurationError(f"Failed to load database configuration for '{section_name}': {str(e)}")


def load_config_value_when_needed(context, section: str, key: str) -> Any:
    """
    Robust convenience function to load a specific config value on-demand.
    
    Args:
        context: Behave context
        section: Config section
        key: Config key
        
    Returns:
        Configuration value
    """
    try:
        logger.debug(f"Loading configuration {section}.{key}")
        
        # Use direct config file reading for reliability
        config_path = Path('config/config.ini')
        if not config_path.exists():
            raise ConfigurationError("Configuration file config.ini not found")
        
        parser = configparser.ConfigParser()
        parser.read(config_path)
        
        if section in parser and key in parser[section]:
            value = parser[section][key]
            # Resolve environment variable if needed
            if value and value.isupper() and '_' in value:
                resolved_value = os.getenv(value, value)
                if resolved_value != value:
                    logger.debug(f"Resolved environment variable {value}")
                    value = resolved_value
            logger.debug(f"✅ Config loaded: {section}.{key} = {value}")
            return value
        elif 'DEFAULT' in parser and key in parser['DEFAULT']:
            value = parser['DEFAULT'][key]
            logger.debug(f"✅ Config loaded from DEFAULT: {section}.{key} = {value}")
            return value
        else:
            raise ConfigurationError(f"Configuration key '{key}' not found in section '{section}'")
            
    except Exception as e:
        logger.error(f"❌ Failed to load config {section}.{key}: {e}")
        raise ConfigurationError(f"Failed to load config '{key}' from section '{section}': {e}")

