"""
On-demand configuration loading helper for test scenarios.
Loads configuration only when needed by specific tests.
"""
from typing import Optional, Dict, Any, Union
from utils.config_loader import ConfigLoader, DatabaseConfig
from utils.logger import logger


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
                # Try to use the config directory if it exists, otherwise use default
                import os
                config_dir = "config" if os.path.exists("config") else None
                self.context.config_loader = ConfigLoader(config_dir=config_dir)
                logger.debug("ConfigLoader initialized on-demand")
            except Exception as e:
                logger.error(f"Failed to initialize ConfigLoader: {e}")
                # Try with the global config_loader instance as fallback
                try:
                    from utils.config_loader import config_loader
                    self.context.config_loader = config_loader
                    logger.debug("Using global config_loader instance as fallback")
                except Exception as fallback_error:
                    logger.error(f"Fallback also failed: {fallback_error}")
                    raise e
    
    def load_database_config(self, section_name: str, required_env_vars: Optional[Dict[str, str]] = None) -> DatabaseConfig:
        """
        Load database configuration for a specific section when needed.
        
        Args:
            section_name: Config section name (e.g., 'DEV_ORACLE', 'QA_POSTGRES')
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
                import os
                for env_var, value in required_env_vars.items():
                    os.environ[env_var] = value
                    logger.debug(f"Set environment variable {env_var} for {section_name}")
            
            # Load the database config
            logger.info(f"Loading database configuration for {section_name}")
            try:
                db_config = self.context.config_loader.get_db_config(section_name)
            except AttributeError:
                # Fallback: create DatabaseConfig manually if get_db_config doesn't exist
                config_data = self.context.config_loader.load_config_file('config.ini')
                section_data = config_data.get(section_name, {})
                if not section_data:
                    raise ConfigurationError(f"Configuration section '{section_name}' not found")
                
                # Create DatabaseConfig from section data
                from utils.config_loader import DatabaseConfig
                db_config = DatabaseConfig(
                    host=section_data.get('host', ''),
                    port=int(section_data.get('port', 5432)),
                    database=section_data.get('database', section_data.get('service_name', '')),
                    username=section_data.get('username', ''),
                    password=section_data.get('password', '')
                )
            
            # Cache it
            if not hasattr(self.context, 'config_cache'):
                self.context.config_cache = {}
            self.context.config_cache[cache_key] = db_config
            
            logger.info(f"✅ Database config loaded: {section_name} -> {db_config.host}:{db_config.port}")
            return db_config
            
        except Exception as e:
            logger.error(f"❌ Failed to load database config for {section_name}: {e}")
            
            # Provide helpful error message based on the error type
            error_msg = str(e).lower()
            if 'environment variable' in error_msg:
                logger.error(f"💡 Hint: Set the required environment variable for {section_name}")
                logger.error(f"   Example: export {section_name}_PWD=your_password")
            elif 'section' in error_msg:
                logger.error(f"💡 Hint: Check that [{section_name}] section exists in config.ini")
            
            raise
    
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
            api_config = self.context.config_loader.get_api_config(section_name)
            
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
            logger.debug(f"Loading configuration {section_name}.{key}")
            
            # Try direct config file reading to avoid validation issues
            try:
                value = self.context.config_loader.get_custom_config(section_name, key)
            except Exception:
                # Fallback: read directly from config file
                import configparser
                from pathlib import Path
                
                config_path = Path('config/config.ini')
                if config_path.exists():
                    parser = configparser.ConfigParser()
                    parser.read(config_path)
                    
                    if section_name in parser and key in parser[section_name]:
                        value = parser[section_name][key]
                    elif 'DEFAULT' in parser and key in parser['DEFAULT']:
                        value = parser['DEFAULT'][key]
                    else:
                        raise ConfigurationError(f"Configuration key '{key}' not found in section '{section_name}'")
                else:
                    raise ConfigurationError("Configuration file config.ini not found")
            
            # Cache it
            if not hasattr(self.context, 'config_cache'):
                self.context.config_cache = {}
            self.context.config_cache[cache_key] = value
            
            logger.debug(f"✅ Config loaded: {section_name}.{key} = {value}")
            return value
            
        except Exception as e:
            logger.error(f"❌ Failed to load config {section_name}.{key}: {e}")
            raise
    
    def get_environment_specific_config(self, base_section: str, environment: str = 'DEV') -> DatabaseConfig:
        """
        Load environment-specific database configuration.
        
        Args:
            base_section: Base section name (e.g., 'ORACLE', 'POSTGRES')
            environment: Environment (e.g., 'DEV', 'QA', 'PROD')
            
        Returns:
            DatabaseConfig for the specified environment
        """
        section_name = f"{environment}_{base_section}"
        return self.load_database_config(section_name)
    
    def load_config_with_fallback(self, primary_section: str, fallback_section: str) -> DatabaseConfig:
        """
        Load configuration with fallback option.
        
        Args:
            primary_section: Primary section to try first
            fallback_section: Fallback section if primary fails
            
        Returns:
            DatabaseConfig from primary or fallback section
        """
        try:
            return self.load_database_config(primary_section)
        except Exception as primary_error:
            logger.warning(f"Primary config {primary_section} failed: {primary_error}")
            logger.info(f"Trying fallback config: {fallback_section}")
            return self.load_database_config(fallback_section)
    
    def clear_config_cache(self):
        """Clear the configuration cache."""
        if hasattr(self.context, 'config_cache'):
            self.context.config_cache.clear()
            logger.debug("Configuration cache cleared")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get information about cached configurations."""
        if not hasattr(self.context, 'config_cache'):
            return {'cached_configs': 0, 'cache_keys': []}
        
        cache = self.context.config_cache
        return {
            'cached_configs': len(cache),
            'cache_keys': list(cache.keys())
        }


# Convenience functions for use in step definitions
def get_config_helper(context) -> TestConfigHelper:
    """Get or create a TestConfigHelper for the current context."""
    if not hasattr(context, '_config_helper'):
        context._config_helper = TestConfigHelper(context)
    return context._config_helper


def load_db_config_when_needed(context, section_name: str, env_vars: Optional[Dict[str, str]] = None) -> DatabaseConfig:
    """
    Convenience function to load database config on-demand.
    
    Args:
        context: Behave context
        section_name: Database section name
        env_vars: Optional environment variables to set
        
    Returns:
        DatabaseConfig object
    """
    helper = get_config_helper(context)
    return helper.load_database_config(section_name, env_vars)


def load_config_value_when_needed(context, section: str, key: str) -> Any:
    """
    Convenience function to load a specific config value on-demand.
    
    Args:
        context: Behave context
        section: Config section
        key: Config key
        
    Returns:
        Configuration value
    """
    helper = get_config_helper(context)
    return helper.load_custom_config(section, key)