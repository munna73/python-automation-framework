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


# environment.py - Behave environment setup

import os
from behave.model import Scenario
from steps.database.base_database_steps import BaseDatabaseSteps
from utils.logger import logger
from config.database_config import db_config_manager


def before_all(context):
    """Setup before all tests."""
    logger.info("Starting test execution")
    
    # Validate all database configurations
    context.config_validation_results = {}
    
    for config_key in db_config_manager.list_available_configs():
        env, db_type = config_key.split('_', 1)
        is_valid = db_config_manager.validate_config(env, db_type)
        context.config_validation_results[config_key] = is_valid
        
        if not is_valid:
            logger.warning(f"Invalid configuration for {config_key}")
    
    # Initialize performance tracking
    context.performance_data = {}
    
    # Setup global test context
    context.test_start_time = time.time()


def before_feature(context, feature):
    """Setup before each feature."""
    logger.info(f"Starting feature: {feature.name}")
    
    # Initialize feature-specific context
    context.feature_start_time = time.time()
    context.scenarios_run = 0
    context.scenarios_passed = 0
    context.scenarios_failed = 0


def before_scenario(context, scenario):
    """Setup before each scenario."""
    logger.info(f"Starting scenario: {scenario.name}")
    
    # Initialize scenario-specific context
    context.scenario_start_time = time.time()
    context.scenarios_run += 1
    
    # Reset any previous results
    context.last_query_results = None
    context.last_query_error = None
    context.validation_results = None
    context.comparison_result = None
    context.quality_check_results = None
    
    # Check if database configurations are needed
    database_tags = [tag for tag in scenario.tags if tag in ['database', 'sql', 'mongodb', 'oracle', 'postgres']]
    
    if database_tags:
        # Pre-validate required configurations based on scenario tags
        required_configs = []
        
        if 'database' in scenario.tags or 'sql' in scenario.tags:
            required_configs.extend(['DEV_ORACLE', 'DEV_POSTGRES'])
        
        if 'mongodb' in scenario.tags:
            required_configs.append('DEV_MONGODB')
        
        # Check configuration availability
        for config_key in required_configs:
            if not context.config_validation_results.get(config_key, False):
                logger.warning(f"Required configuration {config_key} is not available for scenario: {scenario.name}")


def after_scenario(context, scenario):
    """Cleanup after each scenario."""
    scenario_duration = time.time() - context.scenario_start_time
    
    if scenario.status == "passed":
        context.scenarios_passed += 1
        logger.info(f"Scenario passed: {scenario.name} (Duration: {scenario_duration:.2f}s)")
    else:
        context.scenarios_failed += 1
        logger.error(f"Scenario failed: {scenario.name} (Duration: {scenario_duration:.2f}s)")
    
    # Clean up database connections
    if hasattr(context, 'db_steps'):
        context.db_steps.cleanup_connections()
    
    if hasattr(context, 'sql_steps'):
        context.sql_steps.cleanup_connections()
    
    # Record performance data
    context.performance_data[scenario.name] = {
        'duration': scenario_duration,
        'status': scenario.status,
        'tags': scenario.tags
    }


def after_feature(context, feature):
    """Cleanup after each feature."""
    feature_duration = time.time() - context.feature_start_time
    
    logger.info(f"Feature completed: {feature.name}")
    logger.info(f"  Duration: {feature_duration:.2f}s")
    logger.info(f"  Scenarios run: {context.scenarios_run}")
    logger.info(f"  Scenarios passed: {context.scenarios_passed}")
    logger.info(f"  Scenarios failed: {context.scenarios_failed}")
    
    # Generate feature-level performance report
    if hasattr(context, 'performance_data'):
        _generate_feature_performance_report(context, feature)


def after_all(context):
    """Cleanup after all tests."""
    total_duration = time.time() - context.test_start_time
    
    logger.info("Test execution completed")
    logger.info(f"Total duration: {total_duration:.2f}s")
    
    # Generate final performance report
    _generate_final_performance_report(context)


def _generate_feature_performance_report(context, feature):
    """Generate performance report for a feature."""
    feature_scenarios = [
        (name, data) for name, data in context.performance_data.items()
        if name.startswith(feature.name) or any(tag in data['tags'] for tag in feature.tags)
    ]
    
    if feature_scenarios:
        total_time = sum(data['duration'] for _, data in feature_scenarios)
        avg_time = total_time / len(feature_scenarios)
        
        logger.info(f"Feature performance summary for {feature.name}:")
        logger.info(f"  Average scenario duration: {avg_time:.2f}s")
        logger.info(f"  Total feature time: {total_time:.2f}s")


def _generate_final_performance_report(context):
    """Generate final performance report."""
    if not hasattr(context, 'performance_data'):
        return
    
    # Create performance summary
    performance_summary = {
        'total_scenarios': len(context.performance_data),
        'passed_scenarios': len([d for d in context.performance_data.values() if d['status'] == 'passed']),
        'failed_scenarios': len([d for d in context.performance_data.values() if d['status'] == 'failed']),
        'total_duration': sum(d['duration'] for d in context.performance_data.values()),
        'average_duration': 0
    }
    
    if performance_summary['total_scenarios'] > 0:
        performance_summary['average_duration'] = (
            performance_summary['total_duration'] / performance_summary['total_scenarios']
        )
    
    # Log summary
    logger.info("Final Performance Summary:")
    logger.info(f"  Total scenarios: {performance_summary['total_scenarios']}")
    logger.info(f"  Passed: {performance_summary['passed_scenarios']}")
    logger.info(f"  Failed: {performance_summary['failed_scenarios']}")
    logger.info(f"  Total duration: {performance_summary['total_duration']:.2f}s")
    logger.info(f"  Average duration: {performance_summary['average_duration']:.2f}s")
    
    # Save detailed report to file
    try:
        import json
        import pandas as pd
        from datetime import datetime
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save JSON report
        json_report_path = f"output/performance_report_{timestamp}.json"
        with open(json_report_path, 'w') as f:
            json.dump({
                'summary': performance_summary,
                'details': context.performance_data
            }, f, indent=2)
        
        # Save CSV report
        csv_data = []
        for scenario_name, data in context.performance_data.items():
            csv_data.append({
                'scenario': scenario_name,
                'duration': data['duration'],
                'status': data['status'],
                'tags': ', '.join(data['tags'])
            })
        
        df = pd.DataFrame(csv_data)
        csv_report_path = f"output/performance_report_{timestamp}.csv"
        df.to_csv(csv_report_path, index=False)
        
        logger.info(f"Performance reports saved:")
        logger.info(f"  JSON: {json_report_path}")
        logger.info(f"  CSV: {csv_report_path}")
        
    except Exception as e:
        logger.error(f"Error generating performance report: {e}")


# Test configuration validation utility
def validate_test_environment():
    """Validate that the test environment is properly configured."""
    validation_results = {
        'database_configs': {},
        'environment_variables': {},
        'file_permissions': {},
        'overall_status': True
    }
    
    # Validate database configurations
    for config_key in db_config_manager.list_available_configs():
        env, db_type = config_key.split('_', 1)
        is_valid = db_config_manager.validate_config(env, db_type)
        validation_results['database_configs'][config_key] = is_valid
        
        if not is_valid:
            validation_results['overall_status'] = False
    
    # Validate environment variables
    required_env_vars = [
        'DEV_ORACLE_PWD', 'DEV_POSTGRES_PWD', 'DEV_MONGODB_PWD',
        'QA_ORACLE_PWD', 'QA_POSTGRES_PWD', 'QA_MONGODB_PWD'
    ]
    
    for env_var in required_env_vars:
        value = os.getenv(env_var)
        validation_results['environment_variables'][env_var] = bool(value)
        
        if not value:
            validation_results['overall_status'] = False
    
    # Validate file permissions and directories
    required_dirs = ['output', 'logs', 'config']
    
    for dir_name in required_dirs:
        try:
            os.makedirs(dir_name, exist_ok=True)
            validation_results['file_permissions'][dir_name] = True
        except Exception as e:
            logger.error(f"Cannot access directory {dir_name}: {e}")
            validation_results['file_permissions'][dir_name] = False
            validation_results['overall_status'] = False
    
    return validation_results


if __name__ == "__main__":
    # Run validation when script is executed directly
    results = validate_test_environment()
    
    print("Environment Validation Results:")
    print("=" * 50)
    
    print("\nDatabase Configurations:")
    for config, status in results['database_configs'].items():
        status_text = "✓ VALID" if status else "✗ INVALID"
        print(f"  {config}: {status_text}")
    
    print("\nEnvironment Variables:")
    for env_var, status in results['environment_variables'].items():
        status_text = "✓ SET" if status else "✗ MISSING"
        print(f"  {env_var}: {status_text}")
    
    print("\nFile Permissions:")
    for directory, status in results['file_permissions'].items():
        status_text = "✓ OK" if status else "✗ ERROR"
        print(f"  {directory}: {status_text}")
    
    print("\n" + "=" * 50)
    overall_status = "✓ READY" if results['overall_status'] else "✗ ISSUES FOUND"
    print(f"Overall Status: {overall_status}")
    
    if not results['overall_status']:
        print("\nPlease fix the issues above before running tests.")
        exit(1)
    else:
        print("\nEnvironment is ready for testing!")
        exit(0)