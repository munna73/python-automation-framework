# features/environment.py
import os
import time

# Import cleanup functions from the new database steps
try:
    from steps.database_steps import after_scenario as db_comparison_cleanup
except ImportError as e:
    print(f"Warning: Could not import database comparison cleanup function: {e}")
    db_comparison_cleanup = None

# Import MongoDB cleanup function (keep if you still use MongoDB)
try:
    from features.steps.mongodb_steps import mongodb_after_scenario
except ImportError as e:
    print(f"Warning: Could not import MongoDB cleanup function: {e}")
    mongodb_after_scenario = None

# Import Kafka cleanup function (keep if you still use Kafka)
try:
    from steps.kafka_steps import kafka_after_scenario
except ImportError as e:
    print(f"Warning: Could not import Kafka cleanup function: {e}")
    kafka_after_scenario = None

# Import your existing logger
try:
    from utils.logger import logger, test_logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    test_logger = logger


def before_all(context):
    """Setup before all tests."""
    logger.info("Starting test execution with database comparison framework")
    context.test_start_time = time.time()
    
    # Initialize any global test configuration
    context.test_config = {
        'start_time': context.test_start_time,
        'total_scenarios': 0,
        'passed_scenarios': 0,
        'failed_scenarios': 0
    }


def before_feature(context, feature):
    """Setup before each feature."""
    logger.info(f"Starting feature: {feature.name}")
    context.feature_start_time = time.time()
    
    # Initialize feature-level counters
    context.feature_scenarios = 0
    context.feature_passed = 0
    context.feature_failed = 0


def before_scenario(context, scenario):
    """Setup before each scenario."""
    test_logger.info(f"Starting scenario: {scenario.name}")
    context.scenario_start_time = time.time()
    
    # Reset any previous results
    context.last_query_results = None
    context.last_query_error = None
    
    # Reset validation and comparison results
    context.validation_results = None
    context.comparison_result = None
    context.comparison_results = None  # For database comparison
    context.quality_check_results = None
    context.schema_comparison = None
    context.source_quality_metrics = None
    context.target_quality_metrics = None
    
    # Reset current database context
    context.current_env = None
    context.current_db_type = None
    context.current_query = None
    context.current_environment = None
    
    # Reset connection references
    context.oracle_engine = None
    context.postgres_engine = None
    context.oracle_section = None
    context.postgres_section = None
    
    # Reset record counts
    context.source_record_count = 0
    context.target_record_count = 0
    
    # Initialize config loader reference
    context.config_loader = None
    context.config_loaded = False


def after_scenario(context, scenario):
    """Cleanup after each scenario."""
    scenario_duration = time.time() - context.scenario_start_time
    
    # Update counters
    context.test_config['total_scenarios'] += 1
    context.feature_scenarios += 1
    
    if scenario.status.name == "passed":
        test_logger.info(f"✓ Scenario passed: {scenario.name} (Duration: {scenario_duration:.2f}s)")
        context.test_config['passed_scenarios'] += 1
        context.feature_passed += 1
    else:
        test_logger.error(f"✗ Scenario failed: {scenario.name} (Duration: {scenario_duration:.2f}s)")
        context.test_config['failed_scenarios'] += 1
        context.feature_failed += 1
        
        # Log failure details if available
        if hasattr(context, 'last_query_error') and context.last_query_error:
            test_logger.error(f"Last error: {context.last_query_error}")
    
    # Clean up database connections using the new database comparison steps
    try:
        # Database comparison cleanup
        if db_comparison_cleanup:
            db_comparison_cleanup(context, scenario)
        
        # MongoDB-specific cleanup (if you still use it)
        if mongodb_after_scenario:
            mongodb_after_scenario(context, scenario)
        
        # Legacy database manager cleanup (if it exists)
        if hasattr(context, 'db_manager') and context.db_manager:
            try:
                context.db_manager.cleanup_connections()
            except AttributeError:
                # Handle case where cleanup_connections method doesn't exist
                logger.debug("db_manager doesn't have cleanup_connections method")
            except Exception as db_error:
                logger.warning(f"Error cleaning up db_manager: {db_error}")

        # Kafka cleanup (if you still use it)
        if kafka_after_scenario:
            kafka_after_scenario(context, scenario)
            
        # Additional cleanup for database comparison context
        if hasattr(context, 'oracle_engine') and context.oracle_engine:
            try:
                context.oracle_engine.dispose()
                context.oracle_engine = None
            except Exception as oracle_error:
                logger.warning(f"Error disposing Oracle engine: {oracle_error}")
                
        if hasattr(context, 'postgres_engine') and context.postgres_engine:
            try:
                context.postgres_engine.dispose()
                context.postgres_engine = None
            except Exception as postgres_error:
                logger.warning(f"Error disposing PostgreSQL engine: {postgres_error}")
            
    except Exception as e:
        logger.warning(f"Error during cleanup: {e}")


def after_feature(context, feature):
    """Cleanup after each feature."""
    feature_duration = time.time() - context.feature_start_time
    
    # Log feature summary
    logger.info(f"Feature completed: {feature.name} (Duration: {feature_duration:.2f}s)")
    logger.info(f"Feature stats - Total: {context.feature_scenarios}, "
                f"Passed: {context.feature_passed}, Failed: {context.feature_failed}")


def after_all(context):
    """Cleanup after all tests."""
    total_duration = time.time() - context.test_start_time
    
    # Log test execution summary
    logger.info("=" * 60)
    logger.info("TEST EXECUTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total duration: {total_duration:.2f}s")
    logger.info(f"Total scenarios: {context.test_config['total_scenarios']}")
    logger.info(f"Passed scenarios: {context.test_config['passed_scenarios']}")
    logger.info(f"Failed scenarios: {context.test_config['failed_scenarios']}")
    
    if context.test_config['total_scenarios'] > 0:
        pass_rate = (context.test_config['passed_scenarios'] / context.test_config['total_scenarios']) * 100
        logger.info(f"Pass rate: {pass_rate:.1f}%")
    
    logger.info("=" * 60)
    
    # Final cleanup
    try:
        # Database comparison final cleanup
        if db_comparison_cleanup:
            db_comparison_cleanup(context, None)  # Pass None for scenario in final cleanup
            
        # Legacy db_manager cleanup
        if hasattr(context, 'db_manager') and context.db_manager:
            try:
                context.db_manager.cleanup_connections()
                logger.info("Final database cleanup completed")
            except Exception as db_error:
                logger.warning(f"Error during final db_manager cleanup: {db_error}")
                
    except Exception as e:
        logger.warning(f"Error during final cleanup: {e}")


# Error handler for steps
def after_step(context, step):
    """Log step execution details."""
    if step.status.name == "failed":
        test_logger.error(f"Step failed: {step.name}")
        if hasattr(step, 'exception') and step.exception:
            test_logger.error(f"Exception: {step.exception}")
        
        # Store the error for later reference
        context.last_query_error = str(step.exception) if hasattr(step, 'exception') else "Unknown error"


# Tag-based setup
def before_tag(context, tag):
    """Setup based on scenario tags."""
    if tag == "database":
        logger.debug("Setting up for database scenario")
        # Initialize database-specific context if needed
        context.database_tag_active = True
    elif tag == "oracle":
        logger.debug("Setting up for Oracle scenario")
        context.oracle_tag_active = True
    elif tag == "postgres":
        logger.debug("Setting up for PostgreSQL scenario")
        context.postgres_tag_active = True
    elif tag == "mongodb":
        logger.debug("Setting up for MongoDB scenario")
        context.mongodb_tag_active = True
    elif tag == "kafka":
        logger.debug("Setting up for Kafka scenario")
        context.kafka_tag_active = True
    elif tag == "comparison":
        logger.debug("Setting up for data comparison scenario")
        context.comparison_tag_active = True
    elif tag == "validation":
        logger.debug("Setting up for data validation scenario")
        context.validation_tag_active = True


def after_tag(context, tag):
    """Cleanup based on scenario tags."""
    if tag in ["database", "oracle", "postgres", "mongodb", "kafka", "comparison", "validation"]:
        logger.debug(f"Cleaning up after {tag} scenario")
        
        # Reset tag-specific flags
        tag_flag = f"{tag}_tag_active"
        if hasattr(context, tag_flag):
            setattr(context, tag_flag, False)


# Environment variable validation (optional)
def validate_environment_variables():
    """Validate that required environment variables are set."""
    required_env_vars = [
        # Add your required environment variables here
        # Example: 'SAT_ORACLE_USERNAME', 'SAT_ORACLE_PASSWORD', etc.
    ]
    
    missing_vars = []
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.warning(f"Missing environment variables: {missing_vars}")
        print(f"Warning: Missing environment variables: {missing_vars}")
        print("Set these variables before running tests for full functionality.")
    else:
        logger.info("All required environment variables are set")


# Call validation at module load time (optional)
# validate_environment_variables()