# features/environment.py
import os
import time

# Import cleanup functions from enhanced database steps
try:
    from steps.database.base_database_steps import after_scenario as db_cleanup
    from steps.database.mongodb_steps import mongodb_after_scenario
    from steps.database.cross_database_steps import cross_database_cleanup
except ImportError as e:
    print(f"Warning: Could not import database cleanup functions: {e}")
    db_cleanup = None
    mongodb_after_scenario = None
    cross_database_cleanup = None

# Import Kafka cleanup function
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
    logger.info("Starting test execution with enhanced database steps")
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
    context.quality_check_results = None
    context.schema_comparison = None
    
    # Reset current database context
    context.current_env = None
    context.current_db_type = None
    
    # Initialize database manager if not exists
    if not hasattr(context, 'db_manager'):
        try:
            from db.database_manager import DatabaseManager
            context.db_manager = DatabaseManager()
        except ImportError as e:
            logger.warning(f"Could not initialize DatabaseManager: {e}")


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
        if hasattr(context, 'last_query_error'):
            test_logger.error(f"Last error: {context.last_query_error}")
    
    # Clean up database connections using the enhanced steps
    try:
        # Base database cleanup
        if db_cleanup:
            db_cleanup(context, scenario)
        
        # MongoDB-specific cleanup
        if mongodb_after_scenario:
            mongodb_after_scenario(context, scenario)
        
        # Cross-database cleanup
        if cross_database_cleanup:
            cross_database_cleanup(context)
            
        # Clean up database manager connections
        if hasattr(context, 'db_manager'):
            context.db_manager.cleanup_connections()

        # Call Kafka cleanup function
        if kafka_after_scenario:
            kafka_after_scenario(context, scenario)
            
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
        if hasattr(context, 'db_manager'):
            context.db_manager.cleanup_connections()
            logger.info("Final database cleanup completed")
    except Exception as e:
        logger.warning(f"Error during final cleanup: {e}")


# Error handler for steps
def after_step(context, step):
    """Log step execution details."""
    if step.status.name == "failed":
        test_logger.error(f"Step failed: {step.name}")
        if hasattr(step, 'exception'):
            test_logger.error(f"Exception: {step.exception}")


# Tag-based setup (optional)
def before_tag(context, tag):
    """Setup based on scenario tags."""
    if tag == "database":
        logger.debug("Setting up for database scenario")
    elif tag == "mongodb":
        logger.debug("Setting up for MongoDB scenario")
    elif tag == "cross_database":
        logger.debug("Setting up for cross-database scenario")
    elif tag == "kafka":
        logger.debug("Setting up for Kafka scenario")


def after_tag(context, tag):
    """Cleanup based on scenario tags."""
    if tag in ["database", "mongodb", "cross_database", "kafka"]:
        logger.debug(f"Cleaning up after {tag} scenario")
        # Additional tag-specific cleanup can be added here
