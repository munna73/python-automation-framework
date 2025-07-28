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

# Import your existing logger
try:
    from utils.logger import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)


def before_all(context):
    """Setup before all tests."""
    logger.info("Starting test execution with enhanced database steps")
    context.test_start_time = time.time()


def before_feature(context, feature):
    """Setup before each feature."""
    logger.info(f"Starting feature: {feature.name}")
    context.feature_start_time = time.time()


def before_scenario(context, scenario):
    """Setup before each scenario."""
    logger.info(f"Starting scenario: {scenario.name}")
    context.scenario_start_time = time.time()
    
    # Reset any previous results
    context.last_query_results = None
    context.last_query_error = None
    
    # Reset validation and comparison results
    context.validation_results = None
    context.comparison_result = None
    context.quality_check_results = None
    context.schema_comparison = None


def after_scenario(context, scenario):
    """Cleanup after each scenario."""
    scenario_duration = time.time() - context.scenario_start_time
    
    if scenario.status == "passed":
        logger.info(f"Scenario passed: {scenario.name} (Duration: {scenario_duration:.2f}s)")
    else:
        logger.error(f"Scenario failed: {scenario.name} (Duration: {scenario_duration:.2f}s)")
    
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
            
    except Exception as e:
        logger.warning(f"Error during cleanup: {e}")


def after_feature(context, feature):
    """Cleanup after each feature."""
    feature_duration = time.time() - context.feature_start_time
    logger.info(f"Feature completed: {feature.name} (Duration: {feature_duration:.2f}s)")


def after_all(context):
    """Cleanup after all tests."""
    total_duration = time.time() - context.test_start_time
    logger.info(f"Test execution completed (Total duration: {total_duration:.2f}s)")