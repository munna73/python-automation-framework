"""
Behave environment configuration and hooks.
"""
import os
from pathlib import Path
from utils.logger import logger

def before_all(context):
    """Execute before all tests."""
    logger.info("Starting test execution")
    
    # Setup test data directories
    context.test_data_dir = Path(__file__).parent.parent / "data" / "test_data"
    context.output_dir = Path(__file__).parent.parent / "output"
    
    # Create directories if they don't exist
    context.test_data_dir.mkdir(parents=True, exist_ok=True)
    context.output_dir.mkdir(parents=True, exist_ok=True)

def before_feature(context, feature):
    """Execute before each feature."""
    logger.info(f"Starting feature: {feature.name}")

def before_scenario(context, scenario):
    """Execute before each scenario."""
    logger.info(f"Starting scenario: {scenario.name}")

def after_scenario(context, scenario):
    """Execute after each scenario."""
    if scenario.status == "failed":
        logger.error(f"Scenario failed: {scenario.name}")
    else:
        logger.info(f"Scenario completed: {scenario.name}")

def after_all(context):
    """Execute after all tests."""
    logger.info("Test execution completed")