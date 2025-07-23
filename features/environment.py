"""
Behave environment configuration and hooks.
Enhanced with Python path setup for VSCode integration.
"""
import os
import sys
from pathlib import Path

# CRITICAL: Add project root to Python path for VSCode step linking
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Now import after path setup
from utils.logger import logger

def before_all(context):
    """Execute before all tests."""
    logger.info("Starting test execution")
    
    # Setup test data directories
    context.test_data_dir = Path(__file__).parent.parent / "data" / "test_data"
    context.output_dir = Path(__file__).parent.parent / "output"
    context.project_root = project_root
    
    # Create directories if they don't exist
    context.test_data_dir.mkdir(parents=True, exist_ok=True)
    context.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Setup environment variables if not already set
    setup_environment_defaults(context)
    
    logger.info(f"Project root: {project_root}")
    logger.info(f"Test data directory: {context.test_data_dir}")
    logger.info(f"Output directory: {context.output_dir}")

def before_feature(context, feature):
    """Execute before each feature."""
    logger.info(f"Starting feature: {feature.name}")
    
    # Feature-specific setup
    context.feature_name = feature.name
    context.feature_tags = [tag for tag in feature.tags]
    
    # Create feature-specific output directory
    feature_output_dir = context.output_dir / "features" / feature.name.replace(" ", "_")
    feature_output_dir.mkdir(parents=True, exist_ok=True)
    context.feature_output_dir = feature_output_dir

def before_scenario(context, scenario):
    """Execute before each scenario."""
    logger.info(f"Starting scenario: {scenario.name}")
    
    # Scenario-specific setup
    context.scenario_name = scenario.name
    context.scenario_tags = [tag for tag in scenario.tags]
    
    # Initialize scenario data
    context.scenario_data = {}
    context.test_results = {}
    
    # Setup component connections based on tags
    setup_connections_for_tags(context, scenario.tags)

def after_scenario(context, scenario):
    """Execute after each scenario."""
    if scenario.status == "failed":
        logger.error(f"Scenario failed: {scenario.name}")
        
        # Capture failure information
        if hasattr(context, 'scenario_data'):
            context.scenario_data['failure_status'] = scenario.status
            context.scenario_data['failure_reason'] = getattr(scenario, 'exception', 'Unknown error')
        
        # Cleanup on failure
        cleanup_on_failure(context)
    else:
        logger.info(f"Scenario completed: {scenario.name}")
    
    # Always cleanup connections
    cleanup_connections(context)

def after_feature(context, feature):
    """Execute after each feature."""
    logger.info(f"Completed feature: {feature.name}")
    
    # Feature-level cleanup
    if hasattr(context, 'feature_output_dir'):
        logger.info(f"Feature outputs saved to: {context.feature_output_dir}")

def after_all(context):
    """Execute after all tests."""
    logger.info("Test execution completed")
    
    # Final cleanup
    final_cleanup(context)
    
    # Log execution summary
    if hasattr(context, 'output_dir'):
        logger.info(f"All test outputs available in: {context.output_dir}")

def setup_environment_defaults(context):
    """Setup default environment variables if not already set."""
    defaults = {
        'LOG_LEVEL': 'INFO',
        'BEHAVE_DEBUG_ON_ERROR': 'true',
        'PYTHONPATH': str(context.project_root)
    }
    
    for key, value in defaults.items():
        if not os.getenv(key):
            os.environ[key] = value
            logger.debug(f"Set default environment variable: {key}={value}")

def setup_connections_for_tags(context, tags):
    """Setup connections based on scenario tags."""
    try:
        # Database connections
        if 'database' in tags or 'mongodb' in tags:
            logger.debug("Initializing database connectors for scenario")
            # Import here to avoid circular imports
            from db.database_connector import db_connector
            from db.mongodb_connector import mongodb_connector
            context.db_connector = db_connector
            context.mongodb_connector = mongodb_connector
        
        # API client
        if 'api' in tags:
            logger.debug("Initializing API client for scenario")
            from web.api_client import api_client
            context.api_client = api_client
        
        # MQ producer
        if 'mq' in tags:
            logger.debug("Initializing MQ producer for scenario")
            from mq.mq_producer import mq_producer
            context.mq_producer = mq_producer
        
        # AWS connectors
        if 'aws' in tags or 'sqs' in tags or 's3' in tags:
            logger.debug("Initializing AWS connectors for scenario")
            from aws.sqs_connector import sqs_connector
            from aws.s3_connector import s3_connector
            from aws.sql_integration import aws_sql_integration
            context.sqs_connector = sqs_connector
            context.s3_connector = s3_connector
            context.aws_sql_integration = aws_sql_integration
            
    except ImportError as e:
        logger.warning(f"Could not import connector for tags {tags}: {e}")
    except Exception as e:
        logger.error(f"Error setting up connections for tags {tags}: {e}")

def cleanup_connections(context):
    """Cleanup connections after scenario."""
    try:
        # Close database connections
        if hasattr(context, 'db_connector'):
            context.db_connector.close_connections()
        
        if hasattr(context, 'mongodb_connector'):
            context.mongodb_connector.close_connections()
        
        # Clear API client history
        if hasattr(context, 'api_client'):
            context.api_client.clear_history()
        
        # Disconnect MQ
        if hasattr(context, 'mq_producer'):
            context.mq_producer.disconnect()
            
    except Exception as e:
        logger.warning(f"Error during connection cleanup: {e}")

def cleanup_on_failure(context):
    """Additional cleanup when scenario fails."""
    try:
        # Save scenario data for debugging
        if hasattr(context, 'scenario_data') and hasattr(context, 'feature_output_dir'):
            failure_file = context.feature_output_dir / f"failed_scenario_{context.scenario_name.replace(' ', '_')}.json"
            
            import json
            with open(failure_file, 'w') as f:
                # Convert any non-serializable objects to strings
                serializable_data = {}
                for key, value in context.scenario_data.items():
                    try:
                        json.dumps(value)  # Test if serializable
                        serializable_data[key] = value
                    except:
                        serializable_data[key] = str(value)
                
                json.dump(serializable_data, f, indent=2)
            
            logger.info(f"Failure data saved to: {failure_file}")
            
    except Exception as e:
        logger.error(f"Error saving failure data: {e}")

def final_cleanup(context):
    """Final cleanup after all tests."""
    try:
        # Close any remaining connections
        cleanup_connections(context)
        
        # Generate test summary if possible
        generate_test_summary(context)
        
    except Exception as e:
        logger.error(f"Error during final cleanup: {e}")

def generate_test_summary(context):
    """Generate a test execution summary."""
    try:
        if hasattr(context, 'output_dir'):
            summary_file = context.output_dir / "test_execution_summary.txt"
            
            with open(summary_file, 'w') as f:
                f.write("Test Execution Summary\n")
                f.write("=" * 50 + "\n")
                f.write(f"Project Root: {context.project_root}\n")
                f.write(f"Test Data Directory: {context.test_data_dir}\n")
                f.write(f"Output Directory: {context.output_dir}\n")
                f.write(f"Python Path: {sys.path[0]}\n")
                f.write("=" * 50 + "\n")
            
            logger.info(f"Test summary saved to: {summary_file}")
            
    except Exception as e:
        logger.warning(f"Could not generate test summary: {e}")

# Error handler for better debugging
def before_step(context, step):
    """Execute before each step (optional debugging)."""
    if os.getenv('BEHAVE_DEBUG_ON_ERROR', '').lower() == 'true':
        logger.debug(f"Executing step: {step.step_type} {step.name}")

def after_step(context, step):
    """Execute after each step (optional debugging)."""
    if step.status == 'failed' and os.getenv('BEHAVE_DEBUG_ON_ERROR', '').lower() == 'true':
        logger.error(f"Step failed: {step.step_type} {step.name}")
        if hasattr(step, 'exception'):
            logger.error(f"Step exception: {step.exception}")