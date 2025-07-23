"""
Behave environment configuration and hooks.
Universal compatibility: Windows, Linux, macOS, Jenkins CI/CD.
"""
import os
import sys
import platform
from pathlib import Path

def setup_universal_python_path():
    """Setup Python path with universal compatibility for all platforms and CI/CD."""
    try:
        # Detect environment
        is_jenkins = os.getenv('JENKINS_URL') is not None or os.getenv('BUILD_NUMBER') is not None
        is_github_actions = os.getenv('GITHUB_ACTIONS') is not None
        is_ci = is_jenkins or is_github_actions or os.getenv('CI') is not None
        current_platform = platform.system()  # Windows, Linux, Darwin (macOS)
        
        print(f"DEBUG: Platform: {current_platform}, CI Environment: {is_ci}")
        if is_jenkins:
            print(f"DEBUG: Jenkins detected - Workspace: {os.getenv('WORKSPACE', 'Not set')}")
        
        # Get project root - handle multiple scenarios
        current_file = Path(__file__).resolve()
        project_root = current_file.parent.parent  # features/environment.py -> project root
        
        # Validate project root by checking for key directories
        required_dirs = ['features', 'steps', 'utils']
        if not all((project_root / dir_name).exists() for dir_name in required_dirs):
            print(f"WARNING: Expected directories not found in {project_root}")
            # Try alternative locations
            alternative_roots = [
                Path.cwd(),  # Current working directory
                Path(os.getenv('WORKSPACE', '.')),  # Jenkins workspace
                Path(os.getenv('GITHUB_WORKSPACE', '.')),  # GitHub Actions
                current_file.parent.parent.parent  # In case we're nested deeper
            ]
            
            for alt_root in alternative_roots:
                if alt_root.exists() and all((alt_root / dir_name).exists() for dir_name in required_dirs):
                    project_root = alt_root
                    print(f"DEBUG: Found project root at: {project_root}")
                    break
        
        # Normalize path for the platform
        if current_platform == 'Windows':
            project_root_str = str(project_root).replace('\\', '/')
        else:
            project_root_str = str(project_root)
        
        # Add to Python path if not already there
        if project_root_str not in sys.path:
            sys.path.insert(0, project_root_str)
        
        # Set PYTHONPATH environment variable for subprocess compatibility
        current_pythonpath = os.environ.get('PYTHONPATH', '')
        if project_root_str not in current_pythonpath:
            if current_pythonpath:
                separator = ';' if current_platform == 'Windows' else ':'
                os.environ['PYTHONPATH'] = f"{project_root_str}{separator}{current_pythonpath}"
            else:
                os.environ['PYTHONPATH'] = project_root_str
        
        print(f"DEBUG: Project root: {project_root}")
        print(f"DEBUG: Python path updated: {project_root_str}")
        print(f"DEBUG: Working directory: {os.getcwd()}")
        
        # Verify required directories exist
        for dir_name in required_dirs:
            dir_path = project_root / dir_name
            print(f"DEBUG: {dir_name} directory exists: {dir_path.exists()} ({dir_path})")
        
        return project_root
        
    except Exception as e:
        print(f"ERROR: Failed to setup Python path: {e}")
        print(f"ERROR: Current file: {__file__}")
        print(f"ERROR: Current working directory: {os.getcwd()}")
        print(f"ERROR: Python path: {sys.path}")
        
        # Emergency fallback
        fallback_root = Path.cwd()
        sys.path.insert(0, str(fallback_root))
        return fallback_root

# Setup path first
project_root = setup_universal_python_path()

# Import logger with fallback
def get_logger():
    """Get logger with fallback for different environments."""
    try:
        from utils.logger import logger
        print("DEBUG: Framework logger imported successfully")
        return logger
    except ImportError as e:
        print(f"WARNING: Could not import framework logger: {e}")
        # Create fallback logger
        import logging
        
        # Configure logging for CI/CD environments
        log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        if os.getenv('CI'):
            # CI environment - log to stdout
            logging.basicConfig(
                level=getattr(logging, log_level, logging.INFO),
                format=log_format,
                stream=sys.stdout
            )
        else:
            # Local development
            logging.basicConfig(
                level=getattr(logging, log_level, logging.INFO),
                format=log_format
            )
        
        logger = logging.getLogger('behave_framework')
        logger.info("Using fallback logger")
        return logger

logger = get_logger()

def before_all(context):
    """Execute before all tests - Universal compatibility."""
    logger.info("Starting test execution")
    
    # Environment detection
    is_ci = bool(os.getenv('CI') or os.getenv('JENKINS_URL') or os.getenv('GITHUB_ACTIONS'))
    is_jenkins = bool(os.getenv('JENKINS_URL') or os.getenv('BUILD_NUMBER'))
    platform_name = platform.system()
    
    logger.info(f"Environment: Platform={platform_name}, CI={is_ci}, Jenkins={is_jenkins}")
    
    if project_root is None:
        error_msg = "Could not determine project root directory!"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    # Setup directories with CI/CD compatibility
    context.project_root = project_root
    context.is_ci = is_ci
    context.is_jenkins = is_jenkins
    context.platform = platform_name
    
    # Set up data directories
    setup_directories(context)
    
    # Setup environment variables
    setup_environment_variables(context)
    
    # Log environment info
    log_environment_info(context)

def setup_directories(context):
    """Setup directories with cross-platform compatibility."""
    try:
        # Base directories
        context.test_data_dir = context.project_root / "data" / "test_data"
        context.output_dir = context.project_root / "output"
        context.logs_dir = context.project_root / "logs"
        
        # CI-specific output directory
        if context.is_jenkins:
            # Jenkins workspace handling
            workspace = os.getenv('WORKSPACE')
            if workspace:
                jenkins_output = Path(workspace) / "test-results"
                jenkins_output.mkdir(parents=True, exist_ok=True)
                context.jenkins_output_dir = jenkins_output
        
        # Create all directories
        directories_to_create = [
            context.test_data_dir,
            context.output_dir,
            context.output_dir / "exports",
            context.output_dir / "reports", 
            context.output_dir / "junit",
            context.logs_dir / "test_execution"
        ]
        
        for directory in directories_to_create:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Created/verified directory: {directory}")
            except Exception as e:
                logger.warning(f"Could not create directory {directory}: {e}")
                # Try alternative creation method
                try:
                    os.makedirs(str(directory), exist_ok=True)
                    logger.debug(f"Created directory using os.makedirs: {directory}")
                except Exception as e2:
                    logger.error(f"Failed to create directory {directory}: {e2}")
        
        logger.info(f"Test data directory: {context.test_data_dir}")
        logger.info(f"Output directory: {context.output_dir}")
        
    except Exception as e:
        logger.error(f"Error setting up directories: {e}")
        raise

def setup_environment_variables(context):
    """Setup environment variables for different platforms and CI systems."""
    # Universal defaults
    defaults = {
        'LOG_LEVEL': 'INFO',
        'BEHAVE_DEBUG_ON_ERROR': 'false',  # Less verbose for CI
        'PYTHONUNBUFFERED': '1',  # Important for CI logs
        'PYTHONPATH': str(context.project_root)
    }
    
    # CI-specific settings
    if context.is_ci:
        defaults.update({
            'LOG_LEVEL': 'INFO',  # More verbose for CI debugging
            'BEHAVE_DEBUG_ON_ERROR': 'true',
            'PYTHONUNBUFFERED': '1',
            'NO_COLOR': '1' if os.getenv('TERM') != 'xterm-256color' else '0'
        })
    
    # Platform-specific settings
    if context.platform == 'Windows':
        defaults['PATHEXT'] = os.getenv('PATHEXT', '.COM;.EXE;.BAT;.CMD;.VBS;.VBE;.JS;.JSE;.WSF;.WSH;.MSC')
    
    # Apply defaults
    for key, value in defaults.items():
        if not os.getenv(key):
            os.environ[key] = value
            logger.debug(f"Set environment variable: {key}={value}")

def log_environment_info(context):
    """Log comprehensive environment information for debugging."""
    logger.info(f"Project root: {context.project_root}")
    logger.info(f"Platform: {context.platform}")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Python executable: {sys.executable}")
    
    if context.is_jenkins:
        logger.info(f"Jenkins workspace: {os.getenv('WORKSPACE', 'Not set')}")
        logger.info(f"Build number: {os.getenv('BUILD_NUMBER', 'Not set')}")
        logger.info(f"Job name: {os.getenv('JOB_NAME', 'Not set')}")
    
    # Log key paths
    logger.debug(f"Python path: {sys.path[:3]}...")  # First 3 entries
    logger.debug(f"Current working directory: {os.getcwd()}")

def before_feature(context, feature):
    """Execute before each feature."""
    logger.info(f"Starting feature: {feature.name}")
    
    context.feature_name = feature.name
    context.feature_tags = [tag for tag in feature.tags]
    
    # Create feature-specific output directory
    try:
        safe_feature_name = "".join(c for c in feature.name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_feature_name = safe_feature_name.replace(' ', '_')
        
        feature_output_dir = context.output_dir / "features" / safe_feature_name
        feature_output_dir.mkdir(parents=True, exist_ok=True)
        context.feature_output_dir = feature_output_dir
        
        # CI-specific feature reporting
        if context.is_jenkins and hasattr(context, 'jenkins_output_dir'):
            jenkins_feature_dir = context.jenkins_output_dir / safe_feature_name
            jenkins_feature_dir.mkdir(parents=True, exist_ok=True)
            context.jenkins_feature_dir = jenkins_feature_dir
            
    except Exception as e:
        logger.warning(f"Could not create feature output directory: {e}")

def before_scenario(context, scenario):
    """Execute before each scenario."""
    logger.info(f"Starting scenario: {scenario.name}")
    
    context.scenario_name = scenario.name
    context.scenario_tags = [tag for tag in scenario.tags]
    context.scenario_data = {}
    context.test_results = {}
    
    # Setup connections based on tags
    setup_connections_for_tags(context, scenario.tags)

def after_scenario(context, scenario):
    """Execute after each scenario."""
    if scenario.status == "failed":
        logger.error(f"Scenario failed: {scenario.name}")
        save_failure_data(context, scenario)
    else:
        logger.info(f"Scenario completed: {scenario.name}")
    
    # Always cleanup connections
    cleanup_connections(context)

def after_feature(context, feature):
    """Execute after each feature."""
    logger.info(f"Completed feature: {feature.name}")

def after_all(context):
    """Execute after all tests."""
    logger.info("Test execution completed")
    
    # Generate CI-friendly reports
    if context.is_ci:
        generate_ci_reports(context)
    
    logger.info(f"All test outputs available in: {context.output_dir}")

def setup_connections_for_tags(context, tags):
    """Setup connections based on scenario tags with error handling."""
    try:
        # Import connectors only when needed to avoid dependency issues
        if 'database' in tags:
            try:
                from db.database_connector import db_connector
                context.db_connector = db_connector
            except ImportError as e:
                logger.warning(f"Database connector not available: {e}")
        
        if 'mongodb' in tags:
            try:
                from db.mongodb_connector import mongodb_connector
                context.mongodb_connector = mongodb_connector
            except ImportError as e:
                logger.warning(f"MongoDB connector not available: {e}")
        
        if 'api' in tags:
            try:
                from web.api_client import api_client
                context.api_client = api_client
            except ImportError as e:
                logger.warning(f"API client not available: {e}")
        
        if 'mq' in tags:
            try:
                from mq.mq_producer import mq_producer
                context.mq_producer = mq_producer
            except ImportError as e:
                logger.warning(f"MQ producer not available: {e}")
        
        if any(tag in tags for tag in ['aws', 'sqs', 's3']):
            try:
                from aws.sqs_connector import sqs_connector
                from aws.s3_connector import s3_connector
                from aws.sql_integration import aws_sql_integration
                context.sqs_connector = sqs_connector
                context.s3_connector = s3_connector
                context.aws_sql_integration = aws_sql_integration
            except ImportError as e:
                logger.warning(f"AWS connectors not available: {e}")
                
    except Exception as e:
        logger.error(f"Error setting up connections for tags {tags}: {e}")

def cleanup_connections(context):
    """Cleanup connections with error handling."""
    cleanup_operations = [
        ('db_connector', lambda: context.db_connector.close_connections()),
        ('mongodb_connector', lambda: context.mongodb_connector.close_connections()),
        ('api_client', lambda: context.api_client.clear_history() if hasattr(context.api_client, 'clear_history') else None),
        ('mq_producer', lambda: context.mq_producer.disconnect() if hasattr(context.mq_producer, 'disconnect') else None)
    ]
    
    for attr_name, cleanup_func in cleanup_operations:
        if hasattr(context, attr_name):
            try:
                cleanup_func()
            except Exception as e:
                logger.warning(f"Error cleaning up {attr_name}: {e}")

def save_failure_data(context, scenario):
    """Save failure data with CI-friendly format."""
    try:
        if hasattr(context, 'output_dir'):
            timestamp = __import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S')
            failure_file = context.output_dir / f"failed_scenario_{timestamp}.txt"
            
            with open(failure_file, 'w', encoding='utf-8') as f:
                f.write(f"Failed Scenario: {scenario.name}\n")
                f.write(f"Feature: {getattr(context, 'feature_name', 'Unknown')}\n")
                f.write(f"Platform: {getattr(context, 'platform', 'Unknown')}\n")
                f.write(f"CI Environment: {getattr(context, 'is_ci', False)}\n")
                f.write("=" * 60 + "\n")
                
                if hasattr(scenario, 'exception') and scenario.exception:
                    f.write(f"Exception: {scenario.exception}\n")
                
                if hasattr(context, 'scenario_data'):
                    f.write("Scenario Data:\n")
                    for key, value in context.scenario_data.items():
                        f.write(f"  {key}: {value}\n")
            
            logger.info(f"Failure data saved to: {failure_file}")
            
    except Exception as e:
        logger.error(f"Error saving failure data: {e}")

def generate_ci_reports(context):
    """Generate CI-friendly reports."""
    try:
        if hasattr(context, 'output_dir'):
            report_file = context.output_dir / "ci_execution_summary.txt"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("CI Execution Summary\n")
                f.write("=" * 50 + "\n")
                f.write(f"Platform: {getattr(context, 'platform', 'Unknown')}\n")
                f.write(f"CI Environment: {getattr(context, 'is_ci', False)}\n")
                f.write(f"Jenkins: {getattr(context, 'is_jenkins', False)}\n")
                f.write(f"Project Root: {context.project_root}\n")
                f.write(f"Output Directory: {context.output_dir}\n")
                
                # Environment variables
                f.write("\nEnvironment Variables:\n")
                for key in ['BUILD_NUMBER', 'JOB_NAME', 'WORKSPACE', 'PYTHONPATH']:
                    value = os.getenv(key, 'Not set')
                    f.write(f"  {key}: {value}\n")
            
            logger.info(f"CI report saved to: {report_file}")
            
    except Exception as e:
        logger.warning(f"Could not generate CI reports: {e}")