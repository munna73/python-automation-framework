"""
Centralized logging utility using loguru for enhanced logging capabilities.
"""
import os
import sys
from pathlib import Path
from loguru import logger  # Import from loguru, not from logger


class LoggerSetup:
    """Setup and manage application logging."""
    
    def __init__(self):
        """Initialize logger setup."""
        self.logs_dir = Path(__file__).parent.parent / "logs"
        self.setup_log_directories()
        self.setup_loggers()
    
    def setup_log_directories(self):
        """Create log directories if they don't exist."""
        log_dirs = [
            self.logs_dir / "application",
            self.logs_dir / "database",
            self.logs_dir / "api",
            self.logs_dir / "mq",
            self.logs_dir / "test_execution"
        ]
        
        for log_dir in log_dirs:
            log_dir.mkdir(parents=True, exist_ok=True)
    
    def setup_loggers(self):
        """Configure loguru loggers for different components."""
        # Remove default handler
        logger.remove()
        
        # Console handler with color
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level=os.getenv("LOG_LEVEL", "INFO"),
            colorize=True
        )
        
        # Application log file
        logger.add(
            self.logs_dir / "application" / "app.log",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="INFO",
            rotation="10 MB",
            retention="30 days",
            compression="zip"
        )
        
        # Error log file
        logger.add(
            self.logs_dir / "application" / "error.log",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="ERROR",
            rotation="10 MB",
            retention="30 days",
            compression="zip"
        )
    
    def get_component_logger(self, component: str):
        """Get a logger for a specific component."""
        component_logger = logger.bind(component=component)
        
        # Add component-specific file handler
        log_file = self.logs_dir / component / f"{component}.log"
        logger.add(
            log_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[component]} | {name}:{function}:{line} - {message}",
            level="DEBUG",
            rotation="5 MB",
            retention="15 days",
            compression="zip",
            filter=lambda record: record["extra"].get("component") == component
        )
        
        return component_logger


# Global logger setup
logger_setup = LoggerSetup()

# Component-specific loggers
db_logger = logger_setup.get_component_logger("database")
api_logger = logger_setup.get_component_logger("api")
mq_logger = logger_setup.get_component_logger("mq")
test_logger = logger_setup.get_component_logger("test_execution")


def log_function_call(func_name: str, params: dict = None, component: str = "application"):
    """
    Log function call with parameters.
    
    Args:
        func_name: Name of the function being called
        params: Function parameters (optional)
        component: Component name for logging
    """
    if params:
        logger.bind(component=component).info(f"Calling {func_name} with params: {params}")
    else:
        logger.bind(component=component).info(f"Calling {func_name}")


def log_test_result(test_name: str, status: str, duration: float = None, details: str = None):
    """
    Log test execution results.
    
    Args:
        test_name: Name of the test
        status: Test status (PASS/FAIL/SKIP)
        duration: Test execution duration in seconds
        details: Additional test details
    """
    message = f"Test: {test_name} | Status: {status}"
    if duration:
        message += f" | Duration: {duration:.2f}s"
    if details:
        message += f" | Details: {details}"
    
    if status == "PASS":
        test_logger.success(message)
    elif status == "FAIL":
        test_logger.error(message)
    else:
        test_logger.info(message)


def log_database_operation(operation: str, env: str, db_type: str, query: str = None, 
                          duration: float = None, row_count: int = None):
    """
    Log database operations.
    
    Args:
        operation: Database operation type
        env: Environment name
        db_type: Database type
        query: SQL query (optional)
        duration: Operation duration
        row_count: Number of rows affected/returned
    """
    message = f"DB Operation: {operation} | Env: {env} | Type: {db_type}"
    if duration:
        message += f" | Duration: {duration:.3f}s"
    if row_count is not None:
        message += f" | Rows: {row_count}"
    if query:
        message += f" | Query: {query[:100]}{'...' if len(query) > 100 else ''}"
    
    db_logger.info(message)


def log_api_request(method: str, url: str, status_code: int = None, 
                   duration: float = None, response_size: int = None):
    """
    Log API requests.
    
    Args:
        method: HTTP method
        url: Request URL
        status_code: Response status code
        duration: Request duration
        response_size: Response size in bytes
    """
    message = f"API Request: {method} {url}"
    if status_code:
        message += f" | Status: {status_code}"
    if duration:
        message += f" | Duration: {duration:.3f}s"
    if response_size:
        message += f" | Size: {response_size} bytes"
    
    if status_code and 200 <= status_code < 300:
        api_logger.success(message)
    elif status_code and status_code >= 400:
        api_logger.error(message)
    else:
        api_logger.info(message)