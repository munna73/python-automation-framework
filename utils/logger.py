"""
Enhanced Python logging utility with improved features and best practices.
"""
import logging
import logging.handlers
import os
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, Union
from contextlib import contextmanager
import threading
import traceback


class ColoredFormatter(logging.Formatter):
    """Colored formatter for console output."""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def format(self, record):
        """Format log record with colors."""
        if hasattr(record, 'levelname'):
            color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
            record.levelname = f"{color}{record.levelname}{self.COLORS['RESET']}"
        return super().format(record)


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record):
        """Format log record as JSON."""
        log_obj = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'message': record.getMessage(),
            'thread_id': record.thread,
            'thread_name': record.threadName,
            'process_id': record.process
        }
        
        # Add exception info if present
        if record.exc_info:
            log_obj['exception'] = {
                'type': record.exc_info[0].__name__, # type: ignore
                'message': str(record.exc_info[1]),
                'traceback': traceback.format_exception(*record.exc_info)
            }
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                          'filename', 'module', 'exc_info', 'exc_text', 'stack_info',
                          'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
                          'thread', 'threadName', 'processName', 'process', 'message']:
                log_obj[key] = value
        
        return json.dumps(log_obj, default=str, ensure_ascii=False)


class EnhancedLogger:
    """Enhanced logger with additional features."""
    
    def __init__(self):
        self._loggers: Dict[str, logging.Logger] = {}
        self._lock = threading.Lock()
        self._config = self._load_default_config()
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default logging configuration from config.ini or environment variables."""
        # Try to load from config.ini first, fall back to environment variables
        config = {}
        
        try:
            # Try direct INI parsing to avoid ConfigLoader validation issues
            import configparser
            config_path = Path('config/config.ini')
            if config_path.exists():
                parser = configparser.ConfigParser()
                parser.read(config_path)
                
                # Get log_level from DEFAULT section first
                if 'DEFAULT' in parser and 'log_level' in parser['DEFAULT']:
                    config['log_level'] = parser['DEFAULT']['log_level']
                else:
                    # Look for log_level in any section
                    for section_name in parser.sections():
                        if 'log_level' in parser[section_name]:
                            config['log_level'] = parser[section_name]['log_level']
                            break
                    else:
                        config['log_level'] = 'INFO'  # Default fallback
            else:
                config['log_level'] = 'INFO'  # Default if no config file
                    
        except Exception as e:
            # Fallback to default if config.ini loading fails
            print(f"Warning: Could not load log_level from config.ini: {e}")
            config['log_level'] = 'INFO'
        
        # Override with environment variable if set
        config['log_level'] = os.getenv('LOG_LEVEL', config.get('log_level', 'INFO'))
        
        # Set other configuration from environment variables with defaults
        config.update({
            'log_format': os.getenv('LOG_FORMAT', 'standard'),  # standard, json, colored
            'max_file_size': int(os.getenv('LOG_MAX_FILE_SIZE', '10485760')),  # 10MB
            'backup_count': int(os.getenv('LOG_BACKUP_COUNT', '5')),
            'log_to_console': os.getenv('LOG_TO_CONSOLE', 'true').lower() == 'true',
            'log_to_file': os.getenv('LOG_TO_FILE', 'true').lower() == 'true',
            'logs_base_dir': os.getenv('LOGS_BASE_DIR', 'logs'),
            'separate_error_log': os.getenv('SEPARATE_ERROR_LOG', 'false').lower() == 'true',  # Changed to false by default
            'single_log_file': os.getenv('SINGLE_LOG_FILE', 'true').lower() == 'true'  # Use single log file
        })
        
        return config
    
    def setup_logger(
        self, 
        name: str, 
        log_level: Optional[str] = None,
        log_to_file: Optional[bool] = None,
        log_to_console: Optional[bool] = None,
        custom_format: Optional[str] = None,
        extra_handlers: Optional[list] = None
    ) -> logging.Logger:
        """
        Set up an enhanced logger with configurable options.
        
        Args:
            name: Logger name
            log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_to_file: Whether to log to file
            log_to_console: Whether to log to console
            custom_format: Custom log format
            extra_handlers: Additional handlers to add
        
        Returns:
            Configured logger instance
        """
        with self._lock:
            if name in self._loggers:
                return self._loggers[name]
            
            logger = logging.getLogger(name)
            
            # Clear existing handlers to avoid duplicates
            logger.handlers.clear()
            
            # Set log level
            level = log_level or self._config['log_level']
            logger.setLevel(getattr(logging, level.upper()))
            
            # Determine format type
            format_type = self._config['log_format']
            if custom_format:
                formatter = logging.Formatter(custom_format)
            elif format_type == 'json':
                formatter = JSONFormatter()
            elif format_type == 'colored':
                formatter = ColoredFormatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
                )
            else:  # standard
                formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
                )
            
            # Console handler
            if log_to_console if log_to_console is not None else self._config['log_to_console']:
                console_handler = logging.StreamHandler(sys.stdout)
                if format_type == 'colored' and sys.stdout.isatty():
                    console_handler.setFormatter(ColoredFormatter(
                        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
                    ))
                else:
                    console_handler.setFormatter(formatter)
                logger.addHandler(console_handler)
            
            # File handlers
            if log_to_file if log_to_file is not None else self._config['log_to_file']:
                self._add_file_handlers(logger, name, formatter)
            
            # Add extra handlers if provided
            if extra_handlers:
                for handler in extra_handlers:
                    logger.addHandler(handler)
            
            # Prevent propagation to root logger
            logger.propagate = False
            
            self._loggers[name] = logger
            return logger
    
    def _add_file_handlers(self, logger: logging.Logger, name: str, formatter: logging.Formatter):
        """Add file handlers to logger."""
        # Create simple logs directory
        logs_dir = Path(self._config['logs_base_dir'])
        logs_dir.mkdir(parents=True, exist_ok=True)
        
        if self._config.get('single_log_file', True):
            # Single log file for all loggers - much simpler approach
            log_file = logs_dir / "test_automation.log"
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=self._config['max_file_size'],
                backupCount=self._config['backup_count'],
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        else:
            # Legacy approach: separate files per logger  
            app_logs_dir = logs_dir / "application"
            app_logs_dir.mkdir(parents=True, exist_ok=True)
            
            # Main log file with rotation
            log_file = app_logs_dir / f"{name}.log"
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=self._config['max_file_size'],
                backupCount=self._config['backup_count'],
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            # Separate error log file if configured
            if self._config['separate_error_log']:
                error_log_file = app_logs_dir / f"{name}_errors.log"
                error_handler = logging.handlers.RotatingFileHandler(
                    error_log_file,
                    maxBytes=self._config['max_file_size'],
                    backupCount=self._config['backup_count'],
                    encoding='utf-8'
                )
                error_handler.setLevel(logging.ERROR)
                error_handler.setFormatter(formatter)
                logger.addHandler(error_handler)
    
    def get_logger(self, name: str) -> logging.Logger:
        """Get existing logger or create new one with default settings."""
        if name in self._loggers:
            return self._loggers[name]
        return self.setup_logger(name)
    
    def configure_from_dict(self, config: Dict[str, Any]):
        """Configure logging from dictionary."""
        self._config.update(config)
        # Update existing loggers with new configuration
        self._update_existing_loggers()
    
    def _update_existing_loggers(self):
        """Update all existing loggers with current configuration."""
        for name, logger in self._loggers.items():
            level = self._config.get('log_level', 'INFO')
            logger.setLevel(getattr(logging, level.upper()))
    
    def reload_config_from_ini(self):
        """Reload configuration from config.ini file."""
        new_config = self._load_default_config()
        self._config.update(new_config)
        self._update_existing_loggers()
        return self._config
    
    def configure_from_file(self, config_file: Union[str, Path]):
        """Configure logging from JSON config file."""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            self.configure_from_dict(config)
        except Exception as e:
            print(f"Warning: Could not load logging config from {config_file}: {e}")
    
    def add_custom_handler(self, logger_name: str, handler: logging.Handler):
        """Add custom handler to specific logger."""
        if logger_name in self._loggers:
            self._loggers[logger_name].addHandler(handler)
    
    def set_level_for_all(self, level: str):
        """Set log level for all loggers."""
        log_level = getattr(logging, level.upper())
        for logger in self._loggers.values():
            logger.setLevel(log_level)
    
    @contextmanager
    def log_context(self, logger_name: str, **context):
        """Context manager for adding context to log messages."""
        logger = self.get_logger(logger_name)
        
        # Create a custom adapter that adds context
        class ContextAdapter(logging.LoggerAdapter):
            def process(self, msg, kwargs):
                return f"[{', '.join(f'{k}={v}' for k, v in self.extra.items())}] {msg}", kwargs
        
        adapter = ContextAdapter(logger, context)
        try:
            yield adapter
        finally:
            pass
    
    def log_performance(self, logger_name: str, operation: str, duration: float, **extra):
        """Log performance metrics."""
        logger = self.get_logger(logger_name)
        logger.info(
            f"Performance: {operation} completed in {duration:.3f}s",
            extra={'operation': operation, 'duration': duration, **extra}
        )
    
    def log_exception(self, logger_name: str, message: str = "Exception occurred", **extra):
        """Log exception with full traceback."""
        logger = self.get_logger(logger_name)
        logger.exception(message, extra=extra)
    
    def cleanup(self):
        """Cleanup all loggers and handlers."""
        for logger in self._loggers.values():
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)
        self._loggers.clear()


# Global enhanced logger instance
_enhanced_logger = EnhancedLogger()

# Load configuration from environment or file
config_file = os.getenv('LOG_CONFIG_FILE')
if config_file and Path(config_file).exists():
    _enhanced_logger.configure_from_file(config_file)

# Ensure logging level is INFO or DEBUG on startup
try:
    ensure_log_level_info_or_debug()
except Exception:
    # If there's an issue with config loading, ensure we have at least INFO level
    _enhanced_logger.configure_from_dict({'log_level': 'INFO'})


def setup_logger(
    name: str, 
    log_level: str = "INFO",
    log_to_file: bool = True,
    log_to_console: bool = True,
    custom_format: Optional[str] = None
) -> logging.Logger:
    """
    Set up a logger with enhanced features.
    
    Args:
        name: Logger name
        log_level: Log level
        log_to_file: Whether to log to file
        log_to_console: Whether to log to console
        custom_format: Custom format string
    
    Returns:
        Configured logger
    """
    return _enhanced_logger.setup_logger(
        name=name,
        log_level=log_level,
        log_to_file=log_to_file,
        log_to_console=log_to_console,
        custom_format=custom_format
    )


def get_logger(name: str) -> logging.Logger:
    """Get logger by name."""
    return _enhanced_logger.get_logger(name)


def configure_logging(config: Dict[str, Any]):
    """Configure logging from dictionary."""
    _enhanced_logger.configure_from_dict(config)


def configure_logging_from_file(config_file: Union[str, Path]):
    """Configure logging from file."""
    _enhanced_logger.configure_from_file(config_file)


def reload_config_from_ini():
    """Reload logging configuration from config.ini file."""
    return _enhanced_logger.reload_config_from_ini()


def set_log_level(level: str):
    """Set log level for all loggers and update configuration."""
    level = level.upper()
    if level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        raise ValueError(f"Invalid log level: {level}")
    
    _enhanced_logger.configure_from_dict({'log_level': level})
    return level


def get_current_log_level():
    """Get current log level from configuration."""
    return _enhanced_logger._config.get('log_level', 'INFO')


def ensure_log_level_info_or_debug():
    """Ensure log level is set to INFO or DEBUG (not higher)."""
    current_level = get_current_log_level().upper()
    
    # If current level is WARNING, ERROR, or CRITICAL, set to INFO
    if current_level in ['WARNING', 'ERROR', 'CRITICAL']:
        print(f"Current log level {current_level} is too high. Setting to INFO.")
        set_log_level('INFO')
        return 'INFO'
    
    # If already INFO or DEBUG, keep as is
    return current_level


def initialize_test_logging():
    """Initialize logging for test execution with proper configuration."""
    # Ensure we have appropriate log level
    ensure_log_level_info_or_debug()
    
    # Reload config from ini file to get latest settings
    config = reload_config_from_ini()
    
    logger.info("Test logging initialized")
    logger.info(f"Active log level: {get_current_log_level()}")
    
    return config


def log_test_step(step_name: str, **context):
    """Log a test step with context information."""
    context_str = ', '.join(f'{k}={v}' for k, v in context.items()) if context else ''
    message = f"Test Step: {step_name}"
    if context_str:
        message += f" | Context: {context_str}"
    
    test_logger.info(message)


def log_test_result(test_name: str, status: str, **details):
    """Log test result with details."""
    details_str = ', '.join(f'{k}={v}' for k, v in details.items()) if details else ''
    message = f"Test Result: {test_name} - {status.upper()}"
    if details_str:
        message += f" | Details: {details_str}"
    
    if status.upper() in ['PASSED', 'SUCCESS']:
        test_logger.info(message)
    elif status.upper() in ['FAILED', 'ERROR']:
        test_logger.error(message)
    else:
        test_logger.warning(message)


@contextmanager
def log_context(logger_name: str, **context):
    """Context manager for adding context to logs."""
    with _enhanced_logger.log_context(logger_name, **context) as adapter:
        yield adapter


def log_performance(logger_name: str, operation: str, duration: float, **extra):
    """Log performance metrics."""
    _enhanced_logger.log_performance(logger_name, operation, duration, **extra)


def log_exception(logger_name: str, message: str = "Exception occurred", **extra):
    """Log exception with traceback."""
    _enhanced_logger.log_exception(logger_name, message, **extra)


# Performance monitoring decorator
def log_execution_time(logger_name: str, operation_name: Optional[str] = None):
    """Decorator to log function execution time."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            import time
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                op_name = operation_name or f"{func.__module__}.{func.__name__}"
                log_performance(logger_name, op_name, duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                op_name = operation_name or f"{func.__module__}.{func.__name__}"
                logger = get_logger(logger_name)
                logger.error(f"Operation {op_name} failed after {duration:.3f}s: {str(e)}")
                raise
        return wrapper
    return decorator


# Create default loggers with enhanced features
logger = setup_logger(__name__)
db_logger = setup_logger("database")
api_logger = setup_logger("api") 
mq_logger = setup_logger("mq")
test_logger = setup_logger("test_execution")
performance_logger = setup_logger("performance")
security_logger = setup_logger("security")
audit_logger = setup_logger("audit")

# Create specialized loggers for different components
comparison_logger = setup_logger("data_comparison")
validation_logger = setup_logger("data_validation")
export_logger = setup_logger("data_export")
connection_logger = setup_logger("database_connection")


# Export for imports
__all__ = [
    # Main functions
    'setup_logger', 'get_logger', 'configure_logging', 'configure_logging_from_file',
    
    # Configuration management
    'reload_config_from_ini', 'set_log_level', 'get_current_log_level', 'ensure_log_level_info_or_debug',
    'initialize_test_logging', 'log_test_step', 'log_test_result',
    
    # Context and utilities
    'log_context', 'log_performance', 'log_exception', 'log_execution_time',
    
    # Default loggers
    'logger', 'db_logger', 'api_logger', 'mq_logger', 'test_logger',
    
    # Enhanced loggers
    'performance_logger', 'security_logger', 'audit_logger',
    'comparison_logger', 'validation_logger', 'export_logger', 'connection_logger',
    
    # Classes
    'EnhancedLogger', 'ColoredFormatter', 'JSONFormatter'
]


# Example usage and configuration
if __name__ == "__main__":
    # Example configuration
    example_config = {
        'log_level': 'DEBUG',
        'log_format': 'colored',
        'max_file_size': 5242880,  # 5MB
        'backup_count': 3,
        'log_to_console': True,
        'log_to_file': True,
        'separate_error_log': True
    }
    
    configure_logging(example_config)
    
    # Test logging
    test_log = get_logger("test")
    test_log.info("This is an info message")
    test_log.warning("This is a warning message")
    test_log.error("This is an error message")
    
    # Test performance logging
    import time
    start = time.time()
    time.sleep(0.1)
    log_performance("test", "sleep_operation", time.time() - start)
    
    # Test context logging
    with log_context("test", user_id=123, session_id="abc123") as ctx_logger:
        ctx_logger.info("User performed action")
    
    # Test decorator
    @log_execution_time("test", "calculation")
    def slow_calculation():
        time.sleep(0.05)
        return 42
    
    result = slow_calculation()
    print(f"Result: {result}")


    #usage 
    # Basic usage (drop-in replacement)
# from utils.logger import logger, db_logger
# logger.info("Application started")
# db_logger.error("Database connection failed")

# # Enhanced features
# from utils.logger import log_context, log_execution_time, log_performance

# # Context logging
# with log_context("api", user_id=123, request_id="abc") as ctx_logger:
#     ctx_logger.info("Processing request")

# # Performance monitoring
# @log_execution_time("database", "complex_query")
# def execute_complex_query():
#     # Your code here
#     pass

# # Manual performance logging
# import time
# start = time.time()
# # ... operation ...
# log_performance("database", "data_export", time.time() - start)

#config section
# Environment variables
# export LOG_LEVEL=DEBUG
# export LOG_FORMAT=colored  # or json, standard
# export LOG_MAX_FILE_SIZE=10485760  # 10MB
# export LOG_BACKUP_COUNT=5
# export SEPARATE_ERROR_LOG=true