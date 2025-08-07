"""
Standard Python logging utility (alternative to loguru).
"""
import logging
import os
import sys
from pathlib import Path
from datetime import datetime

def setup_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    """Set up a logger with file and console handlers."""
    logger = logging.getLogger(name)
    
    if logger.handlers:  # Avoid duplicate handlers
        return logger
    
    # Create logs directory
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    log_file = logs_dir / "application" / "app.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Set level
    logger.setLevel(getattr(logging, log_level.upper()))
    
    return logger

# Create default loggers
logger = setup_logger(__name__)
db_logger = setup_logger("database")
api_logger = setup_logger("api")
mq_logger = setup_logger("mq")
test_logger = setup_logger("test_execution")

# Export for imports
__all__ = [
    'logger', 'db_logger', 'api_logger', 'mq_logger', 'test_logger', 'setup_logger'
]