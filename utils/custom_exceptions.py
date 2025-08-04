"""Custom exceptions for the automation framework."""
from typing import Optional, Any, Dict


class AutomationFrameworkError(Exception):
    """Base exception for all framework errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}
        
    def __str__(self):
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class DataLoadError(AutomationFrameworkError):
    """Raised when data loading fails."""
    
    def __init__(self, message: str, source: Optional[Any] = None, **kwargs):
        details = {"source": str(source)[:200] if source else None}
        details.update(kwargs)
        super().__init__(message, details)


class DatabaseConnectionError(AutomationFrameworkError):
    """Raised when database connection fails."""
    
    def __init__(self, message: str, db_type: Optional[str] = None, 
                 environment: Optional[str] = None, **kwargs):
        details = {"db_type": db_type, "environment": environment}
        details.update(kwargs)
        super().__init__(message, details)


class QueryExecutionError(AutomationFrameworkError):
    """Raised when query execution fails."""
    
    def __init__(self, message: str, query: str, params: Optional[Any] = None, **kwargs):
        details = {
            "query": query[:200] + "..." if len(query) > 200 else query,
            "params": params
        }
        details.update(kwargs)
        super().__init__(message, details)


class ConfigurationError(AutomationFrameworkError):
    """Raised when configuration is invalid or missing."""
    
    def __init__(self, message: str, config_key: Optional[str] = None, 
                 config_file: Optional[str] = None, **kwargs):
        details = {"config_key": config_key, "config_file": config_file}
        details.update(kwargs)
        super().__init__(message, details)


class APIError(AutomationFrameworkError):
    """Raised when API operations fail."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 endpoint: Optional[str] = None, response: Optional[Dict] = None, **kwargs):
        details = {
            "status_code": status_code,
            "endpoint": endpoint,
            "response": response
        }
        details.update(kwargs)
        super().__init__(message, details)


class MQConnectionError(AutomationFrameworkError):
    """Raised when MQ connection fails."""
    
    def __init__(self, message: str, queue_manager: Optional[str] = None, 
                 queue: Optional[str] = None, **kwargs):
        details = {"queue_manager": queue_manager, "queue": queue}
        details.update(kwargs)
        super().__init__(message, details)


class DataValidationError(AutomationFrameworkError):
    """Raised when data validation fails."""
    
    def __init__(self, message: str, expected: Optional[Any] = None, 
                 actual: Optional[Any] = None, **kwargs):
        details = {"expected": expected, "actual": actual}
        details.update(kwargs)
        super().__init__(message, details)


class ComparisonError(AutomationFrameworkError):
    """Raised when data comparison fails."""
    
    def __init__(self, message: str, source_data: Optional[Any] = None, 
                 target_data: Optional[Any] = None, differences: Optional[Dict] = None, **kwargs):
        details = {
            "source_data_sample": str(source_data)[:200] if source_data else None,
            "target_data_sample": str(target_data)[:200] if target_data else None,
            "differences": differences
        }
        details.update(kwargs)
        super().__init__(message, details)


class QueryNotFoundError(AutomationFrameworkError):
    """Raised when a query file is not found."""
    
    def __init__(self, message: str, query_name: str, query_path: Optional[str] = None, **kwargs):
        details = {"query_name": query_name, "query_path": query_path}
        details.update(kwargs)
        super().__init__(message, details)


class TestDataError(AutomationFrameworkError):
    """Raised when test data is invalid or missing."""
    
    def __init__(self, message: str, data_file: Optional[str] = None, 
                 data_key: Optional[str] = None, **kwargs):
        details = {"data_file": data_file, "data_key": data_key}
        details.update(kwargs)
        super().__init__(message, details)


class TimeoutError(AutomationFrameworkError):
    """Raised when an operation times out."""
    
    def __init__(self, message: str, timeout_seconds: Optional[int] = None, 
                 operation: Optional[str] = None, **kwargs):
        details = {"timeout_seconds": timeout_seconds, "operation": operation}
        details.update(kwargs)
        super().__init__(message, details)
