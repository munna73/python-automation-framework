"""Base database connector with proper typing and error handling."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union
import logging
from contextlib import contextmanager
from datetime import datetime
import time

from utils.custom_exceptions import DatabaseConnectionError, QueryExecutionError


class BaseConnector(ABC):
    """Abstract base class for database connectors."""
    
    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        """
        Initialize the base connector.
        
        Args:
            config: Database configuration dictionary
            logger: Optional logger instance
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.connection = None
        self._connection_time: Optional[datetime] = None
        self._query_count = 0
        
    @abstractmethod
    def connect(self) -> None:
        """Establish database connection."""
        pass
        
    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection."""
        pass
        
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            List of dictionaries containing query results
        """
        pass
        
    @abstractmethod
    def execute_many(self, query: str, data: List[Tuple]) -> int:
        """
        Execute bulk insert/update operations.
        
        Args:
            query: SQL query string
            data: List of tuples containing data
            
        Returns:
            Number of affected rows
        """
        pass
    
    @abstractmethod
    def execute_procedure(self, procedure_name: str, params: Optional[List[Any]] = None) -> Any:
        """
        Execute a stored procedure.
        
        Args:
            procedure_name: Name of the stored procedure
            params: Optional list of parameters
            
        Returns:
            Procedure results
        """
        pass
        
    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        try:
            self.begin_transaction()
            yield self
            self.commit_transaction()
        except Exception as e:
            self.rollback_transaction()
            self.logger.error(f"Transaction failed: {str(e)}")
            raise
            
    def begin_transaction(self) -> None:
        """Begin a database transaction."""
        if self.connection:
            self.connection.begin()
            self.logger.debug("Transaction started")
    
    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if self.connection:
            self.connection.commit()
            self.logger.debug("Transaction committed")
    
    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if self.connection:
            self.connection.rollback()
            self.logger.debug("Transaction rolled back")
        
    def validate_connection(self) -> bool:
        """
        Validate database connection.
        
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            if not self.connection:
                return False
            # Implement specific validation logic in subclasses
            return True
        except Exception as e:
            self.logger.error(f"Connection validation failed: {str(e)}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the current connection."""
        return {
            'connected': self.validate_connection(),
            'connection_time': self._connection_time.isoformat() if self._connection_time else None,
            'query_count': self._query_count,
            'database': self.config.get('database'),
            'host': self.config.get('host'),
            'port': self.config.get('port'),
            'user': self.config.get('username')
        }
    
    def execute_query_with_timeout(self, query: str, timeout_seconds: int, 
                                 params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute query with timeout.
        
        Args:
            query: SQL query string
            timeout_seconds: Timeout in seconds
            params: Optional query parameters
            
        Returns:
            Query results
        """
        # This is a basic implementation - subclasses should override with 
        # database-specific timeout mechanisms
        start_time = time.time()
        
        try:
            results = self.execute_query(query, params)
            
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout_seconds:
                raise QueryExecutionError(
                    f"Query exceeded timeout of {timeout_seconds} seconds",
                    query=query,
                    elapsed_time=elapsed_time
                )
            
            return results
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            self.logger.error(f"Query failed after {elapsed_time:.2f} seconds: {str(e)}")
            raise
    
    def execute_script(self, script_path: str, delimiter: str = ';') -> List[Dict[str, Any]]:
        """
        Execute SQL script from file.
        
        Args:
            script_path: Path to SQL script file
            delimiter: Statement delimiter
            
        Returns:
            Results from the last statement
        """
        try:
            with open(script_path, 'r') as f:
                script_content = f.read()
            
            # Split script into individual statements
            statements = [stmt.strip() for stmt in script_content.split(delimiter) 
                         if stmt.strip()]
            
            results = []
            for statement in statements:
                if statement:
                    self.logger.debug(f"Executing statement: {statement[:50]}...")
                    results = self.execute_query(statement)
            
            return results
            
        except Exception as e:
            raise QueryExecutionError(
                f"Failed to execute script: {str(e)}",
                query=script_path
            )
    
    def get_table_info(self, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get table information (columns, types, etc.).
        
        Args:
            table_name: Name of the table
            schema: Optional schema name
            
        Returns:
            Table information
        """
        # This should be overridden in subclasses with database-specific implementation
        raise NotImplementedError("Subclasses must implement get_table_info")
    
    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Check if a table exists.
        
        Args:
            table_name: Name of the table
            schema: Optional schema name
            
        Returns:
            True if table exists, False otherwise
        """
        # This should be overridden in subclasses with database-specific implementation
        raise NotImplementedError("Subclasses must implement table_exists")
    
    def _log_query_execution(self, query: str, execution_time: float, row_count: int) -> None:
        """Log query execution details."""
        self._query_count += 1
        self.logger.info(
            f"Query executed | Time: {execution_time:.3f}s | Rows: {row_count} | "
            f"Query: {query[:100]}{'...' if len(query) > 100 else ''}"
        )
    
    def __enter__(self):
        """Context manager entry."""
        if not self.validate_connection():
            self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        # Don't disconnect on exit - let the connection pool manage this
        pass
    
    def __repr__(self):
        """String representation of the connector."""
        return (f"{self.__class__.__name__}("
                f"host={self.config.get('host')}, "
                f"database={self.config.get('database')}, "
                f"connected={self.validate_connection()})")