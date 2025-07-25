"""Enhanced database connector with improved error handling and connection pooling."""
import cx_Oracle
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any, Optional, Union, Tuple
import logging
from contextlib import contextmanager
from datetime import datetime
import time
import json

from db.base_connector import BaseConnector
from utils.custom_exceptions import DatabaseConnectionError, QueryExecutionError
from utils.logger import logger


class OracleConnector(BaseConnector):
    """Oracle database connector implementation."""
    
    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        self.connection_pool = None
    
    def connect(self) -> None:
        """Establish Oracle database connection with connection pooling."""
        try:
            # Create connection pool for better performance
            self.connection_pool = cx_Oracle.SessionPool(
                user=self.config['username'],
                password=self.config['password'],
                dsn=f"{self.config['host']}:{self.config['port']}/{self.config['database']}",
                min=2,
                max=self.config.get('pool_size', 5),
                increment=1,
                encoding="UTF-8"
            )
            
            # Test connection
            with self.connection_pool.acquire() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.close()
            
            self._connection_time = datetime.now()
            self.logger.info("Oracle connection pool established successfully")
            
        except cx_Oracle.Error as e:
            error_msg = f"Oracle connection failed: {str(e)}"
            self.logger.error(error_msg)
            raise DatabaseConnectionError(error_msg, db_type="ORACLE", 
                                        environment=self.config.get('environment'))
    
    def disconnect(self) -> None:
        """Close Oracle connection pool."""
        if self.connection_pool:
            try:
                self.connection_pool.close()
                self.logger.info("Oracle connection pool closed")
            except Exception as e:
                self.logger.warning(f"Error closing Oracle connection pool: {str(e)}")
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute Oracle query and return results as list of dictionaries."""
        if not self.connection_pool:
            raise DatabaseConnectionError("No active Oracle connection")
        
        results = []
        start_time = time.time()
        
        try:
            with self.connection_pool.acquire() as conn:
                cursor = conn.cursor()
                
                # Enable array fetching for better performance
                cursor.arraysize = 1000
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Get column names
                columns = [desc[0].lower() for desc in cursor.description] if cursor.description else []
                
                # Fetch all results
                for row in cursor:
                    results.append(dict(zip(columns, self._convert_oracle_types(row))))
                
                cursor.close()
            
            execution_time = time.time() - start_time
            self._log_query_execution(query, execution_time, len(results))
            return results
            
        except cx_Oracle.Error as e:
            error_msg = f"Oracle query execution failed: {str(e)}"
            self.logger.error(error_msg)
            raise QueryExecutionError(error_msg, query=query, params=params)
    
    def execute_many(self, query: str, data: List[Tuple]) -> int:
        """Execute bulk operations in Oracle."""
        if not self.connection_pool:
            raise DatabaseConnectionError("No active Oracle connection")
        
        try:
            with self.connection_pool.acquire() as conn:
                cursor = conn.cursor()
                cursor.executemany(query, data)
                conn.commit()
                
                affected_rows = cursor.rowcount
                cursor.close()
                
            self.logger.info(f"Bulk operation executed successfully, affected {affected_rows} rows")
            return affected_rows
            
        except cx_Oracle.Error as e:
            error_msg = f"Oracle bulk operation failed: {str(e)}"
            self.logger.error(error_msg)
            raise QueryExecutionError(error_msg, query=query)
    
    def execute_procedure(self, procedure_name: str, params: Optional[List[Any]] = None) -> Any:
        """Execute Oracle stored procedure."""
        if not self.connection_pool:
            raise DatabaseConnectionError("No active Oracle connection")
        
        try:
            with self.connection_pool.acquire() as conn:
                cursor = conn.cursor()
                
                if params:
                    cursor.callproc(procedure_name, params)
                else:
                    cursor.callproc(procedure_name)
                
                # Get any output parameters
                result = cursor.var(cx_Oracle.STRING)
                cursor.close()
                
            self.logger.info(f"Procedure {procedure_name} executed successfully")
            return result.getvalue() if result else None
            
        except cx_Oracle.Error as e:
            error_msg = f"Oracle procedure execution failed: {str(e)}"
            self.logger.error(error_msg)
            raise QueryExecutionError(error_msg, query=f"CALL {procedure_name}")
    
    def _convert_oracle_types(self, row: Tuple) -> List[Any]:
        """Convert Oracle-specific types to Python types."""
        converted = []
        for value in row:
            if isinstance(value, cx_Oracle.LOB):
                converted.append(value.read())
            elif isinstance(value, cx_Oracle.Timestamp):
                converted.append(value.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                converted.append(value)
        return converted
    
    def validate_connection(self) -> bool:
        """Validate Oracle connection."""
        if not self.connection_pool:
            return False
        
        try:
            with self.connection_pool.acquire() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.close()
            return True
        except Exception:
            return False
    
    def get_table_info(self, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get Oracle table information."""
        query = """
        SELECT 
            column_name,
            data_type,
            data_length,
            nullable,
            data_default
        FROM user_tab_columns
        WHERE table_name = UPPER(:table_name)
        ORDER BY column_id
        """
        
        return self.execute_query(query, {'table_name': table_name})
    
    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """Check if table exists in Oracle."""
        query = "SELECT COUNT(*) as cnt FROM user_tables WHERE table_name = UPPER(:table_name)"
        result = self.execute_query(query, {'table_name': table_name})
        return result[0]['cnt'] > 0 if result else False


class PostgresConnector(BaseConnector):
    """PostgreSQL database connector implementation."""
    
    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        self.connection_params = None
    
    def connect(self) -> None:
        """Establish PostgreSQL database connection."""
        try:
            self.connection_params = {
                'host': self.config['host'],
                'port': self.config['port'],
                'database': self.config['database'],
                'user': self.config['username'],
                'password': self.config['password'],
                'cursor_factory': RealDictCursor
            }
            
            # Test connection
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
            
            self._connection_time = datetime.now()
            self.logger.info("PostgreSQL connection parameters configured successfully")
            
        except psycopg2.Error as e:
            error_msg = f"PostgreSQL connection failed: {str(e)}"
            self.logger.error(error_msg)
            raise DatabaseConnectionError(error_msg, db_type="POSTGRES",
                                        environment=self.config.get('environment'))
    
    def disconnect(self) -> None:
        """PostgreSQL uses connection pooling at application level."""
        self.logger.info("PostgreSQL connector cleaned up")
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute PostgreSQL query and return results."""
        if not self.connection_params:
            raise DatabaseConnectionError("PostgreSQL connection not configured")
        
        results = []
        start_time = time.time()
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    if params:
                        # Convert dict params to psycopg2 format
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    
                    if cursor.description:
                        results = cursor.fetchall()
            
            execution_time = time.time() - start_time
            self._log_query_execution(query, execution_time, len(results))
            return results
            
        except psycopg2.Error as e:
            error_msg = f"PostgreSQL query execution failed: {str(e)}"
            self.logger.error(error_msg)
            raise QueryExecutionError(error_msg, query=query, params=params)
    
    def execute_many(self, query: str, data: List[Tuple]) -> int:
        """Execute bulk operations in PostgreSQL."""
        if not self.connection_params:
            raise DatabaseConnectionError("PostgreSQL connection not configured")
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, data)
                    affected_rows = cursor.rowcount
            
            self.logger.info(f"Bulk operation executed successfully, affected {affected_rows} rows")
            return affected_rows
            
        except psycopg2.Error as e:
            error_msg = f"PostgreSQL bulk operation failed: {str(e)}"
            self.logger.error(error_msg)
            raise QueryExecutionError(error_msg, query=query)
    
    def execute_procedure(self, procedure_name: str, params: Optional[List[Any]] = None) -> Any:
        """Execute PostgreSQL stored procedure."""
        if not self.connection_params:
            raise DatabaseConnectionError("PostgreSQL connection not configured")
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    if params:
                        cursor.callproc(procedure_name, params)
                    else:
                        cursor.callproc(procedure_name)
                    
                    result = cursor.fetchone() if cursor.description else None
            
            self.logger.info(f"Procedure {procedure_name} executed successfully")
            return result
            
        except psycopg2.Error as e:
            error_msg = f"PostgreSQL procedure execution failed: {str(e)}"
            self.logger.error(error_msg)
            raise QueryExecutionError(error_msg, query=f"CALL {procedure_name}")
    
    def validate_connection(self) -> bool:
        """Validate PostgreSQL connection."""
        if not self.connection_params:
            return False
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
            return True
        except Exception:
            return False
    
    def get_table_info(self, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get PostgreSQL table information."""
        query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length as data_length,
            is_nullable as nullable,
            column_default as data_default
        FROM information_schema.columns
        WHERE table_name = %s
        AND table_schema = %s
        ORDER BY ordinal_position
        """
        
        schema = schema or 'public'
        return self.execute_query(query, (table_name, schema))
    
    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """Check if table exists in PostgreSQL."""
        query = """
        SELECT COUNT(*) as cnt 
        FROM information_schema.tables 
        WHERE table_name = %s AND table_schema = %s
        """
        schema = schema or 'public'
        result = self.execute_query(query, (table_name, schema))
        return result[0]['cnt'] > 0 if result else False


class DatabaseConnectorFactory:
    """Factory class for creating database connectors."""
    
    _connectors = {
        'ORACLE': OracleConnector,
        'POSTGRES': PostgresConnector,
        'POSTGRESQL': PostgresConnector  # Alias
    }
    
    @classmethod
    def create_connector(cls, db_type: str, config: Dict[str, Any]) -> BaseConnector:
        """
        Create a database connector based on type.
        
        Args:
            db_type: Type of database (ORACLE, POSTGRES)
            config: Database configuration
            
        Returns:
            Database connector instance
        """
        db_type_upper = db_type.upper()
        
        if db_type_upper not in cls._connectors:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        connector_class = cls._connectors[db_type_upper]
        return connector_class(config)
    
    @classmethod
    def register_connector(cls, db_type: str, connector_class: type) -> None:
        """Register a new database connector type."""
        cls._connectors[db_type.upper()] = connector_class


class DatabaseManager:
    """Centralized database connection manager."""
    
    def __init__(self):
        self._connections: Dict[str, BaseConnector] = {}
        self.logger = logger
    
    def get_connection(self, env: str, db_type: str, config: Dict[str, Any]) -> BaseConnector:
        """Get or create a database connection."""
        connection_key = f"{env}_{db_type}"
        
        if connection_key not in self._connections:
            self.logger.info(f"Creating new connection for {connection_key}")
            config['environment'] = env  # Add environment to config
            connector = DatabaseConnectorFactory.create_connector(db_type, config)
            connector.connect()
            self._connections[connection_key] = connector
        
        return self._connections[connection_key]
    
    def close_connection(self, env: str, db_type: str) -> None:
        """Close a specific database connection."""
        connection_key = f"{env}_{db_type}"
        
        if connection_key in self._connections:
            self._connections[connection_key].disconnect()
            del self._connections[connection_key]
            self.logger.info(f"Closed connection: {connection_key}")
    
    def close_all_connections(self) -> None:
        """Close all database connections."""
        for connection_key, connector in self._connections.items():
            try:
                connector.disconnect()
                self.logger.info(f"Closed connection: {connection_key}")
            except Exception as e:
                self.logger.warning(f"Error closing {connection_key}: {str(e)}")
        
        self._connections.clear()
    
    def validate_all_connections(self) -> Dict[str, bool]:
        """Validate all active connections."""
        results = {}
        
        for connection_key, connector in self._connections.items():
            results[connection_key] = connector.validate_connection()
        
        return results
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get statistics for all connections."""
        stats = {}
        
        for connection_key, connector in self._connections.items():
            stats[connection_key] = connector.get_connection_info()
        
        return stats


# Singleton instance
db_manager = DatabaseManager()


# Legacy connector wrapper for backward compatibility
class DatabaseConnector:
    """Legacy connector wrapper for backward compatibility."""
    
    def __init__(self):
        self.manager = db_manager
    
    def connect(self, db_type: str, **kwargs) -> BaseConnector:
        """Legacy connect method."""
        # Map legacy parameters to new config format
        config = {
            'host': kwargs.get('host'),
            'port': kwargs.get('port'),
            'database': kwargs.get('database', kwargs.get('service_name')),
            'username': kwargs.get('username'),
            'password': kwargs.get('password')
        }
        return self.manager.get_connection('DEFAULT', db_type, config)
    
    def execute_query(self, connection: Any, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Legacy execute query method."""
        if isinstance(connection, BaseConnector):
            return connection.execute_query(query, params)
        else:
            # Handle legacy connection objects
            raise NotImplementedError("Legacy connection objects not supported")
    
    def disconnect(self, connection: Any) -> None:
        """Legacy disconnect method."""
        if isinstance(connection, BaseConnector):
            # Don't actually disconnect - let the manager handle it
            pass
        else:
            # Handle legacy connection objects
            if hasattr(connection, 'close'):
                connection.close()


# Maintain singleton for backward compatibility
db_connector = DatabaseConnector()