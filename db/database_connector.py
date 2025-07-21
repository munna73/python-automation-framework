"""
Database connection utilities for Oracle and PostgreSQL.
"""
import pandas as pd
import cx_Oracle
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from utils.config_loader import config_loader
from utils.logger import logger, db_logger

class DatabaseConnector:
    """Database connection and query execution utility."""
    
    def __init__(self):
        """Initialize database connector."""
        self.connections = {}
        self.engines = {}
    
    def connect_oracle(self, environment: str) -> cx_Oracle.Connection:
        """
        Connect to Oracle database.
        
        Args:
            environment: Environment name (DEV, QA, PROD)
            
        Returns:
            Oracle database connection
        """
        try:
            config = config_loader.get_database_config(environment, 'ORACLE')
            
            connection_string = (
                f"{config['username']}/{config['password']}@"
                f"{config['host']}:{config['port']}/{config['service_name']}"
            )
            
            connection = cx_Oracle.connect(connection_string)
            self.connections[f"{environment}_ORACLE"] = connection
            
            db_logger.info(f"Connected to Oracle database: {environment}")
            return connection
            
        except Exception as e:
            db_logger.error(f"Failed to connect to Oracle {environment}: {e}")
            raise
    
    def connect_postgresql(self, environment: str) -> psycopg2.extensions.connection:
        """
        Connect to PostgreSQL database.
        
        Args:
            environment: Environment name (DEV, QA, PROD)
            
        Returns:
            PostgreSQL database connection
        """
        try:
            config = config_loader.get_database_config(environment, 'POSTGRES')
            
            connection = psycopg2.connect(
                host=config['host'],
                port=config['port'],
                database=config['database'],
                user=config['username'],
                password=config['password']
            )
            
            self.connections[f"{environment}_POSTGRES"] = connection
            
            db_logger.info(f"Connected to PostgreSQL database: {environment}")
            return connection
            
        except Exception as e:
            db_logger.error(f"Failed to connect to PostgreSQL {environment}: {e}")
            raise
    
    def get_sqlalchemy_engine(self, environment: str, db_type: str):
        """
        Get SQLAlchemy engine for pandas operations.
        
        Args:
            environment: Environment name (DEV, QA, PROD)
            db_type: Database type (ORACLE, POSTGRES)
            
        Returns:
            SQLAlchemy engine
        """
        engine_key = f"{environment}_{db_type}"
        
        if engine_key in self.engines:
            return self.engines[engine_key]
        
        try:
            config = config_loader.get_database_config(environment, db_type)
            
            if db_type.upper() == 'ORACLE':
                connection_string = (
                    f"oracle+cx_oracle://{config['username']}:{config['password']}@"
                    f"{config['host']}:{config['port']}/{config['service_name']}"
                )
            else:  # PostgreSQL
                connection_string = (
                    f"postgresql+psycopg2://{config['username']}:{config['password']}@"
                    f"{config['host']}:{config['port']}/{config['database']}"
                )
            
            engine = create_engine(connection_string)
            self.engines[engine_key] = engine
            
            db_logger.info(f"Created SQLAlchemy engine: {engine_key}")
            return engine
            
        except Exception as e:
            db_logger.error(f"Failed to create SQLAlchemy engine for {engine_key}: {e}")
            raise
    
    def execute_query(self, 
                     environment: str, 
                     db_type: str, 
                     query: str,
                     params: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame.
        
        Args:
            environment: Environment name
            db_type: Database type (ORACLE, POSTGRES)
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query results as pandas DataFrame
        """
        try:
            engine = self.get_sqlalchemy_engine(environment, db_type)
            
            db_logger.info(f"Executing query on {environment} {db_type}")
            db_logger.debug(f"Query: {query}")
            db_logger.debug(f"Parameters: {params}")
            
            df = pd.read_sql_query(query, engine, params=params)
            
            db_logger.info(f"Query executed successfully - {len(df)} rows returned")
            return df
            
        except Exception as e:
            db_logger.error(f"Query execution failed on {environment} {db_type}: {e}")
            raise
    
    def execute_chunked_query(self,
                            environment: str,
                            db_type: str,
                            query: str,
                            date_column: str,
                            start_date: datetime,
                            end_date: datetime,
                            window_minutes: int = 60) -> pd.DataFrame:
        """
        Execute query in time-based chunks to handle large datasets.
        
        Args:
            environment: Environment name
            db_type: Database type
            query: SQL query with :start_date and :end_date parameters
            date_column: Name of the date column for chunking
            start_date: Query start date
            end_date: Query end date
            window_minutes: Time window size in minutes
            
        Returns:
            Combined DataFrame from all chunks
        """
        try:
            db_logger.info(f"Starting chunked query execution - {window_minutes} minute windows")
            
            chunks = []
            current_start = start_date
            window_delta = timedelta(minutes=window_minutes)
            
            while current_start < end_date:
                current_end = min(current_start + window_delta, end_date)
                
                db_logger.debug(f"Processing chunk: {current_start} to {current_end}")
                
                chunk_params = {
                    'start_date': current_start,
                    'end_date': current_end
                }
                
                chunk_df = self.execute_query(environment, db_type, query, chunk_params)
                
                if not chunk_df.empty:
                    chunks.append(chunk_df)
                    db_logger.debug(f"Chunk returned {len(chunk_df)} rows")
                
                current_start = current_end
            
            if chunks:
                combined_df = pd.concat(chunks, ignore_index=True)
                db_logger.info(f"Chunked query completed - {len(combined_df)} total rows")
                return combined_df
            else:
                db_logger.info("Chunked query completed - no data returned")
                return pd.DataFrame()
                
        except Exception as e:
            db_logger.error(f"Chunked query execution failed: {e}")
            raise
    
    def test_connection(self, environment: str, db_type: str) -> bool:
        """
        Test database connection.
        
        Args:
            environment: Environment name
            db_type: Database type
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            engine = self.get_sqlalchemy_engine(environment, db_type)
            
            # Simple test query
            test_query = "SELECT 1 as test_column"
            if db_type.upper() == 'ORACLE':
                test_query = "SELECT 1 as test_column FROM dual"
            
            df = pd.read_sql_query(test_query, engine)
            
            if not df.empty:
                db_logger.info(f"Connection test successful: {environment} {db_type}")
                return True
            
        except Exception as e:
            db_logger.error(f"Connection test failed for {environment} {db_type}: {e}")
            
        return False
    
    def get_table_info(self, environment: str, db_type: str, table_name: str) -> Dict[str, Any]:
        """
        Get table information (column names, data types, row count).
        
        Args:
            environment: Environment name
            db_type: Database type
            table_name: Table name
            
        Returns:
            Dictionary with table information
        """
        try:
            engine = self.get_sqlalchemy_engine(environment, db_type)
            
            # Get column information
            if db_type.upper() == 'ORACLE':
                columns_query = f"""
                SELECT column_name, data_type, nullable
                FROM user_tab_columns 
                WHERE table_name = UPPER('{table_name}')
                ORDER BY column_id
                """
                count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            else:  # PostgreSQL
                columns_query = f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{table_name.lower()}'
                ORDER BY ordinal_position
                """
                count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            
            columns_df = pd.read_sql_query(columns_query, engine)
            count_df = pd.read_sql_query(count_query, engine)
            
            table_info = {
                'table_name': table_name,
                'columns': columns_df.to_dict('records'),
                'row_count': count_df.iloc[0]['row_count'] if not count_df.empty else 0,
                'column_count': len(columns_df)
            }
            
            db_logger.info(f"Retrieved table info for {table_name}: {table_info['column_count']} columns, {table_info['row_count']} rows")
            return table_info
            
        except Exception as e:
            db_logger.error(f"Failed to get table info for {table_name}: {e}")
            raise
    
    def close_connections(self):
        """Close all database connections."""
        for conn_name, connection in self.connections.items():
            try:
                connection.close()
                db_logger.info(f"Closed connection: {conn_name}")
            except Exception as e:
                db_logger.error(f"Error closing connection {conn_name}: {e}")
        
        self.connections.clear()
        
        # Dispose of SQLAlchemy engines
        for engine_name, engine in self.engines.items():
            try:
                engine.dispose()
                db_logger.info(f"Disposed engine: {engine_name}")
            except Exception as e:
                db_logger.error(f"Error disposing engine {engine_name}: {e}")
        
        self.engines.clear()

# Global database connector instance
db_connector = DatabaseConnector()