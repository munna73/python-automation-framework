import time
import logging
from typing import Dict, Any, List, Optional

# Import your existing modules
try:
    from db.database_connector import db_manager
    from db.mongodb_connector import mongodb_connector
    from utils.config_loader import ConfigLoader
    from utils.logger import logger, db_logger
except ImportError as e:
    print(f"Import warning: {e}")
    print("Please adjust imports to match your existing module structure")


class DatabaseManager:
    """Database manager class for handling database connections and operations."""
    
    def __init__(self):
        self.query_results = {}
        
    def get_connection(self, env: str, db_type: str):
        """Get or create database connection."""
        if db_type.upper() in ['ORACLE', 'POSTGRES', 'POSTGRESQL']:
            # Get database configuration
            config = config_loader.get_database_config(env, db_type)
            # Use the enhanced database manager
            return db_manager.get_connection(env, db_type, config.to_dict())
        elif db_type.upper() == 'MONGODB':
            return mongodb_connector.get_connection(env)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def execute_sql_query(self, query: str, env: str, db_type: str) -> List[Dict]:
        """Execute SQL query and return results."""
        start_time = time.time()
        
        if db_type.upper() in ['ORACLE', 'POSTGRES', 'POSTGRESQL']:
            connection = self.get_connection(env, db_type)
            results = connection.execute_query(query)
        elif db_type.upper() == 'MONGODB':
            # For MongoDB, this would need to be adapted for specific collection queries
            raise NotImplementedError("MongoDB queries should use execute_mongodb_query method")
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        execution_time = time.time() - start_time
        db_logger.info(f"Query executed in {execution_time:.2f} seconds: {query[:100]}...")
        return results
    
    def execute_mongodb_query(self, env: str, collection_name: str, 
                             query: Dict[str, Any] = None, **kwargs) -> List[Dict]:
        """Execute MongoDB query and return results."""
        start_time = time.time()
        
        results = mongodb_connector.find_documents(
            environment=env,
            collection_name=collection_name,
            query=query,
            **kwargs
        )
        
        execution_time = time.time() - start_time
        db_logger.info(f"MongoDB query executed in {execution_time:.2f} seconds on {collection_name}")
        return results
    
    def store_result(self, key: str, value: Any):
        """Store query result for later use."""
        self.query_results[key] = value
        db_logger.debug(f"Stored result '{key}': {value}")
    
    def get_stored_result(self, key: str) -> Any:
        """Retrieve stored query result."""
        if key not in self.query_results:
            raise KeyError(f"No result stored with key: {key}")
        return self.query_results[key]
    
    def cleanup_connections(self):
        """Clean up all database connections."""
        try:
            # Clean up SQL database connections
            db_manager.close_all_connections()
            db_logger.debug("SQL database connections cleaned up")
            
            # Clean up MongoDB connections
            mongodb_connector.close_connections()
            db_logger.debug("MongoDB connections cleaned up")
            
        except Exception as e:
            db_logger.warning(f"Error during cleanup: {e}")
        
        # Clear stored results
        self.query_results.clear()