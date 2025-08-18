"""
MongoDB connection utilities for NoSQL database operations.
"""
import pandas as pd
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union
import json
from bson import ObjectId
from utils.config_loader import ConfigLoader, config_loader
from utils.logger import logger, db_logger

class MongoDBConnector:
    """MongoDB connection and query execution utility."""
    
    def __init__(self):
        """Initialize MongoDB connector."""
        self.connections = {}
        self.clients = {}
    
    def connect_mongodb(self, environment: str) -> MongoClient:
        """
        Connect to MongoDB database.
        
        Args:
            environment: Environment name (DEV, QA, PROD)
            
        Returns:
            MongoDB client connection
        """
        try:
            config = config_loader.get_database_config(environment, 'MONGODB')
            
            # Build connection string
            if config.username and config.password:
                connection_string = (
                    f"mongodb://{config.username}:{config.password}@"
                    f"{config.host}:{config.port}/{config.database}"
                )
            else:
                connection_string = f"mongodb://{config.host}:{config.port}/{config.database}"
            
            # Add additional connection options
            client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=30000,  # 30 second timeout
                connectTimeoutMS=20000,  # 20 second connection timeout
                maxPoolSize=50
            )
            
            # Test connection
            client.admin.command('ping')
            
            self.clients[f"{environment}_MONGODB"] = client
            
            db_logger.info(f"Connected to MongoDB database: {environment}")
            return client
            
        except Exception as e:
            db_logger.error(f"Failed to connect to MongoDB {environment}: {e}")
            raise
    
    def get_database(self, environment: str, database_name: str = None):
        """
        Get MongoDB database instance.
        
        Args:
            environment: Environment name
            database_name: Database name (optional, uses config if not provided)
            
        Returns:
            MongoDB database instance
        """
        try:
            client_key = f"{environment}_MONGODB"
            
            if client_key not in self.clients:
                self.connect_mongodb(environment)
            
            client = self.clients[client_key]
            
            if not database_name:
                config = config_loader.get_database_config(environment, 'MONGODB')
                database_name = config.database
            
            db = client[database_name]
            
            db_logger.debug(f"Retrieved database: {database_name}")
            return db
            
        except Exception as e:
            db_logger.error(f"Failed to get database {database_name}: {e}")
            raise
    
    def execute_find_query(self,
                          environment: str,
                          collection_name: str,
                          query: Dict[str, Any] = None,
                          projection: Dict[str, Any] = None,
                          sort: List[tuple] = None,
                          limit: int = None,
                          skip: int = 0,
                          database_name: str = None) -> pd.DataFrame:
        """
        Execute MongoDB find query and return results as DataFrame.
        
        Args:
            environment: Environment name
            collection_name: Collection name
            query: MongoDB query filter
            projection: Fields to include/exclude
            sort: Sort criteria as list of (field, direction) tuples
            limit: Maximum number of documents to return
            skip: Number of documents to skip
            database_name: Database name (optional)
            
        Returns:
            Query results as pandas DataFrame
        """
        try:
            db = self.get_database(environment, database_name)
            collection = db[collection_name]
            
            db_logger.info(f"Executing find query on {environment}.{collection_name}")
            db_logger.debug(f"Query: {query}")
            db_logger.debug(f"Projection: {projection}")
            
            cursor = collection.find(
                filter=query or {},
                projection=projection,
                skip=skip,
                limit=limit
            )
            
            if sort:
                cursor = cursor.sort(sort)
            
            # Convert to list and then to DataFrame
            documents = list(cursor)
            
            if documents:
                # Convert ObjectId to string for DataFrame compatibility
                for doc in documents:
                    if '_id' in doc and isinstance(doc['_id'], ObjectId):
                        doc['_id'] = str(doc['_id'])
                
                df = pd.DataFrame(documents)
            else:
                df = pd.DataFrame()
            
            db_logger.info(f"Query executed successfully - {len(df)} documents returned")
            return df
            
        except Exception as e:
            db_logger.error(f"Find query execution failed on {environment}.{collection_name}: {e}")
            raise
    
    def execute_aggregation_query(self,
                                environment: str,
                                collection_name: str,
                                pipeline: List[Dict[str, Any]],
                                database_name: str = None) -> pd.DataFrame:
        """
        Execute MongoDB aggregation pipeline and return results as DataFrame.
        
        Args:
            environment: Environment name
            collection_name: Collection name
            pipeline: Aggregation pipeline stages
            database_name: Database name (optional)
            
        Returns:
            Aggregation results as pandas DataFrame
        """
        try:
            db = self.get_database(environment, database_name)
            collection = db[collection_name]
            
            db_logger.info(f"Executing aggregation query on {environment}.{collection_name}")
            db_logger.debug(f"Pipeline: {pipeline}")
            
            cursor = collection.aggregate(pipeline)
            documents = list(cursor)
            
            if documents:
                # Convert ObjectId to string for DataFrame compatibility
                for doc in documents:
                    for key, value in doc.items():
                        if isinstance(value, ObjectId):
                            doc[key] = str(value)
                
                df = pd.DataFrame(documents)
            else:
                df = pd.DataFrame()
            
            db_logger.info(f"Aggregation executed successfully - {len(df)} documents returned")
            return df
            
        except Exception as e:
            db_logger.error(f"Aggregation query execution failed on {environment}.{collection_name}: {e}")
            raise
    
    def execute_chunked_date_query(self,
                                 environment: str,
                                 collection_name: str,
                                 date_field: str,
                                 start_date: datetime,
                                 end_date: datetime,
                                 window_minutes: int = 60,
                                 additional_filters: Dict[str, Any] = None,
                                 database_name: str = None) -> pd.DataFrame:
        """
        Execute chunked query based on date range for large collections.
        
        Args:
            environment: Environment name
            collection_name: Collection name
            date_field: Date field name for chunking
            start_date: Query start date
            end_date: Query end date
            window_minutes: Time window size in minutes
            additional_filters: Additional query filters
            database_name: Database name (optional)
            
        Returns:
            Combined DataFrame from all chunks
        """
        try:
            db_logger.info(f"Starting chunked MongoDB query - {window_minutes} minute windows")
            
            chunks = []
            current_start = start_date
            window_delta = timedelta(minutes=window_minutes)
            
            while current_start < end_date:
                current_end = min(current_start + window_delta, end_date)
                
                db_logger.debug(f"Processing chunk: {current_start} to {current_end}")
                
                # Build query with date range
                chunk_query = {
                    date_field: {
                        "$gte": current_start,
                        "$lt": current_end
                    }
                }
                
                # Add additional filters if provided
                if additional_filters:
                    chunk_query.update(additional_filters)
                
                chunk_df = self.execute_find_query(
                    environment, collection_name, chunk_query, database_name=database_name
                )
                
                if not chunk_df.empty:
                    chunks.append(chunk_df)
                    db_logger.debug(f"Chunk returned {len(chunk_df)} documents")
                
                current_start = current_end
            
            if chunks:
                combined_df = pd.concat(chunks, ignore_index=True)
                db_logger.info(f"Chunked query completed - {len(combined_df)} total documents")
                return combined_df
            else:
                db_logger.info("Chunked query completed - no data returned")
                return pd.DataFrame()
                
        except Exception as e:
            db_logger.error(f"Chunked MongoDB query execution failed: {e}")
            raise
    
    def insert_documents(self,
                        environment: str,
                        collection_name: str,
                        documents: Union[Dict, List[Dict]],
                        database_name: str = None) -> Dict[str, Any]:
        """
        Insert document(s) into MongoDB collection.
        
        Args:
            environment: Environment name
            collection_name: Collection name
            documents: Document or list of documents to insert
            database_name: Database name (optional)
            
        Returns:
            Insert result information
        """
        try:
            db = self.get_database(environment, database_name)
            collection = db[collection_name]
            
            if isinstance(documents, dict):
                # Single document insert
                result = collection.insert_one(documents)
                db_logger.info(f"Inserted 1 document with ID: {result.inserted_id}")
                return {
                    'inserted_count': 1,
                    'inserted_ids': [str(result.inserted_id)]
                }
            else:
                # Multiple documents insert
                result = collection.insert_many(documents)
                db_logger.info(f"Inserted {len(result.inserted_ids)} documents")
                return {
                    'inserted_count': len(result.inserted_ids),
                    'inserted_ids': [str(id) for id in result.inserted_ids]
                }
                
        except Exception as e:
            db_logger.error(f"Document insertion failed on {environment}.{collection_name}: {e}")
            raise
    
    def update_documents(self,
                        environment: str,
                        collection_name: str,
                        filter_query: Dict[str, Any],
                        update_query: Dict[str, Any],
                        upsert: bool = False,
                        many: bool = False,
                        database_name: str = None) -> Dict[str, Any]:
        """
        Update document(s) in MongoDB collection.
        
        Args:
            environment: Environment name
            collection_name: Collection name
            filter_query: Query to match documents
            update_query: Update operations
            upsert: Create document if not found
            many: Update multiple documents
            database_name: Database name (optional)
            
        Returns:
            Update result information
        """
        try:
            db = self.get_database(environment, database_name)
            collection = db[collection_name]
            
            db_logger.info(f"Updating documents in {environment}.{collection_name}")
            db_logger.debug(f"Filter: {filter_query}")
            db_logger.debug(f"Update: {update_query}")
            
            if many:
                result = collection.update_many(filter_query, update_query, upsert=upsert)
            else:
                result = collection.update_one(filter_query, update_query, upsert=upsert)
            
            update_info = {
                'matched_count': result.matched_count,
                'modified_count': result.modified_count,
                'upserted_id': str(result.upserted_id) if result.upserted_id else None
            }
            
            db_logger.info(f"Update completed - matched: {update_info['matched_count']}, modified: {update_info['modified_count']}")
            return update_info
            
        except Exception as e:
            db_logger.error(f"Document update failed on {environment}.{collection_name}: {e}")
            raise
    
    def delete_documents(self,
                        environment: str,
                        collection_name: str,
                        filter_query: Dict[str, Any],
                        many: bool = False,
                        database_name: str = None) -> Dict[str, Any]:
        """
        Delete document(s) from MongoDB collection.
        
        Args:
            environment: Environment name
            collection_name: Collection name
            filter_query: Query to match documents for deletion
            many: Delete multiple documents
            database_name: Database name (optional)
            
        Returns:
            Delete result information
        """
        try:
            db = self.get_database(environment, database_name)
            collection = db[collection_name]
            
            db_logger.info(f"Deleting documents from {environment}.{collection_name}")
            db_logger.debug(f"Filter: {filter_query}")
            
            if many:
                result = collection.delete_many(filter_query)
            else:
                result = collection.delete_one(filter_query)
            
            db_logger.info(f"Delete completed - {result.deleted_count} documents deleted")
            return {'deleted_count': result.deleted_count}
            
        except Exception as e:
            db_logger.error(f"Document deletion failed on {environment}.{collection_name}: {e}")
            raise
    
    def get_collection_stats(self,
                           environment: str,
                           collection_name: str,
                           database_name: str = None) -> Dict[str, Any]:
        """
        Get MongoDB collection statistics.
        
        Args:
            environment: Environment name
            collection_name: Collection name
            database_name: Database name (optional)
            
        Returns:
            Collection statistics
        """
        try:
            db = self.get_database(environment, database_name)
            collection = db[collection_name]
            
            # Get collection stats
            stats = db.command("collStats", collection_name)
            
            collection_info = {
                'collection_name': collection_name,
                'document_count': stats.get('count', 0),
                'size_bytes': stats.get('size', 0),
                'avg_obj_size': stats.get('avgObjSize', 0),
                'storage_size': stats.get('storageSize', 0),
                'indexes': len(stats.get('indexSizes', {})),
                'index_sizes': stats.get('indexSizes', {})
            }
            
            db_logger.info(f"Retrieved collection stats for {collection_name}: {collection_info['document_count']} documents")
            return collection_info
            
        except Exception as e:
            db_logger.error(f"Failed to get collection stats for {collection_name}: {e}")
            raise
    
    def test_connection(self, environment: str) -> bool:
        """
        Test MongoDB connection.
        
        Args:
            environment: Environment name
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            client_key = f"{environment}_MONGODB"
            
            if client_key not in self.clients:
                self.connect_mongodb(environment)
            
            client = self.clients[client_key]
            
            # Test connection with ping
            client.admin.command('ping')
            
            db_logger.info(f"Connection test successful: {environment} MongoDB")
            return True
            
        except Exception as e:
            db_logger.error(f"Connection test failed for {environment} MongoDB: {e}")
            return False
    
    def create_indexes(self,
                      environment: str,
                      collection_name: str,
                      indexes: List[Dict[str, Any]],
                      database_name: str = None) -> List[str]:
        """
        Create indexes on MongoDB collection.
        
        Args:
            environment: Environment name
            collection_name: Collection name
            indexes: List of index specifications
            database_name: Database name (optional)
            
        Returns:
            List of created index names
        """
        try:
            db = self.get_database(environment, database_name)
            collection = db[collection_name]
            
            created_indexes = []
            
            for index_spec in indexes:
                result = collection.create_index(
                    index_spec['keys'],
                    **{k: v for k, v in index_spec.items() if k != 'keys'}
                )
                created_indexes.append(result)
            
            db_logger.info(f"Created {len(created_indexes)} indexes on {collection_name}")
            return created_indexes
            
        except Exception as e:
            db_logger.error(f"Index creation failed on {environment}.{collection_name}: {e}")
            raise
    
    def close_connections(self):
        """Close all MongoDB connections."""
        for client_name, client in self.clients.items():
            try:
                client.close()
                db_logger.info(f"Closed MongoDB connection: {client_name}")
            except Exception as e:
                db_logger.error(f"Error closing MongoDB connection {client_name}: {e}")
        
        self.clients.clear()
    
    # ✅ CORRECTED: These are now properly indented as class methods
    def get_connection(self, environment: str):
        """Get MongoDB client connection (for step compatibility)."""
        client_key = f"{environment}_MONGODB"
        
        if client_key not in self.clients:
            self.connect_mongodb(environment)
        
        return self.clients[client_key]

    def get_database_name(self, environment: str) -> str:
        """Get database name for environment (for step compatibility)."""
        config = config_loader.get_database_config(environment, 'MONGODB')
        return config.database

    def count_documents(self, environment: str, collection_name: str, 
                       query: Dict[str, Any] = None, database_name: str = None) -> int:
        """Count documents in collection (for step compatibility)."""
        try:
            db = self.get_database(environment, database_name)
            collection = db[collection_name]
            
            if query:
                count = collection.count_documents(query)
            else:
                count = collection.estimated_document_count()
            
            db_logger.info(f"Document count for {collection_name}: {count}")
            return count
            
        except Exception as e:
            db_logger.error(f"Document count failed for {collection_name}: {e}")
            raise

    def find_documents(self, environment: str, collection_name: str,
                      query: Dict[str, Any] = None, projection: Dict[str, Any] = None,
                      database_name: str = None) -> List[Dict[str, Any]]:
        """Find documents and return as list of dictionaries (for step compatibility)."""
        try:
            db = self.get_database(environment, database_name)
            collection = db[collection_name]
            
            cursor = collection.find(query or {}, projection)
            documents = list(cursor)
            
            # Convert ObjectId to string for compatibility
            for doc in documents:
                if '_id' in doc and isinstance(doc['_id'], ObjectId):
                    doc['_id'] = str(doc['_id'])
            
            db_logger.info(f"Found {len(documents)} documents in {collection_name}")
            return documents
            
        except Exception as e:
            db_logger.error(f"Find documents failed for {collection_name}: {e}")
            raise

    def list_collection_names(self, environment: str, database_name: str = None) -> List[str]:
        """List all collection names (for step compatibility)."""
        try:
            db = self.get_database(environment, database_name)
            collections = db.list_collection_names()
            
            db_logger.info(f"Found {len(collections)} collections in database")
            return collections
            
        except Exception as e:
            db_logger.error(f"List collections failed: {e}")
            raise
    

# Global MongoDB connector instance
mongodb_connector = MongoDBConnector()