"""
MongoDB Query Executor
Handles connection, query execution, and error handling
"""
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ExecutionTimeout
from bson import ObjectId
from bson.errors import InvalidId
import logging
from typing import Dict, List, Any, Tuple
from config import Config

logger = logging.getLogger(__name__)


class MongoDBExecutor:
    """Execute MongoDB queries safely"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize MongoDB connection"""
        try:
            mongo_config = Config.MONGO_CONFIG
            
            # Build connection string
            if mongo_config['username'] and mongo_config['password']:
                uri = (
                    f"mongodb://{mongo_config['username']}:{mongo_config['password']}"
                    f"@{mongo_config['host']}:{mongo_config['port']}"
                    f"/{mongo_config['database']}?authSource={mongo_config['authSource']}"
                )
            else:
                uri = f"mongodb://{mongo_config['host']}:{mongo_config['port']}"
            
            self.client = MongoClient(
                uri,
                serverSelectionTimeoutMS=10000,
                socketTimeoutMS=Config.QUERY_TIMEOUT * 1000,
                maxPoolSize=10
            )
            
            self.db = self.client[mongo_config['database']]
            
            # Test connection
            self.client.admin.command('ping')
            logger.info("MongoDB connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB connection: {e}")
            raise
    
    def execute_find(self, collection: str, query: Dict, projection: Dict = None) -> Tuple[bool, Any]:
        """
        Execute a find query on a collection
        
        Args:
            collection: Collection name
            query: MongoDB query dict
            projection: Fields to return
        
        Returns:
            Tuple of (success: bool, result: List[Dict] or error_message: str)
        """
        try:
            coll = self.db[collection]
            
            # Convert string _id to ObjectId if needed
            query = self._convert_id_to_objectid(query)
            
            # Execute query with limit
            cursor = coll.find(query, projection).limit(Config.MAX_QUERY_ROWS)
            
            # Convert to list
            results = list(cursor)
            
            # Convert ObjectId to string for JSON serialization
            for doc in results:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            logger.info(f"MongoDB query executed successfully, returned {len(results)} documents")
            return True, results
            
        except ExecutionTimeout:
            error_msg = "Query took too long to execute"
            logger.error(f"Timeout error: {error_msg}")
            return False, error_msg
            
        except OperationFailure as e:
            error_msg = "MongoDB operation failed"
            logger.error(f"Operation error: {e}")
            return False, error_msg
            
        except Exception as e:
            error_msg = "Query execution failed"
            logger.error(f"Unexpected error: {e}")
            return False, error_msg
    
    def execute_aggregate(self, collection: str, pipeline: List[Dict]) -> Tuple[bool, Any]:
        """
        Execute an aggregation pipeline
        
        Args:
            collection: Collection name
            pipeline: Aggregation pipeline stages
        
        Returns:
            Tuple of (success: bool, result: List[Dict] or error_message: str)
        """
        try:
            coll = self.db[collection]
            
            # Add limit stage to pipeline
            pipeline_with_limit = pipeline + [{'$limit': Config.MAX_QUERY_ROWS}]
            
            # Execute aggregation
            cursor = coll.aggregate(
                pipeline_with_limit,
                maxTimeMS=Config.QUERY_TIMEOUT * 1000
            )
            
            # Convert to list
            results = list(cursor)
            
            # Convert ObjectId to string
            for doc in results:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            logger.info(f"MongoDB aggregation executed, returned {len(results)} documents")
            return True, results
            
        except ExecutionTimeout:
            error_msg = "Aggregation took too long to execute"
            logger.error(f"Timeout error: {error_msg}")
            return False, error_msg
            
        except Exception as e:
            error_msg = "Aggregation execution failed"
            logger.error(f"Unexpected error: {e}")
            return False, error_msg
    
    def get_schema_info(self) -> Tuple[bool, Any]:
        """
        Get MongoDB schema information (ONLY collections, NOT views/system collections)
        
        Returns:
            Tuple of (success: bool, schema: Dict or error_message: str)
        """
        try:
            schema = {}
            
            # Get ONLY actual collections (not views or system collections)
            collections = self.db.list_collection_names(filter={'type': 'collection'})
            
            # Filter out system collections
            collections = [c for c in collections if not c.startswith('system.')]
            
            for collection_name in collections:
                coll = self.db[collection_name]
                
                # Get sample document to infer schema
                sample = coll.find_one()
                
                if sample:
                    # Extract field names and types
                    fields = []
                    for key, value in sample.items():
                        fields.append({
                            'field': key,
                            'type': type(value).__name__
                        })
                    
                    schema[collection_name] = {
                        'fields': fields,
                        'count': coll.count_documents({})
                    }
                else:
                    schema[collection_name] = {
                        'fields': [],
                        'count': 0
                    }
            
            return True, schema
            
        except Exception as e:
            error_msg = "Failed to fetch schema"
            logger.error(f"Schema error: {e}")
            return False, error_msg
    
    def test_connection(self) -> bool:
        """Test if database connection is working"""
        try:
            self.client.admin.command('ping')
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def _convert_id_to_objectid(self, query: Dict) -> Dict:
        """
        Convert string _id values to ObjectId in MongoDB queries
        
        Args:
            query: MongoDB query dict
        
        Returns:
            Modified query with ObjectId instances
        """
        if not query:
            return query
        
        # Make a copy to avoid modifying the original
        query_copy = query.copy()
        
        # Check if _id is in the query
        if '_id' in query_copy:
            id_value = query_copy['_id']
            
            # If it's a string, convert to ObjectId
            if isinstance(id_value, str):
                try:
                    query_copy['_id'] = ObjectId(id_value)
                    logger.debug(f"Converted _id string to ObjectId: {id_value}")
                except (InvalidId, TypeError) as e:
                    logger.warning(f"Failed to convert _id to ObjectId: {id_value}, Error: {e}")
                    # Keep the original string value
                    pass
            
            # Handle nested queries like {"_id": {"$in": [...]}}
            elif isinstance(id_value, dict):
                for op, val in id_value.items():
                    if isinstance(val, str):
                        try:
                            query_copy['_id'][op] = ObjectId(val)
                        except (InvalidId, TypeError):
                            pass
                    elif isinstance(val, list):
                        # Convert list of strings to ObjectIds
                        query_copy['_id'][op] = [
                            ObjectId(v) if isinstance(v, str) else v 
                            for v in val
                        ]
        
        return query_copy
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")



