"""Query loader utility for managing SQL queries from files."""
import os
from pathlib import Path
from typing import Dict, Optional, Any
from functools import lru_cache
import re

from utils.custom_exceptions import QueryNotFoundError, ConfigurationError


class QueryLoader:
    """Loads and manages SQL queries from files."""
    
    def __init__(self, queries_dir: str = "queries"):
        self.queries_dir = Path(queries_dir)
        self._query_cache: Dict[str, str] = {}
        self._validate_queries_directory()
    
    def _validate_queries_directory(self):
        """Ensure queries directory exists."""
        if not self.queries_dir.exists():
            raise ConfigurationError(
                f"Queries directory not found: {self.queries_dir}",
                config_key="queries_dir"
            )
    
    @lru_cache(maxsize=100)
    def load_query(self, query_name: str, module: Optional[str] = None, 
                   params: Optional[Dict[str, Any]] = None) -> str:
        """
        Load a query from file.
        
        Args:
            query_name: Name of the query file (without .sql extension)
            module: Optional module/subdirectory name
            params: Optional parameters for query templating
            
        Returns:
            SQL query string
            
        Example:
            query = query_loader.load_query("get_active_customers", module="customer")
            query = query_loader.load_query("daily_report", params={"days": 30})
        """
        # Construct file path
        if module:
            query_path = self.queries_dir / module / f"{query_name}.sql"
        else:
            query_path = self.queries_dir / f"{query_name}.sql"
        
        # Check cache
        cache_key = str(query_path)
        if cache_key in self._query_cache and not params:
            return self._query_cache[cache_key]
        
        # Load query from file
        if not query_path.exists():
            raise QueryNotFoundError(
                f"Query file not found: {query_path}",
                query_name=query_name,
                query_path=str(query_path)
            )
        
        try:
            with open(query_path, 'r') as f:
                query = f.read().strip()
            
            # Cache the raw query
            self._query_cache[cache_key] = query
            
            # Apply parameters if provided
            if params:
                query = self._apply_parameters(query, params)
            
            return query
            
        except Exception as e:
            raise QueryNotFoundError(
                f"Failed to load query: {str(e)}",
                query_name=query_name,
                query_path=str(query_path)
            )
    
    def _apply_parameters(self, query: str, params: Dict[str, Any]) -> str:
        """
        Apply parameters to query template.
        
        Supports {parameter} style templating.
        """
        for key, value in params.items():
            # Handle different value types
            if isinstance(value, str):
                query = query.replace(f"{{{key}}}", f"'{value}'")
            elif value is None:
                query = query.replace(f"{{{key}}}", "NULL")
            else:
                query = query.replace(f"{{{key}}}", str(value))
        
        # Check for any remaining placeholders
        remaining = re.findall(r'\{(\w+)\}', query)
        if remaining:
            raise ConfigurationError(
                f"Query has unresolved placeholders: {remaining}",
                config_key="query_parameters"
            )
        
        return query
    
    def list_queries(self, module: Optional[str] = None) -> Dict[str, str]:
        """List all available queries."""
        queries = {}
        
        if module:
            search_dir = self.queries_dir / module
        else:
            search_dir = self.queries_dir
        
        for sql_file in search_dir.rglob("*.sql"):
            relative_path = sql_file.relative_to(self.queries_dir)
            query_key = str(relative_path).replace('.sql', '').replace(os.sep, '.')
            queries[query_key] = str(sql_file)
        
        return queries
    
    def clear_cache(self):
        """Clear the query cache."""
        self._query_cache.clear()
        self.load_query.cache_clear()


# Singleton instance
query_loader = QueryLoader()