"""Query loader utility for managing SQL queries from files."""
import os
from pathlib import Path
from typing import Dict, Optional, Any, List
from functools import lru_cache
import re
from datetime import datetime, timedelta

from utils.custom_exceptions import QueryNotFoundError, ConfigurationError
from utils.logger import logger


class QueryLoader:
    """Loads and manages SQL queries from files."""
    
    def __init__(self, queries_dir: str = "queries"):
        self.queries_dir = Path(queries_dir)
        self._query_cache: Dict[str, str] = {}
        self._ensure_queries_directory()
        
        # Built-in date/time placeholders
        self.built_in_placeholders = {
            'TODAY': datetime.now().strftime('%Y-%m-%d'),
            'YESTERDAY': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
            'CURRENT_MONTH': datetime.now().strftime('%Y-%m'),
            'CURRENT_YEAR': datetime.now().strftime('%Y'),
            'LAST_MONTH': (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m'),
            'START_OF_MONTH': datetime.now().replace(day=1).strftime('%Y-%m-%d'),
            'END_OF_MONTH': ((datetime.now().replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d'),
            'CURRENT_TIMESTAMP': datetime.now().isoformat(),
            'UNIX_TIMESTAMP': str(int(datetime.now().timestamp()))
        }
    
    def _ensure_queries_directory(self):
        """Ensure queries directory exists, create if it doesn't."""
        try:
            if not self.queries_dir.exists():
                logger.warning(f"Queries directory not found: {self.queries_dir}, creating it")
                self.queries_dir.mkdir(parents=True, exist_ok=True)
                
                # Create example subdirectories
                (self.queries_dir / "customer").mkdir(exist_ok=True)
                (self.queries_dir / "reports").mkdir(exist_ok=True)
                (self.queries_dir / "validation").mkdir(exist_ok=True)
                
                # Create a sample query file
                sample_query = """-- Sample query template
-- Use {parameter_name} for parameters
SELECT * 
FROM customers 
WHERE status = '{status}' 
  AND created_date >= '{start_date}'
ORDER BY customer_id
LIMIT {limit};"""
                
                sample_file = self.queries_dir / "sample_query.sql"
                with open(sample_file, 'w') as f:
                    f.write(sample_query)
                
                logger.info(f"Created queries directory structure at: {self.queries_dir}")
                
        except Exception as e:
            raise ConfigurationError(
                f"Failed to create queries directory: {str(e)}",
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
        try:
            # Construct file path
            if module:
                query_path = self.queries_dir / module / f"{query_name}.sql"
            else:
                query_path = self.queries_dir / f"{query_name}.sql"
            
            # Check cache
            cache_key = str(query_path)
            if cache_key in self._query_cache and not params:
                logger.debug(f"Query loaded from cache: {query_name}")
                return self._query_cache[cache_key]
            
            # Load query from file
            if not query_path.exists():
                # Try to find the query in any subdirectory
                found_queries = list(self.queries_dir.rglob(f"{query_name}.sql"))
                if found_queries:
                    query_path = found_queries[0]
                    logger.info(f"Found query in alternative location: {query_path}")
                else:
                    raise QueryNotFoundError(
                        f"Query file not found: {query_path}",
                        query_name=query_name,
                        query_path=str(query_path)
                    )
            
            with open(query_path, 'r', encoding='utf-8') as f:
                query = f.read().strip()
            
            # Cache the raw query
            self._query_cache[cache_key] = query
            
            # Remove comments if requested (optional)
            query = self._clean_query(query)
            
            # Apply built-in placeholders first
            query = self._apply_built_in_placeholders(query)
            
            # Apply custom parameters if provided
            if params:
                query = self._apply_parameters(query, params)
            
            logger.debug(f"Successfully loaded query: {query_name}")
            return query
            
        except Exception as e:
            if isinstance(e, (QueryNotFoundError, ConfigurationError)):
                raise
            raise QueryNotFoundError(
                f"Failed to load query: {str(e)}",
                query_name=query_name,
                query_path=str(query_path) if 'query_path' in locals() else 'unknown'
            )
    
    def _clean_query(self, query: str) -> str:
        """Clean query by removing excessive whitespace and optionally comments."""
        # Remove excessive whitespace
        lines = []
        for line in query.split('\n'):
            line = line.strip()
            if line:  # Keep non-empty lines
                lines.append(line)
        
        return '\n'.join(lines)
    
    def _apply_built_in_placeholders(self, query: str) -> str:
        """Apply built-in date/time placeholders."""
        for placeholder, value in self.built_in_placeholders.items():
            # Support both {PLACEHOLDER} and {{PLACEHOLDER}} formats
            query = query.replace(f"{{{placeholder}}}", str(value))
            query = query.replace(f"{{{{{placeholder}}}}}", str(value))
        
        return query
    
    def _apply_parameters(self, query: str, params: Dict[str, Any]) -> str:
        """
        Apply parameters to query template.
        
        Supports {parameter} style templating with proper SQL escaping.
        """
        try:
            for key, value in params.items():
                placeholder = f"{{{key}}}"
                
                # Handle different value types with proper SQL formatting
                if value is None:
                    replacement = "NULL"
                elif isinstance(value, str):
                    # Escape single quotes in strings
                    escaped_value = value.replace("'", "''")
                    replacement = f"'{escaped_value}'"
                elif isinstance(value, bool):
                    replacement = "1" if value else "0"  # SQL boolean
                elif isinstance(value, (int, float)):
                    replacement = str(value)
                elif isinstance(value, (list, tuple)):
                    # Handle IN clauses
                    formatted_items = []
                    for item in value:
                        if isinstance(item, str):
                            escaped_item = item.replace("'", "''")
                            formatted_items.append(f"'{escaped_item}'")
                        else:
                            formatted_items.append(str(item))
                    replacement = f"({', '.join(formatted_items)})"
                else:
                    # Convert to string and treat as string
                    escaped_value = str(value).replace("'", "''")
                    replacement = f"'{escaped_value}'"
                
                query = query.replace(placeholder, replacement)
            
            # Check for any remaining placeholders
            remaining = re.findall(r'\{(\w+)\}', query)
            if remaining:
                raise ConfigurationError(
                    f"Query has unresolved placeholders: {remaining}",
                    config_key="query_parameters"
                )
            
            return query
            
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(
                f"Error applying query parameters: {str(e)}",
                config_key="query_parameters"
            )
    
    def load_query_with_fallback(self, query_names: List[str], module: Optional[str] = None, 
                                params: Optional[Dict[str, Any]] = None) -> str:
        """
        Load query with fallback options.
        
        Args:
            query_names: List of query names to try in order
            module: Optional module/subdirectory name
            params: Optional parameters for query templating
            
        Returns:
            SQL query string from first found query
        """
        last_error = None
        
        for query_name in query_names:
            try:
                return self.load_query(query_name, module, params)
            except QueryNotFoundError as e:
                last_error = e
                continue
        
        raise QueryNotFoundError(
            f"None of the fallback queries found: {query_names}",
            query_name=', '.join(query_names),
            query_path=str(self.queries_dir)
        )
    
    def query_exists(self, query_name: str, module: Optional[str] = None) -> bool:
        """Check if a query file exists."""
        try:
            if module:
                query_path = self.queries_dir / module / f"{query_name}.sql"
            else:
                query_path = self.queries_dir / f"{query_name}.sql"
            
            return query_path.exists() or len(list(self.queries_dir.rglob(f"{query_name}.sql"))) > 0
        except Exception:
            return False
    
    def list_queries(self, module: Optional[str] = None) -> Dict[str, str]:
        """List all available queries."""
        queries = {}
        
        try:
            if module:
                search_dir = self.queries_dir / module
                if not search_dir.exists():
                    logger.warning(f"Module directory not found: {search_dir}")
                    return queries
            else:
                search_dir = self.queries_dir
            
            for sql_file in search_dir.rglob("*.sql"):
                try:
                    relative_path = sql_file.relative_to(self.queries_dir)
                    query_key = str(relative_path).replace('.sql', '').replace(os.sep, '.')
                    queries[query_key] = str(sql_file)
                except Exception as e:
                    logger.warning(f"Error processing query file {sql_file}: {e}")
                    continue
            
            logger.debug(f"Found {len(queries)} queries in {search_dir}")
            return queries
            
        except Exception as e:
            logger.error(f"Error listing queries: {e}")
            return {}
    
    def list_modules(self) -> List[str]:
        """List all available query modules (subdirectories)."""
        modules = []
        
        try:
            for item in self.queries_dir.iterdir():
                if item.is_dir() and not item.name.startswith('.'):
                    modules.append(item.name)
            
            logger.debug(f"Found modules: {modules}")
            return sorted(modules)
            
        except Exception as e:
            logger.error(f"Error listing modules: {e}")
            return []
    
    def validate_query_syntax(self, query: str) -> Dict[str, Any]:
        """
        Basic SQL syntax validation.
        
        Returns:
            Dictionary with validation results
        """
        validation_result = {
            'is_valid': True,
            'warnings': [],
            'errors': []
        }
        
        try:
            # Basic syntax checks
            query_upper = query.upper().strip()
            
            # Check for dangerous operations in production
            dangerous_keywords = ['DROP', 'TRUNCATE', 'DELETE FROM', 'ALTER TABLE']
            for keyword in dangerous_keywords:
                if keyword in query_upper:
                    validation_result['warnings'].append(f"Potentially dangerous operation: {keyword}")
            
            # Check for common syntax issues
            if query_upper.count('(') != query_upper.count(')'):
                validation_result['errors'].append("Mismatched parentheses")
                validation_result['is_valid'] = False
            
            # Check for unterminated strings
            single_quotes = query.count("'") - query.count("\\'")
            if single_quotes % 2 != 0:
                validation_result['errors'].append("Unterminated string literal")
                validation_result['is_valid'] = False
            
            # Check for basic SQL structure
            if not any(keyword in query_upper for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH']):
                validation_result['warnings'].append("Query doesn't contain common SQL keywords")
            
        except Exception as e:
            validation_result['errors'].append(f"Validation error: {str(e)}")
            validation_result['is_valid'] = False
        
        return validation_result
    
    def create_query_template(self, query_name: str, template_content: str, 
                             module: Optional[str] = None, overwrite: bool = False) -> bool:
        """
        Create a new query template file.
        
        Args:
            query_name: Name for the query file
            template_content: SQL template content
            module: Optional module/subdirectory
            overwrite: Whether to overwrite existing files
            
        Returns:
            True if created successfully
        """
        try:
            if module:
                query_dir = self.queries_dir / module
                query_dir.mkdir(parents=True, exist_ok=True)
                query_path = query_dir / f"{query_name}.sql"
            else:
                query_path = self.queries_dir / f"{query_name}.sql"
            
            if query_path.exists() and not overwrite:
                raise ConfigurationError(
                    f"Query template already exists: {query_path}",
                    config_key="query_template"
                )
            
            with open(query_path, 'w', encoding='utf-8') as f:
                f.write(template_content)
            
            logger.info(f"Created query template: {query_path}")
            
            # Clear cache to ensure new template is loaded
            self.clear_cache()
            
            return True
            
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(
                f"Failed to create query template: {str(e)}",
                config_key="query_template"
            )
    
    def get_query_info(self, query_name: str, module: Optional[str] = None) -> Dict[str, Any]:
        """Get information about a query file."""
        try:
            if module:
                query_path = self.queries_dir / module / f"{query_name}.sql"
            else:
                query_path = self.queries_dir / f"{query_name}.sql"
            
            if not query_path.exists():
                # Try to find in any subdirectory
                found_queries = list(self.queries_dir.rglob(f"{query_name}.sql"))
                if found_queries:
                    query_path = found_queries[0]
                else:
                    return {'exists': False}
            
            stat = query_path.stat()
            
            # Read query to analyze
            with open(query_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Find placeholders
            placeholders = re.findall(r'\{(\w+)\}', content)
            
            return {
                'exists': True,
                'path': str(query_path),
                'size': stat.st_size,
                'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'placeholders': list(set(placeholders)),
                'line_count': len(content.split('\n')),
                'character_count': len(content)
            }
            
        except Exception as e:
            logger.error(f"Error getting query info: {e}")
            return {'exists': False, 'error': str(e)}
    
    def clear_cache(self):
        """Clear the query cache."""
        self._query_cache.clear()
        self.load_query.cache_clear()
        logger.debug("Query cache cleared")
    
    def refresh_built_in_placeholders(self):
        """Refresh built-in date/time placeholders with current values."""
        self.built_in_placeholders.update({
            'TODAY': datetime.now().strftime('%Y-%m-%d'),
            'YESTERDAY': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
            'CURRENT_MONTH': datetime.now().strftime('%Y-%m'),
            'CURRENT_YEAR': datetime.now().strftime('%Y'),
            'LAST_MONTH': (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m'),
            'START_OF_MONTH': datetime.now().replace(day=1).strftime('%Y-%m-%d'),
            'END_OF_MONTH': ((datetime.now().replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d'),
            'CURRENT_TIMESTAMP': datetime.now().isoformat(),
            'UNIX_TIMESTAMP': str(int(datetime.now().timestamp()))
        })
        logger.debug("Built-in placeholders refreshed")


# Singleton instance
query_loader = QueryLoader()