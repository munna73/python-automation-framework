# utils/data_validator.py

from typing import Dict, List, Any, Optional, Union
import re
from datetime import datetime
import pandas as pd
import time
import statistics
from collections import defaultdict

from utils.logger import logger, db_logger

# Import database manager with fallback
try:
    from db.database_manager import DatabaseManager
except ImportError:
    DatabaseManager = None
    logger.warning("DatabaseManager not available for schema validation")


class DataValidator:
    """Utility class for data validation operations."""
    
    def __init__(self):
        self.validation_patterns = {
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'phone': r'^[+]?[0-9\s\-\(\)]+$',
            'ssn': r'^\d{3}-\d{2}-\d{4}$',
            'zip_code': r'^\d{5}(-\d{4})?$',
            'credit_card': r'^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$',
            'url': r'^https?://(?:[-\w.])+(?:[:\d]+)?(?:/[^?\s]*)?(?:\?[^#\s]*)?(?:#[^\s]*)?$',
            'ipv4': r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
        }
        
        # Initialize database manager if available
        if DatabaseManager:
            self.db_manager = DatabaseManager()
        else:
            self.db_manager = None
    
    def validate_data_types(self, data: Union[List[Dict], pd.DataFrame], 
                           column_types: Dict[str, str]) -> Dict[str, Dict]:
        """
        Validate data types for each column.
        
        Args:
            data: Data as list of dictionaries or DataFrame
            column_types: Dictionary mapping column names to expected types
            
        Returns:
            Validation results dictionary
        """
        try:
            # Convert DataFrame to list of dicts if needed
            if isinstance(data, pd.DataFrame):
                data_list = data.to_dict('records')
            else:
                data_list = data
            
            validation_results = {}
            
            for column, expected_type in column_types.items():
                invalid_count = 0
                total_count = len(data_list)
                
                for row in data_list:
                    value = row.get(column)
                    if value is not None and not self._is_valid_type(value, expected_type):
                        invalid_count += 1
                
                validation_results[column] = {
                    'expected_type': expected_type,
                    'invalid_count': invalid_count,
                    'total_count': total_count,
                    'valid_count': total_count - invalid_count,
                    'valid_percentage': ((total_count - invalid_count) / total_count) * 100 if total_count > 0 else 100,
                    'passed': invalid_count == 0
                }
            
            logger.info(f"Data type validation completed for {len(column_types)} columns")
            return validation_results
            
        except Exception as e:
            logger.error(f"Data type validation failed: {e}")
            raise
    
    def validate_patterns(self, data: Union[List[Dict], pd.DataFrame], 
                         column_patterns: Dict[str, str]) -> Dict[str, Dict]:
        """
        Validate data patterns for each column.
        
        Args:
            data: Data as list of dictionaries or DataFrame
            column_patterns: Dictionary mapping column names to pattern names
            
        Returns:
            Validation results dictionary
        """
        try:
            # Convert DataFrame to list of dicts if needed
            if isinstance(data, pd.DataFrame):
                data_list = data.to_dict('records')
            else:
                data_list = data
            
            validation_results = {}
            
            for column, pattern_name in column_patterns.items():
                pattern = self.validation_patterns.get(pattern_name)
                if not pattern:
                    logger.warning(f"Unknown pattern: {pattern_name}")
                    continue
                
                invalid_count = 0
                total_count = len(data_list)
                
                for row in data_list:
                    value = row.get(column)
                    if value is not None and not re.match(pattern, str(value)):
                        invalid_count += 1
                
                validation_results[column] = {
                    'pattern': pattern_name,
                    'pattern_regex': pattern,
                    'invalid_count': invalid_count,
                    'total_count': total_count,
                    'valid_count': total_count - invalid_count,
                    'valid_percentage': ((total_count - invalid_count) / total_count) * 100 if total_count > 0 else 100,
                    'passed': invalid_count == 0
                }
            
            logger.info(f"Pattern validation completed for {len(column_patterns)} columns")
            return validation_results
            
        except Exception as e:
            logger.error(f"Pattern validation failed: {e}")
            raise
    
    def validate_ranges(self, data: Union[List[Dict], pd.DataFrame], 
                       column_ranges: Dict[str, Dict]) -> Dict[str, Dict]:
        """
        Validate numeric ranges for each column.
        
        Args:
            data: Data as list of dictionaries or DataFrame
            column_ranges: Dictionary mapping column names to range configurations
            
        Returns:
            Validation results dictionary
        """
        try:
            # Convert DataFrame to list of dicts if needed
            if isinstance(data, pd.DataFrame):
                data_list = data.to_dict('records')
            else:
                data_list = data
            
            validation_results = {}
            
            for column, range_config in column_ranges.items():
                min_val = range_config.get('min')
                max_val = range_config.get('max')
                out_of_range_count = 0
                total_count = len(data_list)
                
                for row in data_list:
                    value = row.get(column)
                    if value is not None:
                        try:
                            num_value = float(value)
                            if (min_val is not None and num_value < min_val) or \
                               (max_val is not None and num_value > max_val):
                                out_of_range_count += 1
                        except (ValueError, TypeError):
                            out_of_range_count += 1
                
                validation_results[column] = {
                    'range': f"{min_val} to {max_val}",
                    'min_value': min_val,
                    'max_value': max_val,
                    'out_of_range_count': out_of_range_count,
                    'total_count': total_count,
                    'valid_count': total_count - out_of_range_count,
                    'valid_percentage': ((total_count - out_of_range_count) / total_count) * 100 if total_count > 0 else 100,
                    'passed': out_of_range_count == 0
                }
            
            logger.info(f"Range validation completed for {len(column_ranges)} columns")
            return validation_results
            
        except Exception as e:
            logger.error(f"Range validation failed: {e}")
            raise
    
    def validate_business_rules(self, data: Union[List[Dict], pd.DataFrame], 
                               validation_rules: Dict[str, str]) -> Dict[str, Dict]:
        """
        Validate business rules on data (for step compatibility).
        
        Args:
            data: Data as list of dictionaries or DataFrame
            validation_rules: Dictionary mapping field names to validation rules
            
        Returns:
            Validation results dictionary
        """
        try:
            # Convert DataFrame to list of dicts if needed
            if isinstance(data, pd.DataFrame):
                data_list = data.to_dict('records')
            else:
                data_list = data
            
            validation_results = {}
            
            for field_name, rule in validation_rules.items():
                if rule == 'NOT_NULL':
                    null_count = sum(1 for row in data_list if row.get(field_name) is None or 
                                   str(row.get(field_name)).strip() == '')
                    validation_results[field_name] = {
                        'rule': rule,
                        'passed': null_count == 0,
                        'details': f"Found {null_count} NULL/empty values",
                        'failed_count': null_count,
                        'total_count': len(data_list)
                    }
                    
                elif rule == 'POSITIVE_NUMBER':
                    invalid_count = 0
                    for row in data_list:
                        value = row.get(field_name)
                        if value is not None:
                            try:
                                if float(value) <= 0:
                                    invalid_count += 1
                            except (ValueError, TypeError):
                                invalid_count += 1
                    
                    validation_results[field_name] = {
                        'rule': rule,
                        'passed': invalid_count == 0,
                        'details': f"Found {invalid_count} non-positive values",
                        'failed_count': invalid_count,
                        'total_count': len(data_list)
                    }
                    
                elif rule == 'VALID_DATE':
                    invalid_count = 0
                    for row in data_list:
                        value = row.get(field_name)
                        if value is not None:
                            try:
                                pd.to_datetime(value)
                            except (ValueError, TypeError):
                                invalid_count += 1
                    
                    validation_results[field_name] = {
                        'rule': rule,
                        'passed': invalid_count == 0,
                        'details': f"Found {invalid_count} invalid date values",
                        'failed_count': invalid_count,
                        'total_count': len(data_list)
                    }
                
                elif rule == 'UNIQUE':
                    values = [row.get(field_name) for row in data_list if row.get(field_name) is not None]
                    unique_values = set(values)
                    duplicate_count = len(values) - len(unique_values)
                    
                    validation_results[field_name] = {
                        'rule': rule,
                        'passed': duplicate_count == 0,
                        'details': f"Found {duplicate_count} duplicate values",
                        'failed_count': duplicate_count,
                        'total_count': len(data_list)
                    }
                
                else:
                    logger.warning(f"Unknown validation rule: {rule}")
                    validation_results[field_name] = {
                        'rule': rule,
                        'passed': False,
                        'details': f"Unknown rule: {rule}",
                        'failed_count': 0,
                        'total_count': len(data_list)
                    }
            
            logger.info(f"Business rule validation completed for {len(validation_rules)} rules")
            return validation_results
            
        except Exception as e:
            logger.error(f"Business rule validation failed: {e}")
            raise
    
    def validate_completeness(self, data: Union[List[Dict], pd.DataFrame], 
                            required_columns: List[str]) -> Dict[str, Any]:
        """
        Validate data completeness.
        
        Args:
            data: Data to validate
            required_columns: List of required column names
            
        Returns:
            Completeness validation results
        """
        try:
            # Convert DataFrame to list of dicts if needed
            if isinstance(data, pd.DataFrame):
                data_list = data.to_dict('records')
                available_columns = set(data.columns)
            else:
                data_list = data
                available_columns = set()
                if data_list:
                    available_columns = set(data_list[0].keys())
            
            results = {
                'total_records': len(data_list),
                'required_columns': required_columns,
                'available_columns': list(available_columns),
                'missing_columns': [],
                'column_completeness': {},
                'overall_completeness': 0.0
            }
            
            # Check for missing columns
            for col in required_columns:
                if col not in available_columns:
                    results['missing_columns'].append(col)
            
            # Calculate completeness for each required column
            for col in required_columns:
                if col in available_columns:
                    non_null_count = sum(1 for row in data_list 
                                       if row.get(col) is not None and str(row.get(col)).strip() != '')
                    completeness_pct = (non_null_count / len(data_list)) * 100 if data_list else 100
                    
                    results['column_completeness'][col] = {
                        'non_null_count': non_null_count,
                        'total_count': len(data_list),
                        'completeness_percentage': completeness_pct
                    }
            
            # Calculate overall completeness
            if results['column_completeness']:
                avg_completeness = sum(col_data['completeness_percentage'] 
                                     for col_data in results['column_completeness'].values())
                results['overall_completeness'] = avg_completeness / len(results['column_completeness'])
            
            logger.info(f"Completeness validation: {results['overall_completeness']:.2f}% complete")
            return results
            
        except Exception as e:
            logger.error(f"Completeness validation failed: {e}")
            raise
    
    def _is_valid_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected type."""
        try:
            if expected_type.lower() in ['int', 'integer']:
                int(value)
                return True
            elif expected_type.lower() in ['float', 'decimal', 'number']:
                float(value)
                return True
            elif expected_type.lower() in ['date', 'datetime']:
                pd.to_datetime(value)
                return True
            elif expected_type.lower() in ['string', 'str', 'text']:
                return isinstance(value, str)
            elif expected_type.lower() in ['bool', 'boolean']:
                return isinstance(value, bool) or str(value).lower() in ['true', 'false', '1', '0']
            return True
        except (ValueError, TypeError):
            return False
    
    def add_custom_pattern(self, pattern_name: str, regex_pattern: str):
        """Add a custom validation pattern."""
        self.validation_patterns[pattern_name] = regex_pattern
        logger.info(f"Added custom pattern: {pattern_name}")
    
    def get_validation_summary(self, validation_results: Dict[str, Dict]) -> Dict[str, Any]:
        """Get summary of validation results."""
        total_validations = len(validation_results)
        passed_validations = sum(1 for result in validation_results.values() if result.get('passed', False))
        
        summary = {
            'total_validations': total_validations,
            'passed_validations': passed_validations,
            'failed_validations': total_validations - passed_validations,
            'success_rate': (passed_validations / total_validations) * 100 if total_validations > 0 else 100,
            'failed_fields': [field for field, result in validation_results.items() if not result.get('passed', False)]
        }
        
        return summary


class PerformanceMonitor:
    """Monitor and track database performance metrics."""
    
    def __init__(self):
        self.query_times = defaultdict(list)
        self.connection_times = defaultdict(list)
        self.last_execution_time = None
        self.monitoring_sessions = {}
    
    def record_query_time(self, query_identifier: str, execution_time: float):
        """Record query execution time."""
        self.query_times[query_identifier].append(execution_time)
        self.last_execution_time = execution_time
        db_logger.debug(f"Recorded query time: {query_identifier} = {execution_time:.3f}s")
    
    def record_connection_time(self, connection_identifier: str, connection_time: float):
        """Record connection establishment time."""
        self.connection_times[connection_identifier].append(connection_time)
        db_logger.debug(f"Recorded connection time: {connection_identifier} = {connection_time:.3f}s")
    
    def get_last_execution_time(self) -> Optional[float]:
        """Get the last recorded execution time."""
        return self.last_execution_time
    
    def get_average_time(self, identifier: str) -> float:
        """Get average execution time for identifier."""
        times = self.query_times.get(identifier, [])
        return statistics.mean(times) if times else 0.0
    
    def get_performance_stats(self, identifier: str) -> Dict[str, float]:
        """Get comprehensive performance statistics."""
        times = self.query_times.get(identifier, [])
        
        if not times:
            return {'count': 0, 'average': 0, 'min': 0, 'max': 0, 'median': 0, 'std_dev': 0}
        
        return {
            'count': len(times),
            'average': statistics.mean(times),
            'min': min(times),
            'max': max(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }
    
    def start_monitoring(self, session_name: str):
        """Start a performance monitoring session."""
        self.monitoring_sessions[session_name] = {
            'start_time': time.time(),
            'queries': []
        }
        logger.info(f"Started performance monitoring session: {session_name}")
    
    def end_monitoring(self, session_name: str) -> Dict[str, Any]:
        """End a performance monitoring session and return results."""
        if session_name not in self.monitoring_sessions:
            raise ValueError(f"No monitoring session found: {session_name}")
        
        session = self.monitoring_sessions[session_name]
        session['end_time'] = time.time()
        session['total_duration'] = session['end_time'] - session['start_time']
        
        logger.info(f"Ended performance monitoring session: {session_name} (Duration: {session['total_duration']:.2f}s)")
        return session
    
    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate a comprehensive performance report."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'query_performance': {},
            'connection_performance': {},
            'summary': {}
        }
        
        # Query performance
        for identifier, times in self.query_times.items():
            report['query_performance'][identifier] = self.get_performance_stats(identifier)
        
        # Connection performance
        for identifier, times in self.connection_times.items():
            if times:
                report['connection_performance'][identifier] = {
                    'count': len(times),
                    'average': statistics.mean(times),
                    'min': min(times),
                    'max': max(times)
                }
        
        # Summary
        all_query_times = [time for times in self.query_times.values() for time in times]
        if all_query_times:
            report['summary'] = {
                'total_queries': len(all_query_times),
                'overall_average': statistics.mean(all_query_times),
                'fastest_query': min(all_query_times),
                'slowest_query': max(all_query_times)
            }
        
        return report
    
    def clear_metrics(self):
        """Clear all recorded metrics."""
        self.query_times.clear()
        self.connection_times.clear()
        self.monitoring_sessions.clear()
        self.last_execution_time = None
        logger.info("Cleared all performance metrics")


class SchemaValidator:
    """Utility class for database schema validation."""
    
    def __init__(self):
        # Initialize database manager if available
        if DatabaseManager:
            self.db_manager = DatabaseManager()
        else:
            self.db_manager = None
            logger.warning("DatabaseManager not available for schema validation")
    
    def get_table_schema(self, table_name: str, env: str, db_type: str) -> Dict[str, Any]:
        """Get comprehensive table schema information."""
        if not self.db_manager:
            raise RuntimeError("DatabaseManager not available")
        
        try:
            schema_info = {
                'table_name': table_name,
                'environment': env,
                'database_type': db_type,
                'columns': self._get_column_info(table_name, env, db_type),
                'indexes': self._get_index_info(table_name, env, db_type),
                'constraints': self._get_constraint_info(table_name, env, db_type)
            }
            
            logger.info(f"Retrieved schema for {table_name} in {env} ({db_type})")
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get table schema: {e}")
            raise
    
    def _get_column_info(self, table_name: str, env: str, db_type: str) -> List[Dict]:
        """Get column information for a table."""
        if db_type.upper() == 'ORACLE':
            query = """
                SELECT column_name, data_type, data_length, data_precision, 
                       data_scale, nullable, data_default, column_id
                FROM user_tab_columns 
                WHERE table_name = UPPER(:table_name)
                ORDER BY column_id
            """
            params = {'table_name': table_name}
        else:  # PostgreSQL
            query = """
                SELECT column_name, data_type, character_maximum_length as data_length,
                       numeric_precision as data_precision, numeric_scale as data_scale,
                       is_nullable as nullable, column_default as data_default,
                       ordinal_position as column_id
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """
            params = [table_name.lower()]
        
        try:
            if db_type.upper() == 'ORACLE':
                # For Oracle, we need to use the database manager differently
                results = self.db_manager.execute_sql_query(
                    query.replace(':table_name', f"'{table_name.upper()}'"), env, db_type
                )
            else:
                results = self.db_manager.execute_sql_query(
                    query.replace('%s', f"'{table_name.lower()}'"), env, db_type
                )
            
            return results
        except Exception as e:
            logger.error(f"Failed to get column info: {e}")
            return []
    
    def _get_index_info(self, table_name: str, env: str, db_type: str) -> List[Dict]:
        """Get index information for a table."""
        if db_type.upper() == 'ORACLE':
            query = f"""
                SELECT i.index_name, i.index_type, i.uniqueness,
                       ic.column_name, ic.column_position
                FROM user_indexes i
                JOIN user_ind_columns ic ON i.index_name = ic.index_name
                WHERE i.table_name = '{table_name.upper()}'
                ORDER BY i.index_name, ic.column_position
            """
        else:  # PostgreSQL
            query = f"""
                SELECT i.indexname as index_name, 
                       CASE WHEN i.indexdef LIKE '%UNIQUE%' THEN 'UNIQUE' ELSE 'NONUNIQUE' END as uniqueness,
                       a.attname as column_name,
                       a.attnum as column_position
                FROM pg_indexes i
                JOIN pg_class c ON c.relname = i.tablename
                JOIN pg_index idx ON idx.indexrelid = (
                    SELECT oid FROM pg_class WHERE relname = i.indexname
                )
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(idx.indkey)
                WHERE i.tablename = '{table_name.lower()}'
                ORDER BY i.indexname, a.attnum
            """
        
        try:
            results = self.db_manager.execute_sql_query(query, env, db_type)
            return results
        except Exception as e:
            logger.error(f"Failed to get index info: {e}")
            return []
    
    def _get_constraint_info(self, table_name: str, env: str, db_type: str) -> List[Dict]:
        """Get constraint information for a table."""
        if db_type.upper() == 'ORACLE':
            query = f"""
                SELECT constraint_name, constraint_type, search_condition
                FROM user_constraints
                WHERE table_name = '{table_name.upper()}'
                ORDER BY constraint_name
            """
        else:  # PostgreSQL
            query = f"""
                SELECT constraint_name, constraint_type, check_clause as search_condition
                FROM information_schema.table_constraints tc
                LEFT JOIN information_schema.check_constraints cc 
                    ON tc.constraint_name = cc.constraint_name
                WHERE tc.table_name = '{table_name.lower()}'
                ORDER BY constraint_name
            """
        
        try:
            results = self.db_manager.execute_sql_query(query, env, db_type)
            return results
        except Exception as e:
            logger.error(f"Failed to get constraint info: {e}")
            return []
    
    def compare_schemas(self, table_name: str, env1: str, env2: str, 
                       db_type: str) -> Dict[str, Any]:
        """Compare table schemas between environments."""
        try:
            schema1 = self.get_table_schema(table_name, env1, db_type)
            schema2 = self.get_table_schema(table_name, env2, db_type)
            
            comparison_result = {
                'table_name': table_name,
                'environments': [env1, env2],
                'database_type': db_type,
                'column_differences': [],
                'index_differences': [],
                'constraint_differences': []
            }
            
            # Compare columns
            columns1 = {col['column_name']: col for col in schema1['columns']}
            columns2 = {col['column_name']: col for col in schema2['columns']}
            
            for col_name in set(columns1.keys()) | set(columns2.keys()):
                if col_name not in columns1:
                    comparison_result['column_differences'].append({
                        'type': f'missing_in_{env1}',
                        'column': col_name,
                        'details': columns2[col_name]
                    })
                elif col_name not in columns2:
                    comparison_result['column_differences'].append({
                        'type': f'missing_in_{env2}',
                        'column': col_name,
                        'details': columns1[col_name]
                    })
                else:
                    # Compare column properties
                    col1, col2 = columns1[col_name], columns2[col_name]
                    differences = []
                    
                    for prop in ['data_type', 'data_length', 'nullable']:
                        val1, val2 = col1.get(prop), col2.get(prop)
                        if val1 != val2:
                            differences.append(f"{prop}: {val1} vs {val2}")
                    
                    if differences:
                        comparison_result['column_differences'].append({
                            'type': 'property_mismatch',
                            'column': col_name,
                            'differences': differences
                        })
            
            logger.info(f"Schema comparison completed: {len(comparison_result['column_differences'])} column differences")
            return comparison_result
            
        except Exception as e:
            logger.error(f"Schema comparison failed: {e}")
            raise


# Global instances
data_validator = DataValidator()
performance_monitor = PerformanceMonitor()
schema_validator = SchemaValidator()