# utils/data_validator.py

from typing import Dict, List, Any, Optional
import re
from datetime import datetime
import pandas as pd
from utils.logger import logger


class DataValidator:
    """Utility class for data validation operations."""
    
    def __init__(self):
        self.validation_patterns = {
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'phone': r'^[+]?[0-9\s\-\(\)]+$',
            'ssn': r'^\d{3}-\d{2}-\d{4}$',
            'zip_code': r'^\d{5}(-\d{4})?$',
            'credit_card': r'^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$'
        }
    
    def validate_data_types(self, data: List[Dict], column_types: Dict[str, str]) -> Dict:
        """Validate data types for each column."""
        validation_results = {}
        
        for column, expected_type in column_types.items():
            invalid_count = 0
            
            for row in data:
                value = row.get(column)
                if value is not None and not self._is_valid_type(value, expected_type):
                    invalid_count += 1
            
            validation_results[column] = {
                'expected_type': expected_type,
                'invalid_count': invalid_count,
                'total_count': len(data),
                'valid_percentage': ((len(data) - invalid_count) / len(data)) * 100 if data else 100
            }
        
        return validation_results
    
    def validate_patterns(self, data: List[Dict], column_patterns: Dict[str, str]) -> Dict:
        """Validate data patterns for each column."""
        validation_results = {}
        
        for column, pattern_name in column_patterns.items():
            pattern = self.validation_patterns.get(pattern_name)
            if not pattern:
                logger.warning(f"Unknown pattern: {pattern_name}")
                continue
            
            invalid_count = 0
            
            for row in data:
                value = row.get(column)
                if value is not None and not re.match(pattern, str(value)):
                    invalid_count += 1
            
            validation_results[column] = {
                'pattern': pattern_name,
                'invalid_count': invalid_count,
                'total_count': len(data),
                'valid_percentage': ((len(data) - invalid_count) / len(data)) * 100 if data else 100
            }
        
        return validation_results
    
    def validate_ranges(self, data: List[Dict], column_ranges: Dict[str, Dict]) -> Dict:
        """Validate numeric ranges for each column."""
        validation_results = {}
        
        for column, range_config in column_ranges.items():
            min_val = range_config.get('min')
            max_val = range_config.get('max')
            out_of_range_count = 0
            
            for row in data:
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
                'out_of_range_count': out_of_range_count,
                'total_count': len(data),
                'valid_percentage': ((len(data) - out_of_range_count) / len(data)) * 100 if data else 100
            }
        
        return validation_results
    
    def _is_valid_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected type."""
        try:
            if expected_type.lower() == 'int':
                int(value)
                return True
            elif expected_type.lower() == 'float':
                float(value)
                return True
            elif expected_type.lower() == 'date':
                pd.to_datetime(value)
                return True
            elif expected_type.lower() == 'string':
                return isinstance(value, str)
            return True
        except:
            return False


# utils/performance_monitor.py

import time
from typing import Dict, List
from collections import defaultdict
import statistics
from utils.logger import logger


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
        logger.debug(f"Recorded query time: {query_identifier} = {execution_time:.3f}s")
    
    def record_connection_time(self, connection_identifier: str, connection_time: float):
        """Record connection establishment time."""
        self.connection_times[connection_identifier].append(connection_time)
        logger.debug(f"Recorded connection time: {connection_identifier} = {connection_time:.3f}s")
    
    def get_last_execution_time(self) -> Optional[float]:
        """Get the last recorded execution time."""
        return self.last_execution_time
    
    def get_average_time(self, identifier: str) -> float:
        """Get average execution time for identifier."""
        times = self.query_times.get(identifier, [])
        return statistics.mean(times) if times else 0.0
    
    def get_performance_stats(self, identifier: str) -> Dict:
        """Get comprehensive performance statistics."""
        times = self.query_times.get(identifier, [])
        
        if not times:
            return {'count': 0, 'average': 0, 'min': 0, 'max': 0, 'median': 0}
        
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
    
    def end_monitoring(self, session_name: str) -> Dict:
        """End a performance monitoring session and return results."""
        if session_name not in self.monitoring_sessions:
            raise ValueError(f"No monitoring session found: {session_name}")
        
        session = self.monitoring_sessions[session_name]
        session['end_time'] = time.time()
        session['total_duration'] = session['end_time'] - session['start_time']
        
        return session
    
    def generate_performance_report(self) -> Dict:
        """Generate a comprehensive performance report."""
        report = {
            'timestamp': time.time(),
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


# utils/schema_validator.py

from typing import Dict, List, Any
from db.database_connector import db_connector
from utils.logger import logger


class SchemaValidator:
    """Utility class for database schema validation."""
    
    def get_table_schema(self, table_name: str, env: str, db_type: str) -> Dict:
        """Get comprehensive table schema information."""
        connection = db_connector.get_connection(env, db_type)
        
        schema_info = {
            'columns': self._get_column_info(table_name, connection, db_type),
            'indexes': self._get_index_info(table_name, connection, db_type),
            'constraints': self._get_constraint_info(table_name, connection, db_type),
            'triggers': self._get_trigger_info(table_name, connection, db_type)
        }
        
        return schema_info
    
    def _get_column_info(self, table_name: str, connection, db_type: str) -> List[Dict]:
        """Get column information for a table."""
        if db_type.upper() == 'ORACLE':
            query = """
                SELECT column_name, data_type, data_length, data_precision, 
                       data_scale, nullable, data_default
                FROM user_tab_columns 
                WHERE table_name = UPPER(?)
                ORDER BY column_id
            """
        else:  # PostgreSQL
            query = """
                SELECT column_name, data_type, character_maximum_length as data_length,
                       numeric_precision as data_precision, numeric_scale as data_scale,
                       is_nullable as nullable, column_default as data_default
                FROM information_schema.columns
                WHERE table_name = ?
                ORDER BY ordinal_position
            """
        
        return db_connector.execute_query(connection, query, [table_name])
    
    def _get_index_info(self, table_name: str, connection, db_type: str) -> List[Dict]:
        """Get index information for a table."""
        if db_type.upper() == 'ORACLE':
            query = """
                SELECT i.index_name, i.index_type, i.uniqueness,
                       ic.column_name, ic.column_position
                FROM user_indexes i
                JOIN user_ind_columns ic ON i.index_name = ic.index_name
                WHERE i.table_name = UPPER(?)
                ORDER BY i.index_name, ic.column_position
            """
        else:  # PostgreSQL
            query = """
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
                WHERE i.tablename = ?
                ORDER BY i.indexname, a.attnum
            """
        
        return db_connector.execute_query(connection, query, [table_name])
    
    def _get_constraint_info(self, table_name: str, connection, db_type: str) -> List[Dict]:
        """Get constraint information for a table."""
        if db_type.upper() == 'ORACLE':
            query = """
                SELECT constraint_name, constraint_type, search_condition
                FROM user_constraints
                WHERE table_name = UPPER(?)
                ORDER BY constraint_name
            """
        else:  # PostgreSQL
            query = """
                SELECT constraint_name, constraint_type, check_clause as search_condition
                FROM information_schema.table_constraints tc
                LEFT JOIN information_schema.check_constraints cc 
                    ON tc.constraint_name = cc.constraint_name
                WHERE tc.table_name = ?
                ORDER BY constraint_name
            """
        
        return db_connector.execute_query(connection, query, [table_name])
    
    def _get_trigger_info(self, table_name: str, connection, db_type: str) -> List[Dict]:
        """Get trigger information for a table."""
        if db_type.upper() == 'ORACLE':
            query = """
                SELECT trigger_name, trigger_type, triggering_event, status
                FROM user_triggers
                WHERE table_name = UPPER(?)
                ORDER BY trigger_name
            """
        else:  # PostgreSQL
            query = """
                SELECT trigger_name, action_timing as trigger_type, 
                       event_manipulation as triggering_event, 'ENABLED' as status
                FROM information_schema.triggers
                WHERE event_object_table = ?
                ORDER BY trigger_name
            """
        
        return db_connector.execute_query(connection, query, [table_name])
    
    def compare_schemas(self, schema1: Dict, schema2: Dict, 
                       table_name: str, env1: str, env2: str) -> Dict:
        """Compare two table schemas and return differences."""
        comparison_result = {
            'table_name': table_name,
            'environments': [env1, env2],
            'column_differences': [],
            'index_differences': [],
            'constraint_differences': [],
            'trigger_differences': []
        }
        
        # Compare columns
        columns1 = {col['column_name']: col for col in schema1['columns']}
        columns2 = {col['column_name']: col for col in schema2['columns']}
        
        for col_name in set(columns1.keys()) | set(columns2.keys()):
            if col_name not in columns1:
                comparison_result['column_differences'].append({
                    'type': 'missing_in_env1',
                    'column': col_name,
                    'details': columns2[col_name]
                })
            elif col_name not in columns2:
                comparison_result['column_differences'].append({
                    'type': 'missing_in_env2',
                    'column': col_name,
                    'details': columns1[col_name]
                })
            else:
                # Compare column properties
                col1, col2 = columns1[col_name], columns2[col_name]
                differences = []
                
                for prop in ['data_type', 'data_length', 'nullable']:
                    if col1.get(prop) != col2.get(prop):
                        differences.append(f"{prop}: {col1.get(prop)} vs {col2.get(prop)}")
                
                if differences:
                    comparison_result['column_differences'].append({
                        'type': 'property_mismatch',
                        'column': col_name,
                        'differences': differences
                    })
        
        # Similar comparisons for indexes, constraints, and triggers...
        # (Implementation details omitted for brevity)
        
        return comparison_result
    
    # Add this method to your DataValidator class for better business rule validation

def validate_business_rules(self, data: List[Dict], validation_rules: Dict[str, str]) -> Dict:
    """Validate business rules on data (for step compatibility)."""
    validation_results = {}
    
    for field_name, rule in validation_rules.items():
        if rule == 'NOT_NULL':
            null_count = sum(1 for row in data if row.get(field_name) is None)
            validation_results[field_name] = {
                'rule': rule,
                'passed': null_count == 0,
                'details': f"Found {null_count} NULL values",
                'failed_count': null_count,
                'total_count': len(data)
            }
            
        elif rule == 'POSITIVE_NUMBER':
            invalid_count = 0
            for row in data:
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
                'total_count': len(data)
            }
            
        elif rule == 'VALID_DATE':
            invalid_count = 0
            for row in data:
                value = row.get(field_name)
                if value is not None:
                    try:
                        pd.to_datetime(value)
                    except:
                        invalid_count += 1
            
            validation_results[field_name] = {
                'rule': rule,
                'passed': invalid_count == 0,
                'details': f"Found {invalid_count} invalid date values",
                'failed_count': invalid_count,
                'total_count': len(data)
            }
    
    return validation_results


data_validator = DataValidator()
performance_monitor = PerformanceMonitor()
schema_validator = SchemaValidator()