"""
Unit tests for query_loader.py module.

This module contains comprehensive test cases for validating the query loading
functionality including SQL query loading, parameterization, template processing,
and query validation.
"""

import unittest
from unittest.mock import Mock, patch, mock_open, MagicMock
import pytest
import os
import tempfile
import shutil
import json
import yaml
from datetime import datetime, date
import sys

# Add the utils directory to the path to import query_loader
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'utils'))

from query_loader import QueryLoader
from custom_exceptions import ConfigurationError, QueryLoadError, ValidationError


class TestQueryLoader(unittest.TestCase):
    """Test cases for QueryLoader class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.query_loader = QueryLoader()
        
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        
        # Sample SQL queries for testing
        self.sample_queries = {
            'simple_select': 'SELECT * FROM users WHERE id = ?',
            'parameterized_query': 'SELECT name, email FROM users WHERE age > {min_age} AND department = "{department}"',
            'complex_join': '''
                SELECT u.name, u.email, d.department_name
                FROM users u
                JOIN departments d ON u.department_id = d.id
                WHERE u.created_date >= '{start_date}'
                AND u.is_active = {is_active}
            ''',
            'template_query': '''
                SELECT {columns}
                FROM {table_name}
                WHERE {where_conditions}
                ORDER BY {order_by}
                LIMIT {limit}
            '''
        }
        
        # Sample query parameters
        self.sample_parameters = {
            'min_age': 25,
            'department': 'IT',
            'start_date': '2023-01-01',
            'is_active': True,
            'columns': 'id, name, email',
            'table_name': 'users',
            'where_conditions': 'status = "active"',
            'order_by': 'created_date DESC',
            'limit': 100
        }
        
        # Create sample query files
        self.create_sample_query_files()

    def tearDown(self):
        """Clean up after each test method."""
        # Remove temporary directory
        shutil.rmtree(self.temp_dir)
        self.query_loader = None

    def create_sample_query_files(self):
        """Create sample query files for testing."""
        # Create SQL files
        for query_name, query_content in self.sample_queries.items():
            file_path = os.path.join(self.temp_dir, f"{query_name}.sql")
            with open(file_path, 'w') as f:
                f.write(query_content)
        
        # Create JSON query file
        json_queries = {
            'user_queries': {
                'get_user_by_id': 'SELECT * FROM users WHERE id = {user_id}',
                'get_active_users': 'SELECT * FROM users WHERE is_active = true'
            },
            'report_queries': {
                'monthly_report': 'SELECT COUNT(*) as total FROM users WHERE created_date >= "{start_date}"'
            }
        }
        
        json_file_path = os.path.join(self.temp_dir, 'queries.json')
        with open(json_file_path, 'w') as f:
            json.dump(json_queries, f, indent=2)
        
        # Create YAML query file
        yaml_queries = {
            'database_queries': {
                'cleanup_old_records': 'DELETE FROM temp_table WHERE created_date < "{cutoff_date}"',
                'update_user_status': 'UPDATE users SET is_active = {status} WHERE last_login < "{cutoff_date}"'
            }
        }
        
        yaml_file_path = os.path.join(self.temp_dir, 'queries.yaml')
        with open(yaml_file_path, 'w') as f:
            yaml.dump(yaml_queries, f, default_flow_style=False)

    def test_init_default(self):
        """Test QueryLoader initialization with default parameters."""
        loader = QueryLoader()
        self.assertIsNotNone(loader)
        self.assertEqual(loader.query_directory, None)
        self.assertEqual(loader.queries, {})

    def test_init_with_directory(self):
        """Test QueryLoader initialization with query directory."""
        loader = QueryLoader(query_directory=self.temp_dir)
        self.assertEqual(loader.query_directory, self.temp_dir)

    def test_load_query_from_file_sql(self):
        """Test loading SQL query from file."""
        file_path = os.path.join(self.temp_dir, 'simple_select.sql')
        query = self.query_loader.load_query_from_file(file_path)
        
        self.assertEqual(query.strip(), self.sample_queries['simple_select'])

    def test_load_query_from_file_nonexistent(self):
        """Test loading query from non-existent file."""
        with self.assertRaises(FileNotFoundError):
            self.query_loader.load_query_from_file('nonexistent.sql')

    def test_load_query_from_file_invalid_extension(self):
        """Test loading query from file with invalid extension."""
        invalid_file = os.path.join(self.temp_dir, 'test.txt')
        with open(invalid_file, 'w') as f:
            f.write('SELECT * FROM test')
        
        # Should still load the file but might have different handling
        query = self.query_loader.load_query_from_file(invalid_file)
        self.assertIsNotNone(query)

    def test_load_queries_from_directory(self):
        """Test loading all queries from directory."""
        queries = self.query_loader.load_queries_from_directory(self.temp_dir)
        
        self.assertIsInstance(queries, dict)
        self.assertGreater(len(queries), 0)
        self.assertIn('simple_select', queries)
        self.assertIn('parameterized_query', queries)

    def test_load_queries_from_json(self):
        """Test loading queries from JSON file."""
        json_file = os.path.join(self.temp_dir, 'queries.json')
        queries = self.query_loader.load_queries_from_json(json_file)
        
        self.assertIsInstance(queries, dict)
        self.assertIn('user_queries', queries)
        self.assertIn('report_queries', queries)

    def test_load_queries_from_yaml(self):
        """Test loading queries from YAML file."""
        yaml_file = os.path.join(self.temp_dir, 'queries.yaml')
        queries = self.query_loader.load_queries_from_yaml(yaml_file)
        
        self.assertIsInstance(queries, dict)
        self.assertIn('database_queries', queries)

    def test_parameterize_query_simple(self):
        """Test simple query parameterization."""
        template = 'SELECT * FROM users WHERE age > {min_age}'
        parameters = {'min_age': 25}
        
        result = self.query_loader.parameterize_query(template, parameters)
        expected = 'SELECT * FROM users WHERE age > 25'
        
        self.assertEqual(result, expected)

    def test_parameterize_query_string_params(self):
        """Test query parameterization with string parameters."""
        template = 'SELECT * FROM users WHERE department = "{department}"'
        parameters = {'department': 'IT'}
        
        result = self.query_loader.parameterize_query(template, parameters)
        expected = 'SELECT * FROM users WHERE department = "IT"'
        
        self.assertEqual(result, expected)

    def test_parameterize_query_multiple_params(self):
        """Test query parameterization with multiple parameters."""
        template = self.sample_queries['parameterized_query']
        parameters = self.sample_parameters
        
        result = self.query_loader.parameterize_query(template, parameters)
        
        self.assertIn('25', result)  # min_age
        self.assertIn('IT', result)  # department

    def test_parameterize_query_missing_param(self):
        """Test query parameterization with missing parameter."""
        template = 'SELECT * FROM users WHERE age > {min_age} AND name = "{name}"'
        parameters = {'min_age': 25}  # Missing 'name' parameter
        
        with self.assertRaises(KeyError):
            self.query_loader.parameterize_query(template, parameters)

    def test_parameterize_query_date_handling(self):
        """Test query parameterization with date parameters."""
        template = 'SELECT * FROM users WHERE created_date >= "{start_date}"'
        parameters = {'start_date': datetime(2023, 1, 1)}
        
        result = self.query_loader.parameterize_query(template, parameters)
        self.assertIn('2023-01-01', result)

    def test_validate_query_syntax_valid_sql(self):
        """Test SQL syntax validation with valid query."""
        valid_query = 'SELECT id, name FROM users WHERE age > 18'
        
        is_valid = self.query_loader.validate_query_syntax(valid_query)
        self.assertTrue(is_valid)

    def test_validate_query_syntax_invalid_sql(self):
        """Test SQL syntax validation with invalid query."""
        invalid_query = 'SELCT id, name FORM users'  # Typos in SELECT and FROM
        
        is_valid = self.query_loader.validate_query_syntax(invalid_query)
        self.assertFalse(is_valid)

    def test_validate_query_syntax_sql_injection(self):
        """Test SQL injection detection in query validation."""
        malicious_query = "SELECT * FROM users WHERE id = 1; DROP TABLE users; --"
        
        is_valid = self.query_loader.validate_query_syntax(malicious_query, check_injection=True)
        self.assertFalse(is_valid)

    def test_get_query_by_name_exists(self):
        """Test retrieving query by name when it exists."""
        self.query_loader.queries = {'test_query': 'SELECT * FROM test'}
        
        query = self.query_loader.get_query_by_name('test_query')
        self.assertEqual(query, 'SELECT * FROM test')

    def test_get_query_by_name_not_exists(self):
        """Test retrieving query by name when it doesn't exist."""
        with self.assertRaises(KeyError):
            self.query_loader.get_query_by_name('nonexistent_query')

    def test_register_query(self):
        """Test registering a new query."""
        query_name = 'new_query'
        query_content = 'SELECT * FROM new_table'
        
        self.query_loader.register_query(query_name, query_content)
        
        self.assertIn(query_name, self.query_loader.queries)
        self.assertEqual(self.query_loader.queries[query_name], query_content)

    def test_unregister_query(self):
        """Test unregistering an existing query."""
        query_name = 'temp_query'
        self.query_loader.queries[query_name] = 'SELECT * FROM temp'
        
        self.query_loader.unregister_query(query_name)
        
        self.assertNotIn(query_name, self.query_loader.queries)

    def test_list_available_queries(self):
        """Test listing all available queries."""
        self.query_loader.queries = {
            'query1': 'SELECT 1',
            'query2': 'SELECT 2',
            'query3': 'SELECT 3'
        }
        
        query_list = self.query_loader.list_available_queries()
        
        self.assertEqual(len(query_list), 3)
        self.assertIn('query1', query_list)
        self.assertIn('query2', query_list)
        self.assertIn('query3', query_list)

    def test_get_query_metadata(self):
        """Test retrieving query metadata."""
        query_name = 'test_query'
        query_content = 'SELECT id, name FROM users WHERE age > {min_age}'
        self.query_loader.queries[query_name] = query_content
        
        metadata = self.query_loader.get_query_metadata(query_name)
        
        self.assertIsInstance(metadata, dict)
        self.assertIn('name', metadata)
        self.assertIn('parameters', metadata)
        self.assertIn('min_age', metadata['parameters'])

    def test_extract_query_parameters(self):
        """Test extracting parameters from query template."""
        query = 'SELECT * FROM users WHERE age > {min_age} AND department = "{dept}" AND salary > {min_salary}'
        
        parameters = self.query_loader.extract_query_parameters(query)
        
        self.assertIsInstance(parameters, list)
        self.assertIn('min_age', parameters)
        self.assertIn('dept', parameters)
        self.assertIn('min_salary', parameters)

    def test_query_template_processing(self):
        """Test advanced query template processing."""
        template = self.sample_queries['template_query']
        parameters = self.sample_parameters
        
        result = self.query_loader.process_query_template(template, parameters)
        
        self.assertIn('id, name, email', result)  # columns
        self.assertIn('users', result)  # table_name
        self.assertIn('status = "active"', result)  # where_conditions

    def test_conditional_query_building(self):
        """Test conditional query building based on parameters."""
        base_query = 'SELECT * FROM users WHERE 1=1'
        conditions = {
            'age_filter': 'AND age > {min_age}',
            'dept_filter': 'AND department = "{department}"',
            'status_filter': 'AND is_active = {is_active}'
        }
        
        parameters = {
            'min_age': 25,
            'department': 'IT',
            'apply_age_filter': True,
            'apply_dept_filter': True,
            'apply_status_filter': False
        }
        
        result = self.query_loader.build_conditional_query(base_query, conditions, parameters)
        
        self.assertIn('age > 25', result)
        self.assertIn('department = "IT"', result)
        self.assertNotIn('is_active', result)

    def test_query_caching(self):
        """Test query caching mechanism."""
        query_name = 'cached_query'
        query_content = 'SELECT * FROM cache_test'
        
        # First load should cache the query
        self.query_loader.queries[query_name] = query_content
        first_result = self.query_loader.get_query_by_name(query_name, use_cache=True)
        
        # Second load should use cache
        second_result = self.query_loader.get_query_by_name(query_name, use_cache=True)
        
        self.assertEqual(first_result, second_result)

    def test_query_versioning(self):
        """Test query versioning support."""
        query_name = 'versioned_query'
        v1_query = 'SELECT id, name FROM users'
        v2_query = 'SELECT id, name, email FROM users'
        
        # Register different versions
        self.query_loader.register_versioned_query(query_name, v1_query, version='1.0')
        self.query_loader.register_versioned_query(query_name, v2_query, version='2.0')
        
        # Test retrieving specific versions
        v1_result = self.query_loader.get_query_by_name(query_name, version='1.0')
        v2_result = self.query_loader.get_query_by_name(query_name, version='2.0')
        
        self.assertEqual(v1_result, v1_query)
        self.assertEqual(v2_result, v2_query)

    def test_bulk_query_loading(self):
        """Test bulk loading of queries from multiple sources."""
        sources = [
            os.path.join(self.temp_dir, 'queries.json'),
            os.path.join(self.temp_dir, 'queries.yaml'),
            self.temp_dir  # Directory with SQL files
        ]
        
        total_queries = self.query_loader.bulk_load_queries(sources)
        
        self.assertGreater(total_queries, 0)
        self.assertGreater(len(self.query_loader.queries), 0)

    def test_query_dependency_resolution(self):
        """Test query dependency resolution."""
        base_query = 'SELECT * FROM ({subquery}) AS sub WHERE sub.age > {min_age}'
        subquery = 'SELECT id, name, age FROM users WHERE is_active = true'
        
        dependencies = {'subquery': subquery}
        parameters = {'min_age': 25}
        
        result = self.query_loader.resolve_query_dependencies(base_query, dependencies, parameters)
        
        self.assertIn('SELECT id, name, age FROM users', result)
        self.assertIn('age > 25', result)

    def test_query_optimization_hints(self):
        """Test adding query optimization hints."""
        base_query = 'SELECT * FROM users WHERE age > 25'
        hints = ['USE INDEX (idx_age)', 'FORCE INDEX (idx_department)']
        
        optimized_query = self.query_loader.add_optimization_hints(base_query, hints)
        
        for hint in hints:
            self.assertIn(hint, optimized_query)

    def test_dynamic_table_name_substitution(self):
        """Test dynamic table name substitution."""
        template = 'SELECT * FROM {table_prefix}users WHERE id = {user_id}'
        parameters = {
            'table_prefix': 'staging_',
            'user_id': 123
        }
        
        result = self.query_loader.parameterize_query(template, parameters)
        
        self.assertIn('staging_users', result)
        self.assertIn('123', result)

    def test_query_execution_plan_analysis(self):
        """Test query execution plan analysis."""
        query = 'SELECT * FROM users WHERE age > 25 ORDER BY created_date'
        
        analysis = self.query_loader.analyze_query_complexity(query)
        
        self.assertIsInstance(analysis, dict)
        self.assertIn('estimated_complexity', analysis)
        self.assertIn('suggested_indexes', analysis)

    def test_environment_specific_queries(self):
        """Test loading environment-specific queries."""
        dev_query = 'SELECT * FROM dev_users LIMIT 10'
        prod_query = 'SELECT * FROM users'
        
        self.query_loader.register_environment_query('get_users', dev_query, environment='development')
        self.query_loader.register_environment_query('get_users', prod_query, environment='production')
        
        # Test retrieving for specific environment
        dev_result = self.query_loader.get_environment_query('get_users', 'development')
        prod_result = self.query_loader.get_environment_query('get_users', 'production')
        
        self.assertEqual(dev_result, dev_query)
        self.assertEqual(prod_result, prod_query)

    @patch('builtins.open', mock_open(read_data='SELECT * FROM test'))
    def test_file_watching_and_reload(self):
        """Test file watching and automatic query reload."""
        file_path = 'test_query.sql'
        
        # Mock file system watcher
        with patch('os.path.getmtime') as mock_getmtime:
            mock_getmtime.return_value = 12345
            
            # First load
            self.query_loader.load_query_from_file(file_path)
            
            # Simulate file change
            mock_getmtime.return_value = 12346
            
            # Should detect change and reload
            reloaded = self.query_loader.check_and_reload_queries()
            self.assertTrue(reloaded)

    def test_error_handling_malformed_json(self):
        """Test error handling with malformed JSON file."""
        malformed_json = os.path.join(self.temp_dir, 'malformed.json')
        with open(malformed_json, 'w') as f:
            f.write('{"invalid": json content}')
        
        with self.assertRaises(json.JSONDecodeError):
            self.query_loader.load_queries_from_json(malformed_json)

    def test_error_handling_malformed_yaml(self):
        """Test error handling with malformed YAML file."""
        malformed_yaml = os.path.join(self.temp_dir, 'malformed.yaml')
        with open(malformed_yaml, 'w') as f:
            f.write('invalid:\n  yaml:\ncontent')
        
        with self.assertRaises(yaml.YAMLError):
            self.query_loader.load_queries_from_yaml(malformed_yaml)

    def test_permission_denied_handling(self):
        """Test handling of permission denied errors."""
        with patch('builtins.open', side_effect=PermissionError):
            with self.assertRaises(PermissionError):
                self.query_loader.load_query_from_file('restricted_file.sql')

    def test_encoding_handling(self):
        """Test handling of different file encodings."""
        utf8_file = os.path.join(self.temp_dir, 'utf8_query.sql')
        unicode_content = 'SELECT * FROM üsers WHERE nâme = "José"'
        
        with open(utf8_file, 'w', encoding='utf-8') as f:
            f.write(unicode_content)
        
        result = self.query_loader.load_query_from_file(utf8_file, encoding='utf-8')
        self.assertEqual(result.strip(), unicode_content)


class TestQueryLoaderIntegration(unittest.TestCase):
    """Integration tests for QueryLoader with other components."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.query_loader = QueryLoader()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up integration test fixtures."""
        shutil.rmtree(self.temp_dir)

    @patch('utils.config_loader.ConfigLoader')
    def test_config_integration(self, mock_config_loader):
        """Test integration with configuration loader."""
        mock_config = {
            'query_directory': self.temp_dir,
            'default_parameters': {'limit': 100, 'offset': 0}
        }
        mock_config_loader.return_value.get_section.return_value = mock_config
        
        # Initialize with config
        self.query_loader.load_configuration(mock_config_loader.return_value)
        
        self.assertEqual(self.query_loader.query_directory, self.temp_dir)

    @patch('utils.logger.Logger')
    def test_logging_integration(self, mock_logger):
        """Test integration with logging system."""
        # Test that query loading operations are logged
        query_file = os.path.join(self.temp_dir, 'test.sql')
        with open(query_file, 'w') as f:
            f.write('SELECT 1')
        
        self.query_loader.load_query_from_file(query_file)
        
        # Verify logging calls would be made
        self.assertTrue(os.path.exists(query_file))

    def test_database_connector_integration(self):
        """Test integration with database connector for query validation."""
        # Mock database connector
        mock_connector = Mock()
        mock_connector.validate_query_syntax.return_value = True
        
        self.query_loader.set_database_connector(mock_connector)
        
        query = 'SELECT * FROM users'
        is_valid = self.query_loader.validate_query_with_database(query)
        
        mock_connector.validate_query_syntax.assert_called_once_with(query)
        self.assertTrue(is_valid)


class TestQueryLoaderPerformance(unittest.TestCase):
    """Performance tests for QueryLoader."""
    
    def setUp(self):
        """Set up performance test fixtures."""
        self.query_loader = QueryLoader()
        self.temp_dir = tempfile.mkdtemp()
        
        # Create many query files for performance testing
        for i in range(100):
            query_file = os.path.join(self.temp_dir, f'query_{i:03d}.sql')
            with open(query_file, 'w') as f:
                f.write(f'SELECT * FROM table_{i} WHERE id = {{id}}')

    def tearDown(self):
        """Clean up performance test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_bulk_loading_performance(self):
        """Test performance of bulk query loading."""
        start_time = datetime.now()
        
        queries = self.query_loader.load_queries_from_directory(self.temp_dir)
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Should load 100 queries in reasonable time
        self.assertEqual(len(queries), 100)
        self.assertLess(execution_time, 5)  # Should complete within 5 seconds

    def test_parameter_substitution_performance(self):
        """Test performance of parameter substitution with many parameters."""
        template = 'SELECT * FROM users WHERE ' + ' AND '.join([f'col_{i} = {{param_{i}}}' for i in range(50)])
        parameters = {f'param_{i}': f'value_{i}' for i in range(50)}
        
        start_time = datetime.now()
        
        result = self.query_loader.parameterize_query(template, parameters)
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        self.assertIsNotNone(result)
        self.assertLess(execution_time, 1)  # Should complete within 1 second


if __name__ == '__main__':
    # Configure test discovery and execution
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestQueryLoader))
    suite.addTests(loader.loadTestsFromTestCase(TestQueryLoaderIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestQueryLoaderPerformance))
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with appropriate code
    exit(0 if result.wasSuccessful() else 1)