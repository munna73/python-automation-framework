"""
Unit tests for data_validator.py module.

This module contains comprehensive test cases for validating the data validation
functionality including data type validation, format validation, business rule
validation, and data quality checks.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date
from decimal import Decimal
import json
import re
import sys
import os

# Add the utils directory to the path to import data_validator
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'utils'))

from data_validator import DataValidator
from custom_exceptions import ValidationError, DataQualityError, ConfigurationError


class TestDataValidator(unittest.TestCase):
    """Test cases for DataValidator class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.validator = DataValidator()
        
        # Sample test data
        self.sample_data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Davis'],
            'email': ['john@example.com', 'jane@test.com', 'bob@company.org', 'alice@domain.net', 'charlie@mail.com'],
            'age': [25, 30, 35, 28, 42],
            'salary': [50000.00, 75000.50, 80000.25, 65000.75, 90000.00],
            'hire_date': ['2023-01-15', '2022-03-20', '2021-07-10', '2023-05-12', '2020-11-08'],
            'is_active': [True, True, False, True, True],
            'department': ['IT', 'HR', 'Finance', 'IT', 'Marketing']
        }
        
        self.df_sample = pd.DataFrame(self.sample_data)
        
        # Sample validation rules
        self.validation_rules = {
            'id': {
                'type': 'integer',
                'required': True,
                'unique': True,
                'min_value': 1
            },
            'name': {
                'type': 'string',
                'required': True,
                'min_length': 2,
                'max_length': 50,
                'pattern': r'^[A-Za-z\s]+$'
            },
            'email': {
                'type': 'string',
                'required': True,
                'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            },
            'age': {
                'type': 'integer',
                'required': True,
                'min_value': 18,
                'max_value': 100
            },
            'salary': {
                'type': 'float',
                'required': True,
                'min_value': 0,
                'max_value': 1000000
            },
            'hire_date': {
                'type': 'date',
                'required': True,
                'date_format': '%Y-%m-%d'
            },
            'is_active': {
                'type': 'boolean',
                'required': True
            },
            'department': {
                'type': 'string',
                'required': True,
                'allowed_values': ['IT', 'HR', 'Finance', 'Marketing', 'Sales']
            }
        }

    def tearDown(self):
        """Clean up after each test method."""
        self.validator = None

    def test_init_default(self):
        """Test DataValidator initialization with default parameters."""
        validator = DataValidator()
        self.assertIsNotNone(validator)
        self.assertEqual(validator.strict_mode, False)
        self.assertEqual(validator.validation_rules, {})

    def test_init_with_parameters(self):
        """Test DataValidator initialization with custom parameters."""
        rules = {'field1': {'type': 'string'}}
        validator = DataValidator(validation_rules=rules, strict_mode=True)
        self.assertEqual(validator.strict_mode, True)
        self.assertEqual(validator.validation_rules, rules)

    def test_validate_data_type_string_valid(self):
        """Test string data type validation with valid data."""
        test_data = ['hello', 'world', 'test']
        result = self.validator._validate_data_type(test_data, 'string')
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_data_type_string_invalid(self):
        """Test string data type validation with invalid data."""
        test_data = ['hello', 123, 'test']
        result = self.validator._validate_data_type(test_data, 'string')
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_data_type_integer_valid(self):
        """Test integer data type validation with valid data."""
        test_data = [1, 2, 3, 4, 5]
        result = self.validator._validate_data_type(test_data, 'integer')
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_data_type_integer_invalid(self):
        """Test integer data type validation with invalid data."""
        test_data = [1, 2.5, 3, 'four', 5]
        result = self.validator._validate_data_type(test_data, 'integer')
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_data_type_float_valid(self):
        """Test float data type validation with valid data."""
        test_data = [1.0, 2.5, 3.14, 4, 5.0]
        result = self.validator._validate_data_type(test_data, 'float')
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_data_type_float_invalid(self):
        """Test float data type validation with invalid data."""
        test_data = [1.0, 'not_float', 3.14]
        result = self.validator._validate_data_type(test_data, 'float')
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_data_type_boolean_valid(self):
        """Test boolean data type validation with valid data."""
        test_data = [True, False, True, False]
        result = self.validator._validate_data_type(test_data, 'boolean')
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_data_type_boolean_invalid(self):
        """Test boolean data type validation with invalid data."""
        test_data = [True, False, 'yes', 1]
        result = self.validator._validate_data_type(test_data, 'boolean')
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_data_type_date_valid(self):
        """Test date data type validation with valid data."""
        test_data = ['2023-01-15', '2022-12-31', '2024-02-29']
        result = self.validator._validate_data_type(test_data, 'date', date_format='%Y-%m-%d')
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_data_type_date_invalid(self):
        """Test date data type validation with invalid data."""
        test_data = ['2023-01-15', 'invalid_date', '2024-13-45']
        result = self.validator._validate_data_type(test_data, 'date', date_format='%Y-%m-%d')
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_required_fields_valid(self):
        """Test required field validation with valid data."""
        data = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        rules = {'col1': {'required': True}, 'col2': {'required': True}}
        result = self.validator._validate_required_fields(data, rules)
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_required_fields_missing_values(self):
        """Test required field validation with missing values."""
        data = pd.DataFrame({'col1': [1, None, 3], 'col2': ['a', 'b', '']})
        rules = {'col1': {'required': True}, 'col2': {'required': True}}
        result = self.validator._validate_required_fields(data, rules)
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_unique_constraints_valid(self):
        """Test unique constraint validation with valid data."""
        data = pd.DataFrame({'id': [1, 2, 3, 4, 5]})
        rules = {'id': {'unique': True}}
        result = self.validator._validate_unique_constraints(data, rules)
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_unique_constraints_invalid(self):
        """Test unique constraint validation with duplicate values."""
        data = pd.DataFrame({'id': [1, 2, 3, 2, 5]})
        rules = {'id': {'unique': True}}
        result = self.validator._validate_unique_constraints(data, rules)
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_range_constraints_valid(self):
        """Test range constraint validation with valid data."""
        data = pd.DataFrame({'age': [25, 30, 35, 40]})
        rules = {'age': {'min_value': 18, 'max_value': 65}}
        result = self.validator._validate_range_constraints(data, rules)
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_range_constraints_invalid(self):
        """Test range constraint validation with out-of-range values."""
        data = pd.DataFrame({'age': [16, 30, 35, 70]})
        rules = {'age': {'min_value': 18, 'max_value': 65}}
        result = self.validator._validate_range_constraints(data, rules)
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_length_constraints_valid(self):
        """Test length constraint validation with valid data."""
        data = pd.DataFrame({'name': ['John', 'Jane', 'Bob']})
        rules = {'name': {'min_length': 2, 'max_length': 10}}
        result = self.validator._validate_length_constraints(data, rules)
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_length_constraints_invalid(self):
        """Test length constraint validation with invalid lengths."""
        data = pd.DataFrame({'name': ['J', 'Jane', 'VeryLongNameThatExceedsLimit']})
        rules = {'name': {'min_length': 2, 'max_length': 10}}
        result = self.validator._validate_length_constraints(data, rules)
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_pattern_constraints_valid(self):
        """Test pattern constraint validation with valid data."""
        data = pd.DataFrame({'email': ['test@example.com', 'user@domain.org']})
        rules = {'email': {'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'}}
        result = self.validator._validate_pattern_constraints(data, rules)
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_pattern_constraints_invalid(self):
        """Test pattern constraint validation with invalid patterns."""
        data = pd.DataFrame({'email': ['test@example.com', 'invalid_email']})
        rules = {'email': {'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'}}
        result = self.validator._validate_pattern_constraints(data, rules)
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_allowed_values_valid(self):
        """Test allowed values validation with valid data."""
        data = pd.DataFrame({'department': ['IT', 'HR', 'Finance']})
        rules = {'department': {'allowed_values': ['IT', 'HR', 'Finance', 'Marketing']}}
        result = self.validator._validate_allowed_values(data, rules)
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_allowed_values_invalid(self):
        """Test allowed values validation with invalid values."""
        data = pd.DataFrame({'department': ['IT', 'HR', 'InvalidDept']})
        rules = {'department': {'allowed_values': ['IT', 'HR', 'Finance', 'Marketing']}}
        result = self.validator._validate_allowed_values(data, rules)
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_dataframe_complete_valid(self):
        """Test complete DataFrame validation with valid data."""
        self.validator.validation_rules = self.validation_rules
        result = self.validator.validate_dataframe(self.df_sample)
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_dataframe_with_errors(self):
        """Test complete DataFrame validation with invalid data."""
        # Create invalid data
        invalid_data = self.sample_data.copy()
        invalid_data['age'] = [25, 30, 15, 28, 150]  # Invalid ages
        invalid_data['email'] = ['john@example.com', 'invalid_email', 'bob@company.org', 'alice@domain.net', 'charlie@mail.com']
        
        df_invalid = pd.DataFrame(invalid_data)
        self.validator.validation_rules = self.validation_rules
        
        result = self.validator.validate_dataframe(df_invalid)
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_dict_data_valid(self):
        """Test dictionary data validation with valid data."""
        dict_data = {
            'name': 'John Doe',
            'age': 30,
            'email': 'john@example.com'
        }
        rules = {
            'name': {'type': 'string', 'required': True},
            'age': {'type': 'integer', 'min_value': 18, 'max_value': 100},
            'email': {'type': 'string', 'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'}
        }
        
        result = self.validator.validate_dict(dict_data, rules)
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_dict_data_invalid(self):
        """Test dictionary data validation with invalid data."""
        dict_data = {
            'name': '',  # Invalid: empty string
            'age': 15,   # Invalid: below minimum
            'email': 'invalid_email'  # Invalid: doesn't match pattern
        }
        rules = {
            'name': {'type': 'string', 'required': True, 'min_length': 1},
            'age': {'type': 'integer', 'min_value': 18, 'max_value': 100},
            'email': {'type': 'string', 'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'}
        }
        
        result = self.validator.validate_dict(dict_data, rules)
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_json_valid(self):
        """Test JSON validation with valid JSON string."""
        json_data = '{"name": "John", "age": 30}'
        result = self.validator.validate_json(json_data)
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_json_invalid(self):
        """Test JSON validation with invalid JSON string."""
        json_data = '{"name": "John", "age": 30'  # Missing closing brace
        result = self.validator.validate_json(json_data)
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_schema_compliance_valid(self):
        """Test schema compliance validation with valid data."""
        data = {"name": "John", "age": 30}
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "number"}
            },
            "required": ["name", "age"]
        }
        
        result = self.validator.validate_schema_compliance(data, schema)
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_check_data_quality_complete_data(self):
        """Test data quality check with complete data."""
        result = self.validator.check_data_quality(self.df_sample)
        self.assertIsInstance(result, dict)
        self.assertIn('completeness', result)
        self.assertIn('consistency', result)
        self.assertIn('accuracy', result)

    def test_check_data_quality_missing_data(self):
        """Test data quality check with missing data."""
        # Create data with missing values
        data_with_nulls = self.sample_data.copy()
        data_with_nulls['name'][1] = None
        data_with_nulls['age'][2] = None
        
        df_with_nulls = pd.DataFrame(data_with_nulls)
        result = self.validator.check_data_quality(df_with_nulls)
        
        self.assertIsInstance(result, dict)
        self.assertIn('completeness', result)
        self.assertLess(result['completeness']['overall_score'], 1.0)

    def test_validate_business_rules_custom(self):
        """Test custom business rules validation."""
        def age_salary_rule(row):
            """Custom rule: salary should increase with age."""
            if row['age'] > 40 and row['salary'] < 60000:
                return False, "Salary too low for age"
            return True, ""
        
        business_rules = [age_salary_rule]
        result = self.validator.validate_business_rules(self.df_sample, business_rules)
        
        self.assertIsInstance(result, dict)
        self.assertIn('is_valid', result)
        self.assertIn('errors', result)

    def test_generate_validation_report(self):
        """Test validation report generation."""
        self.validator.validation_rules = self.validation_rules
        validation_result = self.validator.validate_dataframe(self.df_sample)
        
        report = self.validator.generate_validation_report(validation_result, self.df_sample)
        
        self.assertIsInstance(report, dict)
        self.assertIn('summary', report)
        self.assertIn('details', report)
        self.assertIn('recommendations', report)

    def test_strict_mode_enabled(self):
        """Test validation behavior with strict mode enabled."""
        validator = DataValidator(strict_mode=True)
        
        # Test with missing required field
        data = pd.DataFrame({'col1': [1, 2, None]})
        rules = {'col1': {'required': True}}
        
        with self.assertRaises(ValidationError):
            validator._validate_required_fields(data, rules, raise_on_error=True)

    def test_custom_validation_function(self):
        """Test custom validation function integration."""
        def custom_validator(value):
            return value % 2 == 0, "Value must be even"
        
        data = pd.DataFrame({'numbers': [2, 4, 5, 8]})
        result = self.validator._apply_custom_validation(data['numbers'], custom_validator)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_with_null_handling(self):
        """Test validation with different null value handling strategies."""
        data_with_nulls = pd.DataFrame({
            'col1': [1, 2, None, 4],
            'col2': ['a', 'b', '', 'd']
        })
        
        rules = {
            'col1': {'type': 'integer', 'required': True},
            'col2': {'type': 'string', 'required': True, 'min_length': 1}
        }
        
        result = self.validator.validate_dataframe(data_with_nulls, rules)
        self.assertFalse(result['is_valid'])

    def test_performance_with_large_dataset(self):
        """Test validation performance with larger dataset."""
        # Create a larger dataset
        large_data = {
            'id': list(range(1, 1001)),
            'name': [f'User_{i}' for i in range(1, 1001)],
            'value': [i * 1.5 for i in range(1, 1001)]
        }
        large_df = pd.DataFrame(large_data)
        
        rules = {
            'id': {'type': 'integer', 'unique': True},
            'name': {'type': 'string', 'required': True},
            'value': {'type': 'float', 'min_value': 0}
        }
        
        start_time = datetime.now()
        result = self.validator.validate_dataframe(large_df, rules)
        end_time = datetime.now()
        
        # Should complete within reasonable time (adjust threshold as needed)
        execution_time = (end_time - start_time).total_seconds()
        self.assertLess(execution_time, 10)  # Should complete within 10 seconds
        self.assertTrue(result['is_valid'])

    @patch('utils.logger.Logger')
    def test_logging_integration(self, mock_logger):
        """Test integration with logging system."""
        self.validator.validation_rules = self.validation_rules
        result = self.validator.validate_dataframe(self.df_sample)
        
        # Verify that logging methods would be called (if logger is integrated)
        self.assertTrue(result['is_valid'])

    def test_error_aggregation(self):
        """Test error message aggregation and formatting."""
        # Create data with multiple types of errors
        invalid_data = {
            'id': [1, 2, 2, 4],  # Duplicate
            'name': ['', 'Jane', 'Bob', 'Alice'],  # Empty string
            'age': [25, 15, 35, 150],  # Out of range
            'email': ['john@example.com', 'invalid', 'bob@company.org', 'alice@domain.net']  # Invalid format
        }
        
        df_invalid = pd.DataFrame(invalid_data)
        rules = {
            'id': {'type': 'integer', 'unique': True},
            'name': {'type': 'string', 'required': True, 'min_length': 1},
            'age': {'type': 'integer', 'min_value': 18, 'max_value': 100},
            'email': {'type': 'string', 'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'}
        }
        
        result = self.validator.validate_dataframe(df_invalid, rules)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)
        
        # Check that errors are properly categorized
        error_types = [error.get('type', 'unknown') for error in result['errors']]
        self.assertIn('uniqueness', error_types)
        self.assertIn('pattern', error_types)
        self.assertIn('range', error_types)


class TestDataValidatorEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions for DataValidator."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.validator = DataValidator()

    def test_empty_dataframe(self):
        """Test validation with empty DataFrame."""
        empty_df = pd.DataFrame()
        rules = {'col1': {'type': 'string'}}
        
        result = self.validator.validate_dataframe(empty_df, rules)
        self.assertFalse(result['is_valid'])

    def test_none_input(self):
        """Test validation with None input."""
        with self.assertRaises(ValueError):
            self.validator.validate_dataframe(None, {})

    def test_invalid_rules_format(self):
        """Test validation with invalid rules format."""
        data = pd.DataFrame({'col1': [1, 2, 3]})
        invalid_rules = "not_a_dict"
        
        with self.assertRaises(ValueError):
            self.validator.validate_dataframe(data, invalid_rules)

    def test_unsupported_data_type(self):
        """Test validation with unsupported data type."""
        data = pd.DataFrame({'col1': [1, 2, 3]})
        rules = {'col1': {'type': 'unsupported_type'}}
        
        result = self.validator.validate_dataframe(data, rules)
        self.assertFalse(result['is_valid'])

    def test_circular_validation_rules(self):
        """Test handling of potentially circular validation dependencies."""
        # This is a conceptual test - implement based on actual validator capabilities
        data = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        rules = {
            'col1': {'type': 'integer', 'depends_on': 'col2'},
            'col2': {'type': 'string', 'depends_on': 'col1'}
        }
        
        # Should handle gracefully without infinite loops
        result = self.validator.validate_dataframe(data, rules)
        self.assertIsInstance(result, dict)


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)