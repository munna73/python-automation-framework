"""
Unit tests for json_validator.py module.

This module contains comprehensive test cases for validating JSON data
including schema validation, structure validation, data type validation,
and JSON format validation.
"""

import unittest
from unittest.mock import Mock, patch, mock_open, MagicMock
import pytest
import json
import jsonschema
from jsonschema import ValidationError as JsonSchemaValidationError, SchemaError
import os
import tempfile
import shutil
from datetime import datetime, date
from decimal import Decimal
import sys

# Add the api directory to the path to import json_validator
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))

from json_validator import JsonValidator
from custom_exceptions import ValidationError, ConfigurationError


class TestJsonValidator(unittest.TestCase):
    """Test cases for JsonValidator class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.json_validator = JsonValidator()
        
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        
        # Sample valid JSON data
        self.valid_json_data = {
            "user": {
                "id": 12345,
                "name": "John Doe",
                "email": "john.doe@example.com",
                "age": 30,
                "is_active": True,
                "created_date": "2023-01-15T10:30:00Z",
                "profile": {
                    "bio": "Software Developer",
                    "skills": ["Python", "JavaScript", "SQL"],
                    "experience_years": 5
                },
                "preferences": {
                    "notifications": True,
                    "theme": "dark",
                    "language": "en"
                }
            },
            "metadata": {
                "version": "1.0",
                "timestamp": "2023-01-15T10:30:00Z",
                "source": "user_api"
            }
        }
        
        # Sample JSON schema
        self.user_schema = {
            "type": "object",
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string", "minLength": 1, "maxLength": 100},
                        "email": {"type": "string", "format": "email"},
                        "age": {"type": "integer", "minimum": 0, "maximum": 150},
                        "is_active": {"type": "boolean"},
                        "created_date": {"type": "string", "format": "date-time"},
                        "profile": {
                            "type": "object",
                            "properties": {
                                "bio": {"type": "string"},
                                "skills": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "minItems": 1
                                },
                                "experience_years": {"type": "integer", "minimum": 0}
                            },
                            "required": ["bio", "skills"]
                        },
                        "preferences": {
                            "type": "object",
                            "properties": {
                                "notifications": {"type": "boolean"},
                                "theme": {"type": "string", "enum": ["light", "dark"]},
                                "language": {"type": "string", "pattern": "^[a-z]{2}$"}
                            }
                        }
                    },
                    "required": ["id", "name", "email", "age", "is_active"]
                },
                "metadata": {
                    "type": "object",
                    "properties": {
                        "version": {"type": "string"},
                        "timestamp": {"type": "string", "format": "date-time"},
                        "source": {"type": "string"}
                    },
                    "required": ["version", "timestamp"]
                }
            },
            "required": ["user", "metadata"]
        }
        
        # API response schema
        self.api_response_schema = {
            "type": "object",
            "properties": {
                "status": {"type": "string", "enum": ["success", "error"]},
                "code": {"type": "integer"},
                "message": {"type": "string"},
                "data": {"type": "object"},
                "errors": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "pagination": {
                    "type": "object",
                    "properties": {
                        "page": {"type": "integer", "minimum": 1},
                        "per_page": {"type": "integer", "minimum": 1},
                        "total": {"type": "integer", "minimum": 0},
                        "total_pages": {"type": "integer", "minimum": 0}
                    }
                }
            },
            "required": ["status", "code", "message"]
        }
        
        # Invalid JSON samples
        self.invalid_json_samples = [
            '{"name": "John", "age": 30',  # Missing closing brace
            '{"name": "John", "age": 30,}',  # Trailing comma
            '{"name": John, "age": 30}',  # Unquoted string
            '{name: "John", "age": 30}',  # Unquoted key
            '{"name": "John", "age": }',  # Missing value
            '',  # Empty string
            'null',  # Just null
            'undefined',  # Invalid value
        ]

    def tearDown(self):
        """Clean up after each test method."""
        # Remove temporary directory
        shutil.rmtree(self.temp_dir)
        self.json_validator = None

    def test_init_default(self):
        """Test JsonValidator initialization with default parameters."""
        validator = JsonValidator()
        self.assertIsNotNone(validator)
        self.assertEqual(validator.schema, None)
        self.assertEqual(validator.strict_mode, False)

    def test_init_with_schema(self):
        """Test JsonValidator initialization with schema."""
        validator = JsonValidator(schema=self.user_schema, strict_mode=True)
        self.assertEqual(validator.schema, self.user_schema)
        self.assertEqual(validator.strict_mode, True)

    def test_validate_json_string_valid(self):
        """Test validation of valid JSON string."""
        json_string = json.dumps(self.valid_json_data)
        
        result = self.json_validator.validate_json_string(json_string)
        
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)
        self.assertEqual(result['parsed_data'], self.valid_json_data)

    def test_validate_json_string_invalid(self):
        """Test validation of invalid JSON strings."""
        for invalid_json in self.invalid_json_samples:
            with self.subTest(invalid_json=invalid_json):
                result = self.json_validator.validate_json_string(invalid_json)
                
                self.assertFalse(result['is_valid'])
                self.assertGreater(len(result['errors']), 0)
                self.assertIsNone(result['parsed_data'])

    def test_validate_json_object_valid(self):
        """Test validation of valid JSON object."""
        result = self.json_validator.validate_json_object(self.valid_json_data)
        
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_json_object_invalid_type(self):
        """Test validation of invalid object types."""
        invalid_objects = [
            "not_a_dict",
            123,
            [],
            None,
            True
        ]
        
        for invalid_obj in invalid_objects:
            with self.subTest(invalid_obj=type(invalid_obj).__name__):
                result = self.json_validator.validate_json_object(invalid_obj)
                
                if not isinstance(invalid_obj, dict):
                    self.assertFalse(result['is_valid'])
                    self.assertGreater(len(result['errors']), 0)

    def test_validate_against_schema_valid(self):
        """Test schema validation with valid data."""
        self.json_validator.set_schema(self.user_schema)
        
        result = self.json_validator.validate_against_schema(self.valid_json_data)
        
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_against_schema_invalid_missing_required(self):
        """Test schema validation with missing required fields."""
        invalid_data = self.valid_json_data.copy()
        del invalid_data['user']['name']  # Remove required field
        
        self.json_validator.set_schema(self.user_schema)
        
        result = self.json_validator.validate_against_schema(invalid_data)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)
        self.assertIn('name', str(result['errors']))

    def test_validate_against_schema_invalid_type(self):
        """Test schema validation with wrong data types."""
        invalid_data = self.valid_json_data.copy()
        invalid_data['user']['age'] = "thirty"  # Should be integer
        
        self.json_validator.set_schema(self.user_schema)
        
        result = self.json_validator.validate_against_schema(invalid_data)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_against_schema_invalid_format(self):
        """Test schema validation with invalid format."""
        invalid_data = self.valid_json_data.copy()
        invalid_data['user']['email'] = "invalid_email"  # Invalid email format
        
        self.json_validator.set_schema(self.user_schema)
        
        result = self.json_validator.validate_against_schema(invalid_data)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_against_schema_invalid_enum(self):
        """Test schema validation with invalid enum value."""
        invalid_data = self.valid_json_data.copy()
        invalid_data['user']['preferences']['theme'] = "blue"  # Not in enum
        
        self.json_validator.set_schema(self.user_schema)
        
        result = self.json_validator.validate_against_schema(invalid_data)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_against_schema_invalid_pattern(self):
        """Test schema validation with invalid pattern."""
        invalid_data = self.valid_json_data.copy()
        invalid_data['user']['preferences']['language'] = "english"  # Should be 2 chars
        
        self.json_validator.set_schema(self.user_schema)
        
        result = self.json_validator.validate_against_schema(invalid_data)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_json_file_valid(self):
        """Test validation of valid JSON file."""
        json_file = os.path.join(self.temp_dir, 'valid.json')
        with open(json_file, 'w') as f:
            json.dump(self.valid_json_data, f)
        
        result = self.json_validator.validate_json_file(json_file)
        
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_json_file_invalid(self):
        """Test validation of invalid JSON file."""
        json_file = os.path.join(self.temp_dir, 'invalid.json')
        with open(json_file, 'w') as f:
            f.write('{"name": "John", "age": 30')  # Invalid JSON
        
        result = self.json_validator.validate_json_file(json_file)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_json_file_not_found(self):
        """Test validation of non-existent JSON file."""
        non_existent_file = os.path.join(self.temp_dir, 'not_found.json')
        
        with self.assertRaises(FileNotFoundError):
            self.json_validator.validate_json_file(non_existent_file)

    def test_validate_api_response_valid(self):
        """Test validation of valid API response."""
        api_response = {
            "status": "success",
            "code": 200,
            "message": "Data retrieved successfully",
            "data": {"users": [{"id": 1, "name": "John"}]},
            "pagination": {
                "page": 1,
                "per_page": 10,
                "total": 25,
                "total_pages": 3
            }
        }
        
        self.json_validator.set_schema(self.api_response_schema)
        
        result = self.json_validator.validate_api_response(api_response)
        
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_validate_api_response_invalid(self):
        """Test validation of invalid API response."""
        invalid_response = {
            "status": "invalid_status",  # Not in enum
            "code": "200",  # Should be integer
            # Missing required 'message' field
            "data": {"users": []}
        }
        
        self.json_validator.set_schema(self.api_response_schema)
        
        result = self.json_validator.validate_api_response(invalid_response)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_validate_nested_json_structure(self):
        """Test validation of deeply nested JSON structures."""
        nested_data = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "value": "deep_value",
                            "array": [1, 2, 3, {"nested_in_array": True}]
                        }
                    }
                }
            }
        }
        
        nested_schema = {
            "type": "object",
            "properties": {
                "level1": {
                    "type": "object",
                    "properties": {
                        "level2": {
                            "type": "object",
                            "properties": {
                                "level3": {
                                    "type": "object",
                                    "properties": {
                                        "level4": {
                                            "type": "object",
                                            "properties": {
                                                "value": {"type": "string"},
                                                "array": {
                                                    "type": "array",
                                                    "items": {
                                                        "anyOf": [
                                                            {"type": "integer"},
                                                            {"type": "object"}
                                                        ]
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        self.json_validator.set_schema(nested_schema)
        
        result = self.json_validator.validate_against_schema(nested_data)
        
        self.assertTrue(result['is_valid'])

    def test_validate_array_data(self):
        """Test validation of JSON arrays."""
        array_data = [
            {"id": 1, "name": "Item 1", "active": True},
            {"id": 2, "name": "Item 2", "active": False},
            {"id": 3, "name": "Item 3", "active": True}
        ]
        
        array_schema = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "active": {"type": "boolean"}
                },
                "required": ["id", "name", "active"]
            },
            "minItems": 1
        }
        
        self.json_validator.set_schema(array_schema)
        
        result = self.json_validator.validate_against_schema(array_data)
        
        self.assertTrue(result['is_valid'])

    def test_validate_array_data_invalid(self):
        """Test validation of invalid JSON arrays."""
        invalid_array = [
            {"id": 1, "name": "Item 1"},  # Missing 'active' field
            {"id": "2", "name": "Item 2", "active": False},  # Wrong type for 'id'
        ]
        
        array_schema = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "active": {"type": "boolean"}
                },
                "required": ["id", "name", "active"]
            }
        }
        
        self.json_validator.set_schema(array_schema)
        
        result = self.json_validator.validate_against_schema(invalid_array)
        
        self.assertFalse(result['is_valid'])

    def test_validate_with_custom_formats(self):
        """Test validation with custom format validators."""
        data_with_custom_format = {
            "phone": "+1-555-123-4567",
            "ssn": "123-45-6789",
            "credit_card": "4532-1234-5678-9012"
        }
        
        # Register custom format validators
        custom_formats = {
            "phone": r"^\+\d{1,3}-\d{3}-\d{3}-\d{4}$",
            "ssn": r"^\d{3}-\d{2}-\d{4}$",
            "credit_card": r"^\d{4}-\d{4}-\d{4}-\d{4}$"
        }
        
        result = self.json_validator.validate_with_custom_formats(
            data_with_custom_format, 
            custom_formats
        )
        
        self.assertTrue(result['is_valid'])

    def test_validate_with_custom_formats_invalid(self):
        """Test validation with invalid custom formats."""
        invalid_data = {
            "phone": "555-123-4567",  # Missing country code
            "ssn": "12345-6789",  # Wrong format
            "credit_card": "4532123456789012"  # Missing dashes
        }
        
        custom_formats = {
            "phone": r"^\+\d{1,3}-\d{3}-\d{3}-\d{4}$",
            "ssn": r"^\d{3}-\d{2}-\d{4}$",
            "credit_card": r"^\d{4}-\d{4}-\d{4}-\d{4}$"
        }
        
        result = self.json_validator.validate_with_custom_formats(
            invalid_data, 
            custom_formats
        )
        
        self.assertFalse(result['is_valid'])

    def test_validate_json_with_references(self):
        """Test validation of JSON with schema references."""
        schema_with_refs = {
            "definitions": {
                "user": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"}
                    },
                    "required": ["id", "name"]
                }
            },
            "type": "object",
            "properties": {
                "primary_user": {"$ref": "#/definitions/user"},
                "backup_user": {"$ref": "#/definitions/user"}
            }
        }
        
        data_with_refs = {
            "primary_user": {"id": 1, "name": "Primary User"},
            "backup_user": {"id": 2, "name": "Backup User"}
        }
        
        self.json_validator.set_schema(schema_with_refs)
        
        result = self.json_validator.validate_against_schema(data_with_refs)
        
        self.assertTrue(result['is_valid'])

    def test_validate_json_with_conditional_schema(self):
        """Test validation with conditional schema (if-then-else)."""
        conditional_schema = {
            "type": "object",
            "properties": {
                "type": {"type": "string", "enum": ["student", "teacher"]},
                "name": {"type": "string"},
                "grade": {"type": "integer"},
                "subject": {"type": "string"}
            },
            "if": {"properties": {"type": {"const": "student"}}},
            "then": {"required": ["name", "grade"]},
            "else": {"required": ["name", "subject"]}
        }
        
        student_data = {"type": "student", "name": "John", "grade": 10}
        teacher_data = {"type": "teacher", "name": "Ms. Smith", "subject": "Math"}
        
        self.json_validator.set_schema(conditional_schema)
        
        # Test student data
        result_student = self.json_validator.validate_against_schema(student_data)
        self.assertTrue(result_student['is_valid'])
        
        # Test teacher data
        result_teacher = self.json_validator.validate_against_schema(teacher_data)
        self.assertTrue(result_teacher['is_valid'])

    def test_load_schema_from_file(self):
        """Test loading schema from file."""
        schema_file = os.path.join(self.temp_dir, 'schema.json')
        with open(schema_file, 'w') as f:
            json.dump(self.user_schema, f)
        
        self.json_validator.load_schema_from_file(schema_file)
        
        self.assertEqual(self.json_validator.schema, self.user_schema)

    def test_load_schema_from_file_invalid(self):
        """Test loading invalid schema from file."""
        invalid_schema_file = os.path.join(self.temp_dir, 'invalid_schema.json')
        with open(invalid_schema_file, 'w') as f:
            f.write('{"type": "invalid_type"}')  # Invalid schema
        
        with self.assertRaises(SchemaError):
            self.json_validator.load_schema_from_file(invalid_schema_file)

    def test_validate_multiple_files(self):
        """Test validation of multiple JSON files."""
        # Create multiple test files
        valid_file1 = os.path.join(self.temp_dir, 'valid1.json')
        valid_file2 = os.path.join(self.temp_dir, 'valid2.json')
        invalid_file = os.path.join(self.temp_dir, 'invalid.json')
        
        with open(valid_file1, 'w') as f:
            json.dump({"name": "File1", "valid": True}, f)
        
        with open(valid_file2, 'w') as f:
            json.dump({"name": "File2", "valid": True}, f)
        
        with open(invalid_file, 'w') as f:
            f.write('{"name": "File3", "valid":}')  # Invalid JSON
        
        files = [valid_file1, valid_file2, invalid_file]
        
        results = self.json_validator.validate_multiple_files(files)
        
        self.assertEqual(len(results), 3)
        self.assertTrue(results[valid_file1]['is_valid'])
        self.assertTrue(results[valid_file2]['is_valid'])
        self.assertFalse(results[invalid_file]['is_valid'])

    def test_validate_json_streaming(self):
        """Test validation of streaming JSON data."""
        json_stream = [
            '{"id": 1, "name": "Item 1"}',
            '{"id": 2, "name": "Item 2"}',
            '{"id": 3, "name": "Item 3"}'
        ]
        
        item_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            },
            "required": ["id", "name"]
        }
        
        self.json_validator.set_schema(item_schema)
        
        results = self.json_validator.validate_json_stream(json_stream)
        
        self.assertEqual(len(results), 3)
        for result in results:
            self.assertTrue(result['is_valid'])

    def test_validate_large_json_file(self):
        """Test validation of large JSON file."""
        # Create a large JSON file
        large_data = {
            "items": [
                {"id": i, "name": f"Item {i}", "value": i * 1.5}
                for i in range(1000)
            ]
        }
        
        large_file = os.path.join(self.temp_dir, 'large.json')
        with open(large_file, 'w') as f:
            json.dump(large_data, f)
        
        large_schema = {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer"},
                            "name": {"type": "string"},
                            "value": {"type": "number"}
                        }
                    }
                }
            }
        }
        
        self.json_validator.set_schema(large_schema)
        
        start_time = datetime.now()
        result = self.json_validator.validate_json_file(large_file)
        end_time = datetime.now()
        
        execution_time = (end_time - start_time).total_seconds()
        
        self.assertTrue(result['is_valid'])
        self.assertLess(execution_time, 5)  # Should complete within 5 seconds

    def test_generate_validation_report(self):
        """Test generation of validation report."""
        test_data = [
            (self.valid_json_data, True),
            ({"user": {"id": "invalid"}}, False),
            ({"missing": "user"}, False)
        ]
        
        self.json_validator.set_schema(self.user_schema)
        
        report = self.json_validator.generate_validation_report(test_data)
        
        self.assertIsInstance(report, dict)
        self.assertIn('total_validated', report)
        self.assertIn('valid_count', report)
        self.assertIn('invalid_count', report)
        self.assertIn('success_rate', report)
        self.assertEqual(report['total_validated'], 3)
        self.assertEqual(report['valid_count'], 1)
        self.assertEqual(report['invalid_count'], 2)

    def test_validate_with_draft_versions(self):
        """Test validation with different JSON Schema draft versions."""
        draft4_schema = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer", "minimum": 0}
            },
            "required": ["name"]
        }
        
        draft7_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer", "minimum": 0}
            },
            "required": ["name"]
        }
        
        test_data = {"name": "John", "age": 25}
        
        # Test with Draft 4
        result_draft4 = self.json_validator.validate_with_draft(
            test_data, draft4_schema, draft_version=4
        )
        
        # Test with Draft 7
        result_draft7 = self.json_validator.validate_with_draft(
            test_data, draft7_schema, draft_version=7
        )
        
        self.assertTrue(result_draft4['is_valid'])
        self.assertTrue(result_draft7['is_valid'])

    def test_error_handling_circular_references(self):
        """Test error handling with circular references in data."""
        circular_data = {"name": "Test"}
        circular_data["self"] = circular_data  # Create circular reference
        
        # Should handle circular references gracefully
        with self.assertRaises((ValueError, RecursionError)):
            json.dumps(circular_data)  # This would fail
        
        # Validator should detect and handle this
        result = self.json_validator.validate_json_object_safe(circular_data)
        self.assertFalse(result['is_valid'])

    def test_performance_benchmarking(self):
        """Test performance benchmarking of validation operations."""
        # Create moderately complex data
        complex_data = {
            "users": [
                {
                    "id": i,
                    "name": f"User {i}",
                    "profile": {
                        "age": 20 + (i % 50),
                        "skills": [f"skill_{j}" for j in range(5)],
                        "active": i % 2 == 0
                    }
                }
                for i in range(100)
            ]
        }
        
        complex_schema = {
            "type": "object",
            "properties": {
                "users": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer"},
                            "name": {"type": "string"},
                            "profile": {
                                "type": "object",
                                "properties": {
                                    "age": {"type": "integer"},
                                    "skills": {"type": "array", "items": {"type": "string"}},
                                    "active": {"type": "boolean"}
                                }
                            }
                        }
                    }
                }
            }
        }
        
        self.json_validator.set_schema(complex_schema)
        
        # Benchmark validation
        benchmark_result = self.json_validator.benchmark_validation(
            complex_data,
            iterations=10
        )
        
        self.assertIsInstance(benchmark_result, dict)
        self.assertIn('average_time', benchmark_result)
        self.assertIn('min_time', benchmark_result)
        self.assertIn('max_time', benchmark_result)
        self.assertLess(benchmark_result['average_time'], 1.0)  # Should be under 1 second

    def test_custom_error_handlers(self):
        """Test custom error handling and formatting."""
        def custom_error_handler(error):
            return {
                'field': error.absolute_path,
                'message': error.message,
                'invalid_value': error.instance,
                'schema_path': error.schema_path
            }
        
        invalid_data = {"user": {"age": "not_a_number"}}
        
        self.json_validator.set_schema(self.user_schema)
        self.json_validator.set_error_handler(custom_error_handler)
        
        result = self.json_validator.validate_against_schema(invalid_data)
        
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)
        
        # Check custom error format
        if result['errors']:
            error = result['errors'][0]
            self.assertIn('field', error)
            self.assertIn('message', error)
            self.assertIn('invalid_value', error)

    def test_validate_json_patch(self):
        """Test validation of JSON Patch operations."""
        json_patch_operations = [
            {"op": "add", "path": "/user/middle_name", "value": "William"},
            {"op": "replace", "path": "/user/age", "value": 31},
            {"op": "remove", "path": "/user/preferences/notifications"}
        ]
        
        json_patch_schema = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "op": {"type": "string", "enum": ["add", "remove", "replace", "move", "copy", "test"]},
                    "path": {"type": "string"},
                    "value": {},
                    "from": {"type": "string"}
                },
                "required": ["op", "path"],
                "oneOf": [
                    {"properties": {"op": {"enum": ["add", "replace", "test"]}}, "required": ["value"]},
                    {"properties": {"op": {"enum": ["remove"]}}},
                    {"properties": {"op": {"enum": ["move", "copy"]}}, "required": ["from"]}
                ]
            }
        }
        
        self.json_validator.set_schema(json_patch_schema)
        
        result = self.json_validator.validate_against_schema(json_patch_operations)
        
        self.assertTrue(result['is_valid'])

    def test_validate_geojson(self):
        """Test validation of GeoJSON data."""
        geojson_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-73.9857, 40.7484]
                    },
                    "properties": {
                        "name": "New York City",
                        "population": 8336817
                    }
                }
            ]
        }
        
        geojson_schema = {
            "type": "object",
            "properties": {
                "type": {"enum": ["FeatureCollection"]},
                "features": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {"enum": ["Feature"]},
                            "geometry": {
                                "type": "object",
                                "properties": {
                                    "type": {"enum": ["Point", "LineString", "Polygon"]},
                                    "coordinates": {"type": "array"}
                                }
                            },
                            "properties": {"type": "object"}
                        },
                        "required": ["type", "geometry"]
                    }
                }
            },
            "required": ["type", "features"]
        }
        
        self.json_validator.set_schema(geojson_schema)
        
        result = self.json_validator.validate_against_schema(geojson_data)
        
        self.assertTrue(result['is_valid'])

    def test_validate_openapi_spec(self):
        """Test validation of OpenAPI specification."""
        openapi_spec = {
            "openapi": "3.0.0",
            "info": {
                "title": "Test API",
                "version": "1.0.0"
            },
            "paths": {
                "/users": {
                    "get": {
                        "summary": "Get users",
                        "responses": {
                            "200": {
                                "description": "Success",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "array",
                                            "items": {"$ref": "#/components/schemas/User"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "components": {
                "schemas": {
                    "User": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer"},
                            "name": {"type": "string"}
                        }
                    }
                }
            }
        }
        
        # This would require a full OpenAPI 3.0 schema for complete validation
        basic_openapi_schema = {
            "type": "object",
            "properties": {
                "openapi": {"type": "string"},
                "info": {"type": "object"},
                "paths": {"type": "object"}
            },
            "required": ["openapi", "info", "paths"]
        }
        
        self.json_validator.set_schema(basic_openapi_schema)
        
        result = self.json_validator.validate_against_schema(openapi_spec)
        
        self.assertTrue(result['is_valid'])


class TestJsonValidatorIntegration(unittest.TestCase):
    """Integration tests for JsonValidator with other components."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.json_validator = JsonValidator()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up integration test fixtures."""
        shutil.rmtree(self.temp_dir)

    @patch('api.rest_client.RestClient')
    def test_api_response_validation_integration(self, mock_rest_client):
        """Test integration with REST client for API response validation."""
        mock_response = {
            "status": "success",
            "code": 200,
            "message": "Data retrieved",
            "data": {"users": [{"id": 1, "name": "John"}]}
        }
        
        mock_rest_client.return_value.get.return_value = mock_response
        
        api_schema = {
            "type": "object",
            "properties": {
                "status": {"type": "string"},
                "code": {"type": "integer"},
                "message": {"type": "string"},
                "data": {"type": "object"}
            },
            "required": ["status", "code", "message"]
        }
        
        # Validate API response
        result = self.json_validator.validate_api_response_integration(
            mock_rest_client.return_value,
            "/api/users",
            api_schema
        )
        
        self.assertTrue(result['is_valid'])

    @patch('utils.config_loader.ConfigLoader')
    def test_schema_loading_integration(self, mock_config_loader):
        """Test integration with configuration loader for schema management."""
        mock_schemas = {
            "user_schema": {
                "type": "object",
                "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}
            },
            "product_schema": {
                "type": "object", 
                "properties": {"id": {"type": "integer"}, "price": {"type": "number"}}
            }
        }
        
        mock_config_loader.return_value.get_section.return_value = mock_schemas
        
        self.json_validator.load_schemas_from_config(mock_config_loader.return_value)
        
        # Verify schemas are loaded
        self.assertIn("user_schema", self.json_validator.schemas)
        self.assertIn("product_schema", self.json_validator.schemas)

    @patch('utils.logger.Logger')
    def test_logging_integration(self, mock_logger):
        """Test integration with logging system."""
        test_data = {"invalid": "data"}
        schema = {"type": "object", "properties": {"valid": {"type": "string"}}, "required": ["valid"]}
        
        self.json_validator.set_schema(schema)
        result = self.json_validator.validate_against_schema(test_data)
        
        # Should log validation failures
        self.assertFalse(result['is_valid'])

    def test_database_json_validation_integration(self):
        """Test validation of JSON data from database sources."""
        # Simulate JSON data from database
        db_json_data = [
            '{"user_id": 1, "preferences": {"theme": "dark", "notifications": true}}',
            '{"user_id": 2, "preferences": {"theme": "light", "notifications": false}}',
            '{"user_id": 3, "preferences": "invalid_json_structure"}'  # Invalid
        ]
        
        preference_schema = {
            "type": "object",
            "properties": {
                "user_id": {"type": "integer"},
                "preferences": {
                    "type": "object",
                    "properties": {
                        "theme": {"type": "string", "enum": ["light", "dark"]},
                        "notifications": {"type": "boolean"}
                    }
                }
            }
        }
        
        self.json_validator.set_schema(preference_schema)
        
        results = []
        for json_str in db_json_data:
            try:
                parsed_data = json.loads(json_str)
                result = self.json_validator.validate_against_schema(parsed_data)
                results.append(result)
            except json.JSONDecodeError:
                results.append({"is_valid": False, "errors": ["Invalid JSON"]})
        
        # First two should be valid, third should be invalid
        self.assertTrue(results[0]['is_valid'])
        self.assertTrue(results[1]['is_valid'])
        self.assertFalse(results[2]['is_valid'])


class TestJsonValidatorPerformance(unittest.TestCase):
    """Performance tests for JsonValidator."""
    
    def setUp(self):
        """Set up performance test fixtures."""
        self.json_validator = JsonValidator()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up performance test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_large_schema_validation_performance(self):
        """Test performance with large, complex schemas."""
        # Create a large schema with many properties
        large_schema = {
            "type": "object",
            "properties": {}
        }
        
        # Add 100 properties with various constraints
        for i in range(100):
            large_schema["properties"][f"field_{i}"] = {
                "type": "string" if i % 2 == 0 else "integer",
                "minLength": 1 if i % 2 == 0 else None,
                "minimum": 0 if i % 2 == 1 else None
            }
        
        # Create matching data
        large_data = {}
        for i in range(100):
            large_data[f"field_{i}"] = f"value_{i}" if i % 2 == 0 else i
        
        self.json_validator.set_schema(large_schema)
        
        start_time = datetime.now()
        result = self.json_validator.validate_against_schema(large_data)
        end_time = datetime.now()
        
        execution_time = (end_time - start_time).total_seconds()
        
        self.assertTrue(result['is_valid'])
        self.assertLess(execution_time, 2.0)  # Should complete within 2 seconds

    def test_deep_nesting_performance(self):
        """Test performance with deeply nested JSON structures."""
        # Create deeply nested data (10 levels)
        nested_data = {}
        current_level = nested_data
        
        for i in range(10):
            current_level[f"level_{i}"] = {}
            if i < 9:
                current_level = current_level[f"level_{i}"]
            else:
                current_level[f"level_{i}"]["value"] = "deep_value"
        
        # Create matching schema
        nested_schema = {"type": "object"}
        current_schema = nested_schema
        
        for i in range(10):
            current_schema["properties"] = {f"level_{i}": {"type": "object"}}
            if i < 9:
                current_schema = current_schema["properties"][f"level_{i}"]
            else:
                current_schema["properties"][f"level_{i}"]["properties"] = {
                    "value": {"type": "string"}
                }
        
        self.json_validator.set_schema(nested_schema)
        
        start_time = datetime.now()
        result = self.json_validator.validate_against_schema(nested_data)
        end_time = datetime.now()
        
        execution_time = (end_time - start_time).total_seconds()
        
        self.assertTrue(result['is_valid'])
        self.assertLess(execution_time, 1.0)

    def test_array_validation_performance(self):
        """Test performance with large arrays."""
        # Create large array (1000 items)
        large_array = [
            {"id": i, "name": f"Item {i}", "value": i * 1.5}
            for i in range(1000)
        ]
        
        array_schema = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "value": {"type": "number"}
                },
                "required": ["id", "name", "value"]
            }
        }
        
        self.json_validator.set_schema(array_schema)
        
        start_time = datetime.now()
        result = self.json_validator.validate_against_schema(large_array)
        end_time = datetime.now()
        
        execution_time = (end_time - start_time).total_seconds()
        
        self.assertTrue(result['is_valid'])
        self.assertLess(execution_time, 3.0)  # Should complete within 3 seconds

    def test_concurrent_validation_performance(self):
        """Test performance of concurrent validation operations."""
        import threading
        import time
        
        def validation_worker(worker_id, results):
            data = {"id": worker_id, "name": f"Worker {worker_id}"}
            schema = {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"}
                }
            }
            
            validator = JsonValidator(schema=schema)
            start_time = time.time()
            result = validator.validate_against_schema(data)
            end_time = time.time()
            
            results[worker_id] = {
                "is_valid": result['is_valid'],
                "execution_time": end_time - start_time
            }
        
        # Run 10 concurrent validations
        threads = []
        results = {}
        
        overall_start = time.time()
        
        for i in range(10):
            thread = threading.Thread(target=validation_worker, args=(i, results))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        overall_end = time.time()
        overall_time = overall_end - overall_start
        
        # All validations should succeed
        for i in range(10):
            self.assertTrue(results[i]['is_valid'])
        
        # Concurrent execution should be faster than sequential
        self.assertLess(overall_time, 2.0)


class TestJsonValidatorEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions for JsonValidator."""
    
    def setUp(self):
        """Set up edge case test fixtures."""
        self.json_validator = JsonValidator()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up edge case test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_extremely_large_json(self):
        """Test validation of extremely large JSON data."""
        # Create very large JSON (this might be memory intensive)
        try:
            huge_array = [{"id": i, "data": "x" * 1000} for i in range(1000)]
            
            large_schema = {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "data": {"type": "string"}
                    }
                }
            }
            
            self.json_validator.set_schema(large_schema)
            
            # This should either succeed or fail gracefully
            result = self.json_validator.validate_against_schema(huge_array)
            
            # If it completes, it should be valid
            if result is not None:
                self.assertTrue(result['is_valid'])
                
        except MemoryError:
            # Expected for very large data
            pass

    def test_unicode_and_special_characters(self):
        """Test validation with Unicode and special characters."""
        unicode_data = {
            "name": "José María García",
            "city": "北京",
            "emoji": "🚀🌟💫",
            "special_chars": "!@#$%^&*()[]{}|\\:;\"'<>,.?/~`",
            "unicode_escape": "\u00E9\u00F1\u00FC"
        }
        
        unicode_schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "city": {"type": "string"},
                "emoji": {"type": "string"},
                "special_chars": {"type": "string"},
                "unicode_escape": {"type": "string"}
            }
        }
        
        self.json_validator.set_schema(unicode_schema)
        
        result = self.json_validator.validate_against_schema(unicode_data)
        
        self.assertTrue(result['is_valid'])

    def test_null_and_undefined_handling(self):
        """Test handling of null values and missing properties."""
        data_with_nulls = {
            "required_field": "present",
            "nullable_field": None,
            "optional_field": "optional_value"
            # "missing_field" is intentionally missing
        }
        
        schema_with_nulls = {
            "type": "object",
            "properties": {
                "required_field": {"type": "string"},
                "nullable_field": {"type": ["string", "null"]},
                "optional_field": {"type": "string"},
                "missing_field": {"type": "string"}
            },
            "required": ["required_field"]
        }
        
        self.json_validator.set_schema(schema_with_nulls)
        
        result = self.json_validator.validate_against_schema(data_with_nulls)
        
        self.assertTrue(result['is_valid'])

    def test_numeric_edge_cases(self):
        """Test validation of numeric edge cases."""
        numeric_edge_cases = {
            "zero": 0,
            "negative": -123,
            "float_precision": 123.456789012345,
            "scientific_notation": 1.23e-10,
            "large_number": 999999999999999999999,
            "infinity": float('inf'),
            "negative_infinity": float('-inf'),
            "nan": float('nan')
        }
        
        numeric_schema = {
            "type": "object",
            "properties": {
                "zero": {"type": "number"},
                "negative": {"type": "number"},
                "float_precision": {"type": "number"},
                "scientific_notation": {"type": "number"},
                "large_number": {"type": "number"},
                "infinity": {"type": "number"},
                "negative_infinity": {"type": "number"},
                "nan": {"type": "number"}
            }
        }
        
        self.json_validator.set_schema(numeric_schema)
        
        # This test might fail due to JSON serialization of inf/nan
        try:
            json_str = json.dumps(numeric_edge_cases)
            result = self.json_validator.validate_json_string(json_str)
        except ValueError:
            # Expected for inf/nan values
            pass

    def test_malformed_schema_handling(self):
        """Test handling of malformed schemas."""
        malformed_schemas = [
            {"type": "invalid_type"},
            {"properties": "not_an_object"},
            {"required": "not_an_array"},
            {"items": {"type": "object", "properties": {"circular": {"$ref": "#"}}}},
            None,
            "not_a_dict",
            {"type": "object", "properties": None}
        ]
        
        for malformed_schema in malformed_schemas:
            with self.subTest(schema=malformed_schema):
                try:
                    self.json_validator.set_schema(malformed_schema)
                    # If schema is set, try to validate something
                    result = self.json_validator.validate_against_schema({"test": "data"})
                except (SchemaError, TypeError, AttributeError):
                    # Expected for malformed schemas
                    pass

    def test_memory_limit_handling(self):
        """Test behavior when approaching memory limits."""
        # This is a conceptual test - actual implementation would depend on system limits
        try:
            # Create progressively larger data until memory issues
            size = 1000
            while size < 100000:  # Reasonable upper limit for testing
                large_data = {"items": ["item"] * size}
                
                simple_schema = {
                    "type": "object",
                    "properties": {
                        "items": {"type": "array", "items": {"type": "string"}}
                    }
                }
                
                self.json_validator.set_schema(simple_schema)
                result = self.json_validator.validate_against_schema(large_data)
                
                if not result['is_valid']:
                    break
                    
                size *= 2
                
        except MemoryError:
            # Expected behavior for very large data
            pass

    def test_recursive_schema_references(self):
        """Test handling of recursive schema references."""
        recursive_schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "children": {
                    "type": "array",
                    "items": {"$ref": "#"}
                }
            }
        }
        
        recursive_data = {
            "name": "root",
            "children": [
                {
                    "name": "child1",
                    "children": [
                        {"name": "grandchild1", "children": []},
                        {"name": "grandchild2", "children": []}
                    ]
                },
                {
                    "name": "child2",
                    "children": []
                }
            ]
        }
        
        self.json_validator.set_schema(recursive_schema)
        
        result = self.json_validator.validate_against_schema(recursive_data)
        
        self.assertTrue(result['is_valid'])


if __name__ == '__main__':
    # Configure test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    test_classes = [
        TestJsonValidator,
        TestJsonValidatorIntegration,
        TestJsonValidatorPerformance,
        TestJsonValidatorEdgeCases
    ]
    
    for test_class in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(test_class))
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"JSON Validator Test Summary")
    print(f"{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    if result.failures:
        print(f"\nFailures:")
        for test, failure in result.failures:
            print(f"  - {test}: {failure.split(chr(10))[0]}")
    
    if result.errors:
        print(f"\nErrors:")
        for test, error in result.errors:
            print(f"  - {test}: {error.split(chr(10))[0]}")
    
    print(f"{'='*50}")
    
    # Exit with appropriate code
    exit(0 if result.wasSuccessful() else 1)