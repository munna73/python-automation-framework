"""
JSON validator utility for schema validation and field operations.
"""
import json
import jsonschema
from jsonschema import validate, ValidationError, Draft7Validator
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
from utils.logger import logger

class JsonValidator:
    """JSON validation and manipulation utility."""
    
    def __init__(self):
        """Initialize JSON validator."""
        self.schema_cache = {}
        self.schema_directory = Path(__file__).parent.parent / "schemas"
        
        # Ensure schema directory exists
        self.schema_directory.mkdir(parents=True, exist_ok=True)
        
        logger.info("JSON validator initialized")
    
    def validate(self, 
                data: Union[Dict, List], 
                schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate JSON data against schema.
        
        Args:
            data: JSON data to validate
            schema: JSON schema
            
        Returns:
            Dictionary with validation results
        """
        try:
            validate(instance=data, schema=schema)
            return {
                'valid': True,
                'errors': []
            }
        except ValidationError as e:
            return {
                'valid': False,
                'errors': [self._format_validation_error(e)]
            }
        except Exception as e:
            return {
                'valid': False,
                'errors': [f"Unexpected error: {str(e)}"]
            }
    
    def validate_with_draft7(self, 
                           data: Union[Dict, List], 
                           schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate using Draft 7 validator with detailed errors.
        
        Args:
            data: JSON data to validate
            schema: JSON schema
            
        Returns:
            Dictionary with validation results
        """
        validator = Draft7Validator(schema)
        errors = []
        
        for error in validator.iter_errors(data):
            errors.append(self._format_validation_error(error))
        
        return {
            'valid': len(errors) == 0,
            'errors': errors
        }
    
    def _format_validation_error(self, error: ValidationError) -> Dict[str, Any]:
        """Format validation error for better readability."""
        path = '.'.join(str(p) for p in error.path) if error.path else 'root'
        
        return {
            'path': path,
            'message': error.message,
            'schema_path': '.'.join(str(p) for p in error.schema_path),
            'instance': error.instance
        }
    
    def load_schema(self, schema_name: str) -> Dict[str, Any]:
        """
        Load schema from file.
        
        Args:
            schema_name: Name of schema file (without .json extension)
            
        Returns:
            Schema dictionary
        """
        if schema_name in self.schema_cache:
            return self.schema_cache[schema_name]
        
        schema_path = self.schema_directory / f"{schema_name}.json"
        
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema not found: {schema_path}")
        
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        
        self.schema_cache[schema_name] = schema
        logger.debug(f"Loaded schema: {schema_name}")
        
        return schema
    
    def load_schema_for_endpoint(self, endpoint: str, method: str) -> Dict[str, Any]:
        """
        Load schema based on endpoint and method.
        
        Args:
            endpoint: API endpoint
            method: HTTP method
            
        Returns:
            Schema dictionary
        """
        # Clean endpoint for schema name
        schema_name = endpoint.strip('/').replace('/', '_').replace('{', '').replace('}', '')
        schema_name = f"{method.lower()}_{schema_name}"
        
        try:
            return self.load_schema(schema_name)
        except FileNotFoundError:
            # Return a generic schema if specific one not found
            logger.warning(f"Schema not found for {method} {endpoint}, using generic schema")
            return self._get_generic_schema()
    
    def _get_generic_schema(self) -> Dict[str, Any]:
        """Get generic schema for basic validation."""
        return {
            "type": "object",
            "additionalProperties": True
        }
    
    def field_exists(self, 
                    data: Union[Dict, List], 
                    field_path: str) -> bool:
        """
        Check if field exists in JSON data.
        
        Args:
            data: JSON data
            field_path: Dot-separated field path (e.g., "user.profile.email")
            
        Returns:
            True if field exists, False otherwise
        """
        try:
            self.get_field_value(data, field_path)
            return True
        except (KeyError, IndexError, TypeError):
            return False
    
    def get_field_value(self, 
                       data: Union[Dict, List], 
                       field_path: str) -> Any:
        """
        Get field value from JSON data using dot notation.
        
        Args:
            data: JSON data
            field_path: Dot-separated field path
            
        Returns:
            Field value or None if not found
        """
        if not field_path:
            return data
        
        parts = field_path.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict):
                if part not in current:
                    return None
                current = current[part]
            elif isinstance(current, list):
                # Handle array index
                if part.isdigit():
                    index = int(part)
                    if 0 <= index < len(current):
                        current = current[index]
                    else:
                        return None
                else:
                    # Handle array element property
                    return None
            else:
                return None
        
        return current
    
    def set_field_value(self,
                       data: Dict[str, Any],
                       field_path: str,
                       value: Any) -> bool:
        """
        Set field value in JSON data using dot notation.
        
        Args:
            data: JSON data (must be dict)
            field_path: Dot-separated field path
            value: Value to set
            
        Returns:
            True if successful, False otherwise
        """
        if not isinstance(data, dict):
            return False
        
        parts = field_path.split('.')
        current = data
        
        # Navigate to the parent of the target field
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
            
            if not isinstance(current, dict):
                return False
        
        # Set the value
        if parts:
            current[parts[-1]] = value
            return True
        
        return False
    
    def remove_field(self,
                    data: Dict[str, Any],
                    field_path: str) -> bool:
        """
        Remove field from JSON data.
        
        Args:
            data: JSON data
            field_path: Dot-separated field path
            
        Returns:
            True if successful, False otherwise
        """
        parts = field_path.split('.')
        current = data
        
        # Navigate to the parent of the target field
        for part in parts[:-1]:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return False
        
        # Remove the field
        if isinstance(current, dict) and parts[-1] in current:
            del current[parts[-1]]
            return True
        
        return False
    
    def filter_fields(self,
                     data: Union[Dict, List],
                     fields_to_keep: List[str]) -> Union[Dict, List]:
        """
        Filter JSON data to keep only specified fields.
        
        Args:
            data: JSON data
            fields_to_keep: List of field paths to keep
            
        Returns:
            Filtered data
        """
        if isinstance(data, list):
            return [self.filter_fields(item, fields_to_keep) for item in data]
        
        if not isinstance(data, dict):
            return data
        
        result = {}
        
        for field_path in fields_to_keep:
            value = self.get_field_value(data, field_path)
            if value is not None:
                self.set_field_value(result, field_path, value)
        
        return result
    
    def compare_json(self,
                    actual: Union[Dict, List],
                    expected: Union[Dict, List],
                    ignore_fields: List[str] = None,
                    strict: bool = True) -> Dict[str, Any]:
        """
        Compare two JSON objects.
        
        Args:
            actual: Actual JSON data
            expected: Expected JSON data
            ignore_fields: List of field paths to ignore
            strict: If True, check for extra fields in actual
            
        Returns:
            Comparison results
        """
        differences = []
        ignore_fields = ignore_fields or []
        
        def compare_values(path: str, actual_val: Any, expected_val: Any):
            # Skip ignored fields
            if any(path.startswith(ignore_field) for ignore_field in ignore_fields):
                return
            
            if type(actual_val) != type(expected_val):
                differences.append({
                    'path': path,
                    'type': 'type_mismatch',
                    'actual_type': type(actual_val).__name__,
                    'expected_type': type(expected_val).__name__,
                    'actual': actual_val,
                    'expected': expected_val
                })
        
        def compare_dicts(path: str, actual_dict: Dict, expected_dict: Dict):
            # Check for missing keys
            for key in expected_dict:
                if key not in actual_dict:
                    differences.append({
                        'path': f"{path}.{key}" if path else key,
                        'type': 'missing_field',
                        'expected': expected_dict[key]
                    })
                else:
                    new_path = f"{path}.{key}" if path else key
                    compare_values(new_path, actual_dict[key], expected_dict[key])
            
            # Check for extra keys if strict mode
            if strict:
                for key in actual_dict:
                    if key not in expected_dict:
                        new_path = f"{path}.{key}" if path else key
                        if not any(new_path.startswith(ignore_field) for ignore_field in ignore_fields):
                            differences.append({
                                'path': new_path,
                                'type': 'extra_field',
                                'actual': actual_dict[key]
                            })
        
        def compare_lists(path: str, actual_list: List, expected_list: List):
            if len(actual_list) != len(expected_list):
                differences.append({
                    'path': path,
                    'type': 'array_length_mismatch',
                    'actual_length': len(actual_list),
                    'expected_length': len(expected_list)
                })
            
            # Compare each element
            for i in range(min(len(actual_list), len(expected_list))):
                compare_values(f"{path}[{i}]", actual_list[i], expected_list[i])
        
        # Start comparison
        compare_values('', actual, expected)
        
        return {
            'match': len(differences) == 0,
            'differences': differences
        }
    
    def merge_json(self,
                  base: Dict[str, Any],
                  updates: Dict[str, Any],
                  deep: bool = True) -> Dict[str, Any]:
        """
        Merge two JSON objects.
        
        Args:
            base: Base JSON object
            updates: Updates to apply
            deep: If True, perform deep merge
            
        Returns:
            Merged JSON object
        """
        import copy
        result = copy.deepcopy(base)
        
        if not deep:
            result.update(updates)
            return result
        
        def deep_merge(target: Dict, source: Dict):
            for key, value in source.items():
                if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                    deep_merge(target[key], value)
                else:
                    target[key] = copy.deepcopy(value)
        
        deep_merge(result, updates)
        return result
    
    def transform_json(self,
                      data: Union[Dict, List],
                      mapping: Dict[str, str]) -> Union[Dict, List]:
        """
        Transform JSON data using field mapping.
        
        Args:
            data: Source JSON data
            mapping: Field mapping (old_path: new_path)
            
        Returns:
            Transformed data
        """
        if isinstance(data, list):
            return [self.transform_json(item, mapping) for item in data]
        
        if not isinstance(data, dict):
            return data
        
        result = {}
        
        for old_path, new_path in mapping.items():
            value = self.get_field_value(data, old_path)
            if value is not None:
                self.set_field_value(result, new_path, value)
        
        return result
    
    def generate_schema_from_json(self,
                                 data: Union[Dict, List],
                                 required_fields: List[str] = None) -> Dict[str, Any]:
        """
        Generate JSON schema from sample data.
        
        Args:
            data: Sample JSON data
            required_fields: List of required field paths
            
        Returns:
            Generated JSON schema
        """
        required_fields = required_fields or []
        
        def generate_schema_for_value(value: Any, path: str = '') -> Dict[str, Any]:
            if value is None:
                return {"type": "null"}
            elif isinstance(value, bool):
                return {"type": "boolean"}
            elif isinstance(value, int):
                return {"type": "integer"}
            elif isinstance(value, float):
                return {"type": "number"}
            elif isinstance(value, str):
                schema = {"type": "string"}
                # Add format detection
                if '@' in value and '.' in value:
                    schema["format"] = "email"
                elif value.count('-') == 2 and value.count('T') == 1:
                    schema["format"] = "date-time"
                return schema
            elif isinstance(value, list):
                if not value:
                    return {"type": "array", "items": {}}
                
                # Assume all items have same schema (use first item)
                return {
                    "type": "array",
                    "items": generate_schema_for_value(value[0], f"{path}[0]")
                }
            elif isinstance(value, dict):
                properties = {}
                required = []
                
                for key, val in value.items():
                    new_path = f"{path}.{key}" if path else key
                    properties[key] = generate_schema_for_value(val, new_path)
                    
                    if new_path in required_fields:
                        required.append(key)
                
                schema = {
                    "type": "object",
                    "properties": properties
                }
                
                if required:
                    schema["required"] = required
                
                return schema
            else:
                return {"type": "string"}  # Default fallback
        
        return generate_schema_for_value(data)
    
    def validate_required_fields(self,
                               data: Union[Dict, List],
                               required_fields: List[str]) -> Dict[str, Any]:
        """
        Validate that all required fields are present.
        
        Args:
            data: JSON data
            required_fields: List of required field paths
            
        Returns:
            Validation results
        """
        missing_fields = []
        
        for field_path in required_fields:
            if not self.field_exists(data, field_path):
                missing_fields.append(field_path)
        
        return {
            'valid': len(missing_fields) == 0,
            'missing_fields': missing_fields
        }
    
    def validate_field_types(self,
                           data: Union[Dict, List],
                           field_types: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate field types.
        
        Args:
            data: JSON data
            field_types: Dictionary of field_path: expected_type
            
        Returns:
            Validation results
        """
        type_errors = []
        
        type_mapping = {
            'string': str,
            'integer': int,
            'number': (int, float),
            'boolean': bool,
            'array': list,
            'object': dict,
            'null': type(None)
        }
        
        for field_path, expected_type in field_types.items():
            value = self.get_field_value(data, field_path)
            
            if value is not None:
                expected_python_type = type_mapping.get(expected_type, str)
                
                if not isinstance(value, expected_python_type):
                    type_errors.append({
                        'field': field_path,
                        'expected_type': expected_type,
                        'actual_type': type(value).__name__,
                        'value': value
                    })
        
        return {
            'valid': len(type_errors) == 0,
            'type_errors': type_errors
        }
    
    def save_schema(self, schema_name: str, schema: Dict[str, Any]):
        """Save schema to file."""
        schema_path = self.schema_directory / f"{schema_name}.json"
        
        with open(schema_path, 'w') as f:
            json.dump(schema, f, indent=2)
        
        # Update cache
        self.schema_cache[schema_name] = schema
        logger.info(f"Saved schema: {schema_name}")

# Create singleton instance
json_validator = JsonValidator()
            elif isinstance(expected_val, dict):
                compare_dicts(path, actual_val, expected_val)
            elif isinstance(expected_val, list):
                compare_lists(path, actual_val, expected_val)
            elif actual_val != expected_val:
                differences.append({
                    'path': path,
                    'type': 'value_mismatch',
                    'actual': actual_val,
                    'expected': expected_val
                })