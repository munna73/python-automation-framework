"""
REST API client for testing web services.
"""
import requests
import json
import jsonschema
from typing import Dict, Any, Optional, List
from pathlib import Path
from utils.config_loader import config_loader
from utils.logger import logger, api_logger
import time

class APIClient:
    """REST API client with comprehensive testing capabilities."""
    
    def __init__(self):
        """Initialize API client."""
        self.config = config_loader.get_api_config()
        self.session = requests.Session()
        self.setup_session()
        self.response_history = []
    
    def setup_session(self):
        """Setup session with default headers and authentication."""
        try:
            # Set default headers
            self.session.headers.update({
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })
            
            # Setup authentication
            if self.config.get('auth_type') == 'bearer' and self.config.get('token'):
                self.session.headers.update({
                    'Authorization': f"Bearer {self.config['token']}"
                })
            
            # Set timeout
            self.timeout = int(self.config.get('timeout', 30))
            
            api_logger.info("API client session configured successfully")
            
        except Exception as e:
            api_logger.error(f"Failed to setup API session: {e}")
            raise
    
    def get(self, 
            endpoint: str, 
            params: Dict[str, Any] = None,
            headers: Dict[str, str] = None) -> requests.Response:
        """
        Send GET request to API endpoint.
        
        Args:
            endpoint: API endpoint (e.g., '/customers')
            params: Query parameters
            headers: Additional headers
            
        Returns:
            Response object
        """
        try:
            url = self._build_url(endpoint)
            
            api_logger.info(f"Sending GET request to: {url}")
            api_logger.debug(f"Parameters: {params}")
            
            response = self._make_request('GET', url, params=params, headers=headers)
            
            return response
            
        except Exception as e:
            api_logger.error(f"GET request failed for {endpoint}: {e}")
            raise
    
    def post(self,
             endpoint: str,
             data: Dict[str, Any] = None,
             json_data: Dict[str, Any] = None,
             headers: Dict[str, str] = None) -> requests.Response:
        """
        Send POST request to API endpoint.
        
        Args:
            endpoint: API endpoint
            data: Form data
            json_data: JSON payload
            headers: Additional headers
            
        Returns:
            Response object
        """
        try:
            url = self._build_url(endpoint)
            
            api_logger.info(f"Sending POST request to: {url}")
            api_logger.debug(f"JSON data: {json_data}")
            
            response = self._make_request('POST', url, data=data, json=json_data, headers=headers)
            
            return response
            
        except Exception as e:
            api_logger.error(f"POST request failed for {endpoint}: {e}")
            raise
    
    def put(self,
            endpoint: str,
            data: Dict[str, Any] = None,
            json_data: Dict[str, Any] = None,
            headers: Dict[str, str] = None) -> requests.Response:
        """
        Send PUT request to API endpoint.
        
        Args:
            endpoint: API endpoint
            data: Form data
            json_data: JSON payload
            headers: Additional headers
            
        Returns:
            Response object
        """
        try:
            url = self._build_url(endpoint)
            
            api_logger.info(f"Sending PUT request to: {url}")
            api_logger.debug(f"JSON data: {json_data}")
            
            response = self._make_request('PUT', url, data=data, json=json_data, headers=headers)
            
            return response
            
        except Exception as e:
            api_logger.error(f"PUT request failed for {endpoint}: {e}")
            raise
    
    def delete(self,
               endpoint: str,
               params: Dict[str, Any] = None,
               headers: Dict[str, str] = None) -> requests.Response:
        """
        Send DELETE request to API endpoint.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            headers: Additional headers
            
        Returns:
            Response object
        """
        try:
            url = self._build_url(endpoint)
            
            api_logger.info(f"Sending DELETE request to: {url}")
            api_logger.debug(f"Parameters: {params}")
            
            response = self._make_request('DELETE', url, params=params, headers=headers)
            
            return response
            
        except Exception as e:
            api_logger.error(f"DELETE request failed for {endpoint}: {e}")
            raise
    
    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make HTTP request with retry logic and logging."""
        retry_attempts = int(self.config.get('retry_attempts', 3))
        
        for attempt in range(retry_attempts):
            try:
                start_time = time.time()
                
                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=self.timeout,
                    **kwargs
                )
                
                end_time = time.time()
                response_time = (end_time - start_time) * 1000  # Convert to milliseconds
                
                # Log response details
                api_logger.info(f"{method} {url} - Status: {response.status_code} - Time: {response_time:.2f}ms")
                
                # Store response in history
                self.response_history.append({
                    'method': method,
                    'url': url,
                    'status_code': response.status_code,
                    'response_time': response_time,
                    'timestamp': time.time()
                })
                
                # Don't retry on successful responses (2xx) or client errors (4xx)
                if response.status_code < 500:
                    return response
                
                # Retry on server errors (5xx)
                if attempt < retry_attempts - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    api_logger.warning(f"Server error {response.status_code}, retrying in {wait_time}s (attempt {attempt + 1}/{retry_attempts})")
                    time.sleep(wait_time)
                else:
                    return response
                    
            except requests.exceptions.Timeout:
                if attempt < retry_attempts - 1:
                    api_logger.warning(f"Request timeout, retrying (attempt {attempt + 1}/{retry_attempts})")
                    time.sleep(2 ** attempt)
                else:
                    api_logger.error(f"Request timeout after {retry_attempts} attempts")
                    raise
                    
            except requests.exceptions.ConnectionError:
                if attempt < retry_attempts - 1:
                    api_logger.warning(f"Connection error, retrying (attempt {attempt + 1}/{retry_attempts})")
                    time.sleep(2 ** attempt)
                else:
                    api_logger.error(f"Connection error after {retry_attempts} attempts")
                    raise
        
        raise Exception(f"Failed to complete request after {retry_attempts} attempts")
    
    def _build_url(self, endpoint: str) -> str:
        """Build full URL from base URL and endpoint."""
        base_url = self.config.get('base_url', '').rstrip('/')
        endpoint = endpoint.lstrip('/')
        return f"{base_url}/{endpoint}"
    
    def validate_response_status(self, response: requests.Response, expected_status: int) -> bool:
        """
        Validate response status code.
        
        Args:
            response: Response object
            expected_status: Expected status code
            
        Returns:
            True if status matches, False otherwise
        """
        actual_status = response.status_code
        is_valid = actual_status == expected_status
        
        if is_valid:
            api_logger.info(f"Status validation passed: {actual_status}")
        else:
            api_logger.error(f"Status validation failed: expected {expected_status}, got {actual_status}")
        
        return is_valid
    
    def validate_json_response(self, response: requests.Response) -> bool:
        """
        Validate that response contains valid JSON.
        
        Args:
            response: Response object
            
        Returns:
            True if valid JSON, False otherwise
        """
        try:
            response.json()
            api_logger.info("JSON validation passed")
            return True
        except json.JSONDecodeError as e:
            api_logger.error(f"JSON validation failed: {e}")
            return False
    
    def validate_response_schema(self, response: requests.Response, schema: Dict[str, Any]) -> bool:
        """
        Validate response JSON against schema.
        
        Args:
            response: Response object
            schema: JSON schema dictionary
            
        Returns:
            True if schema validation passes, False otherwise
        """
        try:
            response_json = response.json()
            jsonschema.validate(response_json, schema)
            api_logger.info("Schema validation passed")
            return True
        except jsonschema.ValidationError as e:
            api_logger.error(f"Schema validation failed: {e}")
            return False
        except json.JSONDecodeError as e:
            api_logger.error(f"Cannot validate schema - invalid JSON: {e}")
            return False
    
    def validate_response_field(self, response: requests.Response, field_path: str, expected_value: Any) -> bool:
        """
        Validate specific field value in response.
        
        Args:
            response: Response object
            field_path: Dot notation path to field (e.g., 'data.customer.id')
            expected_value: Expected field value
            
        Returns:
            True if field validation passes, False otherwise
        """
        try:
            response_json = response.json()
            
            # Navigate to field using dot notation
            current_value = response_json
            for key in field_path.split('.'):
                current_value = current_value[key]
            
            is_valid = current_value == expected_value
            
            if is_valid:
                api_logger.info(f"Field validation passed: {field_path} = {current_value}")
            else:
                api_logger.error(f"Field validation failed: {field_path} expected {expected_value}, got {current_value}")
            
            return is_valid
            
        except (KeyError, TypeError, json.JSONDecodeError) as e:
            api_logger.error(f"Field validation error for {field_path}: {e}")
            return False
    
    def load_payload_from_file(self, filename: str) -> Dict[str, Any]:
        """
        Load JSON payload from file.
        
        Args:
            filename: Filename in data directory
            
        Returns:
            Payload dictionary
        """
        try:
            file_path = Path(__file__).parent.parent / "data" / "test_data" / filename
            
            with open(file_path, 'r', encoding='utf-8') as f:
                payload = json.load(f)
            
            api_logger.info(f"Loaded payload from {filename}")
            return payload
            
        except Exception as e:
            api_logger.error(f"Failed to load payload from {filename}: {e}")
            raise
    
    def get_response_time_stats(self) -> Dict[str, float]:
        """Get response time statistics from history."""
        if not self.response_history:
            return {}
        
        response_times = [r['response_time'] for r in self.response_history]
        
        return {
            'count': len(response_times),
            'min': min(response_times),
            'max': max(response_times),
            'average': sum(response_times) / len(response_times),
            'total': sum(response_times)
        }
    
    def clear_history(self):
        """Clear response history."""
        self.response_history.clear()
        api_logger.info("Response history cleared")

# Global API client instance
api_client = APIClient()