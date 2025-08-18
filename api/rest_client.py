

"""
REST API client for making HTTP requests with retry logic and authentication.
This version has been refactored to remove redundant retry logic and improve error handling.
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, Any, Optional, List, Union, Tuple
import time
import json
from pathlib import Path
import urllib3
import mimetypes

# Assuming these are available in your project structure
from utils.config_loader import ConfigLoader, config_loader
from utils.logger import logger

# Disable SSL warnings for test environments
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class RestClient:
    """
    REST API client with advanced features.
    
    This class wraps the `requests` library to provide a consistent interface with
    built-in features for retry logic, authentication, and logging.
    """
    
    def __init__(self, config_name: str = 'API'):
        """
        Initialize REST client with configuration from a file.
        
        Args:
            config_name (str): The name of the configuration section to load.
        """
        self.config = config_loader.get_api_config(config_name)
        self.base_url = self.config.get('base_url', '').rstrip('/')
        self.default_timeout = self.config.get('timeout', 30)
        self.verify_ssl = self.config.get('verify_ssl', True)
        
        # Consolidate retry configuration to a single source of truth
        self.retry_config = {
            'max_retries': self.config.get('max_retries', 3),
            'retry_delay': self.config.get('retry_delay', 1),
            # Use status codes from config, with a sensible default
            'retry_status_codes': self.config.get('retry_status_codes', [500, 502, 503, 504, 429])
        }
        
        # Initialize session with a single, definitive retry strategy
        self.session = self._create_session()
        
        # Default headers
        self.default_headers = {
            'User-Agent': 'BDD-Test-Framework/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        # Custom headers that can be set per request
        self.custom_headers = {}
        
        # Authentication
        self.auth_token = None
        self.auth_type = self.config.get('auth_type', 'bearer')
        
        # Request/Response interceptors
        self.request_interceptors = []
        self.response_interceptors = []
        
        logger.info(f"REST client initialized with base URL: {self.base_url}")
    
    def _create_session(self) -> requests.Session:
        """
        Create requests session with a single, unified retry strategy.
        This method is now the only place where retry logic is configured.
        """
        session = requests.Session()
        
        # Configure retry strategy based on the unified retry_config
        retry_strategy = Retry(
            total=self.retry_config['max_retries'],
            backoff_factor=self.retry_config['retry_delay'],
            status_forcelist=self.retry_config['retry_status_codes'],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST", "PATCH"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def set_timeout(self, timeout: int):
        """Set request timeout."""
        self.default_timeout = timeout
        logger.debug(f"Set timeout to {timeout} seconds")
    
    def set_headers(self, headers: Dict[str, str]):
        """Set custom headers for requests."""
        self.custom_headers.update(headers)
        logger.debug(f"Set custom headers: {headers}")
    
    def set_auth_token(self, token: str):
        """Set authentication token."""
        self.auth_token = token
        logger.debug("Authentication token set")
    
    def set_retry_config(self, config: Dict[str, Any]):
        """
        Set retry configuration.
        Note: The changes will only apply to new sessions if needed,
        but for this implementation, the session is created once.
        A more advanced version would re-create the session here.
        """
        self.retry_config.update(config)
        logger.debug(f"Updated retry configuration: {self.retry_config}")
    
    def add_request_interceptor(self, interceptor: callable):
        """Add request interceptor."""
        self.request_interceptors.append(interceptor)
    
    def add_response_interceptor(self, interceptor: callable):
        """Add response interceptor."""
        self.response_interceptors.append(interceptor)
    
    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint."""
        if endpoint.startswith('http'):
            return endpoint
        
        endpoint = endpoint.lstrip('/')
        return f"{self.base_url}/{endpoint}"
    
    def _prepare_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Prepare headers for request."""
        request_headers = self.default_headers.copy()
        request_headers.update(self.custom_headers)
        
        if self.auth_token:
            auth_type_lower = self.auth_type.lower()
            if auth_type_lower == 'bearer':
                request_headers['Authorization'] = f"Bearer {self.auth_token}"
            elif auth_type_lower == 'basic':
                request_headers['Authorization'] = f"Basic {self.auth_token}"
            elif auth_type_lower == 'apikey':
                request_headers['X-API-Key'] = self.auth_token
        
        if headers:
            request_headers.update(headers)
        
        return request_headers
    
    def _execute_request(self, 
                        method: str, 
                        url: str,
                        headers: Optional[Dict[str, str]] = None,
                        params: Optional[Dict[str, Any]] = None,
                        json_data: Optional[Dict[str, Any]] = None,
                        data: Optional[Union[Dict[str, Any], str]] = None,
                        files: Optional[Dict[str, Tuple[str, Union[str, bytes, Path]]]] = None,
                        timeout: Optional[int] = None,
                        **kwargs) -> requests.Response:
        """
        Execute HTTP request.
        
        The manual retry loop has been removed; retries are now handled automatically
        by the HTTPAdapter configured in the session.
        """
        
        headers = self._prepare_headers(headers)
        timeout = timeout or self.default_timeout
        
        for interceptor in self.request_interceptors:
            method, url, headers, params, json_data, data, files = interceptor(
                method, url, headers, params, json_data, data, files
            )
        
        logger.info(f"{method} {url}")
        if params:
            logger.debug(f"Query params: {params}")
        if json_data:
            logger.debug(f"Request body: {json.dumps(json_data, indent=2)}")
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_data,
                data=data,
                files=files,
                timeout=timeout,
                verify=self.verify_ssl,
                **kwargs
            )
            
            # Apply response interceptors
            for interceptor in self.response_interceptors:
                response = interceptor(response)
            
            logger.info(f"Response: {response.status_code} in {response.elapsed.total_seconds():.2f}s")
            
            return response
                
        except requests.exceptions.RequestException as e:
            # Narrow the exception handling to specific requests exceptions
            logger.error(f"Request failed: {type(e).__name__} - {e}")
            raise # Re-raise the exception after logging
        except Exception as e:
            # Catch any other unexpected exceptions
            logger.error(f"An unexpected error occurred during the request: {e}")
            raise

    # --- HTTP Method Wrappers ---
    def get(self, endpoint: str, **kwargs) -> requests.Response:
        """Send GET request."""
        url = self._build_url(endpoint)
        return self._execute_request('GET', url, **kwargs)
    
    def post(self, endpoint: str, **kwargs) -> requests.Response:
        """Send POST request."""
        url = self._build_url(endpoint)
        return self._execute_request('POST', url, **kwargs)
    
    def post_multipart(self, endpoint: str, **kwargs) -> requests.Response:
        """
        Send POST request with multipart/form-data.
        Note: The 'Content-Type' header is automatically handled by `requests`
        when the 'files' parameter is used.
        """
        url = self._build_url(endpoint)
        return self._execute_request('POST', url, **kwargs)
    
    def put(self, endpoint: str, **kwargs) -> requests.Response:
        """Send PUT request."""
        url = self._build_url(endpoint)
        return self._execute_request('PUT', url, **kwargs)
    
    def patch(self, endpoint: str, **kwargs) -> requests.Response:
        """Send PATCH request."""
        url = self._build_url(endpoint)
        return self._execute_request('PATCH', url, **kwargs)
    
    def delete(self, endpoint: str, **kwargs) -> requests.Response:
        """Send DELETE request."""
        url = self._build_url(endpoint)
        return self._execute_request('DELETE', url, **kwargs)
    
    def head(self, endpoint: str, **kwargs) -> requests.Response:
        """Send HEAD request."""
        url = self._build_url(endpoint)
        return self._execute_request('HEAD', url, **kwargs)
    
    def options(self, endpoint: str, **kwargs) -> requests.Response:
        """Send OPTIONS request."""
        url = self._build_url(endpoint)
        return self._execute_request('OPTIONS', url, **kwargs)
    
    def download_file(self,
                     endpoint: str,
                     save_path: str,
                     **kwargs) -> bool:
        """
        Download a file from an endpoint and save it to a specified path.
        
        Args:
            endpoint (str): The API endpoint to download the file from.
            save_path (str): The local path to save the downloaded file.
            **kwargs: Additional arguments passed to the GET request.
        
        Returns:
            bool: True if the file was downloaded successfully, False otherwise.
        """
        try:
            url = self._build_url(endpoint)
            
            # Use `stream=True` to download the file in chunks
            response = self.get(endpoint, stream=True, **kwargs)
            response.raise_for_status()
            
            save_path_obj = Path(save_path)
            save_path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            with open(save_path_obj, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded file to: {save_path}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download file: {type(e).__name__} - {e}")
            return False
    
    def upload_file(self,
                   endpoint: str,
                   file_path: str,
                   field_name: str = 'file',
                   additional_data: Optional[Dict[str, Any]] = None,
                   **kwargs) -> requests.Response:
        """
        Upload a file to an endpoint using multipart/form-data.
        
        Args:
            endpoint (str): The API endpoint for the upload.
            file_path (str): The path to the local file to upload.
            field_name (str): The name of the form field for the file.
            additional_data (Dict): Optional additional data to send in the form.
            **kwargs: Additional arguments passed to the POST request.
        
        Returns:
            requests.Response: The response from the server.
        """
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # The `files` dictionary now prepares the file tuple
        files_to_upload = {
            field_name: (
                file_path_obj.name,
                open(file_path_obj, 'rb'),
                mimetypes.guess_type(file_path_obj.name)[0] or 'application/octet-stream'
            )
        }
        
        # The data and files are now passed directly to the generic post method
        return self.post_multipart(endpoint, files=files_to_upload, data=additional_data, **kwargs)
    
    def reset(self):
        """Reset client state."""
        self.custom_headers.clear()
        self.auth_token = None
        logger.debug("REST client state reset")
    
    def close(self):
        """Close the session."""
        self.session.close()
        logger.debug("REST client session closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

# Create a singleton instance for easy import
# rest_client = RestClient()
