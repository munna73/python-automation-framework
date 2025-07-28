"""
REST API client for making HTTP requests with retry logic and authentication.
"""
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Dict, Any, Optional, List, Union, Tuple
import time
import json
from pathlib import Path
from utils.config_loader import config_loader
from utils.logger import logger
import urllib3

# Disable SSL warnings for test environments
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class RestClient:
    """REST API client with advanced features."""
    
    def __init__(self, config_name: str = 'api'):
        """Initialize REST client with configuration."""
        self.config = config_loader.get_api_config(config_name)
        self.base_url = self.config.get('base_url', '').rstrip('/')
        self.default_timeout = self.config.get('timeout', 30)
        self.verify_ssl = self.config.get('verify_ssl', True)
        
        # Initialize session with retry strategy
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
        
        # Retry configuration
        self.retry_config = {
            'max_retries': 3,
            'retry_delay': 1,
            'retry_status_codes': [502, 503, 504]
        }
        
        # Request/Response interceptors
        self.request_interceptors = []
        self.response_interceptors = []
        
        logger.info(f"REST client initialized with base URL: {self.base_url}")
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST", "PATCH"]
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
        """Set retry configuration."""
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
        # Start with default headers
        request_headers = self.default_headers.copy()
        
        # Add custom headers
        request_headers.update(self.custom_headers)
        
        # Add authentication header if token is set
        if self.auth_token:
            if self.auth_type.lower() == 'bearer':
                request_headers['Authorization'] = f"Bearer {self.auth_token}"
            elif self.auth_type.lower() == 'basic':
                request_headers['Authorization'] = f"Basic {self.auth_token}"
            elif self.auth_type.lower() == 'apikey':
                request_headers['X-API-Key'] = self.auth_token
        
        # Add request-specific headers
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
                        files: Optional[Dict[str, Tuple[str, str]]] = None,
                        timeout: Optional[int] = None,
                        **kwargs) -> requests.Response:
        """Execute HTTP request with retry logic."""
        
        # Prepare request
        headers = self._prepare_headers(headers)
        timeout = timeout or self.default_timeout
        
        # Apply request interceptors
        for interceptor in self.request_interceptors:
            method, url, headers, params, json_data, data = interceptor(
                method, url, headers, params, json_data, data
            )
        
        # Log request details
        logger.info(f"{method} {url}")
        if params:
            logger.debug(f"Query params: {params}")
        if json_data:
            logger.debug(f"Request body: {json.dumps(json_data, indent=2)}")
        
        # Prepare files for multipart upload
        if files:
            files_to_upload = {}
            for field_name, (file_path, content_type) in files.items():
                file_path_obj = Path(file_path)
                if file_path_obj.exists():
                    files_to_upload[field_name] = (
                        file_path_obj.name,
                        open(file_path_obj, 'rb'),
                        content_type
                    )
                else:
                    logger.warning(f"File not found: {file_path}")
            
            # Remove Content-Type header for multipart
            headers.pop('Content-Type', None)
        else:
            files_to_upload = None
        
        # Execute request with retry logic
        retry_count = 0
        last_exception = None
        
        while retry_count <= self.retry_config['max_retries']:
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_data,
                    data=data,
                    files=files_to_upload,
                    timeout=timeout,
                    verify=self.verify_ssl,
                    **kwargs
                )
                
                # Add retry count to response
                response.retry_count = retry_count
                
                # Check if we should retry based on status code
                if (retry_count < self.retry_config['max_retries'] and 
                    response.status_code in self.retry_config['retry_status_codes']):
                    logger.warning(f"Retrying request due to status code {response.status_code}")
                    retry_count += 1
                    time.sleep(self.retry_config['retry_delay'] * retry_count)
                    continue
                
                # Apply response interceptors
                for interceptor in self.response_interceptors:
                    response = interceptor(response)
                
                # Close files if uploaded
                if files_to_upload:
                    for _, (_, file_obj, _) in files_to_upload.items():
                        file_obj.close()
                
                # Log response
                logger.info(f"Response: {response.status_code} in {response.elapsed.total_seconds():.2f}s")
                
                return response
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                logger.warning(f"Request timeout (attempt {retry_count + 1})")
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                logger.warning(f"Connection error (attempt {retry_count + 1}): {e}")
            except Exception as e:
                last_exception = e
                logger.error(f"Unexpected error: {e}")
                break
            
            retry_count += 1
            if retry_count <= self.retry_config['max_retries']:
                time.sleep(self.retry_config['retry_delay'] * retry_count)
        
        # Close files if upload failed
        if files_to_upload:
            for _, (_, file_obj, _) in files_to_upload.items():
                try:
                    file_obj.close()
                except:
                    pass
        
        # If all retries failed, raise the last exception
        raise last_exception or Exception("Request failed after all retries")
    
    def get(self, 
            endpoint: str,
            params: Optional[Dict[str, Any]] = None,
            headers: Optional[Dict[str, str]] = None,
            **kwargs) -> requests.Response:
        """Send GET request."""
        url = self._build_url(endpoint)
        return self._execute_request('GET', url, params=params, headers=headers, **kwargs)
    
    def post(self,
             endpoint: str,
             json: Optional[Dict[str, Any]] = None,
             data: Optional[Union[Dict[str, Any], str]] = None,
             params: Optional[Dict[str, Any]] = None,
             headers: Optional[Dict[str, str]] = None,
             **kwargs) -> requests.Response:
        """Send POST request."""
        url = self._build_url(endpoint)
        return self._execute_request('POST', url, json_data=json, data=data, 
                                   params=params, headers=headers, **kwargs)
    
    def post_multipart(self,
                      endpoint: str,
                      data: Optional[Dict[str, Any]] = None,
                      files: Optional[Dict[str, Tuple[str, str]]] = None,
                      headers: Optional[Dict[str, str]] = None,
                      **kwargs) -> requests.Response:
        """Send POST request with multipart/form-data."""
        url = self._build_url(endpoint)
        return self._execute_request('POST', url, data=data, files=files, 
                                   headers=headers, **kwargs)
    
    def put(self,
            endpoint: str,
            json: Optional[Dict[str, Any]] = None,
            data: Optional[Union[Dict[str, Any], str]] = None,
            params: Optional[Dict[str, Any]] = None,
            headers: Optional[Dict[str, str]] = None,
            **kwargs) -> requests.Response:
        """Send PUT request."""
        url = self._build_url(endpoint)
        return self._execute_request('PUT', url, json_data=json, data=data,
                                   params=params, headers=headers, **kwargs)
    
    def patch(self,
              endpoint: str,
              json: Optional[Dict[str, Any]] = None,
              data: Optional[Union[Dict[str, Any], str]] = None,
              params: Optional[Dict[str, Any]] = None,
              headers: Optional[Dict[str, str]] = None,
              **kwargs) -> requests.Response:
        """Send PATCH request."""
        url = self._build_url(endpoint)
        return self._execute_request('PATCH', url, json_data=json, data=data,
                                   params=params, headers=headers, **kwargs)
    
    def delete(self,
               endpoint: str,
               params: Optional[Dict[str, Any]] = None,
               headers: Optional[Dict[str, str]] = None,
               **kwargs) -> requests.Response:
        """Send DELETE request."""
        url = self._build_url(endpoint)
        return self._execute_request('DELETE', url, params=params, headers=headers, **kwargs)
    
    def head(self,
             endpoint: str,
             params: Optional[Dict[str, Any]] = None,
             headers: Optional[Dict[str, str]] = None,
             **kwargs) -> requests.Response:
        """Send HEAD request."""
        url = self._build_url(endpoint)
        return self._execute_request('HEAD', url, params=params, headers=headers, **kwargs)
    
    def options(self,
                endpoint: str,
                headers: Optional[Dict[str, str]] = None,
                **kwargs) -> requests.Response:
        """Send OPTIONS request."""
        url = self._build_url(endpoint)
        return self._execute_request('OPTIONS', url, headers=headers, **kwargs)
    
    def download_file(self,
                     endpoint: str,
                     save_path: str,
                     params: Optional[Dict[str, Any]] = None,
                     headers: Optional[Dict[str, str]] = None,
                     chunk_size: int = 8192) -> bool:
        """Download file from endpoint."""
        try:
            url = self._build_url(endpoint)
            
            response = self.get(endpoint, params=params, headers=headers, stream=True)
            response.raise_for_status()
            
            # Ensure directory exists
            save_path_obj = Path(save_path)
            save_path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            # Write file in chunks
            with open(save_path_obj, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
            
            logger.info(f"Downloaded file to: {save_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download file: {e}")
            return False
    
    def upload_file(self,
                   endpoint: str,
                   file_path: str,
                   field_name: str = 'file',
                   additional_data: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Upload file to endpoint."""
        file_path_obj = Path(file_path)
        
        if not file_path_obj.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Detect content type
        import mimetypes
        content_type, _ = mimetypes.guess_type(str(file_path_obj))
        
        files = {field_name: (file_path_obj.name, content_type or 'application/octet-stream')}
        
        return self.post_multipart(endpoint, data=additional_data, files=files)
    
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
rest_client = RestClient()