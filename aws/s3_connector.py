"""
AWS S3 connector for downloading and uploading files from S3 buckets.
"""
import boto3
import os
import json
import hashlib
import mimetypes
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config
from utils.config_loader import config_loader
from utils.logger import logger
import time

class S3Connector:
    """AWS S3 connector for file operations."""
    
    def __init__(self, profile_name: Optional[str] = None):
        """Initialize S3 connector."""
        self.s3_client = None
        self.s3_resource = None
        self.aws_config = None
        self.profile_name = profile_name
        self.setup_aws_connection()
    
    def setup_aws_connection(self):
        """Setup AWS S3 connection using environment variables or profile."""
        try:
            # Configure retry behavior
            config = Config(
                retries={
                    'max_attempts': 3,
                    'mode': 'adaptive'
                }
            )
            
            if self.profile_name:
                # Use AWS profile
                session = boto3.Session(profile_name=self.profile_name)
                self.s3_client = session.client('s3', config=config)
                self.s3_resource = session.resource('s3', config=config)
                logger.info(f"AWS S3 connection established using profile: {self.profile_name}")
            else:
                # Use environment variables
                self.aws_config = config_loader.get_aws_config()
                
                # Create S3 client and resource
                self.s3_client = boto3.client(
                    's3',
                    region_name=self.aws_config.get('region', 'us-east-1'),
                    aws_access_key_id=self.aws_config.get('access_key_id'),
                    aws_secret_access_key=self.aws_config.get('secret_access_key'),
                    config=config
                )
                
                self.s3_resource = boto3.resource(
                    's3',
                    region_name=self.aws_config.get('region', 'us-east-1'),
                    aws_access_key_id=self.aws_config.get('access_key_id'),
                    aws_secret_access_key=self.aws_config.get('secret_access_key'),
                    config=config
                )
                
                logger.info("AWS S3 connection established successfully")
            
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
            raise
        except Exception as e:
            logger.error(f"Failed to setup AWS S3 connection: {e}")
            raise
    
    def download_file(self, 
                     bucket_name: str, 
                     s3_key: str, 
                     local_file_path: str,
                     verify_checksum: bool = False) -> bool:
        """
        Download a single file from S3.
        
        Args:
            bucket_name: S3 bucket name
            s3_key: S3 object key (file path in bucket)
            local_file_path: Local path where file should be saved
            verify_checksum: Whether to verify file integrity after download
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure local directory exists
            local_path = Path(local_file_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Get object info for verification
            if verify_checksum:
                object_info = self.get_object_info(bucket_name, s3_key)
                expected_etag = object_info['etag']
            
            # Download file with progress callback
            start_time = time.time()
            self.s3_client.download_file(
                bucket_name, 
                s3_key, 
                str(local_path),
                Callback=self._create_progress_callback(s3_key, 'download')
            )
            download_time = time.time() - start_time
            
            # Verify checksum if requested
            if verify_checksum:
                if not self._verify_file_checksum(str(local_path), expected_etag):
                    logger.error(f"Checksum verification failed for: {s3_key}")
                    os.remove(str(local_path))
                    return False
            
            file_size = local_path.stat().st_size
            logger.info(f"Downloaded S3 file: s3://{bucket_name}/{s3_key} -> {local_file_path} "
                       f"({file_size:,} bytes in {download_time:.2f}s)")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.error(f"S3 object not found: s3://{bucket_name}/{s3_key}")
            elif error_code == 'NoSuchBucket':
                logger.error(f"S3 bucket not found: {bucket_name}")
            elif error_code == 'AccessDenied':
                logger.error(f"Access denied to S3 object: s3://{bucket_name}/{s3_key}")
            else:
                logger.error(f"Failed to download S3 file: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading S3 file: {e}")
            return False
    
    def download_file_parallel(self,
                             bucket_name: str,
                             s3_keys: List[str],
                             local_directory: str,
                             max_workers: int = 5) -> Dict[str, Any]:
        """
        Download multiple files in parallel.
        
        Args:
            bucket_name: S3 bucket name
            s3_keys: List of S3 object keys
            local_directory: Local directory for downloads
            max_workers: Maximum number of parallel downloads
            
        Returns:
            Download results summary
        """
        results = {
            'successful': [],
            'failed': [],
            'total_size': 0,
            'total_time': 0
        }
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit download tasks
            future_to_key = {}
            for s3_key in s3_keys:
                filename = Path(s3_key).name
                local_path = os.path.join(local_directory, filename)
                
                future = executor.submit(
                    self.download_file, 
                    bucket_name, 
                    s3_key, 
                    local_path
                )
                future_to_key[future] = (s3_key, local_path)
            
            # Process completed downloads
            for future in as_completed(future_to_key):
                s3_key, local_path = future_to_key[future]
                try:
                    if future.result():
                        results['successful'].append(s3_key)
                        if os.path.exists(local_path):
                            results['total_size'] += os.path.getsize(local_path)
                    else:
                        results['failed'].append(s3_key)
                except Exception as e:
                    logger.error(f"Error downloading {s3_key}: {e}")
                    results['failed'].append(s3_key)
        
        results['total_time'] = time.time() - start_time
        results['success_rate'] = len(results['successful']) / len(s3_keys) * 100 if s3_keys else 0
        
        logger.info(f"Parallel download completed: {len(results['successful'])} successful, "
                   f"{len(results['failed'])} failed in {results['total_time']:.2f}s")
        
        return results
    
    def download_directory(self, 
                          bucket_name: str, 
                          s3_prefix: str, 
                          local_directory: str,
                          create_subdirs: bool = True,
                          file_extensions: List[str] = None,
                          exclude_patterns: List[str] = None,
                          parallel: bool = True,
                          max_workers: int = 5) -> Dict[str, Any]:
        """
        Download all files from S3 bucket/directory to local directory.
        
        Args:
            bucket_name: S3 bucket name
            s3_prefix: S3 prefix (directory path in bucket)
            local_directory: Local directory where files should be saved
            create_subdirs: Whether to recreate S3 directory structure locally
            file_extensions: List of file extensions to include (e.g., ['.txt', '.csv'])
            exclude_patterns: List of patterns to exclude
            parallel: Whether to use parallel downloads
            max_workers: Maximum number of parallel downloads
            
        Returns:
            Download results summary
        """
        try:
            logger.info(f"Downloading S3 directory: s3://{bucket_name}/{s3_prefix} -> {local_directory}")
            
            # Ensure local directory exists
            local_dir = Path(local_directory)
            local_dir.mkdir(parents=True, exist_ok=True)
            
            # List objects with the prefix
            objects_to_download = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        s3_key = obj['Key']
                        
                        # Skip directories
                        if s3_key.endswith('/'):
                            continue
                        
                        # Apply filters
                        if file_extensions:
                            if not any(s3_key.endswith(ext) for ext in file_extensions):
                                continue
                        
                        if exclude_patterns:
                            if any(pattern in s3_key for pattern in exclude_patterns):
                                continue
                        
                        objects_to_download.append({
                            'key': s3_key,
                            'size': obj['Size']
                        })
            
            if not objects_to_download:
                logger.warning(f"No objects found matching criteria in s3://{bucket_name}/{s3_prefix}")
                return {
                    'downloaded_count': 0,
                    'failed_count': 0,
                    'total_size_bytes': 0
                }
            
            # Download files
            if parallel and len(objects_to_download) > 1:
                return self._download_directory_parallel(
                    bucket_name, s3_prefix, local_dir, 
                    objects_to_download, create_subdirs, max_workers
                )
            else:
                return self._download_directory_sequential(
                    bucket_name, s3_prefix, local_dir, 
                    objects_to_download, create_subdirs
                )
            
        except ClientError as e:
            logger.error(f"Failed to download S3 directory: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error downloading S3 directory: {e}")
            raise
    
    def _download_directory_sequential(self, bucket_name, s3_prefix, local_dir, 
                                     objects_to_download, create_subdirs):
        """Sequential download implementation."""
        downloaded_files = []
        failed_files = []
        total_size = 0
        start_time = time.time()
        
        for obj in objects_to_download:
            s3_key = obj['key']
            file_size = obj['size']
            
            # Determine local file path
            if create_subdirs:
                relative_path = s3_key[len(s3_prefix):].lstrip('/')
                local_file_path = local_dir / relative_path
            else:
                filename = Path(s3_key).name
                local_file_path = local_dir / filename
            
            # Download file
            if self.download_file(bucket_name, s3_key, str(local_file_path)):
                downloaded_files.append({
                    's3_key': s3_key,
                    'local_path': str(local_file_path),
                    'size': file_size
                })
                total_size += file_size
            else:
                failed_files.append(s3_key)
        
        total_time = time.time() - start_time
        
        return {
            'bucket_name': bucket_name,
            's3_prefix': s3_prefix,
            'local_directory': str(local_dir),
            'downloaded_count': len(downloaded_files),
            'failed_count': len(failed_files),
            'total_size_bytes': total_size,
            'total_time_seconds': total_time,
            'downloaded_files': downloaded_files,
            'failed_files': failed_files,
            'success_rate': (len(downloaded_files) / len(objects_to_download) * 100)
        }
    
    def _download_directory_parallel(self, bucket_name, s3_prefix, local_dir, 
                                   objects_to_download, create_subdirs, max_workers):
        """Parallel download implementation."""
        downloaded_files = []
        failed_files = []
        total_size = 0
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_obj = {}
            
            for obj in objects_to_download:
                s3_key = obj['key']
                
                # Determine local file path
                if create_subdirs:
                    relative_path = s3_key[len(s3_prefix):].lstrip('/')
                    local_file_path = local_dir / relative_path
                else:
                    filename = Path(s3_key).name
                    local_file_path = local_dir / filename
                
                future = executor.submit(
                    self.download_file, 
                    bucket_name, 
                    s3_key, 
                    str(local_file_path)
                )
                future_to_obj[future] = (obj, str(local_file_path))
            
            for future in as_completed(future_to_obj):
                obj, local_path = future_to_obj[future]
                try:
                    if future.result():
                        downloaded_files.append({
                            's3_key': obj['key'],
                            'local_path': local_path,
                            'size': obj['size']
                        })
                        total_size += obj['size']
                    else:
                        failed_files.append(obj['key'])
                except Exception as e:
                    logger.error(f"Error in parallel download: {e}")
                    failed_files.append(obj['key'])
        
        total_time = time.time() - start_time
        
        return {
            'bucket_name': bucket_name,
            's3_prefix': s3_prefix,
            'local_directory': str(local_dir),
            'downloaded_count': len(downloaded_files),
            'failed_count': len(failed_files),
            'total_size_bytes': total_size,
            'total_time_seconds': total_time,
            'downloaded_files': downloaded_files,
            'failed_files': failed_files,
            'success_rate': (len(downloaded_files) / len(objects_to_download) * 100)
        }
    
    def upload_file(self, 
                   local_file_path: str, 
                   bucket_name: str, 
                   s3_key: str = None,
                   storage_class: str = 'STANDARD',
                   metadata: Dict[str, str] = None,
                   content_type: str = None) -> bool:
        """
        Upload a file to S3.
        
        Args:
            local_file_path: Local file path
            bucket_name: S3 bucket name
            s3_key: S3 object key (if None, uses filename)
            storage_class: S3 storage class ('STANDARD', 'GLACIER', etc.)
            metadata: Custom metadata for the object
            content_type: MIME type of the file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            local_path = Path(local_file_path)
            
            if not local_path.exists():
                logger.error(f"Local file not found: {local_file_path}")
                return False
            
            # Use filename as S3 key if not provided
            if s3_key is None:
                s3_key = local_path.name
            
            # Prepare upload arguments
            extra_args = {'StorageClass': storage_class}
            
            # Auto-detect content type if not provided
            if content_type is None:
                content_type, _ = mimetypes.guess_type(str(local_path))
            
            if content_type:
                extra_args['ContentType'] = content_type
            
            if metadata:
                extra_args['Metadata'] = metadata
            
            # Upload file with progress
            file_size = local_path.stat().st_size
            start_time = time.time()
            
            self.s3_client.upload_file(
                str(local_path), 
                bucket_name, 
                s3_key,
                ExtraArgs=extra_args,
                Callback=self._create_progress_callback(s3_key, 'upload', file_size)
            )
            
            upload_time = time.time() - start_time
            logger.info(f"Uploaded file to S3: {local_file_path} -> s3://{bucket_name}/{s3_key} "
                       f"({file_size:,} bytes in {upload_time:.2f}s)")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                logger.error(f"S3 bucket not found: {bucket_name}")
            elif error_code == 'AccessDenied':
                logger.error(f"Access denied to S3 bucket: {bucket_name}")
            else:
                logger.error(f"Failed to upload file to S3: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading file to S3: {e}")
            return False
    
    def upload_directory(self,
                        local_directory: str,
                        bucket_name: str,
                        s3_prefix: str = '',
                        file_extensions: List[str] = None,
                        exclude_patterns: List[str] = None,
                        parallel: bool = True,
                        max_workers: int = 5) -> Dict[str, Any]:
        """
        Upload entire directory to S3.
        
        Args:
            local_directory: Local directory to upload
            bucket_name: S3 bucket name
            s3_prefix: S3 prefix for uploaded files
            file_extensions: List of file extensions to include
            exclude_patterns: List of patterns to exclude
            parallel: Whether to use parallel uploads
            max_workers: Maximum number of parallel uploads
            
        Returns:
            Upload results summary
        """
        try:
            local_dir = Path(local_directory)
            if not local_dir.exists():
                raise ValueError(f"Local directory not found: {local_directory}")
            
            # Collect files to upload
            files_to_upload = []
            for file_path in local_dir.rglob('*'):
                if file_path.is_file():
                    # Apply filters
                    if file_extensions:
                        if not any(str(file_path).endswith(ext) for ext in file_extensions):
                            continue
                    
                    if exclude_patterns:
                        if any(pattern in str(file_path) for pattern in exclude_patterns):
                            continue
                    
                    # Calculate S3 key
                    relative_path = file_path.relative_to(local_dir)
                    s3_key = os.path.join(s3_prefix, str(relative_path)).replace('\\', '/')
                    
                    files_to_upload.append({
                        'local_path': str(file_path),
                        's3_key': s3_key,
                        'size': file_path.stat().st_size
                    })
            
            if not files_to_upload:
                logger.warning(f"No files found matching criteria in {local_directory}")
                return {'uploaded_count': 0, 'failed_count': 0}
            
            # Upload files
            if parallel and len(files_to_upload) > 1:
                return self._upload_directory_parallel(
                    bucket_name, files_to_upload, max_workers
                )
            else:
                return self._upload_directory_sequential(
                    bucket_name, files_to_upload
                )
            
        except Exception as e:
            logger.error(f"Failed to upload directory: {e}")
            raise
    
    def _upload_directory_sequential(self, bucket_name, files_to_upload):
        """Sequential upload implementation."""
        uploaded_files = []
        failed_files = []
        total_size = 0
        start_time = time.time()
        
        for file_info in files_to_upload:
            if self.upload_file(file_info['local_path'], bucket_name, file_info['s3_key']):
                uploaded_files.append(file_info)
                total_size += file_info['size']
            else:
                failed_files.append(file_info['local_path'])
        
        total_time = time.time() - start_time
        
        return {
            'uploaded_count': len(uploaded_files),
            'failed_count': len(failed_files),
            'total_size_bytes': total_size,
            'total_time_seconds': total_time,
            'uploaded_files': uploaded_files,
            'failed_files': failed_files,
            'success_rate': (len(uploaded_files) / len(files_to_upload) * 100)
        }
    
    def _upload_directory_parallel(self, bucket_name, files_to_upload, max_workers):
        """Parallel upload implementation."""
        uploaded_files = []
        failed_files = []
        total_size = 0
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {}
            
            for file_info in files_to_upload:
                future = executor.submit(
                    self.upload_file,
                    file_info['local_path'],
                    bucket_name,
                    file_info['s3_key']
                )
                future_to_file[future] = file_info
            
            for future in as_completed(future_to_file):
                file_info = future_to_file[future]
                try:
                    if future.result():
                        uploaded_files.append(file_info)
                        total_size += file_info['size']
                    else:
                        failed_files.append(file_info['local_path'])
                except Exception as e:
                    logger.error(f"Error in parallel upload: {e}")
                    failed_files.append(file_info['local_path'])
        
        total_time = time.time() - start_time
        
        return {
            'uploaded_count': len(uploaded_files),
            'failed_count': len(failed_files),
            'total_size_bytes': total_size,
            'total_time_seconds': total_time,
            'uploaded_files': uploaded_files,
            'failed_files': failed_files,
            'success_rate': (len(uploaded_files) / len(files_to_upload) * 100)
        }
    
    def copy_object(self,
                   source_bucket: str,
                   source_key: str,
                   dest_bucket: str,
                   dest_key: str,
                   metadata_directive: str = 'COPY') -> bool:
        """
        Copy object within S3.
        
        Args:
            source_bucket: Source bucket name
            source_key: Source object key
            dest_bucket: Destination bucket name
            dest_key: Destination object key
            metadata_directive: 'COPY' or 'REPLACE' for metadata handling
            
        Returns:
            True if successful, False otherwise
        """
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key,
                MetadataDirective=metadata_directive
            )
            
            logger.info(f"Copied S3 object: s3://{source_bucket}/{source_key} -> "
                       f"s3://{dest_bucket}/{dest_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to copy S3 object: {e}")
            return False
    
    def object_exists(self, bucket_name: str, s3_key: str) -> bool:
        """
        Check if an object exists in S3.
        
        Args:
            bucket_name: S3 bucket name
            s3_key: S3 object key
            
        Returns:
            True if object exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking object existence: {e}")
                raise
    
    def list_objects(self, 
                    bucket_name: str, 
                    prefix: str = '', 
                    max_keys: int = 1000,
                    delimiter: str = None) -> List[Dict[str, Any]]:
        """
        List objects in S3 bucket with optional prefix.
        
        Args:
            bucket_name: S3 bucket name
            prefix: Object prefix to filter by
            max_keys: Maximum number of objects to return
            delimiter: Delimiter for grouping keys (e.g., '/' for directory-like listing)
            
        Returns:
            List of object information dictionaries
        """
        try:
            objects = []
            common_prefixes = []
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            pagination_config = {'MaxItems': max_keys}
            list_kwargs = {
                'Bucket': bucket_name,
                'Prefix': prefix,
                'PaginationConfig': pagination_config
            }
            
            if delimiter:
                list_kwargs['Delimiter'] = delimiter
            
            page_iterator = paginator.paginate(**list_kwargs)
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'etag': obj['ETag'].strip('"'),
                            'storage_class': obj.get('StorageClass', 'STANDARD')
                        })
                
                # Collect common prefixes (simulated directories)
                if delimiter and 'CommonPrefixes' in page:
                    for prefix_info in page['CommonPrefixes']:
                        common_prefixes.append(prefix_info['Prefix'])
            
            result = {
                'objects': objects,
                'count': len(objects)
            }
            
            if common_prefixes:
                result['common_prefixes'] = common_prefixes
            
            logger.info(f"Listed {len(objects)} objects from s3://{bucket_name}/{prefix}")
            return objects
            
        except ClientError as e:
            logger.error(f"Failed to list S3 objects: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing S3 objects: {e}")
            raise
    
    def delete_object(self, bucket_name: str, s3_key: str) -> bool:
        """
        Delete an object from S3.
        
        Args:
            bucket_name: S3 bucket name
            s3_key: S3 object key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
            
            logger.info(f"Deleted S3 object: s3://{bucket_name}/{s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete S3 object: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting S3 object: {e}")
            return False
    
    def delete_objects(self, bucket_name: str, s3_keys: List[str]) -> Dict[str, Any]:
        """
        Delete multiple objects from S3.
        
        Args:
            bucket_name: S3 bucket name
            s3_keys: List of S3 object keys
            
        Returns:
            Deletion results
        """
        try:
            # Prepare delete request
            objects = [{'Key': key} for key in s3_keys]
            
            # S3 delete_objects has a limit of 1000 objects per request
            deleted_count = 0
            errors = []
            
            for i in range(0, len(objects), 1000):
                batch = objects[i:i+1000]
                
                response = self.s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={
                        'Objects': batch,
                        'Quiet': False
                    }
                )
                
                if 'Deleted' in response:
                    deleted_count += len(response['Deleted'])
                
                if 'Errors' in response:
                    errors.extend(response['Errors'])
            
            logger.info(f"Deleted {deleted_count} objects from S3 bucket: {bucket_name}")
            
            return {
                'deleted_count': deleted_count,
                'error_count': len(errors),
                'errors': errors
            }
            
        except ClientError as e:
            logger.error(f"Failed to delete objects: {e}")
            raise
    
    def get_object_info(self, bucket_name: str, s3_key: str) -> Dict[str, Any]:
        """
        Get information about an S3 object.
        
        Args:
            bucket_name: S3 bucket name
            s3_key: S3 object key
            
        Returns:
            Object information dictionary
        """
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            
            object_info = {
                'bucket': bucket_name,
                'key': s3_key,
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'content_type': response.get('ContentType', 'binary/octet-stream'),
                'etag': response['ETag'].strip('"'),
                'storage_class': response.get('StorageClass', 'STANDARD'),
                'metadata': response.get('Metadata', {}),
                'version_id': response.get('VersionId'),
                'cache_control': response.get('CacheControl'),
                'content_encoding': response.get('ContentEncoding'),
                'expires': response.get('Expires')
            }
            
            logger.info(f"Retrieved S3 object info: s3://{bucket_name}/{s3_key}")
            return object_info
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"S3 object not found: s3://{bucket_name}/{s3_key}")
            else:
                logger.error(f"Failed to get S3 object info: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting S3 object info: {e}")
            raise
    
    def generate_presigned_url(self,
                             bucket_name: str,
                             s3_key: str,
                             expiration: int = 3600,
                             http_method: str = 'GET') -> Optional[str]:
        """
        Generate a presigned URL for S3 object access.
        
        Args:
            bucket_name: S3 bucket name
            s3_key: S3 object key
            expiration: URL expiration time in seconds
            http_method: HTTP method ('GET' or 'PUT')
            
        Returns:
            Presigned URL or None if failed
        """
        try:
            if http_method.upper() == 'GET':
                client_method = 'get_object'
            elif http_method.upper() == 'PUT':
                client_method = 'put_object'
            else:
                raise ValueError(f"Unsupported HTTP method: {http_method}")
            
            url = self.s3_client.generate_presigned_url(
                client_method,
                Params={'Bucket': bucket_name, 'Key': s3_key},
                ExpiresIn=expiration
            )
            
            logger.info(f"Generated presigned URL for s3://{bucket_name}/{s3_key} "
                       f"(expires in {expiration}s)")
            return url
            
        except Exception as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            return None
    
    def get_bucket_info(self, bucket_name: str) -> Dict[str, Any]:
        """
        Get information about an S3 bucket.
        
        Args:
            bucket_name: S3 bucket name
            
        Returns:
            Bucket information
        """
        try:
            # Get bucket location
            location_response = self.s3_client.get_bucket_location(Bucket=bucket_name)
            location = location_response.get('LocationConstraint', 'us-east-1')
            
            # Get bucket versioning
            versioning_response = self.s3_client.get_bucket_versioning(Bucket=bucket_name)
            versioning_status = versioning_response.get('Status', 'Disabled')
            
            # Get bucket encryption
            try:
                encryption_response = self.s3_client.get_bucket_encryption(Bucket=bucket_name)
                encryption = encryption_response.get('ServerSideEncryptionConfiguration', {})
            except ClientError as e:
                if e.response['Error']['Code'] == 'ServerSideEncryptionConfigurationNotFoundError':
                    encryption = None
                else:
                    raise
            
            # Get bucket size and object count
            size_info = self._get_bucket_size_info(bucket_name)
            
            bucket_info = {
                'name': bucket_name,
                'location': location or 'us-east-1',
                'versioning': versioning_status,
                'encryption': encryption,
                'size_bytes': size_info['total_size'],
                'object_count': size_info['object_count'],
                'created': None  # Creation date not available via API
            }
            
            logger.info(f"Retrieved bucket info for: {bucket_name}")
            return bucket_info
            
        except ClientError as e:
            logger.error(f"Failed to get bucket info: {e}")
            raise
    
    def _get_bucket_size_info(self, bucket_name: str) -> Dict[str, Any]:
        """Get bucket size and object count."""
        total_size = 0
        object_count = 0
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        total_size += obj['Size']
                        object_count += 1
            
            return {
                'total_size': total_size,
                'object_count': object_count
            }
        except Exception as e:
            logger.error(f"Error calculating bucket size: {e}")
            return {'total_size': 0, 'object_count': 0}
    
    def create_bucket(self, 
                     bucket_name: str, 
                     region: str = None,
                     enable_versioning: bool = False) -> bool:
        """
        Create a new S3 bucket.
        
        Args:
            bucket_name: Bucket name
            region: AWS region (if None, uses default)
            enable_versioning: Whether to enable versioning
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create bucket
            if region and region != 'us-east-1':
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            else:
                self.s3_client.create_bucket(Bucket=bucket_name)
            
            logger.info(f"Created S3 bucket: {bucket_name}")
            
            # Enable versioning if requested
            if enable_versioning:
                self.s3_client.put_bucket_versioning(
                    Bucket=bucket_name,
                    VersioningConfiguration={'Status': 'Enabled'}
                )
                logger.info(f"Enabled versioning for bucket: {bucket_name}")
            
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'BucketAlreadyExists':
                logger.error(f"Bucket already exists: {bucket_name}")
            else:
                logger.error(f"Failed to create bucket: {e}")
            return False
    
    def sync_directories(self,
                        source_bucket: str,
                        source_prefix: str,
                        dest_bucket: str,
                        dest_prefix: str,
                        delete_removed: bool = False) -> Dict[str, Any]:
        """
        Sync objects between S3 locations.
        
        Args:
            source_bucket: Source bucket
            source_prefix: Source prefix
            dest_bucket: Destination bucket
            dest_prefix: Destination prefix
            delete_removed: Whether to delete objects in dest not in source
            
        Returns:
            Sync results
        """
        try:
            # Get source objects
            source_objects = self.list_objects(source_bucket, source_prefix)
            source_keys = {obj['key'] for obj in source_objects}
            
            # Get destination objects
            dest_objects = self.list_objects(dest_bucket, dest_prefix)
            dest_keys = {obj['key'] for obj in dest_objects}
            
            # Calculate operations needed
            to_copy = []
            to_delete = []
            
            # Objects to copy (new or updated)
            for src_obj in source_objects:
                src_key = src_obj['key']
                relative_key = src_key[len(source_prefix):].lstrip('/')
                dest_key = os.path.join(dest_prefix, relative_key).replace('\\', '/')
                
                # Check if needs copying
                needs_copy = True
                for dest_obj in dest_objects:
                    if dest_obj['key'] == dest_key:
                        # Compare ETags to see if content changed
                        if dest_obj['etag'] == src_obj['etag']:
                            needs_copy = False
                        break
                
                if needs_copy:
                    to_copy.append({
                        'source_key': src_key,
                        'dest_key': dest_key,
                        'size': src_obj['size']
                    })
            
            # Objects to delete (if delete_removed is True)
            if delete_removed:
                for dest_obj in dest_objects:
                    dest_key = dest_obj['key']
                    relative_key = dest_key[len(dest_prefix):].lstrip('/')
                    src_key = os.path.join(source_prefix, relative_key).replace('\\', '/')
                    
                    if src_key not in source_keys:
                        to_delete.append(dest_key)
            
            # Execute sync operations
            copied_count = 0
            copy_errors = []
            
            for copy_info in to_copy:
                try:
                    self.copy_object(
                        source_bucket, copy_info['source_key'],
                        dest_bucket, copy_info['dest_key']
                    )
                    copied_count += 1
                except Exception as e:
                    logger.error(f"Failed to copy {copy_info['source_key']}: {e}")
                    copy_errors.append(str(e))
            
            deleted_count = 0
            if to_delete:
                delete_result = self.delete_objects(dest_bucket, to_delete)
                deleted_count = delete_result['deleted_count']
            
            return {
                'source_objects': len(source_objects),
                'dest_objects': len(dest_objects),
                'copied': copied_count,
                'deleted': deleted_count,
                'copy_errors': copy_errors,
                'to_copy_count': len(to_copy),
                'to_delete_count': len(to_delete)
            }
            
        except Exception as e:
            logger.error(f"Failed to sync directories: {e}")
            raise
    
    def test_connection(self, bucket_name: str = None) -> bool:
        """
        Test S3 connection.
        
        Args:
            bucket_name: Optional bucket name to test specific bucket access
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if bucket_name:
                # Test specific bucket access
                response = self.s3_client.head_bucket(Bucket=bucket_name)
                logger.info(f"S3 connection test successful for bucket: {bucket_name}")
            else:
                # Test general S3 access by listing buckets
                response = self.s3_client.list_buckets()
                bucket_count = len(response.get('Buckets', []))
                logger.info(f"S3 connection test successful - {bucket_count} buckets accessible")
            
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '403':
                logger.error("S3 connection test failed: Access denied")
            elif error_code == '404':
                logger.error(f"S3 connection test failed: Bucket not found - {bucket_name}")
            else:
                logger.error(f"S3 connection test failed: {e}")
            return False
        except Exception as e:
            logger.error(f"S3 connection test failed: {e}")
            return False
    
    def _create_progress_callback(self, filename: str, operation: str, total_size: int = None):
        """Create a progress callback for file transfers."""
        class ProgressTracker:
            def __init__(self):
                self.bytes_transferred = 0
                self.last_reported = 0
            
            def __call__(self, bytes_amount):
                self.bytes_transferred += bytes_amount
                
                # Report progress every 10MB
                if self.bytes_transferred - self.last_reported > 10 * 1024 * 1024:
                    if total_size:
                        percentage = (self.bytes_transferred / total_size) * 100
                        logger.debug(f"{operation} progress for {filename}: "
                                   f"{percentage:.1f}% ({self.bytes_transferred:,}/{total_size:,} bytes)")
                    else:
                        logger.debug(f"{operation} progress for {filename}: "
                                   f"{self.bytes_transferred:,} bytes")
                    self.last_reported = self.bytes_transferred
        
        return ProgressTracker()
    
    def _verify_file_checksum(self, file_path: str, expected_etag: str) -> bool:
        """Verify file checksum against S3 ETag."""
        try:
            # For files uploaded as single part, ETag is MD5
            # For multipart uploads, ETag has a different format
            if '-' in expected_etag:
                # Multipart upload - skip verification
                logger.debug("Skipping checksum verification for multipart upload")
                return True
            
            # Calculate MD5 of local file
            md5_hash = hashlib.md5()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    md5_hash.update(chunk)
            
            calculated_md5 = md5_hash.hexdigest()
            
            if calculated_md5 == expected_etag:
                logger.debug(f"Checksum verified for {file_path}")
                return True
            else:
                logger.error(f"Checksum mismatch for {file_path}: "
                           f"expected {expected_etag}, got {calculated_md5}")
                return False
                
        except Exception as e:
            logger.error(f"Error verifying checksum: {e}")
            return False

# Global S3 connector instance
s3_connector = S3Connector()