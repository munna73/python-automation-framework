"""
AWS S3 connector for downloading and uploading files from S3 buckets.
This is a robust and feature-rich connector that handles single and parallel
file operations, including directory synchronization, with progress reporting
and error handling.
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
from utils.config_loader import ConfigLoader, config_loader
from utils.logger import logger
import time

class S3Connector:
    """AWS S3 connector for file operations."""
    
    def __init__(self, profile_name: Optional[str] = None, config_section: str = "S101_S3"):
        """
        Initialize S3 connector.
        
        Args:
            profile_name: AWS profile name to use. If None, uses environment variables from config.
            config_section: S3 configuration section name (e.g., 'S101_S3', 'S102_S3')
        """
        self.s3_client = None
        self.s3_resource = None
        self.aws_config = None
        self.profile_name = profile_name
        self.config_section = config_section
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
                # Use environment variables from config.ini
                self.aws_config = config_loader.get_s3_config(self.config_section)
                
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
                
                logger.info(f"AWS S3 connection established using config section: {self.config_section}")
            
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
            raise
        except Exception as e:
            logger.error(f"Failed to setup AWS S3 connection: {e}")
            raise

    def get_bucket_name(self) -> str:
        """
        Get the bucket name from configuration.
        
        Returns:
            Bucket name from configuration
        """
        if self.aws_config:
            return self.aws_config.get('bucket_name', '')
        return ''

    def get_region(self) -> str:
        """
        Get the region from configuration.
        
        Returns:
            Region from configuration
        """
        if self.aws_config:
            return self.aws_config.get('region', 'us-east-1')
        return 'us-east-1'

    def _create_progress_callback(self, filename: str, operation: str, total_size: int = None):
        """
        Creates a callback function to report progress of S3 operations.
        
        Args:
            filename: The name of the file being processed.
            operation: The operation type, e.g., 'download' or 'upload'.
            total_size: The total size of the file in bytes.
        
        Returns:
            A callable progress callback function.
        """
        # This is an example callback. In a real-world application,
        # you might update a UI or use a more sophisticated logger.
        if total_size:
            uploaded_bytes = [0]
            
            def progress_callback(bytes_amount: int):
                uploaded_bytes[0] += bytes_amount
                progress_percent = (uploaded_bytes[0] / total_size) * 100
                if progress_percent % 10 == 0:
                    logger.info(f"Progress for {operation} {filename}: {progress_percent:.0f}%")
        else:
            # Simple version for when total size isn't known upfront (e.g., download)
            bytes_transferred = [0]
            
            def progress_callback(bytes_amount: int):
                bytes_transferred[0] += bytes_amount
                logger.debug(f"Transferred {bytes_transferred[0]} bytes for {filename}")
        
        return progress_callback
    
    def _verify_file_checksum(self, file_path: str, expected_etag: str) -> bool:
        """
        Calculates MD5 checksum of a local file and compares it to an S3 ETag.
        Note: ETag is not always a simple MD5 hash.
        
        Args:
            file_path: Path to the local file.
            expected_etag: The ETag from the S3 object.
            
        Returns:
            True if checksums match, False otherwise.
        """
        try:
            file_hash = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    file_hash.update(chunk)
            
            # For multi-part uploads, ETag is hash-of-hashes. Simple MD5 is for single-part.
            # This simplified check works for objects uploaded in a single part.
            local_etag = file_hash.hexdigest()
            return local_etag == expected_etag
        except Exception as e:
            logger.error(f"Error verifying checksum for {file_path}: {e}")
            return False
            
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
            object_info = {}
            if verify_checksum:
                object_info = self.get_object_info(bucket_name, s3_key)
                if not object_info:
                    return False
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
                'storage_class': response.get('StorageClass'),
                'metadata': response.get('Metadata', {})
            }
            return object_info
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.warning(f"Object not found: s3://{bucket_name}/{s3_key}")
                return None
            else:
                logger.error(f"Error getting object info: {e}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error getting object info: {e}")
            raise
            
    def get_object_metadata(self, bucket_name: str, s3_key: str) -> Optional[Dict[str, str]]:
        """
        Get custom metadata for an S3 object.
        
        Args:
            bucket_name: S3 bucket name
            s3_key: S3 object key
        
        Returns:
            A dictionary of metadata, or None if the object is not found.
        """
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            return response.get('Metadata')
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return None
            else:
                logger.error(f"Error getting object metadata: {e}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error getting object metadata: {e}")
            raise

    # ========================================
    # MESSAGE-STYLE OPERATIONS FOR S3
    # ========================================
    
    def get_object_content(self, bucket_name: str, s3_key: str) -> Optional[str]:
        """
        Get the content of an S3 object as a string.
        
        Args:
            bucket_name: S3 bucket name
            s3_key: S3 object key
            
        Returns:
            Object content as string, or None if object not found
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            
            logger.debug(f"Retrieved content from s3://{bucket_name}/{s3_key} ({len(content)} characters)")
            return content
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.warning(f"Object not found: s3://{bucket_name}/{s3_key}")
                return None
            else:
                logger.error(f"Failed to get object content: {e}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error getting object content: {e}")
            raise
    
    def put_object_content(self, bucket_name: str, s3_key: str, content: str, 
                          content_type: str = 'text/plain') -> bool:
        """
        Put content directly to S3 as an object.
        
        Args:
            bucket_name: S3 bucket name
            s3_key: S3 object key
            content: Content to upload as string
            content_type: Content type (default: text/plain)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=content.encode('utf-8'),
                ContentType=content_type
            )
            
            logger.info(f"Put content to s3://{bucket_name}/{s3_key} ({len(content)} characters)")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to put object content: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error putting object content: {e}")
            return False
    
    def download_objects_as_messages(self, bucket_name: str, prefix: str, 
                                   output_file: str, one_message_per_line: bool = True,
                                   max_objects: int = None) -> Dict[str, Any]:
        """
        Download S3 objects and write their content as messages to a file.
        
        Args:
            bucket_name: S3 bucket name
            prefix: S3 prefix to filter objects
            output_file: Local file path to write messages
            one_message_per_line: If True, each object content = one line in file
                                If False, concatenate all object content as single file
            max_objects: Maximum number of objects to process (None for all)
            
        Returns:
            Dictionary with download results and statistics
        """
        try:
            # List objects with the given prefix
            objects = self.list_objects(bucket_name, prefix)
            
            if max_objects:
                objects = objects[:max_objects]
            
            logger.info(f"Processing {len(objects)} objects from s3://{bucket_name}/{prefix}")
            
            messages_written = 0
            total_size = 0
            failed_objects = []
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for i, obj in enumerate(objects):
                    try:
                        s3_key = obj['key']
                        content = self.get_object_content(bucket_name, s3_key)
                        
                        if content is not None:
                            if one_message_per_line:
                                # Write each object content as a separate line
                                # Strip any existing newlines and add single newline
                                clean_content = content.strip().replace('\n', ' ').replace('\r', ' ')
                                f.write(clean_content + '\n')
                            else:
                                # Concatenate all content (preserve original formatting)
                                f.write(content)
                                if not content.endswith('\n'):
                                    f.write('\n')
                            
                            messages_written += 1
                            total_size += len(content)
                            
                            logger.debug(f"Processed object {i+1}/{len(objects)}: {s3_key}")
                        else:
                            failed_objects.append(s3_key)
                            
                    except Exception as e:
                        logger.error(f"Failed to process object {obj['key']}: {e}")
                        failed_objects.append(obj['key'])
            
            results = {
                'success': True,
                'messages_written': messages_written,
                'total_objects': len(objects),
                'output_file': output_file,
                'total_content_size': total_size,
                'failed_objects': failed_objects,
                'success_rate': (messages_written / len(objects) * 100) if objects else 100
            }
            
            logger.info(f"Downloaded {messages_written}/{len(objects)} objects as messages to {output_file}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to download S3 objects as messages: {e}")
            return {
                'success': False,
                'error': str(e),
                'messages_written': 0,
                'total_objects': 0
            }
    
    def upload_file_as_s3_messages(self, filename: str, bucket_name: str, 
                                  prefix: str, line_by_line: bool = True,
                                  message_prefix: str = 'message') -> Dict[str, Any]:
        """
        Read a file and upload its content to S3 as separate objects or single object.
        
        Args:
            filename: Local file path to read
            bucket_name: S3 bucket name
            prefix: S3 prefix for uploaded objects
            line_by_line: If True, each line becomes a separate S3 object
                         If False, entire file becomes single S3 object
            message_prefix: Prefix for S3 object keys (used with line numbers)
            
        Returns:
            Dictionary with upload results and statistics
        """
        try:
            if not os.path.exists(filename):
                raise FileNotFoundError(f"File not found: {filename}")
            
            uploaded_objects = []
            failed_uploads = []
            total_lines = 0
            
            with open(filename, 'r', encoding='utf-8') as f:
                if line_by_line:
                    # Upload each line as a separate S3 object
                    for line_num, line in enumerate(f, 1):
                        if line.strip():  # Skip empty lines
                            total_lines += 1
                            s3_key = f"{prefix}/{message_prefix}_{line_num:06d}.txt"
                            
                            if self.put_object_content(bucket_name, s3_key, line.strip()):
                                uploaded_objects.append({
                                    'line_number': line_num,
                                    's3_key': s3_key,
                                    'content_length': len(line.strip())
                                })
                                logger.debug(f"Uploaded line {line_num} to s3://{bucket_name}/{s3_key}")
                            else:
                                failed_uploads.append({
                                    'line_number': line_num,
                                    's3_key': s3_key,
                                    'error': 'Upload failed'
                                })
                else:
                    # Upload entire file content as single S3 object
                    f.seek(0)  # Reset file pointer
                    content = f.read()
                    total_lines = len(content.splitlines()) if content else 0
                    
                    s3_key = f"{prefix}/{os.path.basename(filename)}"
                    
                    if self.put_object_content(bucket_name, s3_key, content):
                        uploaded_objects.append({
                            'line_number': 'all',
                            's3_key': s3_key,
                            'content_length': len(content)
                        })
                        logger.info(f"Uploaded entire file to s3://{bucket_name}/{s3_key}")
                    else:
                        failed_uploads.append({
                            'line_number': 'all',
                            's3_key': s3_key,
                            'error': 'Upload failed'
                        })
            
            results = {
                'success': len(failed_uploads) == 0,
                'total_lines': total_lines,
                'uploaded_count': len(uploaded_objects),
                'failed_count': len(failed_uploads),
                'uploaded_objects': uploaded_objects,
                'failed_uploads': failed_uploads,
                'success_rate': (len(uploaded_objects) / max(total_lines, 1) * 100) if line_by_line else (100 if uploaded_objects else 0)
            }
            
            logger.info(f"Upload completed: {len(uploaded_objects)}/{total_lines} {'lines' if line_by_line else 'files'} uploaded successfully")
            return results
            
        except Exception as e:
            logger.error(f"Failed to upload file as S3 messages: {e}")
            return {
                'success': False,
                'error': str(e),
                'total_lines': 0,
                'uploaded_count': 0,
                'failed_count': 1
            }
    
    def retrieve_s3_messages_to_file(self, bucket_name: str, prefix: str,
                                   output_file: str, retrieve_mode: str = 'line_by_line',
                                   max_messages: int = None) -> Dict[str, Any]:
        """
        Retrieve S3 objects as messages and write to file with different modes.
        
        Args:
            bucket_name: S3 bucket name
            prefix: S3 prefix to filter objects
            output_file: Local file path to write messages
            retrieve_mode: 'line_by_line' (each object = 1 line) or 
                          'whole_file' (concatenate all objects)
            max_messages: Maximum number of messages to retrieve
            
        Returns:
            Dictionary with retrieval results
        """
        one_message_per_line = (retrieve_mode == 'line_by_line')
        
        return self.download_objects_as_messages(
            bucket_name=bucket_name,
            prefix=prefix,
            output_file=output_file,
            one_message_per_line=one_message_per_line,
            max_objects=max_messages
        )
    
    def send_file_to_s3_messages(self, filename: str, bucket_name: str,
                               prefix: str, send_mode: str = 'line_by_line') -> Dict[str, Any]:
        """
        Send file content to S3 as messages with different modes.
        
        Args:
            filename: Local file path to read
            bucket_name: S3 bucket name
            prefix: S3 prefix for uploaded objects
            send_mode: 'line_by_line' (each line = separate object) or 
                      'whole_file' (entire file = single object)
        
        Returns:
            Dictionary with upload results
        """
        line_by_line = (send_mode == 'line_by_line')
        
        return self.upload_file_as_s3_messages(
            filename=filename,
            bucket_name=bucket_name,
            prefix=prefix,
            line_by_line=line_by_line
        )
