"""
AWS S3 connector for downloading and uploading files from S3 buckets.
"""
import boto3
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
from botocore.exceptions import ClientError, NoCredentialsError
from utils.config_loader import config_loader
from utils.logger import logger

class S3Connector:
    """AWS S3 connector for file operations."""
    
    def __init__(self):
        """Initialize S3 connector."""
        self.s3_client = None
        self.s3_resource = None
        self.aws_config = None
        self.setup_aws_connection()
    
    def setup_aws_connection(self):
        """Setup AWS S3 connection using environment variables."""
        try:
            self.aws_config = config_loader.get_aws_config()
            
            # Create S3 client and resource
            self.s3_client = boto3.client(
                's3',
                region_name=self.aws_config.get('region', 'us-east-1'),
                aws_access_key_id=self.aws_config.get('access_key_id'),
                aws_secret_access_key=self.aws_config.get('secret_access_key')
            )
            
            self.s3_resource = boto3.resource(
                's3',
                region_name=self.aws_config.get('region', 'us-east-1'),
                aws_access_key_id=self.aws_config.get('access_key_id'),
                aws_secret_access_key=self.aws_config.get('secret_access_key')
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
                     local_file_path: str) -> bool:
        """
        Download a single file from S3.
        
        Args:
            bucket_name: S3 bucket name
            s3_key: S3 object key (file path in bucket)
            local_file_path: Local path where file should be saved
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure local directory exists
            local_path = Path(local_file_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Download file
            self.s3_client.download_file(bucket_name, s3_key, str(local_path))
            
            logger.info(f"Downloaded S3 file: s3://{bucket_name}/{s3_key} -> {local_file_path}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.error(f"S3 object not found: s3://{bucket_name}/{s3_key}")
            elif error_code == 'NoSuchBucket':
                logger.error(f"S3 bucket not found: {bucket_name}")
            else:
                logger.error(f"Failed to download S3 file: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading S3 file: {e}")
            return False
    
    def download_directory(self, 
                          bucket_name: str, 
                          s3_prefix: str, 
                          local_directory: str,
                          create_subdirs: bool = True) -> Dict[str, Any]:
        """
        Download all files from S3 bucket/directory to local directory.
        
        Args:
            bucket_name: S3 bucket name
            s3_prefix: S3 prefix (directory path in bucket)
            local_directory: Local directory where files should be saved
            create_subdirs: Whether to recreate S3 directory structure locally
            
        Returns:
            Download results summary
        """
        try:
            logger.info(f"Downloading S3 directory: s3://{bucket_name}/{s3_prefix} -> {local_directory}")
            
            # Ensure local directory exists
            local_dir = Path(local_directory)
            local_dir.mkdir(parents=True, exist_ok=True)
            
            # List objects with the prefix
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)
            
            downloaded_files = []
            failed_files = []
            total_size = 0
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        s3_key = obj['Key']
                        file_size = obj['Size']
                        
                        # Skip directories (objects ending with /)
                        if s3_key.endswith('/'):
                            continue
                        
                        # Determine local file path
                        if create_subdirs:
                            # Maintain S3 directory structure
                            relative_path = s3_key[len(s3_prefix):].lstrip('/')
                            local_file_path = local_dir / relative_path
                        else:
                            # Flatten all files to local directory
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
            
            results = {
                'bucket_name': bucket_name,
                's3_prefix': s3_prefix,
                'local_directory': str(local_dir),
                'downloaded_count': len(downloaded_files),
                'failed_count': len(failed_files),
                'total_size_bytes': total_size,
                'downloaded_files': downloaded_files,
                'failed_files': failed_files,
                'success_rate': (len(downloaded_files) / (len(downloaded_files) + len(failed_files)) * 100) if (len(downloaded_files) + len(failed_files)) > 0 else 0
            }
            
            logger.info(f"Directory download completed: {len(downloaded_files)} files, {total_size} bytes")
            return results
            
        except ClientError as e:
            logger.error(f"Failed to download S3 directory: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error downloading S3 directory: {e}")
            raise
    
    def upload_file(self, 
                   local_file_path: str, 
                   bucket_name: str, 
                   s3_key: str = None) -> bool:
        """
        Upload a file to S3.
        
        Args:
            local_file_path: Local file path
            bucket_name: S3 bucket name
            s3_key: S3 object key (if None, uses filename)
            
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
            
            # Upload file
            self.s3_client.upload_file(str(local_path), bucket_name, s3_key)
            
            logger.info(f"Uploaded file to S3: {local_file_path} -> s3://{bucket_name}/{s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to upload file to S3: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading file to S3: {e}")
            return False
    
    def list_objects(self, 
                    bucket_name: str, 
                    prefix: str = '', 
                    max_keys: int = 1000) -> List[Dict[str, Any]]:
        """
        List objects in S3 bucket with optional prefix.
        
        Args:
            bucket_name: S3 bucket name
            prefix: Object prefix to filter by
            max_keys: Maximum number of objects to return
            
        Returns:
            List of object information dictionaries
        """
        try:
            objects = []
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket_name,
                Prefix=prefix,
                PaginationConfig={'MaxItems': max_keys}
            )
            
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
                'content_type': response['ContentType'],
                'etag': response['ETag'].strip('"'),
                'metadata': response.get('Metadata', {})
            }
            
            logger.info(f"Retrieved S3 object info: s3://{bucket_name}/{s3_key}")
            return object_info
            
        except ClientError as e:
            logger.error(f"Failed to get S3 object info: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting S3 object info: {e}")
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
            
        except Exception as e:
            logger.error(f"S3 connection test failed: {e}")
            return False

# Global S3 connector instance
s3_connector = S3Connector()