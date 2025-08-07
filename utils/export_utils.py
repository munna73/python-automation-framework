"""
Export utilities for writing data to CSV and Excel files with enhanced error handling.
"""
import pandas as pd
import os
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List, Union

from utils.logger import logger, db_logger

# Import optional dependencies with fallbacks
try:
    from utils.data_cleaner import data_cleaner
except ImportError:
    data_cleaner = None
    logger.warning("data_cleaner module not found, using basic data processing")

try:
    from utils.config_loader import ConfigLoader
except ImportError:
    config_loader = None
    logger.warning("config_loader module not found, using default export settings")


class ExportUtils:
    """Utility class for exporting data to various formats."""
    
    def __init__(self):
        """Initialize export utilities."""
        self.output_dir = Path(__file__).parent.parent / "output" / "exports"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load export settings with fallback
        self.export_settings = self._get_export_settings()
        
        logger.info(f"ExportUtils initialized with output directory: {self.output_dir}")
    
    def _get_export_settings(self) -> Dict[str, Any]:
        """Get export settings from config or use defaults."""
        default_settings = {
            'csv_encoding': 'utf-8-sig',
            'date_format': '%Y-%m-%d %H:%M:%S',
            'decimal_places': 2,
            'max_cell_length': 32767,  # Excel cell limit
            'chunk_size': 10000,
            'clean_data_by_default': True
        }
        
        if config_loader:
            try:
                custom_settings = config_loader.get_custom_config('EXPORT_SETTINGS')
                default_settings.update(custom_settings)
            except Exception as e:
                logger.debug(f"Could not load export settings from config: {e}")
        
        return default_settings
    
    def write_to_csv_safe(self, 
                         df: pd.DataFrame, 
                         filepath: str, 
                         clean_data: bool = None,
                         encoding: str = None,
                         **kwargs) -> bool:
        """
        Safe CSV writing with special character handling.
        
        Args:
            df: DataFrame to export
            filepath: Output file path
            clean_data: Whether to clean data before export (default from settings)
            encoding: File encoding (default from config)
            **kwargs: Additional pandas.to_csv parameters
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if df is None or df.empty:
                logger.warning("DataFrame is empty or None, creating empty CSV file")
                Path(filepath).parent.mkdir(parents=True, exist_ok=True)
                pd.DataFrame().to_csv(filepath, index=False)
                return True
            
            if encoding is None:
                encoding = self.export_settings.get('csv_encoding', 'utf-8-sig')
            
            if clean_data is None:
                clean_data = self.export_settings.get('clean_data_by_default', True)
            
            logger.info(f"Exporting {len(df)} rows to CSV: {filepath}")
            
            # Clean data if requested and data_cleaner is available
            export_df = df.copy()
            if clean_data and data_cleaner:
                export_df = data_cleaner.clean_data_for_export(export_df, 'csv')
            elif clean_data:
                # Basic cleaning without data_cleaner
                export_df = self._basic_clean_for_csv(export_df)
            
            # Ensure output directory exists
            output_path = Path(filepath)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Prepare CSV export parameters
            csv_params = {
                'index': False,
                'encoding': encoding,
                'quoting': 1,  # QUOTE_ALL to handle special characters
                'date_format': self.export_settings.get('date_format', '%Y-%m-%d %H:%M:%S'),
                'float_format': f"%.{self.export_settings.get('decimal_places', 2)}f"
            }
            csv_params.update(kwargs)  # Allow override of default parameters
            
            # Write to CSV with error handling
            export_df.to_csv(filepath, **csv_params)
            
            logger.info(f"Successfully exported data to CSV: {filepath}")
            return True
            
        except UnicodeEncodeError as e:
            logger.error(f"Unicode encoding error while writing CSV: {e}")
            # Retry with different encoding
            try:
                logger.info("Retrying CSV export with UTF-8 encoding...")
                export_df.to_csv(filepath, index=False, encoding='utf-8', errors='replace')
                logger.warning(f"CSV exported with UTF-8 encoding (some characters may have been replaced)")
                return True
            except Exception as retry_error:
                logger.error(f"Failed to export CSV even with UTF-8 encoding: {retry_error}")
                return False
                
        except Exception as e:
            logger.error(f"Error writing CSV file {filepath}: {e}")
            return False
    
    def write_to_excel_with_sheets(self, 
                                  data_dict: Dict[str, pd.DataFrame], 
                                  filepath: str,
                                  clean_data: bool = None,
                                  handle_clob: bool = True,
                                  **kwargs) -> bool:
        """
        Write multiple DataFrames to Excel file with separate sheets.
        
        Args:
            data_dict: Dictionary with sheet names as keys and DataFrames as values
            filepath: Output Excel file path
            clean_data: Whether to clean data before export
            handle_clob: Whether to handle CLOB data
            **kwargs: Additional parameters for Excel writer
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not data_dict:
                logger.warning("No data provided for Excel export")
                return False
            
            if clean_data is None:
                clean_data = self.export_settings.get('clean_data_by_default', True)
            
            logger.info(f"Exporting {len(data_dict)} sheets to Excel: {filepath}")
            
            # Ensure output directory exists
            output_path = Path(filepath)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Process data for each sheet
            processed_data = {}
            for sheet_name, df in data_dict.items():
                if df is None or df.empty:
                    logger.warning(f"Sheet '{sheet_name}' is empty, adding placeholder")
                    processed_data[sheet_name] = pd.DataFrame({'Note': ['No data available']})
                    continue
                
                processed_df = df.copy()
                
                # Clean data if requested
                if clean_data:
                    if data_cleaner:
                        processed_df = data_cleaner.clean_data_for_export(processed_df, 'excel')
                        if handle_clob:
                            processed_df = data_cleaner.handle_clob_data(processed_df)
                    else:
                        # Basic cleaning without data_cleaner
                        processed_df = self._basic_clean_for_excel(processed_df)
                
                processed_data[sheet_name] = processed_df
                logger.debug(f"Processed sheet '{sheet_name}': {len(processed_df)} rows")
            
            if not processed_data:
                logger.warning("No data to export to Excel")
                return False
            
            # Prepare Excel writer parameters
            excel_params = {'engine': 'openpyxl'}
            excel_params.update(kwargs)
            
            # Write to Excel
            with pd.ExcelWriter(filepath, **excel_params) as writer:
                for sheet_name, df in processed_data.items():
                    # Ensure sheet name is valid
                    clean_sheet_name = self._clean_sheet_name(sheet_name)
                    
                    # Write sheet
                    df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                    logger.debug(f"Written sheet: {clean_sheet_name}")
            
            logger.info(f"Successfully exported data to Excel: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing Excel file {filepath}: {e}")
            return False
    
    def write_single_dataframe_to_excel(self, 
                                       df: pd.DataFrame, 
                                       filepath: str,
                                       sheet_name: str = "Data",
                                       clean_data: bool = None,
                                       **kwargs) -> bool:
        """
        Write single DataFrame to Excel file.
        
        Args:
            df: DataFrame to export
            filepath: Output Excel file path
            sheet_name: Name of the Excel sheet
            clean_data: Whether to clean data before export
            **kwargs: Additional parameters for Excel export
            
        Returns:
            True if successful, False otherwise
        """
        data_dict = {sheet_name: df}
        return self.write_to_excel_with_sheets(data_dict, filepath, clean_data, **kwargs)
    
    def write_to_json(self, 
                     data: Union[pd.DataFrame, Dict, List], 
                     filepath: str,
                     **kwargs) -> bool:
        """
        Write data to JSON file.
        
        Args:
            data: Data to export (DataFrame, Dict, or List)
            filepath: Output JSON file path
            **kwargs: Additional parameters for JSON export
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure output directory exists
            output_path = Path(filepath)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert DataFrame to dict if needed
            if isinstance(data, pd.DataFrame):
                json_data = data.to_dict('records')
            else:
                json_data = data
            
            # Prepare JSON parameters
            json_params = {
                'indent': 2,
                'ensure_ascii': False,
                'default': str  # Handle datetime and other non-serializable types
            }
            json_params.update(kwargs)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, **json_params)
            
            logger.info(f"Successfully exported data to JSON: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing JSON file {filepath}: {e}")
            return False
    
    def _basic_clean_for_csv(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic data cleaning for CSV export when data_cleaner is not available."""
        cleaned_df = df.copy()
        
        # Replace problematic characters
        for col in cleaned_df.select_dtypes(include=['object']).columns:
            cleaned_df[col] = cleaned_df[col].astype(str).str.replace('\n', ' ').str.replace('\r', ' ')
        
        return cleaned_df
    
    def _basic_clean_for_excel(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic data cleaning for Excel export when data_cleaner is not available."""
        cleaned_df = df.copy()
        max_length = self.export_settings.get('max_cell_length', 32767)
        
        # Truncate long text fields
        for col in cleaned_df.select_dtypes(include=['object']).columns:
            cleaned_df[col] = cleaned_df[col].astype(str).str[:max_length]
        
        return cleaned_df
    
    def _clean_sheet_name(self, sheet_name: str) -> str:
        """Clean sheet name to be valid for Excel."""
        # Excel sheet names cannot exceed 31 characters and cannot contain certain characters
        invalid_chars = ['\\', '/', '*', '?', '[', ']', ':']
        
        clean_name = str(sheet_name)  # Ensure it's a string
        for char in invalid_chars:
            clean_name = clean_name.replace(char, '_')
        
        # Truncate if too long
        if len(clean_name) > 31:
            clean_name = clean_name[:28] + "..."
        
        # Ensure not empty
        if not clean_name.strip():
            clean_name = "Sheet1"
        
        return clean_name
    
    def export_comparison_summary(self, 
                                 comparison_results: Dict[str, Any], 
                                 filepath: str = None,
                                 format_type: str = 'excel') -> str:
        """
        Export comparison summary to file.
        
        Args:
            comparison_results: Dictionary of comparison results
            filepath: Output file path (auto-generated if None)
            format_type: Export format ('excel', 'csv', or 'json')
            
        Returns:
            Path to exported file
        """
        try:
            if filepath is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                ext = 'xlsx' if format_type == 'excel' else format_type
                filepath = self.output_dir / f"comparison_summary_{timestamp}.{ext}"
            
            # Prepare summary data
            summary_data = []
            for name, results in comparison_results.items():
                # Handle different result structures
                if isinstance(results, dict):
                    summary_info = results.get('summary', results)
                    summary_data.append({
                        'Comparison': name,
                        'Timestamp': results.get('timestamp', datetime.now().isoformat()),
                        'Source_Records': summary_info.get('source_rows', summary_info.get('source_count', 0)),
                        'Target_Records': summary_info.get('target_rows', summary_info.get('target_count', 0)),
                        'Total_Differences': summary_info.get('total_differences', summary_info.get('differences_count', 0)),
                        'Match_Percentage': f"{summary_info.get('match_percentage', 0):.2f}%",
                        'Source_Only': summary_info.get('source_only', summary_info.get('missing_in_target_count', 0)),
                        'Target_Only': summary_info.get('target_only', summary_info.get('missing_in_source_count', 0)),
                        'Modified_Records': summary_info.get('modified', 0)
                    })
            
            if not summary_data:
                logger.warning("No comparison results to export")
                return str(filepath)
            
            # Export based on format
            if format_type.lower() == 'excel':
                summary_df = pd.DataFrame(summary_data)
                success = self.write_single_dataframe_to_excel(
                    summary_df, str(filepath), "Comparison_Summary"
                )
            elif format_type.lower() == 'csv':
                summary_df = pd.DataFrame(summary_data)
                success = self.write_to_csv_safe(summary_df, str(filepath))
            elif format_type.lower() == 'json':
                success = self.write_to_json(summary_data, str(filepath))
            else:
                raise ValueError(f"Unsupported format: {format_type}")
            
            if success:
                logger.info(f"Comparison summary exported to: {filepath}")
                return str(filepath)
            else:
                raise Exception(f"Failed to export comparison summary in {format_type} format")
                
        except Exception as e:
            logger.error(f"Error exporting comparison summary: {e}")
            raise
    
    def create_test_data_file(self, 
                            filename: str, 
                            data: List[Dict[str, Any]], 
                            format_type: str = 'json',
                            subdirectory: str = None) -> str:
        """
        Create test data file for framework testing.
        
        Args:
            filename: Name of the file to create
            data: Test data as list of dictionaries
            format_type: File format ('json', 'csv', or 'excel')
            subdirectory: Optional subdirectory within test_data
            
        Returns:
            Path to created file
        """
        try:
            test_data_dir = Path(__file__).parent.parent / "data" / "test_data"
            if subdirectory:
                test_data_dir = test_data_dir / subdirectory
            test_data_dir.mkdir(parents=True, exist_ok=True)
            
            # Ensure proper file extension
            if not any(filename.endswith(ext) for ext in ['.json', '.csv', '.xlsx']):
                filename = f"{filename}.{format_type}"
            
            file_path = test_data_dir / filename
            
            if format_type.lower() == 'json':
                success = self.write_to_json(data, str(file_path))
            elif format_type.lower() == 'csv':
                df = pd.DataFrame(data)
                success = self.write_to_csv_safe(df, str(file_path), clean_data=False)
            elif format_type.lower() == 'excel':
                df = pd.DataFrame(data)
                success = self.write_single_dataframe_to_excel(df, str(file_path), "TestData")
            else:
                raise ValueError(f"Unsupported format: {format_type}")
            
            if success:
                logger.info(f"Test data file created: {file_path}")
                return str(file_path)
            else:
                raise Exception(f"Failed to create test data file")
                
        except Exception as e:
            logger.error(f"Error creating test data file: {e}")
            raise
    
    def get_export_stats(self) -> Dict[str, Any]:
        """Get statistics about exported files."""
        try:
            stats = {
                'export_directory': str(self.output_dir),
                'total_files': 0,
                'total_size_mb': 0,
                'file_types': {},
                'recent_exports': []
            }
            
            if self.output_dir.exists():
                all_files = []
                for file_path in self.output_dir.rglob('*'):
                    if file_path.is_file():
                        try:
                            stats['total_files'] += 1
                            
                            # File size in MB
                            size_mb = file_path.stat().st_size / (1024 * 1024)
                            stats['total_size_mb'] += size_mb
                            
                            # File type count
                            ext = file_path.suffix.lower()
                            stats['file_types'][ext] = stats['file_types'].get(ext, 0) + 1
                            
                            # Collect for recent exports
                            all_files.append({
                                'filename': file_path.name,
                                'size_mb': round(size_mb, 2),
                                'modified': datetime.fromtimestamp(file_path.stat().st_mtime),
                                'path': str(file_path)
                            })
                        except Exception as e:
                            logger.debug(f"Error processing file {file_path}: {e}")
                            continue
                
                # Sort by modification time and get recent exports
                all_files.sort(key=lambda x: x['modified'], reverse=True)
                stats['recent_exports'] = all_files[:10]
            
            stats['total_size_mb'] = round(stats['total_size_mb'], 2)
            return stats
            
        except Exception as e:
            logger.error(f"Error getting export stats: {e}")
            return {
                'export_directory': str(self.output_dir),
                'total_files': 0,
                'total_size_mb': 0,
                'file_types': {},
                'recent_exports': [],
                'error': str(e)
            }
    
    def cleanup_old_exports(self, days_old: int = 30) -> int:
        """
        Clean up old export files.
        
        Args:
            days_old: Delete files older than this many days
            
        Returns:
            Number of files deleted
        """
        try:
            cutoff_date = datetime.now() - pd.Timedelta(days=days_old)
            deleted_count = 0
            
            if self.output_dir.exists():
                for file_path in self.output_dir.rglob('*'):
                    if file_path.is_file():
                        try:
                            file_date = datetime.fromtimestamp(file_path.stat().st_mtime)
                            if file_date < cutoff_date:
                                file_path.unlink()
                                deleted_count += 1
                                logger.debug(f"Deleted old export file: {file_path.name}")
                        except Exception as e:
                            logger.debug(f"Error deleting file {file_path}: {e}")
                            continue
            
            logger.info(f"Cleanup completed: {deleted_count} old files deleted")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return 0
    
    def validate_export_path(self, filepath: str) -> bool:
        """
        Validate if export path is writable.
        
        Args:
            filepath: Path to validate
            
        Returns:
            True if path is valid and writable
        """
        try:
            path = Path(filepath)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Test write access
            test_file = path.parent / '.write_test'
            test_file.write_text('test')
            test_file.unlink()
            
            return True
            
        except Exception as e:
            logger.error(f"Export path validation failed for {filepath}: {e}")
            return False
    
    def get_supported_formats(self) -> List[str]:
        """Get list of supported export formats."""
        return ['csv', 'excel', 'json']
    
    def estimate_export_size(self, df: pd.DataFrame, format_type: str = 'csv') -> Dict[str, Any]:
        """
        Estimate export file size.
        
        Args:
            df: DataFrame to estimate
            format_type: Export format
            
        Returns:
            Size estimation details
        """
        try:
            if df is None or df.empty:
                return {'estimated_size_mb': 0, 'row_count': 0, 'column_count': 0}
            
            # Basic estimation based on DataFrame memory usage
            memory_usage = df.memory_usage(deep=True).sum()
            
            # Format-specific multipliers (rough estimates)
            multipliers = {
                'csv': 1.2,  # Text overhead
                'excel': 1.8,  # Excel formatting overhead
                'json': 2.0   # JSON structure overhead
            }
            
            multiplier = multipliers.get(format_type.lower(), 1.5)
            estimated_size_mb = (memory_usage * multiplier) / (1024 * 1024)
            
            return {
                'estimated_size_mb': round(estimated_size_mb, 2),
                'row_count': len(df),
                'column_count': len(df.columns),
                'format': format_type,
                'memory_usage_mb': round(memory_usage / (1024 * 1024), 2)
            }
            
        except Exception as e:
            logger.error(f"Error estimating export size: {e}")
            return {'estimated_size_mb': 0, 'error': str(e)}


# Global export utils instance
export_utils = ExportUtils()