"""
Export utilities for writing data to CSV and Excel files with enhanced error handling.
"""
import pandas as pd
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List
from utils.logger import logger
from utils.data_cleaner import data_cleaner
from utils.config_loader import config_loader

class ExportUtils:
    """Utility class for exporting data to various formats."""
    
    def __init__(self):
        """Initialize export utilities."""
        self.output_dir = Path(__file__).parent.parent / "output" / "exports"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.export_settings = config_loader.get_export_settings()
    
    def write_to_csv_safe(self, 
                         df: pd.DataFrame, 
                         filepath: str, 
                         clean_data: bool = True,
                         encoding: str = None) -> bool:
        """
        Safe CSV writing with special character handling.
        
        Args:
            df: DataFrame to export
            filepath: Output file path
            clean_data: Whether to clean data before export
            encoding: File encoding (default from config)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if encoding is None:
                encoding = self.export_settings.get('csv_encoding', 'utf-8-sig')
            
            logger.info(f"Exporting {len(df)} rows to CSV: {filepath}")
            
            # Clean data if requested
            if clean_data:
                df = data_cleaner.clean_data_for_export(df, 'csv')
            
            # Ensure output directory exists
            output_path = Path(filepath)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to CSV with error handling
            df.to_csv(
                filepath,
                index=False,
                encoding=encoding,
                quoting=1,  # QUOTE_ALL to handle special characters
                date_format=self.export_settings.get('date_format', '%Y-%m-%d %H:%M:%S'),
                float_format=f"%.{self.export_settings.get('decimal_places', 2)}f"
            )
            
            logger.info(f"Successfully exported data to CSV: {filepath}")
            return True
            
        except UnicodeEncodeError as e:
            logger.error(f"Unicode encoding error while writing CSV: {e}")
            # Retry with different encoding
            try:
                logger.info("Retrying CSV export with UTF-8 encoding...")
                df.to_csv(filepath, index=False, encoding='utf-8', errors='replace')
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
                                  clean_data: bool = True,
                                  handle_clob: bool = True) -> bool:
        """
        Write multiple DataFrames to Excel file with separate sheets.
        
        Args:
            data_dict: Dictionary with sheet names as keys and DataFrames as values
            filepath: Output Excel file path
            clean_data: Whether to clean data before export
            handle_clob: Whether to handle CLOB data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Exporting {len(data_dict)} sheets to Excel: {filepath}")
            
            # Ensure output directory exists
            output_path = Path(filepath)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Process data for each sheet
            processed_data = {}
            for sheet_name, df in data_dict.items():
                if df.empty:
                    logger.warning(f"Sheet '{sheet_name}' is empty, skipping")
                    continue
                
                processed_df = df.copy()
                
                # Clean data if requested
                if clean_data:
                    processed_df = data_cleaner.clean_data_for_export(processed_df, 'excel')
                
                # Handle CLOB data if requested
                if handle_clob:
                    processed_df = data_cleaner.handle_clob_data(processed_df)
                
                processed_data[sheet_name] = processed_df
                logger.debug(f"Processed sheet '{sheet_name}': {len(processed_df)} rows")
            
            if not processed_data:
                logger.warning("No data to export to Excel")
                return False
            
            # Write to Excel
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for sheet_name, df in processed_data.items():
                    # Ensure sheet name is valid
                    clean_sheet_name = self._clean_sheet_name(sheet_name)
                    
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
                                       clean_data: bool = True) -> bool:
        """
        Write single DataFrame to Excel file.
        
        Args:
            df: DataFrame to export
            filepath: Output Excel file path
            sheet_name: Name of the Excel sheet
            clean_data: Whether to clean data before export
            
        Returns:
            True if successful, False otherwise
        """
        data_dict = {sheet_name: df}
        return self.write_to_excel_with_sheets(data_dict, filepath, clean_data)
    
    def _clean_sheet_name(self, sheet_name: str) -> str:
        """Clean sheet name to be valid for Excel."""
        # Excel sheet names cannot exceed 31 characters and cannot contain certain characters
        invalid_chars = ['\\', '/', '*', '?', '[', ']', ':']
        
        clean_name = sheet_name
        for char in invalid_chars:
            clean_name = clean_name.replace(char, '_')
        
        # Truncate if too long
        if len(clean_name) > 31:
            clean_name = clean_name[:28] + "..."
        
        return clean_name
    
    def export_comparison_summary(self, 
                                 comparison_results: Dict[str, Any], 
                                 filepath: str = None) -> str:
        """
        Export comparison summary to file.
        
        Args:
            comparison_results: Dictionary of comparison results
            filepath: Output file path (auto-generated if None)
            
        Returns:
            Path to exported file
        """
        try:
            if filepath is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filepath = self.output_dir / f"comparison_summary_{timestamp}.xlsx"
            
            summary_data = []
            for name, results in comparison_results.items():
                summary_data.append({
                    'Comparison': name,
                    'Timestamp': results.get('timestamp', ''),
                    'Source_Records': results.get('source_count', 0),
                    'Target_Records': results.get('target_count', 0),
                    'Differences': results.get('differences_count', 0),
                    'Match_Percentage': f"{results.get('match_percentage', 0):.2f}%",
                    'Missing_in_Target': results.get('missing_in_target_count', 0),
                    'Missing_in_Source': results.get('missing_in_source_count', 0)
                })
            
            summary_df = pd.DataFrame(summary_data)
            
            success = self.write_single_dataframe_to_excel(
                summary_df, str(filepath), "Comparison_Summary"
            )
            
            if success:
                logger.info(f"Comparison summary exported to: {filepath}")
                return str(filepath)
            else:
                raise Exception("Failed to export comparison summary")
                
        except Exception as e:
            logger.error(f"Error exporting comparison summary: {e}")
            raise
    
    def create_test_data_file(self, 
                            filename: str, 
                            data: List[Dict[str, Any]], 
                            format_type: str = 'json') -> str:
        """
        Create test data file for framework testing.
        
        Args:
            filename: Name of the file to create
            data: Test data as list of dictionaries
            format_type: File format ('json' or 'csv')
            
        Returns:
            Path to created file
        """
        try:
            test_data_dir = Path(__file__).parent.parent / "data" / "test_data"
            test_data_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = test_data_dir / filename
            
            if format_type.lower() == 'json':
                import json
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            elif format_type.lower() == 'csv':
                df = pd.DataFrame(data)
                self.write_to_csv_safe(df, str(file_path), clean_data=False)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
            
            logger.info(f"Test data file created: {file_path}")
            return str(file_path)
            
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
                for file_path in self.output_dir.iterdir():
                    if file_path.is_file():
                        stats['total_files'] += 1
                        
                        # File size in MB
                        size_mb = file_path.stat().st_size / (1024 * 1024)
                        stats['total_size_mb'] += size_mb
                        
                        # File type count
                        ext = file_path.suffix.lower()
                        stats['file_types'][ext] = stats['file_types'].get(ext, 0) + 1
                        
                        # Recent exports (last 10)
                        if len(stats['recent_exports']) < 10:
                            stats['recent_exports'].append({
                                'filename': file_path.name,
                                'size_mb': round(size_mb, 2),
                                'modified': datetime.fromtimestamp(file_path.stat().st_mtime)
                            })
            
            stats['total_size_mb'] = round(stats['total_size_mb'], 2)
            return stats
            
        except Exception as e:
            logger.error(f"Error getting export stats: {e}")
            return {}
    
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
                for file_path in self.output_dir.iterdir():
                    if file_path.is_file():
                        file_date = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if file_date < cutoff_date:
                            file_path.unlink()
                            deleted_count += 1
                            logger.debug(f"Deleted old export file: {file_path.name}")
            
            logger.info(f"Cleanup completed: {deleted_count} old files deleted")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return 0

# Global export utils instance
export_utils = ExportUtils()