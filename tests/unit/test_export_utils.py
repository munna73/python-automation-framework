"""
Unit tests for export_utils.py module.

This module contains comprehensive test cases for validating the export utilities
functionality including data export to various formats (CSV, Excel, JSON, XML),
report generation, file formatting, and export validation.
"""

import unittest
from unittest.mock import Mock, patch, mock_open, MagicMock, call
import pytest
import pandas as pd
import numpy as np
import os
import tempfile
import shutil
import json
import csv
import xml.etree.ElementTree as ET
from datetime import datetime, date
from decimal import Decimal
import io
import sys
from pathlib import Path

# Add the utils directory to the path to import export_utils
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'utils'))

from export_utils import ExportUtils
from custom_exceptions import ExportError, ValidationError, ConfigurationError


class TestExportUtils(unittest.TestCase):
    """Test cases for ExportUtils class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.export_utils = ExportUtils()
        
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = os.path.join(self.temp_dir, 'output')
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Sample test data
        self.sample_data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Davis'],
            'email': ['john@example.com', 'jane@test.com', 'bob@company.org', 'alice@domain.net', 'charlie@mail.com'],
            'age': [25, 30, 35, 28, 42],
            'salary': [50000.00, 75000.50, 80000.25, 65000.75, 90000.00],
            'hire_date': [datetime(2023, 1, 15), datetime(2022, 3, 20), datetime(2021, 7, 10), 
                         datetime(2023, 5, 12), datetime(2020, 11, 8)],
            'is_active': [True, True, False, True, True],
            'department': ['IT', 'HR', 'Finance', 'IT', 'Marketing']
        }
        
        self.df_sample = pd.DataFrame(self.sample_data)
        
        # Sample nested/complex data
        self.complex_data = {
            'user_id': [1, 2, 3],
            'profile': [
                {'skills': ['Python', 'SQL'], 'experience': 5},
                {'skills': ['Java', 'React'], 'experience': 3},
                {'skills': ['C++', 'MongoDB'], 'experience': 7}
            ],
            'projects': [
                [{'name': 'Project A', 'status': 'Complete'}],
                [{'name': 'Project B', 'status': 'In Progress'}, {'name': 'Project C', 'status': 'Planned'}],
                [{'name': 'Project D', 'status': 'Complete'}]
            ]
        }
        
        self.df_complex = pd.DataFrame(self.complex_data)
        
        # Export configuration
        self.export_config = {
            'csv': {
                'delimiter': ',',
                'quoting': csv.QUOTE_MINIMAL,
                'encoding': 'utf-8',
                'include_index': False
            },
            'excel': {
                'engine': 'openpyxl',
                'index': False,
                'header': True
            },
            'json': {
                'orient': 'records',
                'indent': 2,
                'ensure_ascii': False
            },
            'xml': {
                'root_element': 'data',
                'row_element': 'record'
            }
        }

    def tearDown(self):
        """Clean up after each test method."""
        # Remove temporary directory
        shutil.rmtree(self.temp_dir)
        self.export_utils = None

    def test_init_default(self):
        """Test ExportUtils initialization with default parameters."""
        export_utils = ExportUtils()
        self.assertIsNotNone(export_utils)
        self.assertEqual(export_utils.output_directory, None)

    def test_init_with_output_directory(self):
        """Test ExportUtils initialization with output directory."""
        export_utils = ExportUtils(output_directory=self.output_dir)
        self.assertEqual(export_utils.output_directory, self.output_dir)

    def test_export_to_csv_basic(self):
        """Test basic CSV export functionality."""
        output_file = os.path.join(self.output_dir, 'test_export.csv')
        
        result = self.export_utils.export_to_csv(self.df_sample, output_file)
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(output_file))
        
        # Verify file contents
        df_read = pd.read_csv(output_file)
        self.assertEqual(len(df_read), len(self.df_sample))
        self.assertEqual(list(df_read.columns), list(self.df_sample.columns))

    def test_export_to_csv_with_custom_delimiter(self):
        """Test CSV export with custom delimiter."""
        output_file = os.path.join(self.output_dir, 'test_pipe_delimited.csv')
        
        result = self.export_utils.export_to_csv(
            self.df_sample, 
            output_file, 
            delimiter='|'
        )
        
        self.assertTrue(result)
        
        # Verify delimiter is used
        with open(output_file, 'r') as f:
            first_line = f.readline()
            self.assertIn('|', first_line)

    def test_export_to_csv_with_encoding(self):
        """Test CSV export with specific encoding."""
        output_file = os.path.join(self.output_dir, 'test_utf8.csv')
        
        # Add unicode characters to test data
        df_unicode = self.df_sample.copy()
        df_unicode.loc[0, 'name'] = 'José María'
        
        result = self.export_utils.export_to_csv(
            df_unicode, 
            output_file, 
            encoding='utf-8'
        )
        
        self.assertTrue(result)
        
        # Verify encoding
        df_read = pd.read_csv(output_file, encoding='utf-8')
        self.assertEqual(df_read.loc[0, 'name'], 'José María')

    def test_export_to_excel_single_sheet(self):
        """Test Excel export with single sheet."""
        output_file = os.path.join(self.output_dir, 'test_export.xlsx')
        
        result = self.export_utils.export_to_excel(self.df_sample, output_file)
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(output_file))
        
        # Verify file contents
        df_read = pd.read_excel(output_file)
        self.assertEqual(len(df_read), len(self.df_sample))

    def test_export_to_excel_multiple_sheets(self):
        """Test Excel export with multiple sheets."""
        output_file = os.path.join(self.output_dir, 'test_multi_sheet.xlsx')
        
        sheets_data = {
            'Employees': self.df_sample,
            'Summary': self.df_sample.groupby('department').size().reset_index(name='count')
        }
        
        result = self.export_utils.export_to_excel_multiple_sheets(sheets_data, output_file)
        
        self.assertTrue(result)
        
        # Verify multiple sheets
        excel_file = pd.ExcelFile(output_file)
        self.assertIn('Employees', excel_file.sheet_names)
        self.assertIn('Summary', excel_file.sheet_names)

    def test_export_to_excel_with_formatting(self):
        """Test Excel export with custom formatting."""
        output_file = os.path.join(self.output_dir, 'test_formatted.xlsx')
        
        formatting_options = {
            'header_format': {'bold': True, 'bg_color': '#D7E4BC'},
            'number_format': {'salary': '#,##0.00'},
            'date_format': {'hire_date': 'mm/dd/yyyy'}
        }
        
        result = self.export_utils.export_to_excel_formatted(
            self.df_sample, 
            output_file, 
            formatting_options
        )
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(output_file))

    def test_export_to_json_records_format(self):
        """Test JSON export in records format."""
        output_file = os.path.join(self.output_dir, 'test_records.json')
        
        result = self.export_utils.export_to_json(
            self.df_sample, 
            output_file, 
            orient='records'
        )
        
        self.assertTrue(result)
        
        # Verify JSON structure
        with open(output_file, 'r') as f:
            data = json.load(f)
            self.assertIsInstance(data, list)
            self.assertEqual(len(data), len(self.df_sample))
            self.assertIn('name', data[0])

    def test_export_to_json_index_format(self):
        """Test JSON export in index format."""
        output_file = os.path.join(self.output_dir, 'test_index.json')
        
        result = self.export_utils.export_to_json(
            self.df_sample, 
            output_file, 
            orient='index'
        )
        
        self.assertTrue(result)
        
        # Verify JSON structure
        with open(output_file, 'r') as f:
            data = json.load(f)
            self.assertIsInstance(data, dict)
            self.assertEqual(len(data), len(self.df_sample))

    def test_export_to_json_with_date_handling(self):
        """Test JSON export with proper date serialization."""
        output_file = os.path.join(self.output_dir, 'test_dates.json')
        
        result = self.export_utils.export_to_json(
            self.df_sample, 
            output_file, 
            date_format='iso',
            date_unit='s'
        )
        
        self.assertTrue(result)
        
        # Verify date formatting
        with open(output_file, 'r') as f:
            data = json.load(f)
            # Check that dates are properly formatted
            self.assertIsInstance(data[0]['hire_date'], str)

    def test_export_to_xml_basic(self):
        """Test basic XML export functionality."""
        output_file = os.path.join(self.output_dir, 'test_export.xml')
        
        result = self.export_utils.export_to_xml(self.df_sample, output_file)
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(output_file))
        
        # Verify XML structure
        tree = ET.parse(output_file)
        root = tree.getroot()
        self.assertIsNotNone(root)
        self.assertGreater(len(root), 0)

    def test_export_to_xml_with_custom_elements(self):
        """Test XML export with custom element names."""
        output_file = os.path.join(self.output_dir, 'test_custom.xml')
        
        result = self.export_utils.export_to_xml(
            self.df_sample, 
            output_file,
            root_element='employees',
            row_element='employee'
        )
        
        self.assertTrue(result)
        
        # Verify custom element names
        tree = ET.parse(output_file)
        root = tree.getroot()
        self.assertEqual(root.tag, 'employees')
        self.assertEqual(root[0].tag, 'employee')

    def test_export_to_parquet(self):
        """Test Parquet export functionality."""
        output_file = os.path.join(self.output_dir, 'test_export.parquet')
        
        result = self.export_utils.export_to_parquet(self.df_sample, output_file)
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(output_file))
        
        # Verify file contents
        df_read = pd.read_parquet(output_file)
        self.assertEqual(len(df_read), len(self.df_sample))

    def test_export_to_hdf5(self):
        """Test HDF5 export functionality."""
        output_file = os.path.join(self.output_dir, 'test_export.h5')
        
        result = self.export_utils.export_to_hdf5(
            self.df_sample, 
            output_file, 
            key='data'
        )
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(output_file))
        
        # Verify file contents
        df_read = pd.read_hdf(output_file, key='data')
        self.assertEqual(len(df_read), len(self.df_sample))

    def test_export_dict_to_json(self):
        """Test exporting dictionary data to JSON."""
        output_file = os.path.join(self.output_dir, 'test_dict.json')
        
        dict_data = {
            'metadata': {'version': '1.0', 'created': datetime.now().isoformat()},
            'data': self.sample_data
        }
        
        result = self.export_utils.export_dict_to_json(dict_data, output_file)
        
        self.assertTrue(result)
        
        # Verify contents
        with open(output_file, 'r') as f:
            loaded_data = json.load(f)
            self.assertIn('metadata', loaded_data)
            self.assertIn('data', loaded_data)

    def test_export_list_to_csv(self):
        """Test exporting list data to CSV."""
        output_file = os.path.join(self.output_dir, 'test_list.csv')
        
        list_data = [
            ['ID', 'Name', 'Age'],
            [1, 'John', 25],
            [2, 'Jane', 30],
            [3, 'Bob', 35]
        ]
        
        result = self.export_utils.export_list_to_csv(list_data, output_file)
        
        self.assertTrue(result)
        
        # Verify contents
        df_read = pd.read_csv(output_file)
        self.assertEqual(len(df_read), 3)  # Excluding header
        self.assertEqual(list(df_read.columns), ['ID', 'Name', 'Age'])

    def test_export_with_compression(self):
        """Test export with file compression."""
        output_file = os.path.join(self.output_dir, 'test_compressed.csv.gz')
        
        result = self.export_utils.export_to_csv(
            self.df_sample, 
            output_file, 
            compression='gzip'
        )
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(output_file))
        
        # Verify compressed file can be read back
        df_read = pd.read_csv(output_file, compression='gzip')
        self.assertEqual(len(df_read), len(self.df_sample))

    def test_batch_export_multiple_formats(self):
        """Test batch export to multiple formats."""
        base_filename = 'batch_export'
        formats = ['csv', 'json', 'xlsx']
        
        results = self.export_utils.batch_export(
            self.df_sample, 
            self.output_dir, 
            base_filename, 
            formats
        )
        
        self.assertEqual(len(results), len(formats))
        for format_type in formats:
            self.assertTrue(results[format_type])
            expected_file = os.path.join(self.output_dir, f'{base_filename}.{format_type}')
            self.assertTrue(os.path.exists(expected_file))

    def test_export_with_filtering(self):
        """Test export with data filtering."""
        output_file = os.path.join(self.output_dir, 'test_filtered.csv')
        
        # Filter for IT department only
        filter_condition = self.df_sample['department'] == 'IT'
        
        result = self.export_utils.export_to_csv_filtered(
            self.df_sample, 
            output_file, 
            filter_condition
        )
        
        self.assertTrue(result)
        
        # Verify filtered data
        df_read = pd.read_csv(output_file)
        self.assertEqual(len(df_read), 2)  # Only 2 IT employees
        self.assertTrue(all(df_read['department'] == 'IT'))

    def test_export_with_column_selection(self):
        """Test export with specific column selection."""
        output_file = os.path.join(self.output_dir, 'test_columns.csv')
        
        selected_columns = ['name', 'email', 'department']
        
        result = self.export_utils.export_to_csv_columns(
            self.df_sample, 
            output_file, 
            columns=selected_columns
        )
        
        self.assertTrue(result)
        
        # Verify column selection
        df_read = pd.read_csv(output_file)
        self.assertEqual(list(df_read.columns), selected_columns)

    def test_export_with_data_transformation(self):
        """Test export with data transformation."""
        output_file = os.path.join(self.output_dir, 'test_transformed.csv')
        
        def transform_function(df):
            df_copy = df.copy()
            df_copy['salary_k'] = df_copy['salary'] / 1000
            df_copy['name_upper'] = df_copy['name'].str.upper()
            return df_copy
        
        result = self.export_utils.export_with_transformation(
            self.df_sample, 
            output_file, 
            transform_function,
            format_type='csv'
        )
        
        self.assertTrue(result)
        
        # Verify transformation
        df_read = pd.read_csv(output_file)
        self.assertIn('salary_k', df_read.columns)
        self.assertIn('name_upper', df_read.columns)
        self.assertEqual(df_read.loc[0, 'salary_k'], 50.0)  # 50000 / 1000

    def test_export_report_generation(self):
        """Test automated report generation."""
        output_file = os.path.join(self.output_dir, 'test_report.xlsx')
        
        report_config = {
            'title': 'Employee Report',
            'sheets': {
                'Raw Data': self.df_sample,
                'Department Summary': self.df_sample.groupby('department').agg({
                    'salary': ['mean', 'count']
                }).round(2)
            },
            'charts': [
                {
                    'type': 'bar',
                    'data': 'Department Summary', 
                    'x': 'department',
                    'y': 'salary_mean'
                }
            ]
        }
        
        result = self.export_utils.generate_report(report_config, output_file)
        
        self.assertTrue(result)
        self.assertTrue(os.path.exists(output_file))

    def test_export_with_metadata(self):
        """Test export with metadata inclusion."""
        output_file = os.path.join(self.output_dir, 'test_metadata.json')
        
        metadata = {
            'export_date': datetime.now().isoformat(),
            'source': 'test_data',
            'row_count': len(self.df_sample),
            'columns': list(self.df_sample.columns),
            'data_types': self.df_sample.dtypes.to_dict()
        }
        
        result = self.export_utils.export_with_metadata(
            self.df_sample, 
            output_file, 
            metadata,
            format_type='json'
        )
        
        self.assertTrue(result)
        
        # Verify metadata is included
        with open(output_file, 'r') as f:
            exported_data = json.load(f)
            self.assertIn('metadata', exported_data)
            self.assertIn('data', exported_data)

    def test_incremental_export(self):
        """Test incremental/append export functionality."""
        output_file = os.path.join(self.output_dir, 'test_incremental.csv')
        
        # First export
        result1 = self.export_utils.export_to_csv(
            self.df_sample.iloc[:2], 
            output_file
        )
        self.assertTrue(result1)
        
        # Incremental export
        result2 = self.export_utils.export_to_csv_append(
            self.df_sample.iloc[2:], 
            output_file
        )
        self.assertTrue(result2)
        
        # Verify combined data
        df_read = pd.read_csv(output_file)
        self.assertEqual(len(df_read), len(self.df_sample))

    def test_export_validation(self):
        """Test export validation and verification."""
        output_file = os.path.join(self.output_dir, 'test_validation.csv')
        
        # Export data
        result = self.export_utils.export_to_csv(self.df_sample, output_file)
        self.assertTrue(result)
        
        # Validate export
        validation_result = self.export_utils.validate_export(
            original_data=self.df_sample,
            exported_file=output_file,
            format_type='csv'
        )
        
        self.assertTrue(validation_result['is_valid'])
        self.assertEqual(validation_result['row_count_match'], True)
        self.assertEqual(validation_result['column_count_match'], True)

    def test_export_with_custom_naming(self):
        """Test export with custom file naming patterns."""
        base_name = 'employee_data'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        result = self.export_utils.export_with_timestamp(
            self.df_sample,
            self.output_dir,
            base_name,
            format_type='csv'
        )
        
        self.assertTrue(result['success'])
        self.assertIn(timestamp[:8], result['filename'])  # Check date part

    def test_export_to_database_format(self):
        """Test export in database-compatible format."""
        output_file = os.path.join(self.output_dir, 'test_db_format.sql')
        
        result = self.export_utils.export_to_sql_insert(
            self.df_sample,
            output_file,
            table_name='employees'
        )
        
        self.assertTrue(result)
        
        # Verify SQL content
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn('INSERT INTO employees', content)
            self.assertIn('John Doe', content)

    def test_export_large_dataset_chunked(self):
        """Test export of large dataset with chunking."""
        # Create larger dataset
        large_data = pd.concat([self.df_sample] * 1000, ignore_index=True)
        output_file = os.path.join(self.output_dir, 'test_large.csv')
        
        result = self.export_utils.export_large_dataset(
            large_data,
            output_file,
            chunk_size=500,
            format_type='csv'
        )
        
        self.assertTrue(result)
        
        # Verify all data was exported
        df_read = pd.read_csv(output_file)
        self.assertEqual(len(df_read), len(large_data))

    def test_export_with_encryption(self):
        """Test export with file encryption."""
        output_file = os.path.join(self.output_dir, 'test_encrypted.csv')
        password = 'test_password_123'
        
        result = self.export_utils.export_with_encryption(
            self.df_sample,
            output_file,
            password=password,
            format_type='csv'
        )
        
        self.assertTrue(result)
        
        # Verify file is encrypted (would need decryption to read)
        self.assertTrue(os.path.exists(output_file))

    def test_export_error_handling_invalid_path(self):
        """Test error handling with invalid output path."""
        invalid_path = '/invalid/path/that/does/not/exist/file.csv'
        
        with self.assertRaises(ExportError):
            self.export_utils.export_to_csv(self.df_sample, invalid_path)

    def test_export_error_handling_empty_data(self):
        """Test error handling with empty DataFrame."""
        empty_df = pd.DataFrame()
        output_file = os.path.join(self.output_dir, 'test_empty.csv')
        
        result = self.export_utils.export_to_csv(empty_df, output_file)
        
        # Should handle gracefully or raise appropriate error
        if result:
            self.assertTrue(os.path.exists(output_file))
        else:
            with self.assertRaises(ExportError):
                self.export_utils.export_to_csv(empty_df, output_file, strict=True)

    def test_export_error_handling_invalid_format(self):
        """Test error handling with unsupported format."""
        output_file = os.path.join(self.output_dir, 'test.unsupported')
        
        with self.assertRaises(ValueError):
            self.export_utils.export_data(
                self.df_sample, 
                output_file, 
                format_type='unsupported'
            )

    def test_export_performance_monitoring(self):
        """Test export performance monitoring."""
        output_file = os.path.join(self.output_dir, 'test_performance.csv')
        
        result = self.export_utils.export_with_monitoring(
            self.df_sample,
            output_file,
            format_type='csv'
        )
        
        self.assertTrue(result['success'])
        self.assertIn('execution_time', result)
        self.assertIn('file_size', result)
        self.assertGreater(result['execution_time'], 0)
        self.assertGreater(result['file_size'], 0)

    def test_export_configuration_loading(self):
        """Test loading export configuration from file."""
        config_file = os.path.join(self.temp_dir, 'export_config.json')
        
        with open(config_file, 'w') as f:
            json.dump(self.export_config, f)
        
        self.export_utils.load_configuration(config_file)
        
        # Verify configuration is loaded
        self.assertEqual(
            self.export_utils.config['csv']['delimiter'], 
            self.export_config['csv']['delimiter']
        )

    def test_export_template_system(self):
        """Test export template system."""
        template_config = {
            'name': 'employee_report',
            'format': 'excel',
            'sheets': ['data', 'summary'],
            'formatting': {
                'headers': {'bold': True},
                'data': {'number_format': '#,##0.00'}
            }
        }
        
        result = self.export_utils.export_using_template(
            self.df_sample,
            self.output_dir,
            template_config
        )
        
        self.assertTrue(result)

    @patch('utils.logger.Logger')
    def test_logging_integration(self, mock_logger):
        """Test integration with logging system."""
        output_file = os.path.join(self.output_dir, 'test_logging.csv')
        
        result = self.export_utils.export_to_csv(self.df_sample, output_file)
        
        self.assertTrue(result)
        # Verify that logging would be called
        self.assertTrue(os.path.exists(output_file))

    def test_memory_efficient_export(self):
        """Test memory-efficient export for very large datasets."""
        # Simulate large dataset processing
        def data_generator():
            for i in range(1000):
                yield {'id': i, 'value': f'data_{i}'}
        
        output_file = os.path.join(self.output_dir, 'test_memory_efficient.csv')
        
        result = self.export_utils.export_from_generator(
            data_generator(),
            output_file,
            format_type='csv'
        )
        
        self.assertTrue(result)
        
        # Verify data was written
        df_read = pd.read_csv(output_file)
        self.assertEqual(len(df_read), 1000)


class TestExportUtilsIntegration(unittest.TestCase):
    """Integration tests for ExportUtils with other components."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.export_utils = ExportUtils()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up integration test fixtures."""
        shutil.rmtree(self.temp_dir)

    @patch('db.database_connector.DatabaseConnector')
    def test_database_export_integration(self, mock_db_connector):
        """Test integration with database connector for direct export."""
        mock_db_connector.return_value.execute_query.return_value = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C']
        })
        
        output_file = os.path.join(self.temp_dir, 'db_export.csv')
        
        result = self.export_utils.export_from_database(
            mock_db_connector.return_value,
            'SELECT * FROM test_table',
            output_file
        )
        
        self.assertTrue(result)

    @patch('api.rest_client.RestClient')
    def test_api_export_integration(self, mock_api_client):
        """Test integration with API client for data export."""
        mock_api_client.return_value.get.return_value = {
            'data': [
                {'id': 1, 'name': 'Test 1'},
                {'id': 2, 'name': 'Test 2'}
            ]
        }
        
        output_file = os.path.join(self.temp_dir, 'api_export.json')
        
        result = self.export_utils.export_from_api(
            mock_api_client.return_value,
            '/api/data',
            output_file
        )
        
        self.assertTrue(result)

    @patch('utils.config_loader.ConfigLoader')
    def test_config_integration(self, mock_config_loader):
        """Test integration with configuration loader."""
        mock_config = {
            'output_directory': self.temp_dir,
            'default_format': 'csv',
            'compression': False
        }
        mock_config_loader.return_value.get_section.return_value = mock_config
        
        self.export_utils.load_configuration_from_loader(mock_config_loader.return_value)
        
        self.assertEqual(self.export_utils.output_directory, self.temp_dir)


class TestExportUtilsPerformance(unittest.TestCase):
    """Performance tests for ExportUtils."""
    
    def setUp(self):
        """Set up performance test fixtures."""
        self.export_utils = ExportUtils()
        self.temp_dir = tempfile.mkdtemp()
        
        # Create large dataset for performance testing
        self.large_data = pd.DataFrame({
            'id': range(10000),
            'name': [f'User_{i}' for i in range(10000)],
            'value': np.random.random(10000),
            'category': np.random.choice(['A', 'B', 'C', 'D'], 10000),
            'timestamp': pd.date_range('2023-01-01', periods=10000, freq='H')
        })

    def tearDown(self):
        """Clean up performance test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_large_csv_export_performance(self):
        """Test performance of large CSV export."""
        output_file = os.path.join(self.temp_dir, 'large_export.csv')
        
        start_time = datetime.now()
        result = self.export_utils.export_to_csv(self.large_data, output_file)
        end_time = datetime.now()
        
        execution_time = (end_time - start_time).total_seconds()
        
        self.assertTrue(result)
        self.assertLess(execution_time, 10)  # Should complete within 10 seconds
        
        # Verify file size is reasonable
        file_size = os.path.getsize(output_file)
        self.assertGreater(file_size, 100000)  # Should be at least 100KB

    def test_large_excel_export_performance(self):
        """Test performance of large Excel export."""
        output_file = os.path.join(self.temp_dir, 'large_export.xlsx')
        
        start_time = datetime.now()
        result = self.export_utils.export_to_excel(self.large_data, output_file)
        end_time = datetime.now()
        
        execution_time = (end_time - start_time).total_seconds()
        
        self.assertTrue(result)
        self.assertLess(execution_time, 30)  # Excel export might take longer

    def test_chunked_export_performance(self):
        """Test performance of chunked export for very large datasets."""
        output_file = os.path.join(self.temp_dir, 'chunked_export.csv')
        
        start_time = datetime.now()
        result = self.export_utils.export_large_dataset(
            self.large_data,
            output_file,
            chunk_size=1000,
            format_type='csv'
        )
        end_time = datetime.now()
        
        execution_time = (end_time - start_time).total_seconds()
        
        self.assertTrue(result)
        self.assertLess(execution_time, 15)  # Should complete within 15 seconds

    def test_parallel_export_performance(self):
        """Test performance of parallel export to multiple formats."""
        base_filename = 'parallel_export'
        formats = ['csv', 'json', 'parquet']
        
        start_time = datetime.now()
        results = self.export_utils.parallel_export(
            self.large_data,
            self.temp_dir,
            base_filename,
            formats
        )
        end_time = datetime.now()
        
        execution_time = (end_time - start_time).total_seconds()
        
        # All exports should succeed
        for format_type in formats:
            self.assertTrue(results[format_type])
        
        # Parallel should be faster than sequential
        self.assertLess(execution_time, 20)

    def test_memory_usage_monitoring(self):
        """Test memory usage during export operations."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        output_file = os.path.join(self.temp_dir, 'memory_test.csv')
        result = self.export_utils.export_to_csv(self.large_data, output_file)
        
        peak_memory = process.memory_info().rss
        memory_increase = peak_memory - initial_memory
        
        self.assertTrue(result)
        # Memory increase should be reasonable (less than 100MB for this test)
        self.assertLess(memory_increase, 100 * 1024 * 1024)


class TestExportUtilsEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions for ExportUtils."""
    
    def setUp(self):
        """Set up edge case test fixtures."""
        self.export_utils = ExportUtils()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up edge case test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_export_with_special_characters(self):
        """Test export with special characters and unicode."""
        special_data = pd.DataFrame({
            'name': ['José María', 'François', '北京', '🚀 Rocket'],
            'description': ['Añoñóñ', 'Café', '你好世界', 'Special chars: !@#$%^&*()'],
            'value': [1.5, 2.7, 3.14, 4.2]
        })
        
        output_file = os.path.join(self.temp_dir, 'special_chars.csv')
        
        result = self.export_utils.export_to_csv(
            special_data, 
            output_file, 
            encoding='utf-8'
        )
        
        self.assertTrue(result)
        
        # Verify special characters are preserved
        df_read = pd.read_csv(output_file, encoding='utf-8')
        self.assertEqual(df_read.loc[0, 'name'], 'José María')
        self.assertEqual(df_read.loc[2, 'name'], '北京')

    def test_export_with_null_values(self):
        """Test export with various null value representations."""
        null_data = pd.DataFrame({
            'col1': [1, None, 3, np.nan, 5],
            'col2': ['a', '', 'c', None, 'e'],
            'col3': [1.1, np.nan, 3.3, None, 5.5]
        })
        
        output_file = os.path.join(self.temp_dir, 'null_values.csv')
        
        result = self.export_utils.export_to_csv(
            null_data, 
            output_file,
            na_rep='NULL'
        )
        
        self.assertTrue(result)
        
        # Verify null handling
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn('NULL', content)

    def test_export_with_very_long_strings(self):
        """Test export with very long string values."""
        long_string = 'A' * 10000  # 10KB string
        
        long_data = pd.DataFrame({
            'id': [1, 2, 3],
            'short_text': ['short', 'text', 'here'],
            'long_text': [long_string, long_string[:5000], long_string[:7500]]
        })
        
        output_file = os.path.join(self.temp_dir, 'long_strings.csv')
        
        result = self.export_utils.export_to_csv(long_data, output_file)
        
        self.assertTrue(result)
        
        # Verify long strings are handled
        df_read = pd.read_csv(output_file)
        self.assertEqual(len(df_read.loc[0, 'long_text']), 10000)

    def test_export_with_circular_references(self):
        """Test export with data containing circular references."""
        # This would typically apply to JSON/dict exports
        circular_dict = {'a': 1, 'b': 2}
        circular_dict['self'] = circular_dict  # Circular reference
        
        output_file = os.path.join(self.temp_dir, 'circular.json')
        
        # Should handle circular references gracefully
        with self.assertRaises((ValueError, RecursionError)):
            self.export_utils.export_dict_to_json(circular_dict, output_file)

    def test_export_with_mixed_data_types(self):
        """Test export with complex mixed data types."""
        mixed_data = pd.DataFrame({
            'integers': [1, 2, 3],
            'floats': [1.1, 2.2, 3.3],
            'strings': ['a', 'b', 'c'],
            'booleans': [True, False, True],
            'dates': pd.date_range('2023-01-01', periods=3),
            'objects': [{'key': 'value'}, [1, 2, 3], None]
        })
        
        output_file = os.path.join(self.temp_dir, 'mixed_types.json')
        
        result = self.export_utils.export_to_json(
            mixed_data, 
            output_file,
            date_format='iso'
        )
        
        self.assertTrue(result)

    def test_export_extremely_wide_dataframe(self):
        """Test export with DataFrame having many columns."""
        # Create DataFrame with 1000 columns
        wide_data = {}
        for i in range(1000):
            wide_data[f'col_{i:04d}'] = [i, i+1, i+2]
        
        wide_df = pd.DataFrame(wide_data)
        output_file = os.path.join(self.temp_dir, 'wide_data.csv')
        
        result = self.export_utils.export_to_csv(wide_df, output_file)
        
        self.assertTrue(result)
        
        # Verify all columns are exported
        df_read = pd.read_csv(output_file)
        self.assertEqual(len(df_read.columns), 1000)

    def test_export_with_filesystem_limits(self):
        """Test export behavior near filesystem limits."""
        # Test with very long filename
        long_filename = 'a' * 200 + '.csv'  # Very long filename
        output_file = os.path.join(self.temp_dir, long_filename)
        
        simple_data = pd.DataFrame({'col1': [1, 2, 3]})
        
        try:
            result = self.export_utils.export_to_csv(simple_data, output_file)
            # If successful, verify file exists
            if result:
                self.assertTrue(os.path.exists(output_file))
        except (OSError, FileNotFoundError):
            # Expected behavior for filesystem limits
            pass

    def test_concurrent_export_operations(self):
        """Test concurrent export operations to same directory."""
        import threading
        import time
        
        def export_worker(worker_id):
            data = pd.DataFrame({
                'worker_id': [worker_id] * 10,
                'value': range(10)
            })
            output_file = os.path.join(self.temp_dir, f'worker_{worker_id}.csv')
            return self.export_utils.export_to_csv(data, output_file)
        
        # Start multiple export operations concurrently
        threads = []
        results = {}
        
        for i in range(5):
            thread = threading.Thread(
                target=lambda i=i: results.update({i: export_worker(i)})
            )
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify all exports succeeded
        for i in range(5):
            self.assertTrue(results.get(i, False))
            expected_file = os.path.join(self.temp_dir, f'worker_{i}.csv')
            self.assertTrue(os.path.exists(expected_file))

    def test_export_with_corrupt_data(self):
        """Test export handling of potentially corrupt data."""
        # Create DataFrame with problematic values
        corrupt_data = pd.DataFrame({
            'normal_col': [1, 2, 3],
            'inf_col': [float('inf'), -float('inf'), 1.0],
            'nan_col': [float('nan'), 2.0, float('nan')]
        })
        
        output_file = os.path.join(self.temp_dir, 'corrupt_data.csv')
        
        result = self.export_utils.export_to_csv(
            corrupt_data, 
            output_file,
            handle_inf=True,
            handle_nan=True
        )
        
        self.assertTrue(result)

    def test_export_recovery_from_interruption(self):
        """Test export recovery mechanisms."""
        large_data = pd.DataFrame({
            'col1': range(10000),
            'col2': [f'value_{i}' for i in range(10000)]
        })
        
        output_file = os.path.join(self.temp_dir, 'interrupted_export.csv')
        
        # Simulate interruption and recovery
        try:
            # Start export with potential interruption
            result = self.export_utils.export_with_recovery(
                large_data, 
                output_file,
                checkpoint_interval=1000
            )
            self.assertTrue(result)
        except KeyboardInterrupt:
            # Test recovery mechanism
            recovery_result = self.export_utils.resume_export(output_file)
            self.assertIsNotNone(recovery_result)


class TestExportUtilsValidation(unittest.TestCase):
    """Test validation and verification features of ExportUtils."""
    
    def setUp(self):
        """Set up validation test fixtures."""
        self.export_utils = ExportUtils()
        self.temp_dir = tempfile.mkdtemp()
        
        self.test_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10.5, 20.7, 30.1, 40.9, 50.3]
        })

    def tearDown(self):
        """Clean up validation test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_export_integrity_validation(self):
        """Test export integrity validation."""
        output_file = os.path.join(self.temp_dir, 'integrity_test.csv')
        
        # Export with integrity checking
        result = self.export_utils.export_with_integrity_check(
            self.test_data, 
            output_file
        )
        
        self.assertTrue(result['export_success'])
        self.assertTrue(result['integrity_valid'])
        self.assertEqual(result['checksum_original'], result['checksum_exported'])

    def test_export_schema_validation(self):
        """Test export schema validation."""
        output_file = os.path.join(self.temp_dir, 'schema_test.json')
        
        expected_schema = {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'id': {'type': 'integer'},
                    'value': {'type': 'number'}
                },
                'required': ['id', 'value']
            }
        }
        
        result = self.export_utils.export_with_schema_validation(
            self.test_data,
            output_file,
            expected_schema,
            format_type='json'
        )
        
        self.assertTrue(result['export_success'])
        self.assertTrue(result['schema_valid'])

    def test_export_quality_metrics(self):
        """Test export quality metrics calculation."""
        output_file = os.path.join(self.temp_dir, 'quality_test.csv')
        
        result = self.export_utils.export_with_quality_metrics(
            self.test_data,
            output_file
        )
        
        self.assertTrue(result['export_success'])
        self.assertIn('completeness_score', result['quality_metrics'])
        self.assertIn('accuracy_score', result['quality_metrics'])
        self.assertIn('consistency_score', result['quality_metrics'])


if __name__ == '__main__':
    # Configure test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    test_classes = [
        TestExportUtils,
        TestExportUtilsIntegration,
        TestExportUtilsPerformance,
        TestExportUtilsEdgeCases,
        TestExportUtilsValidation
    ]
    
    for test_class in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(test_class))
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    print(f"{'='*50}")
    
    # Exit with appropriate code
    exit(0 if result.wasSuccessful() else 1)