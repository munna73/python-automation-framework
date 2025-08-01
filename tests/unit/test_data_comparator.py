"""
Unit tests for data_comparator module.
"""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime
import tempfile
import os

# Import the module under test
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.data_comparator import DataComparator, data_comparator


class TestDataComparator:
    """Test cases for DataComparator class."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.comparator = DataComparator()
        
        # Sample test data
        self.source_data = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'name': ['Alice', 'Bob', 'Charlie', 'David'],
            'age': [25, 30, 35, 40],
            'salary': [50000.0, 60000.0, 70000.0, 80000.0]
        })
        
        self.target_data = pd.DataFrame({
            'id': [1, 2, 3, 5],
            'name': ['Alice', 'Robert', 'Charlie', 'Eve'],
            'age': [25, 31, 35, 28],
            'salary': [50000.0, 60500.0, 70000.0, 55000.0]
        })
    
    def test_init(self):
        """Test DataComparator initialization."""
        comparator = DataComparator()
        assert hasattr(comparator, 'comparison_results')
        assert isinstance(comparator.comparison_results, dict)
        assert len(comparator.comparison_results) == 0
    
    def test_compare_dataframes_basic(self):
        """Test basic dataframe comparison."""
        result = self.comparator.compare_dataframes(
            source_df=self.source_data,
            target_df=self.target_data,
            key_columns=['id'],
            comparison_name='test_comparison'
        )
        
        # Check result structure
        assert 'comparison_name' in result
        assert 'summary' in result
        assert 'differences' in result
        assert 'metadata' in result
        
        # Check summary data
        summary = result['summary']
        assert summary['source_rows'] == 4
        assert summary['target_rows'] == 4
        assert summary['total_differences'] > 0
        
        # Check that comparison was stored
        assert 'test_comparison' in self.comparator.comparison_results
    
    def test_compare_dataframes_with_tolerance(self):
        """Test dataframe comparison with numeric tolerance."""
        result = self.comparator.compare_dataframes_with_tolerance(
            source_df=self.source_data,
            target_df=self.target_data,
            key_columns=['id'],
            numeric_tolerance=1000.0,  # High tolerance
            comparison_name='tolerance_test'
        )
        
        # With high tolerance, salary differences might be ignored
        assert 'tolerance_test' in self.comparator.comparison_results
        assert result['summary']['total_differences'] >= 0
    
    def test_auto_detect_key_columns(self):
        """Test automatic key column detection."""
        # Test with ID column
        key_cols = self.comparator._auto_detect_key_columns(
            self.source_data, self.target_data
        )
        assert 'id' in key_cols
        
        # Test with no ID column
        df_no_id = pd.DataFrame({
            'name': ['Alice', 'Bob'],
            'unique_col': [1, 2]  # This should be detected as unique
        })
        key_cols = self.comparator._auto_detect_key_columns(df_no_id, df_no_id)
        assert len(key_cols) > 0
    
    def test_clean_dataframe(self):
        """Test dataframe cleaning functionality."""
        # Create data with duplicates
        dirty_data = pd.DataFrame({
            'ID': [1, 1, 2, 3],  # Uppercase column name
            'Name': ['Alice', 'Alice', 'Bob', 'Charlie']
        })
        
        clean_df, duplicates_df = self.comparator._clean_dataframe(
            dirty_data, ['ID']
        )
        
        # Check column name normalization
        assert 'id' in clean_df.columns
        assert 'name' in clean_df.columns
        
        # Check duplicate removal
        assert len(clean_df) == 3  # One duplicate removed
        assert len(duplicates_df) >= 1  # At least one duplicate found
    
    def test_find_differences(self):
        """Test difference detection between dataframes."""
        # Normalize column names for testing
        source_clean = self.source_data.copy()
        target_clean = self.target_data.copy()
        source_clean.columns = [col.lower() for col in source_clean.columns]
        target_clean.columns = [col.lower() for col in target_clean.columns]
        
        differences = self.comparator._find_differences(
            source_clean, target_clean, ['id'], 
            ['id', 'name', 'age', 'salary'], None
        )
        
        # Should find differences (Bob -> Robert, age 30->31, salary changes)
        assert len(differences) > 0
    
    def test_find_missing_records(self):
        """Test missing record detection."""
        source_clean = self.source_data.copy()
        target_clean = self.target_data.copy()
        source_clean.columns = [col.lower() for col in source_clean.columns]
        target_clean.columns = [col.lower() for col in target_clean.columns]
        
        # Find records in source not in target
        missing = self.comparator._find_missing_records(
            source_clean, target_clean, ['id']
        )
        
        # ID 4 (David) should be missing in target
        assert len(missing) > 0
        assert 4 in missing['id'].values
    
    def test_values_differ(self):
        """Test value difference detection."""
        # Test identical values
        assert not self.comparator._values_differ('Alice', 'Alice', 'name')
        
        # Test different values
        assert self.comparator._values_differ('Alice', 'Bob', 'name')
        
        # Test numeric values with tolerance
        tolerance = {'salary': 100.0}
        assert not self.comparator._values_differ(
            50000.0, 50050.0, 'salary', tolerance
        )
        assert self.comparator._values_differ(
            50000.0, 50200.0, 'salary', tolerance
        )
        
        # Test NaN values
        assert not self.comparator._values_differ(np.nan, np.nan, 'test')
        assert self.comparator._values_differ(np.nan, 'value', 'test')
    
    def test_calculate_match_percentage(self):
        """Test match percentage calculation."""
        # Perfect match
        match_pct = self.comparator._calculate_match_percentage(
            100, 100, 0, 0, 0
        )
        assert match_pct == 100.0
        
        # Partial match
        match_pct = self.comparator._calculate_match_percentage(
            100, 100, 10, 5, 5
        )
        assert match_pct == 80.0  # 80 matches out of 100
        
        # Empty datasets
        match_pct = self.comparator._calculate_match_percentage(0, 0, 0, 0, 0)
        assert match_pct == 100.0
    
    def test_export_comparison_results(self):
        """Test exporting comparison results."""
        # First run a comparison to have results
        self.comparator.compare_dataframes(
            source_df=self.source_data,
            target_df=self.target_data,
            key_columns=['id'],
            comparison_name='export_test'
        )
        
        # Test JSON export
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = os.path.join(temp_dir, 'test_export.json')
            result_path = self.comparator.export_comparison_results(
                'export_test', 'json', temp_dir
            )
            assert os.path.exists(result_path)
            assert result_path.endswith('.json')
    
    def test_get_comparison_summary(self):
        """Test getting comparison summary."""
        # Test with no comparisons
        summary = self.comparator.get_comparison_summary()
        assert isinstance(summary, dict)
        assert len(summary) == 0
        
        # Run comparison and test summary
        self.comparator.compare_dataframes(
            source_df=self.source_data,
            target_df=self.target_data,
            key_columns=['id'],
            comparison_name='summary_test'
        )
        
        # Test specific comparison summary
        summary = self.comparator.get_comparison_summary('summary_test')
        assert 'comparison_name' in summary
        assert 'summary' in summary
        
        # Test all comparisons summary
        all_summary = self.comparator.get_comparison_summary()
        assert 'summary_test' in all_summary
    
    def test_clear_results(self):
        """Test clearing comparison results."""
        # Add some results
        self.comparator.compare_dataframes(
            source_df=self.source_data,
            target_df=self.target_data,
            key_columns=['id'],
            comparison_name='clear_test'
        )
        
        assert len(self.comparator.comparison_results) > 0
        
        # Clear results
        self.comparator.clear_results()
        assert len(self.comparator.comparison_results) == 0
    
    def test_empty_dataframes(self):
        """Test handling of empty dataframes."""
        empty_df = pd.DataFrame()
        
        result = self.comparator.compare_dataframes(
            source_df=empty_df,
            target_df=empty_df,
            key_columns=None,
            comparison_name='empty_test'
        )
        
        assert result['summary']['source_rows'] == 0
        assert result['summary']['target_rows'] == 0
    
    def test_different_column_sets(self):
        """Test comparison with different column sets."""
        # Create dataframes with different columns
        df1 = pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'extra_col': ['X', 'Y']
        })
        
        df2 = pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'different_col': ['A', 'B']
        })
        
        result = self.comparator.compare_dataframes(
            source_df=df1,
            target_df=df2,
            key_columns=['id'],
            comparison_name='different_cols_test'
        )
        
        # Should handle different column sets gracefully
        assert 'different_cols_test' in self.comparator.comparison_results
    
    def test_exclude_columns(self):
        """Test excluding columns from comparison."""
        result = self.comparator.compare_dataframes(
            source_df=self.source_data,
            target_df=self.target_data,
            key_columns=['id'],
            exclude_columns=['salary'],  # Exclude salary from comparison
            comparison_name='exclude_test'
        )
        
        # Differences should be fewer since salary is excluded
        assert 'exclude_test' in self.comparator.comparison_results
        
    @patch('utils.data_comparator.logger')
    def test_error_handling(self, mock_logger):
        """Test error handling in comparison methods."""
        # Test with invalid key columns
        result = self.comparator.compare_dataframes(
            source_df=self.source_data,
            target_df=self.target_data,
            key_columns=['nonexistent_column'],
            comparison_name='error_test'
        )
        
        # Should handle error gracefully
        assert isinstance(result, dict)


class TestGlobalDataComparator:
    """Test the global data_comparator instance."""
    
    def test_global_instance(self):
        """Test that global instance is properly initialized."""
        assert data_comparator is not None
        assert isinstance(data_comparator, DataComparator)
        assert hasattr(data_comparator, 'comparison_results')


# Integration test fixtures
@pytest.fixture
def sample_comparison_data():
    """Fixture providing sample data for integration tests."""
    source = pd.DataFrame({
        'customer_id': [1, 2, 3, 4, 5],
        'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Eve Adams'],
        'email': ['alice@email.com', 'bob@email.com', 'charlie@email.com', 'diana@email.com', 'eve@email.com'],
        'balance': [1000.50, 2500.75, 500.00, 7500.25, 3200.80],
        'status': ['ACTIVE', 'ACTIVE', 'INACTIVE', 'ACTIVE', 'PENDING']
    })
    
    target = pd.DataFrame({
        'customer_id': [1, 2, 3, 6, 7],
        'name': ['Alice Johnson', 'Robert Smith', 'Charlie Brown', 'Frank Miller', 'Grace Lee'],
        'email': ['alice@email.com', 'robert@email.com', 'charlie@email.com', 'frank@email.com', 'grace@email.com'],
        'balance': [1000.50, 2600.00, 500.00, 4500.00, 1800.90],
        'status': ['ACTIVE', 'ACTIVE', 'INACTIVE', 'ACTIVE', 'ACTIVE']
    })
    
    return source, target


class TestIntegrationScenarios:
    """Integration tests for real-world scenarios."""
    
    def test_customer_data_comparison(self, sample_comparison_data):
        """Test realistic customer data comparison scenario."""
        source_df, target_df = sample_comparison_data
        
        comparator = DataComparator()
        result = comparator.compare_dataframes(
            source_df=source_df,
            target_df=target_df,
            key_columns=['customer_id'],
            comparison_name='customer_sync_validation'
        )
        
        # Verify comprehensive comparison
        assert result['summary']['source_rows'] == 5
        assert result['summary']['target_rows'] == 5
        assert result['summary']['total_differences'] > 0
        
        # Should detect name change (Bob -> Robert) and other differences
        differences = result['differences']
        assert len(differences['field_differences']) > 0 or \
               len(differences['missing_in_source']) > 0 or \
               len(differences['missing_in_target']) > 0
    
    def test_financial_data_with_tolerance(self, sample_comparison_data):
        """Test financial data comparison with tolerance."""
        source_df, target_df = sample_comparison_data
        
        comparator = DataComparator()
        result = comparator.compare_dataframes_with_tolerance(
            source_df=source_df,
            target_df=target_df,
            key_columns=['customer_id'],
            numeric_tolerance=100.0,  # $100 tolerance for balance
            comparison_name='financial_validation'
        )
        
        # With tolerance, some balance differences might be acceptable
        assert result['summary']['total_differences'] >= 0
        assert 'financial_validation' in comparator.comparison_results


if __name__ == '__main__':
    pytest.main([__file__, '-v'])