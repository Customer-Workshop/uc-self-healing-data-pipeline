"""
Unit tests for data quality components and Great Expectations integration.
"""

import json
import os
import pandas as pd
import pytest
import tempfile
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

import sys
sys.path.append('/workspace')


class TestDataQualityExpectations:
    """Test suite for data quality expectations and validations."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.sample_data = pd.DataFrame({
            'id': ['cust_001', 'cust_002', 'cust_003', 'cust_004', 'cust_005'],
            'name': ['John Doe', 'Jane Smith', 'Bob Wilson', 'Alice Brown', 'Charlie Davis'],
            'email': [
                'john@example.com', 
                'jane@example.com', 
                'invalid-email',  # Invalid email for testing
                'alice@example.com', 
                'charlie@example.com'
            ],
            'signup_timestamp': [
                datetime(2023, 1, 1),
                datetime(2023, 6, 15),
                datetime(2023, 12, 1),
                datetime(2025, 1, 1),  # Future date for testing
                datetime(2023, 8, 20)
            ],
            'event_timestamp': [
                datetime(2024, 1, 15),
                datetime(2024, 1, 15),
                datetime(2024, 1, 15),
                datetime(2024, 1, 15),
                datetime(2023, 5, 1)   # Before signup for testing
            ],
            'country': ['US', 'CA', 'INVALID', 'GB', 'US'],  # Invalid country for testing
            'plan': ['free', 'premium', 'invalid_plan', 'basic', 'enterprise'],  # Invalid plan
            'version': [1, 1, 1, 1, 1]
        })
    
    def test_email_validation(self):
        """Test email format validation."""
        # Valid emails
        valid_emails = [
            'user@example.com',
            'test.email@domain.co.uk',
            'user+tag@example.org'
        ]
        
        # Invalid emails
        invalid_emails = [
            'invalid-email',
            '@example.com',
            'user@',
            'user space@example.com'
        ]
        
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        for email in valid_emails:
            assert pd.Series([email]).str.match(email_regex).iloc[0], f"Valid email failed: {email}"
        
        for email in invalid_emails:
            assert not pd.Series([email]).str.match(email_regex).iloc[0], f"Invalid email passed: {email}"
    
    def test_column_existence(self):
        """Test that required columns exist in the dataset."""
        required_columns = ['id', 'email', 'name', 'signup_timestamp', 'event_timestamp']
        
        for column in required_columns:
            assert column in self.sample_data.columns, f"Required column missing: {column}"
    
    def test_null_validation(self):
        """Test null value validation for critical fields."""
        critical_fields = ['id', 'email']
        
        for field in critical_fields:
            null_count = self.sample_data[field].isnull().sum()
            assert null_count == 0, f"Critical field {field} has {null_count} null values"
    
    def test_uniqueness_validation(self):
        """Test uniqueness of ID field."""
        unique_ids = self.sample_data['id'].nunique()
        total_records = len(self.sample_data)
        
        assert unique_ids == total_records, f"ID field not unique: {unique_ids} unique vs {total_records} total"
    
    def test_plan_values_validation(self):
        """Test that plan values are from allowed set."""
        allowed_plans = ['free', 'basic', 'premium', 'enterprise']
        
        invalid_plans = self.sample_data[~self.sample_data['plan'].isin(allowed_plans)]
        
        # We expect one invalid plan in our test data
        assert len(invalid_plans) == 1
        assert invalid_plans.iloc[0]['plan'] == 'invalid_plan'
    
    def test_country_values_validation(self):
        """Test that country values are from allowed set."""
        allowed_countries = ['US', 'CA', 'GB', 'DE', 'FR', 'AU', 'JP', 'IN', 'BR', 'MX']
        
        invalid_countries = self.sample_data[~self.sample_data['country'].isin(allowed_countries)]
        
        # We expect one invalid country in our test data
        assert len(invalid_countries) == 1
        assert invalid_countries.iloc[0]['country'] == 'INVALID'
    
    def test_timestamp_validation(self):
        """Test timestamp validation rules."""
        # Check for future signup dates
        current_time = datetime.now()
        future_signups = self.sample_data[self.sample_data['signup_timestamp'] > current_time]
        
        # We expect one future signup in our test data
        assert len(future_signups) == 1
        
        # Check that event_timestamp >= signup_timestamp
        invalid_sequence = self.sample_data[
            self.sample_data['event_timestamp'] < self.sample_data['signup_timestamp']
        ]
        
        # We expect one invalid sequence in our test data
        assert len(invalid_sequence) == 1
    
    def test_data_quality_summary(self):
        """Test comprehensive data quality summary."""
        total_records = len(self.sample_data)
        
        # Email validation
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        valid_emails = self.sample_data['email'].str.match(email_regex).sum()
        
        # Plan validation
        allowed_plans = ['free', 'basic', 'premium', 'enterprise']
        valid_plans = self.sample_data['plan'].isin(allowed_plans).sum()
        
        # Country validation
        allowed_countries = ['US', 'CA', 'GB', 'DE', 'FR', 'AU', 'JP', 'IN', 'BR', 'MX']
        valid_countries = self.sample_data['country'].isin(allowed_countries).sum()
        
        # Timestamp validation
        current_time = datetime.now()
        valid_signup_dates = (self.sample_data['signup_timestamp'] <= current_time).sum()
        valid_event_sequence = (
            self.sample_data['event_timestamp'] >= self.sample_data['signup_timestamp']
        ).sum()
        
        # ID validation
        unique_ids = self.sample_data['id'].nunique()
        
        dq_summary = {
            'total_records': total_records,
            'valid_emails': valid_emails,
            'valid_plans': valid_plans,
            'valid_countries': valid_countries,
            'valid_signup_dates': valid_signup_dates,
            'valid_event_sequence': valid_event_sequence,
            'unique_ids': unique_ids,
            'email_success_rate': valid_emails / total_records,
            'plan_success_rate': valid_plans / total_records,
            'country_success_rate': valid_countries / total_records,
            'signup_date_success_rate': valid_signup_dates / total_records,
            'event_sequence_success_rate': valid_event_sequence / total_records,
            'id_uniqueness_rate': unique_ids / total_records
        }
        
        # Verify expected failure rates based on our test data
        assert dq_summary['email_success_rate'] == 0.8  # 4/5 valid emails
        assert dq_summary['plan_success_rate'] == 0.8   # 4/5 valid plans
        assert dq_summary['country_success_rate'] == 0.8 # 4/5 valid countries
        assert dq_summary['signup_date_success_rate'] == 0.8 # 4/5 valid signup dates
        assert dq_summary['event_sequence_success_rate'] == 0.8 # 4/5 valid sequences
        assert dq_summary['id_uniqueness_rate'] == 1.0  # All IDs unique
        
        return dq_summary


class TestGreatExpectationsIntegration:
    """Test integration with Great Expectations framework."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.expectations_config = {
            "expectation_suite_name": "customers_expectation_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "id"}
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id"}
                },
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "id"}
                },
                {
                    "expectation_type": "expect_column_values_to_match_regex",
                    "kwargs": {
                        "column": "email",
                        "regex": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
                    }
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "plan",
                        "value_set": ["free", "basic", "premium", "enterprise"]
                    }
                }
            ]
        }
    
    def test_expectations_config_structure(self):
        """Test that expectations configuration has the correct structure."""
        assert "expectation_suite_name" in self.expectations_config
        assert "expectations" in self.expectations_config
        assert isinstance(self.expectations_config["expectations"], list)
        assert len(self.expectations_config["expectations"]) > 0
        
        for expectation in self.expectations_config["expectations"]:
            assert "expectation_type" in expectation
            assert "kwargs" in expectation
    
    def test_checkpoint_config_structure(self):
        """Test checkpoint configuration structure."""
        checkpoint_config = {
            "name": "customers_data_quality_checkpoint",
            "config_version": 1.0,
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "filesystem_datasource",
                        "data_connector_name": "default_inferred_data_connector_name",
                        "data_asset_name": "customers"
                    },
                    "expectation_suite_name": "customers_expectation_suite"
                }
            ]
        }
        
        assert "name" in checkpoint_config
        assert "config_version" in checkpoint_config
        assert "validations" in checkpoint_config
        assert len(checkpoint_config["validations"]) > 0
    
    def test_mock_validation_results(self):
        """Test mock validation results structure."""
        mock_results = {
            "success": True,
            "statistics": {
                "evaluated_expectations": 15,
                "successful_expectations": 12,
                "unsuccessful_expectations": 3,
                "success_percent": 80.0
            },
            "results": [
                {
                    "expectation_config": {
                        "expectation_type": "expect_column_values_to_match_regex",
                        "kwargs": {"column": "email"}
                    },
                    "success": False,
                    "result": {
                        "element_count": 5,
                        "unexpected_count": 1,
                        "unexpected_percent": 20.0,
                        "partial_unexpected_list": ["invalid-email"]
                    }
                }
            ]
        }
        
        assert "success" in mock_results
        assert "statistics" in mock_results
        assert "results" in mock_results
        
        stats = mock_results["statistics"]
        assert stats["success_percent"] == 80.0
        assert stats["unsuccessful_expectations"] == 3
        
        # Test specific expectation result
        email_result = mock_results["results"][0]
        assert email_result["success"] is False
        assert email_result["result"]["unexpected_percent"] == 20.0


class TestDataQualityThresholds:
    """Test data quality thresholds and alerting logic."""
    
    def test_failure_rate_calculation(self):
        """Test calculation of failure rates."""
        total_expectations = 15
        failed_expectations = 3
        failure_rate = failed_expectations / total_expectations
        
        assert failure_rate == 0.2  # 20% failure rate
    
    def test_threshold_based_alerting(self):
        """Test threshold-based alerting logic."""
        thresholds = {
            'critical': 0.01,   # 1%
            'warning': 0.05,    # 5%
            'acceptable': 0.10  # 10%
        }
        
        # Test different failure rates
        test_cases = [
            (0.005, 'acceptable'),  # 0.5% - below all thresholds
            (0.03, 'warning'),      # 3% - above warning threshold
            (0.15, 'critical')      # 15% - above critical threshold
        ]
        
        for failure_rate, expected_severity in test_cases:
            if failure_rate > thresholds['critical']:
                severity = 'critical'
            elif failure_rate > thresholds['warning']:
                severity = 'warning'
            else:
                severity = 'acceptable'
            
            assert severity == expected_severity, f"Failure rate {failure_rate} should be {expected_severity}"
    
    def test_data_quality_metrics(self):
        """Test comprehensive data quality metrics calculation."""
        validation_results = {
            'completeness': {'score': 0.95, 'failures': 50, 'total': 1000},
            'validity': {'score': 0.88, 'failures': 120, 'total': 1000},
            'consistency': {'score': 0.92, 'failures': 80, 'total': 1000},
            'timeliness': {'score': 0.85, 'failures': 150, 'total': 1000},
            'uniqueness': {'score': 0.99, 'failures': 10, 'total': 1000}
        }
        
        # Calculate overall score
        total_score = sum(dim['score'] for dim in validation_results.values())
        overall_score = total_score / len(validation_results)
        
        assert overall_score == pytest.approx(0.918, rel=1e-3)
        
        # Calculate total failures
        total_failures = sum(dim['failures'] for dim in validation_results.values())
        total_records = sum(dim['total'] for dim in validation_results.values())
        overall_failure_rate = total_failures / total_records
        
        assert overall_failure_rate == 0.082  # 8.2% failure rate
    
    def test_quality_trend_analysis(self):
        """Test quality trend analysis over time."""
        historical_scores = [
            {'timestamp': '2024-01-01', 'score': 0.95},
            {'timestamp': '2024-01-02', 'score': 0.93},
            {'timestamp': '2024-01-03', 'score': 0.90},
            {'timestamp': '2024-01-04', 'score': 0.87},
            {'timestamp': '2024-01-05', 'score': 0.85}
        ]
        
        # Calculate trend (declining quality)
        scores = [entry['score'] for entry in historical_scores]
        
        # Simple trend calculation
        trend = (scores[-1] - scores[0]) / len(scores)
        
        assert trend < 0, "Quality trend should be declining"
        assert abs(trend) == pytest.approx(0.025, rel=1e-3)  # 2.5% decline per day


class TestDataQualityRules:
    """Test data quality rules and business logic validation."""
    
    def test_business_rule_mapping(self):
        """Test mapping of business rules to technical validations."""
        business_rules = {
            'customer_identification': ['id_not_null', 'id_unique', 'email_not_null', 'email_valid'],
            'subscription_integrity': ['plan_valid', 'signup_date_valid'],
            'temporal_consistency': ['event_after_signup', 'recent_events'],
            'geographic_validity': ['country_code_valid']
        }
        
        technical_validations = {
            'id_not_null': 'expect_column_values_to_not_be_null',
            'id_unique': 'expect_column_values_to_be_unique',
            'email_not_null': 'expect_column_values_to_not_be_null',
            'email_valid': 'expect_column_values_to_match_regex',
            'plan_valid': 'expect_column_values_to_be_in_set',
            'signup_date_valid': 'expect_column_values_to_be_between',
            'event_after_signup': 'expect_column_pair_values_A_to_be_greater_than_or_equal_to_B',
            'recent_events': 'expect_column_values_to_be_between',
            'country_code_valid': 'expect_column_values_to_be_in_set'
        }
        
        # Verify all business rules have corresponding technical validations
        for rule_category, rules in business_rules.items():
            for rule in rules:
                assert rule in technical_validations, f"No technical validation for rule: {rule}"
        
        # Verify all technical validations are Great Expectations types
        ge_expectation_types = [
            'expect_column_values_to_not_be_null',
            'expect_column_values_to_be_unique', 
            'expect_column_values_to_match_regex',
            'expect_column_values_to_be_in_set',
            'expect_column_values_to_be_between',
            'expect_column_pair_values_A_to_be_greater_than_or_equal_to_B'
        ]
        
        for validation in technical_validations.values():
            assert validation in ge_expectation_types, f"Invalid GE expectation type: {validation}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])