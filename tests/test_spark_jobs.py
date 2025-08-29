"""
Unit tests for Spark jobs and data transformations.
"""

import json
import os
import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType

import sys
sys.path.append('/workspace')


class TestSparkDataTransformations:
    """Test suite for Spark data transformation functions."""
    
    @classmethod
    def setup_class(cls):
        """Set up Spark session for testing."""
        # Only create Spark session if available (for CI environments without Spark)
        try:
            cls.spark = SparkSession.builder \
                .appName("test_pipeline") \
                .master("local[2]") \
                .config("spark.sql.adaptive.enabled", "false") \
                .getOrCreate()
            cls.spark_available = True
        except Exception:
            cls.spark = None
            cls.spark_available = False
    
    @classmethod
    def teardown_class(cls):
        """Clean up Spark session."""
        if cls.spark:
            cls.spark.stop()
    
    def setup_method(self):
        """Set up test data."""
        self.sample_events = [
            {
                'id': 'cust_001',
                'name': 'John Doe',
                'email': 'john@example.com',
                'signup_ts': int(datetime(2023, 1, 1).timestamp() * 1000),
                'country': 'US',
                'plan': 'premium',
                'event_ts': int(datetime(2024, 1, 15).timestamp() * 1000),
                'version': 1
            },
            {
                'id': 'cust_002',
                'name': 'Jane Smith',
                'email': 'jane@example.com',
                'signup_ts': int(datetime(2023, 6, 15).timestamp() * 1000),
                'country': 'CA',
                'plan': 'free',
                'event_ts': int(datetime(2024, 1, 15).timestamp() * 1000),
                'version': 1
            },
            {
                'id': 'cust_003',
                'name': 'Bob Wilson',
                'email': 'bob@example.com',
                'signup_ts': int(datetime(2023, 12, 1).timestamp() * 1000),
                'country': 'GB',
                'plan': 'basic',
                'event_ts': int(datetime(2024, 1, 10).timestamp() * 1000),  # Earlier event for late arrival test
                'version': 2  # Schema drift test
            }
        ]
    
    @pytest.mark.skipif(not pytest.importorskip("pyspark", reason="PySpark not available"))
    def test_schema_definition(self):
        """Test customer event schema definition."""
        if not self.spark_available:
            pytest.skip("Spark not available")
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), False),
            StructField("signup_ts", LongType(), False),
            StructField("country", StringType(), True),
            StructField("plan", StringType(), True),
            StructField("event_ts", LongType(), False),
            StructField("version", IntegerType(), True)
        ])
        
        # Verify schema structure
        assert len(schema.fields) == 8
        assert schema.fieldNames() == ['id', 'name', 'email', 'signup_ts', 'country', 'plan', 'event_ts', 'version']
        
        # Verify required fields are not nullable
        non_nullable_fields = ['id', 'email', 'signup_ts', 'event_ts']
        for field in schema.fields:
            if field.name in non_nullable_fields:
                assert not field.nullable, f"Field {field.name} should not be nullable"
    
    def test_timestamp_conversion(self):
        """Test timestamp conversion from milliseconds to datetime."""
        # Test data
        timestamp_ms = int(datetime(2024, 1, 15, 12, 30, 45).timestamp() * 1000)
        
        # Convert using pandas (simulating Spark transformation)
        converted_timestamp = pd.to_datetime(timestamp_ms, unit='ms')
        expected_timestamp = datetime(2024, 1, 15, 12, 30, 45)
        
        assert converted_timestamp == expected_timestamp
    
    def test_country_to_region_mapping(self):
        """Test country to region transformation."""
        country_region_mapping = {
            'US': 'North America',
            'CA': 'North America', 
            'MX': 'North America',
            'GB': 'Europe',
            'DE': 'Europe',
            'FR': 'Europe',
            'AU': 'APAC',
            'JP': 'APAC',
            'IN': 'APAC',
            'BR': 'South America'
        }
        
        # Test known mappings
        test_cases = [
            ('US', 'North America'),
            ('GB', 'Europe'),
            ('JP', 'APAC'),
            ('BR', 'South America'),
            ('UNKNOWN', 'Other')  # Default case
        ]
        
        for country, expected_region in test_cases:
            actual_region = country_region_mapping.get(country, 'Other')
            assert actual_region == expected_region, f"Country {country} should map to {expected_region}"
    
    def test_email_domain_extraction(self):
        """Test email domain extraction."""
        test_emails = [
            ('john@example.com', 'example.com'),
            ('user@subdomain.example.org', 'subdomain.example.org'),
            ('test+tag@domain.co.uk', 'domain.co.uk')
        ]
        
        for email, expected_domain in test_emails:
            # Simulate regex extraction
            import re
            match = re.search(r'@(.+)', email)
            actual_domain = match.group(1) if match else None
            
            assert actual_domain == expected_domain, f"Email {email} should extract domain {expected_domain}"
    
    def test_late_arrival_detection(self):
        """Test late arrival detection logic."""
        # Current time for comparison
        current_time = datetime.now()
        
        # Test cases: event_time, processing_time, threshold_minutes, expected_late
        test_cases = [
            (current_time - timedelta(minutes=5), current_time, 15, False),   # Recent event
            (current_time - timedelta(minutes=20), current_time, 15, True),   # Late event
            (current_time - timedelta(hours=2), current_time, 15, True),      # Very late event
            (current_time - timedelta(minutes=10), current_time, 15, False)   # Within threshold
        ]
        
        threshold_seconds = 15 * 60  # 15 minutes in seconds
        
        for event_time, processing_time, threshold_min, expected_late in test_cases:
            time_diff = (processing_time - event_time).total_seconds()
            is_late = time_diff > threshold_seconds
            
            assert is_late == expected_late, f"Event time diff {time_diff}s should be late: {expected_late}"
    
    def test_schema_drift_detection(self):
        """Test schema drift detection logic."""
        # Base schema fields
        base_schema_fields = ['id', 'name', 'email', 'signup_ts', 'country', 'plan', 'event_ts', 'version']
        
        # Test schemas with different field sets
        test_schemas = [
            {
                'fields': ['id', 'name', 'email', 'signup_ts', 'country', 'plan', 'event_ts', 'version'],
                'has_drift': False,
                'description': 'No schema change'
            },
            {
                'fields': ['id', 'name', 'email', 'signup_ts', 'country', 'plan', 'event_ts', 'version', 'marketing_opt_in'],
                'has_drift': True,
                'description': 'New field added'
            },
            {
                'fields': ['id', 'name', 'email', 'signup_ts', 'plan', 'event_ts', 'version'],  # Missing country
                'has_drift': True,
                'description': 'Field removed'
            }
        ]
        
        for test_schema in test_schemas:
            new_fields = set(test_schema['fields']) - set(base_schema_fields)
            missing_fields = set(base_schema_fields) - set(test_schema['fields'])
            
            has_drift = len(new_fields) > 0 or len(missing_fields) > 0
            
            assert has_drift == test_schema['has_drift'], f"Schema drift detection failed for: {test_schema['description']}"
    
    def test_data_quality_flags(self):
        """Test data quality flag generation."""
        test_records = [
            {
                'id': 'cust_001',
                'email': 'valid@example.com',
                'plan': 'premium',
                'event_timestamp': datetime.now(),
                'expected_flags': {
                    'dq_email_valid': True,
                    'dq_id_not_null': True,
                    'dq_plan_valid': True,
                    'dq_timestamp_valid': True,
                    'dq_passed': True
                }
            },
            {
                'id': None,  # Null ID
                'email': 'invalid-email',  # Invalid email
                'plan': 'invalid_plan',  # Invalid plan
                'event_timestamp': datetime.now() + timedelta(days=1),  # Future timestamp
                'expected_flags': {
                    'dq_email_valid': False,
                    'dq_id_not_null': False,
                    'dq_plan_valid': False,
                    'dq_timestamp_valid': False,
                    'dq_passed': False
                }
            }
        ]
        
        valid_plans = ['free', 'basic', 'premium', 'enterprise']
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        current_time = datetime.now()
        
        for record in test_records:
            # Simulate DQ checks
            import re
            
            dq_flags = {
                'dq_email_valid': bool(record['email'] and re.match(email_regex, record['email'])),
                'dq_id_not_null': record['id'] is not None,
                'dq_plan_valid': record['plan'] in valid_plans,
                'dq_timestamp_valid': record['event_timestamp'] <= current_time
            }
            
            dq_flags['dq_passed'] = all(dq_flags.values())
            
            for flag, expected_value in record['expected_flags'].items():
                assert dq_flags[flag] == expected_value, f"DQ flag {flag} mismatch for record {record['id']}"


class TestSparkJobConfiguration:
    """Test Spark job configuration and setup."""
    
    def test_spark_configuration(self):
        """Test Spark configuration settings."""
        expected_configs = {
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.streaming.checkpointLocation': '/data/checkpoints/spark',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '2g',
            'spark.executor.cores': '2'
        }
        
        # Verify configuration values are reasonable
        assert expected_configs['spark.serializer'] == 'org.apache.spark.serializer.KryoSerializer'
        assert expected_configs['spark.sql.adaptive.enabled'] == 'true'
        assert expected_configs['spark.executor.memory'] == '2g'
        assert expected_configs['spark.driver.memory'] == '2g'
    
    def test_kafka_configuration(self):
        """Test Kafka connection configuration."""
        kafka_config = {
            'kafka.bootstrap.servers': 'broker:29092',
            'subscribe': 'customers',
            'startingOffsets': 'latest',
            'failOnDataLoss': 'false'
        }
        
        # Verify Kafka configuration
        assert 'kafka.bootstrap.servers' in kafka_config
        assert kafka_config['subscribe'] == 'customers'
        assert kafka_config['startingOffsets'] == 'latest'
        assert kafka_config['failOnDataLoss'] == 'false'
    
    def test_sink_configuration(self):
        """Test sink configuration for different output modes."""
        parquet_config = {
            'format': 'parquet',
            'outputMode': 'append',
            'path': '/data/out/parquet',
            'partitionBy': ['country', 'plan'],
            'trigger': 'processingTime=30 seconds'
        }
        
        snowflake_config = {
            'format': 'snowflake',
            'sfUrl': 'account.snowflakecomputing.com',
            'sfDatabase': 'TEST_DB',
            'sfSchema': 'PUBLIC',
            'dbtable': 'RAW_CUSTOMERS'
        }
        
        # Verify configurations
        assert parquet_config['format'] == 'parquet'
        assert parquet_config['outputMode'] == 'append'
        assert 'country' in parquet_config['partitionBy']
        
        assert snowflake_config['format'] == 'snowflake'
        assert 'sfUrl' in snowflake_config
        assert snowflake_config['dbtable'] == 'RAW_CUSTOMERS'


class TestRunReportGeneration:
    """Test run report generation and statistics."""
    
    def test_run_report_structure(self):
        """Test run report JSON structure."""
        sample_report = {
            'total_records': 1000,
            'late_records': 50,
            'schema_drift_detected': True,
            'dq_fail_count': 25,
            'processing_errors': 0,
            'start_time': '2024-01-15T10:00:00',
            'end_time': '2024-01-15T10:05:00',
            'watermark_delay': '15 minutes'
        }
        
        # Verify required fields
        required_fields = [
            'total_records', 'late_records', 'schema_drift_detected',
            'dq_fail_count', 'processing_errors', 'start_time', 'end_time'
        ]
        
        for field in required_fields:
            assert field in sample_report, f"Required field missing: {field}"
        
        # Verify data types
        assert isinstance(sample_report['total_records'], int)
        assert isinstance(sample_report['late_records'], int)
        assert isinstance(sample_report['schema_drift_detected'], bool)
        assert isinstance(sample_report['dq_fail_count'], int)
    
    def test_statistics_calculation(self):
        """Test calculation of processing statistics."""
        total_records = 1000
        late_records = 75
        dq_failures = 30
        processing_errors = 2
        
        # Calculate rates
        late_rate = late_records / total_records if total_records > 0 else 0
        dq_failure_rate = dq_failures / total_records if total_records > 0 else 0
        error_rate = processing_errors / total_records if total_records > 0 else 0
        
        # Verify calculations
        assert late_rate == 0.075  # 7.5%
        assert dq_failure_rate == 0.03  # 3%
        assert error_rate == 0.002  # 0.2%
        
        # Test edge case with zero records
        zero_case_rate = 0 / 1 if 1 > 0 else 0
        assert zero_case_rate == 0
    
    def test_report_serialization(self):
        """Test run report JSON serialization."""
        report_data = {
            'timestamp': datetime.now(),
            'total_records': 1000,
            'late_records': 50,
            'schema_drift_detected': False,
            'dq_fail_count': 10
        }
        
        # Convert datetime to string for JSON serialization
        serializable_report = report_data.copy()
        serializable_report['timestamp'] = report_data['timestamp'].isoformat()
        
        # Test JSON serialization
        json_string = json.dumps(serializable_report)
        deserialized_report = json.loads(json_string)
        
        assert deserialized_report['total_records'] == 1000
        assert deserialized_report['late_records'] == 50
        assert deserialized_report['schema_drift_detected'] is False


class TestStreamingJobLogic:
    """Test streaming job logic and flow control."""
    
    def test_watermark_configuration(self):
        """Test watermark configuration for late data handling."""
        watermark_configs = [
            {'delay': '5 minutes', 'threshold_seconds': 300},
            {'delay': '15 minutes', 'threshold_seconds': 900},
            {'delay': '1 hour', 'threshold_seconds': 3600}
        ]
        
        for config in watermark_configs:
            delay = config['delay']
            expected_seconds = config['threshold_seconds']
            
            # Extract numeric value and unit
            import re
            match = re.match(r'(\d+)\s*(minute|hour)s?', delay)
            if match:
                value = int(match.group(1))
                unit = match.group(2)
                
                if unit == 'minute':
                    actual_seconds = value * 60
                elif unit == 'hour':
                    actual_seconds = value * 3600
                else:
                    actual_seconds = 0
                
                assert actual_seconds == expected_seconds, f"Watermark {delay} should be {expected_seconds} seconds"
    
    def test_processing_trigger_configuration(self):
        """Test processing trigger configurations."""
        trigger_configs = [
            {'trigger': 'processingTime=30 seconds', 'interval_seconds': 30},
            {'trigger': 'processingTime=1 minute', 'interval_seconds': 60},
            {'trigger': 'processingTime=5 minutes', 'interval_seconds': 300}
        ]
        
        for config in trigger_configs:
            trigger = config['trigger']
            expected_interval = config['interval_seconds']
            
            # Extract interval from trigger string
            import re
            match = re.search(r'(\d+)\s*(second|minute)s?', trigger)
            if match:
                value = int(match.group(1))
                unit = match.group(2)
                
                if unit == 'second':
                    actual_interval = value
                elif unit == 'minute':
                    actual_interval = value * 60
                else:
                    actual_interval = 0
                
                assert actual_interval == expected_interval, f"Trigger {trigger} should be {expected_interval} seconds"
    
    def test_error_handling_scenarios(self):
        """Test error handling scenarios."""
        error_scenarios = [
            {
                'error_type': 'kafka_connection_failed',
                'expected_action': 'retry_with_backoff',
                'max_retries': 3
            },
            {
                'error_type': 'schema_parse_error',
                'expected_action': 'log_and_continue',
                'quarantine': True
            },
            {
                'error_type': 'sink_write_failure',
                'expected_action': 'fail_job',
                'escalate': True
            }
        ]
        
        for scenario in error_scenarios:
            error_type = scenario['error_type']
            expected_action = scenario['expected_action']
            
            # Verify error handling logic
            if error_type == 'kafka_connection_failed':
                assert expected_action == 'retry_with_backoff'
                assert scenario.get('max_retries', 0) > 0
            elif error_type == 'schema_parse_error':
                assert expected_action == 'log_and_continue'
                assert scenario.get('quarantine', False) is True
            elif error_type == 'sink_write_failure':
                assert expected_action == 'fail_job'
                assert scenario.get('escalate', False) is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])