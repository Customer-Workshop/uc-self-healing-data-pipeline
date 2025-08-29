#!/usr/bin/env python3
"""
Spark Structured Streaming Job for Customer Event Ingestion and Transformation
Processes events from Kafka, applies transformations, detects anomalies, and writes to sink.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, lit, current_timestamp,
    window, count, max as spark_max, min as spark_min, expr,
    regexp_extract, coalesce, unix_timestamp, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, 
    BooleanType, TimestampType
)
from pyspark.sql.streaming import StreamingQuery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataPipelineProcessor:
    """Spark Structured Streaming processor for customer events."""
    
    def __init__(self):
        self.spark = self._create_spark_session()
        self.config = self._load_config()
        self.schema_registry = {}
        self.run_stats = {
            'total_records': 0,
            'late_records': 0,
            'schema_drift_detected': False,
            'dq_failures': 0,
            'processing_errors': 0,
            'start_time': datetime.now().isoformat(),
            'watermark_delay': '15 minutes'
        }
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with required configurations."""
        spark = SparkSession.builder \
            .appName("DataPipelineIngest") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        return {
            'kafka_broker': os.getenv('KAFKA_BROKER', 'localhost:9092'),
            'kafka_topic': os.getenv('KAFKA_TOPIC', 'customers'),
            'sink_mode': os.getenv('SINK_MODE', 'PARQUET'),
            'parquet_output': os.getenv('PARQUET_OUT', '/data/out/parquet'),
            'checkpoint_location': os.getenv('SPARK_CHECKPOINT_LOCATION', '/data/checkpoints/spark'),
            'snowflake': {
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'user': os.getenv('SNOWFLAKE_USER'),
                'password': os.getenv('SNOWFLAKE_PASSWORD'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
                'database': os.getenv('SNOWFLAKE_DATABASE'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA'),
                'table': os.getenv('SNOWFLAKE_TABLE', 'RAW_CUSTOMERS')
            }
        }
    
    def _get_customer_schema(self) -> StructType:
        """Define the expected schema for customer events."""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), False),
            StructField("signup_ts", LongType(), False),
            StructField("country", StringType(), True),
            StructField("plan", StringType(), True),
            StructField("event_ts", LongType(), False),
            StructField("version", IntegerType(), True),
            StructField("marketing_opt_in", BooleanType(), True),
            StructField("customer_segment", StringType(), True)
        ])
    
    def _create_kafka_stream(self) -> DataFrame:
        """Create streaming DataFrame from Kafka source."""
        logger.info(f"Connecting to Kafka: {self.config['kafka_broker']}, topic: {self.config['kafka_topic']}")
        
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config['kafka_broker']) \
            .option("subscribe", self.config['kafka_topic']) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def _parse_kafka_messages(self, df: DataFrame) -> DataFrame:
        """Parse Kafka messages and extract customer event data."""
        schema = self._get_customer_schema()
        
        # Parse JSON from Kafka value (assuming JSON serialization for simplicity)
        # In production, you'd use AVRO deserialization
        parsed_df = df.select(
            col("key").cast("string").alias("kafka_key"),
            col("timestamp").alias("kafka_timestamp"),
            col("offset").alias("kafka_offset"),
            col("partition").alias("kafka_partition"),
            from_json(col("value").cast("string"), schema).alias("data")
        )
        
        # Flatten the nested structure
        flattened_df = parsed_df.select(
            col("kafka_key"),
            col("kafka_timestamp"),
            col("kafka_offset"),
            col("kafka_partition"),
            col("data.*")
        )
        
        return flattened_df
    
    def _detect_schema_drift(self, df: DataFrame) -> DataFrame:
        """Detect and log schema drift events."""
        # Check for unknown columns or version changes
        expected_columns = [f.name for f in self._get_customer_schema().fields]
        actual_columns = df.columns
        
        # Log schema differences
        new_columns = set(actual_columns) - set(expected_columns)
        missing_columns = set(expected_columns) - set(actual_columns)
        
        if new_columns:
            logger.warning(f"Schema drift detected - new columns: {new_columns}")
            self.run_stats['schema_drift_detected'] = True
        
        if missing_columns:
            logger.warning(f"Schema drift detected - missing columns: {missing_columns}")
            self.run_stats['schema_drift_detected'] = True
        
        # Add schema drift flag
        df_with_drift = df.withColumn(
            "schema_drift_detected", 
            when(col("version") > 1, True).otherwise(False)
        )
        
        return df_with_drift
    
    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        """Apply data transformations and enrichments."""
        # Convert timestamps
        transformed_df = df.withColumn(
            "signup_timestamp", 
            from_unixtime(col("signup_ts") / 1000).cast(TimestampType())
        ).withColumn(
            "event_timestamp", 
            from_unixtime(col("event_ts") / 1000).cast(TimestampType())
        )
        
        # Add processing timestamp
        transformed_df = transformed_df.withColumn(
            "processing_timestamp", 
            current_timestamp()
        )
        
        # Country to region mapping
        region_mapping = {
            'US': 'North America', 'CA': 'North America', 'MX': 'North America',
            'GB': 'Europe', 'DE': 'Europe', 'FR': 'Europe',
            'AU': 'APAC', 'JP': 'APAC', 'IN': 'APAC',
            'BR': 'South America'
        }
        
        # Add region column
        region_expr = "CASE "
        for country, region in region_mapping.items():
            region_expr += f"WHEN country = '{country}' THEN '{region}' "
        region_expr += "ELSE 'Other' END"
        
        transformed_df = transformed_df.withColumn("region", expr(region_expr))
        
        # Email domain extraction
        transformed_df = transformed_df.withColumn(
            "email_domain",
            regexp_extract(col("email"), "@(.+)", 1)
        )
        
        # Calculate customer age (days since signup)
        transformed_df = transformed_df.withColumn(
            "customer_age_days",
            expr("datediff(processing_timestamp, signup_timestamp)")
        )
        
        return transformed_df
    
    def _detect_late_arrivals(self, df: DataFrame) -> DataFrame:
        """Detect late arriving events based on watermark."""
        # Define watermark threshold (15 minutes)
        watermark_threshold = 15  # minutes
        
        # Mark late arrivals
        df_with_late_flag = df.withColumn(
            "is_late_arrival",
            when(
                unix_timestamp("processing_timestamp") - unix_timestamp("event_timestamp") > watermark_threshold * 60,
                True
            ).otherwise(False)
        )
        
        return df_with_late_flag
    
    def _apply_data_quality_checks(self, df: DataFrame) -> DataFrame:
        """Apply basic data quality checks."""
        # Add DQ flags
        dq_df = df.withColumn(
            "dq_email_valid",
            col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
        ).withColumn(
            "dq_id_not_null",
            col("id").isNotNull()
        ).withColumn(
            "dq_timestamp_valid",
            col("event_timestamp") <= current_timestamp()
        ).withColumn(
            "dq_plan_valid",
            col("plan").isin(["free", "basic", "premium", "enterprise"])
        )
        
        # Overall DQ flag
        dq_df = dq_df.withColumn(
            "dq_passed",
            col("dq_email_valid") & 
            col("dq_id_not_null") & 
            col("dq_timestamp_valid") & 
            col("dq_plan_valid")
        )
        
        return dq_df
    
    def _write_to_parquet(self, df: DataFrame) -> StreamingQuery:
        """Write DataFrame to partitioned Parquet files."""
        output_path = self.config['parquet_output']
        checkpoint_path = f"{self.config['checkpoint_location']}/parquet"
        
        logger.info(f"Writing to Parquet: {output_path}")
        
        query = df.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("country", "plan") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def _write_to_snowflake(self, df: DataFrame) -> StreamingQuery:
        """Write DataFrame to Snowflake using foreachBatch."""
        checkpoint_path = f"{self.config['checkpoint_location']}/snowflake"
        
        def write_batch_to_snowflake(batch_df, batch_id):
            """Write batch to Snowflake."""
            if batch_df.count() > 0:
                logger.info(f"Writing batch {batch_id} to Snowflake ({batch_df.count()} records)")
                
                snowflake_options = {
                    "sfUrl": f"{self.config['snowflake']['account']}.snowflakecomputing.com",
                    "sfUser": self.config['snowflake']['user'],
                    "sfPassword": self.config['snowflake']['password'],
                    "sfDatabase": self.config['snowflake']['database'],
                    "sfSchema": self.config['snowflake']['schema'],
                    "sfWarehouse": self.config['snowflake']['warehouse'],
                    "sfRole": self.config['snowflake']['role']
                }
                
                batch_df.write \
                    .format("snowflake") \
                    .options(**snowflake_options) \
                    .option("dbtable", self.config['snowflake']['table']) \
                    .mode("append") \
                    .save()
        
        query = df.writeStream \
            .foreachBatch(write_batch_to_snowflake) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="60 seconds") \
            .start()
        
        return query
    
    def _write_run_report(self, df: DataFrame):
        """Write processing statistics to run report."""
        try:
            # Calculate statistics
            batch_stats = df.agg(
                count("*").alias("total_count"),
                count(when(col("is_late_arrival") == True, 1)).alias("late_count"),
                count(when(col("dq_passed") == False, 1)).alias("dq_fail_count"),
                count(when(col("schema_drift_detected") == True, 1)).alias("drift_count")
            ).collect()[0]
            
            self.run_stats.update({
                'total_records': batch_stats['total_count'],
                'late_records': batch_stats['late_count'],
                'dq_failures': batch_stats['dq_fail_count'],
                'schema_drift_count': batch_stats['drift_count'],
                'end_time': datetime.now().isoformat()
            })
            
            # Write to file
            os.makedirs('/data/ops', exist_ok=True)
            report_path = '/data/ops/run_report.json'
            
            with open(report_path, 'w') as f:
                json.dump(self.run_stats, f, indent=2)
            
            logger.info(f"Run report written to {report_path}")
            
        except Exception as e:
            logger.error(f"Failed to write run report: {e}")
    
    def process_stream(self, duration_minutes: int = 60):
        """Main processing pipeline."""
        logger.info("Starting Spark Structured Streaming pipeline")
        
        try:
            # Create Kafka stream
            kafka_df = self._create_kafka_stream()
            
            # Parse messages
            parsed_df = self._parse_kafka_messages(kafka_df)
            
            # Apply watermark for late data handling
            watermarked_df = parsed_df.withWatermark("event_timestamp", "15 minutes")
            
            # Apply transformations
            transformed_df = self._apply_transformations(watermarked_df)
            
            # Detect schema drift
            drift_df = self._detect_schema_drift(transformed_df)
            
            # Detect late arrivals
            late_df = self._detect_late_arrivals(drift_df)
            
            # Apply data quality checks
            final_df = self._apply_data_quality_checks(late_df)
            
            # Write to sink based on configuration
            if self.config['sink_mode'].upper() == 'SNOWFLAKE':
                if not all(self.config['snowflake'].values()):
                    logger.warning("Snowflake credentials not complete, falling back to Parquet")
                    query = self._write_to_parquet(final_df)
                else:
                    query = self._write_to_snowflake(final_df)
            else:
                query = self._write_to_parquet(final_df)
            
            # Monitor the query
            logger.info(f"Query started: {query.id}")
            logger.info(f"Will run for {duration_minutes} minutes")
            
            # Wait for termination or timeout
            query.awaitTermination(duration_minutes * 60)
            
            # Write final run report
            self._write_run_report(final_df)
            
        except Exception as e:
            logger.error(f"Stream processing failed: {e}")
            self.run_stats['processing_errors'] += 1
            raise
        finally:
            # Cleanup
            if 'query' in locals():
                query.stop()
            self.spark.stop()


def main():
    """Main entry point."""
    logger.info("=== Spark Data Pipeline Processor ===")
    
    # Get runtime parameters
    duration = int(os.getenv('PROCESSING_DURATION_MINUTES', '60'))
    
    try:
        processor = DataPipelineProcessor()
        processor.process_stream(duration_minutes=duration)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()