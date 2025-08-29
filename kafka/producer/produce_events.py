#!/usr/bin/env python3
"""
Kafka Event Producer for Customer Events
Generates customer events with configurable anomalies for testing the pipeline.
"""

import json
import time
import logging
import random
import io
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import click
import avro.schema
import avro.io
from kafka import KafkaProducer
from faker import Faker
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CustomerEventProducer:
    """Producer for customer events with anomaly simulation capabilities."""
    
    def __init__(self, broker: str, topic: str, schema_path: str):
        self.broker = broker
        self.topic = topic
        self.fake = Faker()
        Faker.seed(42)  # Deterministic random seed for reproducibility
        random.seed(42)
        
        # Load AVRO schema
        self.schema = self._load_avro_schema(schema_path)
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda x: self._serialize_avro(x),
            key_serializer=lambda x: x.encode('utf-8'),
            acks='all',
            retries=3,
            retry_backoff_ms=100
        )
        
        # Event statistics
        self.stats = {
            'total_events': 0,
            'late_events': 0,
            'schema_drift_events': 0,
            'missing_field_events': 0,
            'errors': 0
        }
        
        # Customer data cache for consistency
        self.customers = self._generate_customer_base(1000)
        
        logger.info(f"Producer initialized for topic '{topic}' on broker '{broker}'")

    def _load_avro_schema(self, schema_path: str) -> avro.schema.Schema:
        """Load AVRO schema from file."""
        try:
            with open(schema_path, 'r') as f:
                schema_dict = json.load(f)
            return avro.schema.parse(json.dumps(schema_dict))
        except Exception as e:
            logger.error(f"Failed to load AVRO schema from {schema_path}: {e}")
            raise

    def _serialize_avro(self, data: Dict[str, Any]) -> bytes:
        """Serialize data to AVRO format."""
        try:
            writer = avro.io.DatumWriter(self.schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(data, encoder)
            return bytes_writer.getvalue()
        except Exception as e:
            logger.error(f"Failed to serialize data: {e}")
            raise

    def _generate_customer_base(self, count: int) -> list:
        """Generate a base set of customers for consistent event generation."""
        customers = []
        countries = ['US', 'CA', 'GB', 'DE', 'FR', 'AU', 'JP', 'IN', 'BR', 'MX']
        plans = ['free', 'basic', 'premium', 'enterprise']
        
        for _ in range(count):
            signup_date = self.fake.date_time_between(start_date='-2y', end_date='now')
            customer = {
                'id': self.fake.uuid4(),
                'name': self.fake.name(),
                'email': self.fake.email(),
                'signup_ts': int(signup_date.timestamp() * 1000),
                'country': random.choice(countries),
                'plan': random.choice(plans)
            }
            customers.append(customer)
        
        return customers

    def _generate_base_event(self) -> Dict[str, Any]:
        """Generate a base customer event."""
        customer = random.choice(self.customers)
        now = datetime.now()
        
        event = {
            'id': customer['id'],
            'name': customer['name'],
            'email': customer['email'],
            'signup_ts': customer['signup_ts'],
            'country': customer['country'],
            'plan': customer['plan'],
            'event_ts': int(now.timestamp() * 1000),
            'version': 1
        }
        
        return event

    def _introduce_late_arrival(self, event: Dict[str, Any], delay_hours: int = 2) -> Dict[str, Any]:
        """Introduce late arrival by backdating the event timestamp."""
        delay_ms = delay_hours * 60 * 60 * 1000
        event['event_ts'] = event['event_ts'] - delay_ms
        self.stats['late_events'] += 1
        logger.debug(f"Generated late event with {delay_hours}h delay")
        return event

    def _introduce_missing_fields(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Randomly remove optional fields to simulate missing data."""
        optional_fields = ['country', 'plan']
        field_to_remove = random.choice(optional_fields)
        
        if field_to_remove in event:
            del event[field_to_remove]
            self.stats['missing_field_events'] += 1
            logger.debug(f"Removed field '{field_to_remove}' from event")
        
        return event

    def _introduce_schema_drift(self, event: Dict[str, Any], version: int = 2) -> Dict[str, Any]:
        """Introduce schema drift by adding new fields."""
        event['version'] = version
        
        if version >= 2:
            # Add marketing opt-in field (nullable boolean)
            event['marketing_opt_in'] = random.choice([True, False, None])
        
        if version >= 3:
            # Add customer segment field (nullable string)
            segments = ['high_value', 'standard', 'churn_risk', None]
            event['customer_segment'] = random.choice(segments)
        
        self.stats['schema_drift_events'] += 1
        logger.debug(f"Applied schema drift to version {version}")
        return event

    def generate_event(self, late_rate: float = 0.05, drift_frequency: int = 100, 
                      missing_rate: float = 0.02) -> Dict[str, Any]:
        """Generate a single event with configurable anomalies."""
        event = self._generate_base_event()
        
        # Apply anomalies based on rates
        if random.random() < late_rate:
            delay_hours = random.randint(1, 24)  # 1-24 hour delay
            event = self._introduce_late_arrival(event, delay_hours)
        
        if random.random() < missing_rate:
            event = self._introduce_missing_fields(event)
        
        # Schema drift every N events
        if self.stats['total_events'] % drift_frequency == 0 and self.stats['total_events'] > 0:
            version = random.choice([2, 3])
            event = self._introduce_schema_drift(event, version)
        
        return event

    def produce_event(self, event: Dict[str, Any]) -> bool:
        """Produce a single event to Kafka."""
        try:
            # Use customer ID as the key for partitioning
            key = event['id']
            
            self.producer.send(
                topic=self.topic,
                key=key,
                value=event
            )
            
            self.stats['total_events'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Failed to produce event: {e}")
            self.stats['errors'] += 1
            return False

    def run_continuous(self, rate: float = 10.0, duration: Optional[int] = None, 
                      late_rate: float = 0.05, drift_frequency: int = 100, 
                      missing_rate: float = 0.02):
        """Run continuous event production."""
        logger.info(f"Starting continuous production at {rate} events/sec")
        logger.info(f"Anomaly rates - Late: {late_rate*100:.1f}%, Missing fields: {missing_rate*100:.1f}%")
        logger.info(f"Schema drift every {drift_frequency} events")
        
        start_time = time.time()
        last_stats_time = start_time
        
        try:
            while True:
                # Check duration limit
                if duration and (time.time() - start_time) > duration:
                    break
                
                # Generate and produce event
                event = self.generate_event(late_rate, drift_frequency, missing_rate)
                self.produce_event(event)
                
                # Print stats every 30 seconds
                current_time = time.time()
                if current_time - last_stats_time >= 30:
                    self._print_stats()
                    last_stats_time = current_time
                
                # Rate limiting
                time.sleep(1.0 / rate)
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        finally:
            self.shutdown()

    def _print_stats(self):
        """Print current production statistics."""
        logger.info("=== Production Statistics ===")
        logger.info(f"Total events: {self.stats['total_events']}")
        logger.info(f"Late events: {self.stats['late_events']}")
        logger.info(f"Schema drift events: {self.stats['schema_drift_events']}")
        logger.info(f"Missing field events: {self.stats['missing_field_events']}")
        logger.info(f"Errors: {self.stats['errors']}")
        
        if self.stats['total_events'] > 0:
            logger.info(f"Late rate: {self.stats['late_events']/self.stats['total_events']*100:.1f}%")
            logger.info(f"Error rate: {self.stats['errors']/self.stats['total_events']*100:.1f}%")

    def shutdown(self):
        """Gracefully shutdown the producer."""
        logger.info("Shutting down producer...")
        self.producer.flush()
        self.producer.close()
        self._print_stats()
        logger.info("Producer shutdown complete")


@click.command()
@click.option('--broker', default=lambda: os.getenv('KAFKA_BROKER', 'localhost:9092'), 
              help='Kafka broker address')
@click.option('--topic', default=lambda: os.getenv('KAFKA_TOPIC', 'customers'), 
              help='Kafka topic name')
@click.option('--schema-path', default='/app/schemas/customer_events.avsc',
              help='Path to AVRO schema file')
@click.option('--rate', default=10.0, help='Events per second')
@click.option('--duration', default=None, type=int, help='Duration in seconds (None for continuous)')
@click.option('--late-rate', default=0.05, help='Rate of late arrival events (0.0-1.0)')
@click.option('--drift-frequency', default=100, help='Schema drift every N events')
@click.option('--missing-rate', default=0.02, help='Rate of missing field events (0.0-1.0)')
def main(broker, topic, schema_path, rate, duration, late_rate, drift_frequency, missing_rate):
    """Kafka Event Producer for Customer Events with Anomaly Simulation."""
    
    logger.info("=== Customer Event Producer ===")
    logger.info(f"Broker: {broker}")
    logger.info(f"Topic: {topic}")
    logger.info(f"Schema: {schema_path}")
    
    try:
        producer = CustomerEventProducer(broker, topic, schema_path)
        producer.run_continuous(
            rate=rate,
            duration=duration,
            late_rate=late_rate,
            drift_frequency=drift_frequency,
            missing_rate=missing_rate
        )
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        exit(1)


if __name__ == '__main__':
    main()