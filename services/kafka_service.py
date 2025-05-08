# services/kafka_service.py
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError as KafkaLibError
import json
import logging
import threading
import time
import asyncio
from typing import Dict, Any, List, Optional
from services.db_service import DatabaseService
from models.transition import TransitionRequest
from datetime import datetime
from utils.error_handler import KafkaError, handle_exceptions
import backoff

logger = logging.getLogger("bas_gateway.kafka")

def convert_iso_to_unix_ms(iso_timestamp):
    """Convert ISO format timestamp to Unix timestamp in milliseconds"""
    dt = datetime.fromisoformat(str(iso_timestamp))
    return int(dt.timestamp() * 1000)

class KafkaService:
    def __init__(self, bootstrap_servers: List[str], db_service: DatabaseService):
        self.bootstrap_servers = bootstrap_servers
        self.db_service = db_service
        self._initialize_producer()
    
    def _initialize_producer(self):
        """Initialize the Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1),
                acks='all',
                retries=3,
                retry_backoff_ms=1000
            )
            logger.info(f"Kafka producer initialized to {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {str(e)}", exc_info=True)
            raise KafkaError(f"Failed to initialize Kafka connection: {str(e)}")
    
    def _check_connection(self):
        """Check if the Kafka producer is connected"""
        try:
            self.producer.partitions_for("test_topic")
            return True
        except Exception:
            try:
                self._initialize_producer()
                return True
            except Exception:
                return False
    
    @backoff.on_exception(backoff.expo, (KafkaLibError, ConnectionError), max_tries=3)
    async def _send_with_retry(self, topic: str, value: Any) -> Dict[str, Any]:
        """Send message to Kafka with automatic retry"""
        if not self._check_connection():
            raise KafkaError("Kafka producer is not connected")
        
        try:
            future = self.producer.send(topic, value=value)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Message sent to {topic} [partition: {record_metadata.partition}, offset: {record_metadata.offset}]")
            
            return {
                "status": "success",
                "message": "Message sent successfully",
                "details": {
                    "topic": record_metadata.topic,
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset
                }
            }
        except Exception as e:
            logger.error(f"Error sending message to {topic}: {str(e)}", exc_info=True)
            raise

    async def send_sensor_data(self, topic: str, data: Any):
        """Send sensor data to Kafka"""
        try:
            # Format the data if needed
            if isinstance(data, str):
                try:
                    value = json.loads(data)
                except json.JSONDecodeError:
                    value = data
            else:
                value = data
            
            return await self._send_with_retry(topic, value)
            
        except Exception as e:
            logger.error(f"Failed to send sensor data: {str(e)}", exc_info=True)
            raise KafkaError(f"Failed to send sensor data: {str(e)}")
    
    async def process_transition(self, transition: TransitionRequest):
        """Process a vessel transition event"""
        topic = "bas_transition_event"
        try:
            # Convert the Pydantic object to a dictionary
            if hasattr(transition, "model_dump"):  # Pydantic v2
                transition_dict = transition.model_dump()
            else:  # Pydantic v1
                transition_dict = transition.dict()
                
            # Handle datetime serialization
            if transition.timestamp and isinstance(transition.timestamp, datetime):
                transition_dict["timestamp"] = convert_iso_to_unix_ms(transition.timestamp)
            elif transition.timestamp is None:
                transition_dict["timestamp"] = int(datetime.now().timestamp() * 1000)
            
            return await self._send_with_retry(topic, transition_dict)
        except Exception as e:
            logger.error(f"Failed to process transition: {str(e)}", exc_info=True)
            raise KafkaError(f"Failed to process transition: {str(e)}")


class KafkaConfigConsumer:
    def __init__(
        self, 
        bootstrap_servers: List[str], 
        db_service: DatabaseService, 
        config_service,
        topic: str = "bas_config_event",
        group_id: str = "gateway-group"
    ):
        self.bootstrap_servers = bootstrap_servers
        self.db_service = db_service
        self.config_service = config_service
        self.group_id = group_id
        self.topic = topic
        self.running = False
        self.consumer = None
        self.consumer_thread = None
        
    def _initialize_consumer(self):
        """Initialize the Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                session_timeout_ms=30000
            )
            logger.info(f"Kafka consumer initialized for topic {self.topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {str(e)}", exc_info=True)
            return False
    
    def start(self):
        """Start the Kafka consumer in a background thread"""
        if self.running:
            logger.info("Consumer is already running")
            return
            
        if not self._initialize_consumer():
            logger.info("Scheduling consumer reconnect in 30 seconds")
            threading.Timer(30, self.start).start()
            return
        
        self.running = True
        self.consumer_thread = threading.Thread(target=self._consume, daemon=True)
        self.consumer_thread.start()
        logger.info(f"Kafka consumer started for topic {self.topic}")

    def stop(self):
        """Stop the consumer"""
        self.running = False
        if self.consumer:
            try:
                self.consumer.close(autocommit=True)
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {str(e)}")

    def _consume(self):
        """Consume messages from Kafka"""
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        while self.running:
            try:
                for message in self.consumer:
                    if not self.running:
                        break
                    
                    try:
                        config = message.value
                        logger.debug(f"Received config message: {config}")
                        
                        # Extract org_id and berth_id
                        org_id = config.get('orgId') or config.get('org_id')
                        berth_id = config.get('berthId') or config.get('berth_id')
                        
                        if not org_id or not berth_id:
                            logger.warning(f"Config message missing org_id or berth_id: {config}")
                            continue
                        
                        # Get the data app
                        data_app = self.db_service.get_data_app_by_berth_sync(org_id, berth_id)
                        
                        if data_app:
                            # Check if add_config is a coroutine function and handle accordingly
                            if asyncio.iscoroutinefunction(self.config_service.add_config):
                                # Run the coroutine in this thread's event loop
                                loop.run_until_complete(self.config_service.add_config(data_app.code, config))
                            else:
                                # If it's not async, call it directly
                                self.config_service.add_config(data_app.code, config)
                                
                            logger.info(f"Updated config for data app {data_app.code} at berth {org_id}_{berth_id}")
                        else:
                            logger.warning(f"No data app found for berth {org_id}_{berth_id}")
                            
                    except Exception as e:
                        logger.error(f"Error processing config message: {str(e)}", exc_info=True)
            
            except Exception as e:
                logger.error(f"Error in Kafka consumer: {str(e)}", exc_info=True)
                
                if not self.running:
                    break
                
                # Try to reinitialize the consumer
                try:
                    self.consumer.close()
                except:
                    pass
                
                time.sleep(5)
                
                if not self._initialize_consumer():
                    logger.critical("Failed to reinitialize consumer, exiting consumer loop")
                    break
        
        # Clean up when stopped
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {str(e)}")
        
        # Close the event loop
        loop.close()