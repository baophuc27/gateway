# services/kafka_service.py
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError as KafkaLibError
import json
import logging
import threading
import time
import asyncio
from typing import Dict, Any, List, Optional, Callable
from services.db_service import DatabaseService
from models.transition import TransitionRequest
from datetime import datetime
import copy
from utils.error_handler import KafkaError, handle_exceptions, RequestContext
import backoff

logger = logging.getLogger("bas_gateway.kafka")

def convert_iso_to_unix_ms(iso_timestamp):
    """
    Convert ISO format timestamp to Unix timestamp in milliseconds
    Example: "2025-04-29T04:51:08.699915+00:00" -> 1745909065078
    """
    # Parse the ISO timestamp
    dt = datetime.fromisoformat(str(iso_timestamp))
    
    # Convert to Unix timestamp (seconds since epoch) and multiply by 1000 for milliseconds
    unix_ms = int(dt.timestamp() * 1000)
    
    return unix_ms

def format_speed_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format speed values in the data payload to 2 decimal points.
    
    Args:
        data: The sensor data payload
        
    Returns:
        Dict with formatted speed values
    """
    try:
        # Create a deep copy to avoid modifying the original data
        if isinstance(data, str):
            formatted_data = copy.deepcopy(json.loads(data))
        else:
            formatted_data = copy.deepcopy(data)
        
        # Check if speed exists in the data
        if "speed" not in formatted_data:
            logger.debug("No speed data found in payload")
            return formatted_data
            
        # Format the speed values to 2 decimal points
        if isinstance(formatted_data["speed"], dict):
            # Handle the first payload format with nested speed values
            for sensor_id in formatted_data["speed"]:
                if "value" in formatted_data["speed"][sensor_id]:
                    formatted_data["speed"][sensor_id]["value"] = round(
                        formatted_data["speed"][sensor_id]["value"], 2
                    )
        else:
            # Handle the second payload format with a single speed value
            try:
                formatted_data["speed"] = round(float(formatted_data["speed"]), 2)
            except (TypeError, ValueError):
                logger.warning(f"Invalid speed value: {formatted_data['speed']}")
        
        return formatted_data
        
    except Exception as e:
        logger.error(f"Error formatting speed values: {str(e)}")
        # Return original data if formatting fails
        return data


class KafkaService:
    def __init__(
        self, 
        bootstrap_servers: List[str], 
        db_service: DatabaseService,
        topic_prefix: str = "bas",
        max_retries: int = 3,
        retry_backoff: float = 1.0
    ):
        self.bootstrap_servers = bootstrap_servers
        self.db_service = db_service
        self.topic_prefix = topic_prefix
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self._initialize_producer()
    
    def _initialize_producer(self):
        """Initialize the Kafka producer with retry logic"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1),
                acks='all',  # Wait for all replicas to acknowledge
                retries=self.max_retries,  # Retry if producer fails
                retry_backoff_ms=int(self.retry_backoff * 1000),  # Time between retries
                request_timeout_ms=30000,  # Longer timeout for requests
                max_block_ms=60000  # Maximum time to block during send
            )
            logger.info(f"Kafka producer initialized successfully to {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {str(e)}", exc_info=True)
            raise KafkaError(f"Failed to initialize Kafka connection: {str(e)}")
    
    def _check_connection(self):
        """Check if the Kafka producer is connected and reinitialize if needed"""
        try:
            # Try to get metadata to check connection
            self.producer.partitions_for("test_topic")
            return True
        except Exception as e:
            logger.warning(f"Kafka producer connection check failed: {str(e)}")
            try:
                self._initialize_producer()
                return True
            except Exception as e:
                logger.error(f"Failed to reconnect Kafka producer: {str(e)}")
                return False
    
    @backoff.on_exception(
        backoff.expo, 
        (KafkaLibError, ConnectionError), 
        max_tries=3, 
        jitter=backoff.full_jitter
    )
    async def _send_with_retry(self, topic: str, value: Any) -> Dict[str, Any]:
        """Send message to Kafka with automatic retry"""
        if not self._check_connection():
            raise KafkaError("Kafka producer is not connected")
        
        try:
            request_id = RequestContext.get_request_id()
            logger.debug(f"Sending message to topic {topic} - Request ID: {request_id}")
            
            future = self.producer.send(topic, value=value)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Message sent to {topic} - Partition: {record_metadata.partition}, "
                f"Offset: {record_metadata.offset} - Request ID: {request_id}"
            )
            
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
        try:
            # Format the data if needed
            if isinstance(data, str):
                try:
                    parsed_data = json.loads(data)
                    formatted_data = format_speed_values(parsed_data)
                    value = formatted_data
                except json.JSONDecodeError:
                    # Not JSON, send as is
                    value = data
            else:
                # Data is already a object (dict, etc.)
                formatted_data = format_speed_values(data)
                value = formatted_data
            
            return await self._send_with_retry(topic, value)
            
        except Exception as e:
            logger.error(f"Failed to send sensor data: {str(e)}", exc_info=True)
            raise KafkaError(f"Failed to send sensor data: {str(e)}")
    
    async def process_transition(self, transition: TransitionRequest):
        topic = f"bas_transition_event"
        try:
            # Convert the Pydantic object to a dictionary
            if hasattr(transition, "model_dump"):  # Pydantic v2
                transition_dict = transition.model_dump()
            else:  # Pydantic v1
                transition_dict = transition.dict()
                
            # Handle datetime serialization explicitly
            if transition.timestamp and isinstance(transition.timestamp, datetime):
                transition_dict["timestamp"] = transition.timestamp.isoformat()
                transition_dict["timestamp"] = convert_iso_to_unix_ms(transition.timestamp)
            elif transition.timestamp is None:
                # Set current time if not provided
                transition_dict["timestamp"] = int(datetime.now().timestamp() * 1000)
            
            return await self._send_with_retry(topic, transition_dict)
        except Exception as e:
            logger.error(f"Failed to process transition: {str(e)}", exc_info=True)
            raise KafkaError(f"Failed to process transition: {str(e)}")

    async def send_status_update(self, code: str, status: Dict[str, Any]):
        topic = f"{self.topic_prefix}_status"
        try:
            await self._send_with_retry(topic, {"code": code, **status})
            return {
                "status": "success",
                "message": f"Status update for {code} sent successfully"
            }
        except Exception as e:
            logger.error(f"Failed to send status update: {str(e)}", exc_info=True)
            raise KafkaError(f"Failed to send status update: {str(e)}")


class KafkaConfigConsumer:
    def __init__(
        self, 
        bootstrap_servers: List[str], 
        db_service: DatabaseService, 
        config_service,
        group_id: str = "gateway-group",
        topic: str = "bas_config_event",
        auto_reconnect: bool = True,
        reconnect_interval: int = 30  # Seconds
    ):
        self.bootstrap_servers = bootstrap_servers
        self.db_service = db_service
        self.config_service = config_service
        self.group_id = group_id
        self.topic = topic
        self.running = False
        self.auto_reconnect = auto_reconnect
        self.reconnect_interval = reconnect_interval
        self.consumer = None
        self.loop = None
        
    def _initialize_consumer(self):
        """Initialize the Kafka consumer with error handling"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                session_timeout_ms=30000,  # 30 seconds timeout
                request_timeout_ms=40000,  # 40 seconds timeout
                max_poll_interval_ms=300000  # 5 minutes max poll interval
            )
            logger.info(f"Kafka consumer initialized successfully for topic {self.topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {str(e)}", exc_info=True)
            return False
    
    def start(self):
        """Start the Kafka consumer in a background thread"""
        if not self._initialize_consumer():
            if self.auto_reconnect:
                logger.info(f"Scheduling consumer reconnect in {self.reconnect_interval} seconds")
                threading.Timer(self.reconnect_interval, self.start).start()
            return
        
        # Create a new event loop for async operations in the consumer thread
        self.loop = asyncio.new_event_loop()
        
        self.running = True
        threading.Thread(target=self._consume, daemon=True).start()
        logger.info(f"Kafka consumer started for topic {self.topic}")

    def _consume(self):
        """Consume messages from Kafka with error handling and reconnection"""
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        # Set the event loop for this thread
        asyncio.set_event_loop(self.loop)
        
        while self.running:
            try:
                # Reset error counter on successful iteration
                consecutive_errors = 0
                
                for message in self.consumer:
                    if not self.running:
                        break
                    
                    try:
                        config = message.value
                        logger.debug(f"Received config message: {config}")
                        
                        # Extract organization ID
                        if 'orgId' in config:
                            org_id = config['orgId']
                        elif 'org_id' in config:
                            org_id = config['org_id']
                        else:
                            logger.warning(f"Config message missing organization ID: {config}")
                            continue
                        
                        # Extract berth ID
                        if 'berthId' in config:
                            berth_id = config['berthId']
                        elif 'berth_id' in config:
                            berth_id = config['berth_id']
                        else:
                            logger.warning(f"Config message missing berth ID: {config}")
                            continue
                        
                        # Find the data app for this berth - run async code in the event loop
                        data_app_future = asyncio.run_coroutine_threadsafe(
                            self.db_service.get_data_app_by_berth(org_id, berth_id),
                            self.loop
                        )
                        
                        # Wait for the future to complete
                        data_app = data_app_future.result()
                        
                        if data_app:
                            self.config_service.add_config(data_app.code, config)
                            logger.info(f"Updated config for data app {data_app.code} at berth {org_id}_{berth_id}")
                        else:
                            logger.warning(f"No data app found for berth {org_id}_{berth_id}")
                            
                    except Exception as e:
                        logger.error(f"Error processing config message: {str(e)}", exc_info=True)
                        # Continue processing other messages
            
            except Exception as e:
                consecutive_errors += 1
                logger.error(
                    f"Error in Kafka consumer (attempt {consecutive_errors}/{max_consecutive_errors}): {str(e)}", 
                    exc_info=True
                )
                
                if not self.running:
                    break
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.critical(
                        f"Too many consecutive errors ({consecutive_errors}). "
                        f"Reinitializing consumer in {self.reconnect_interval} seconds."
                    )
                    # Close the current consumer
                    try:
                        self.consumer.close()
                    except:
                        pass
                    
                    # Wait before reconnecting
                    time.sleep(self.reconnect_interval)
                    
                    # Try to reinitialize
                    if not self._initialize_consumer():
                        logger.critical("Failed to reinitialize consumer, exiting consumer loop")
                        break
                    
                    # Reset error counter after reconnect
                    consecutive_errors = 0
                else:
                    # Short sleep to avoid tight loop on persistent errors
                    time.sleep(1)
        
        # Clean up when stopped
        try:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka consumer: {str(e)}")


if __name__ == "__main__":
    # Example of sending a transition message
    transition_data = {
        "dataAppCode": "N017C7",  # Data app code
        "berthId": 13,            # Berth ID
        "orgId": 17,              # Organization ID
        "fromState": "DEPARTING", 
        "toState": "AVAILABLE",   # State transitions: AVAILABLE -> BERTHING -> MOORING -> DEPARTING -> AVAILABLE
        "timestamp": int(datetime.now().timestamp() * 1000)  # Current time in milliseconds
    }

    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Create a test instance of KafkaService
    kafka_service = KafkaService(['localhost:29092'], None)
    
    # Send message to Kafka topic
    import asyncio
    
    async def test_send():
        topic = "bas_transition_event"
        try:
            result = await kafka_service._send_with_retry(topic, transition_data)
            print(f"Message sent successfully: {result}")
        except Exception as e:
            print(f"Failed to send message: {str(e)}")
    
    # Run the test
    asyncio.run(test_send())