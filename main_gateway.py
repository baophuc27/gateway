# main_gateway.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Depends
from models.data_app import DataAppHeartbeat, HeartbeatRequest
from models.data_record import MessageRequest
from models.transition import TransitionRequest
from services.config_service import ConfigService
from services.kafka_service import KafkaService, KafkaConfigConsumer
from services.db_service import DatabaseService
from services.health_service import HealthService
from services.monitoring_service import MonitoringService
from utils.error_handler import (
    register_exception_handlers, 
    request_middleware, 
    handle_exceptions,
    NotFoundError, 
    KafkaError,
    ValidationError
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import time
import os
from typing import Optional
from datetime import datetime
import logging

# Configure logging
logger = logging.getLogger("bas_gateway")

def convert_iso_to_unix_ms(iso_timestamp):
    """
    Convert ISO format timestamp to Unix timestamp in milliseconds
    Example: "2025-04-29T04:51:08.699915+00:00" -> 1745909065078
    """
    # Parse the ISO timestamp
    dt = datetime.fromisoformat(iso_timestamp)
    
    # Convert to Unix timestamp (seconds since epoch) and multiply by 1000 for milliseconds
    unix_ms = int(dt.timestamp() * 1000)
    
    return unix_ms

# Get configuration from environment variables with defaults
DB_CONNECTION = os.getenv("DATABASE_URL", "postgresql://root:rootReccotech@localhost:56432/bas_db")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:29092").split(",")
CONFIG_DIR = os.getenv("CONFIG_DIR", "data_app_config")
HEARTBEAT_CHECK_INTERVAL = int(os.getenv("HEARTBEAT_CHECK_INTERVAL", "10"))
CONFIG_SYNC_INTERVAL = int(os.getenv("CONFIG_SYNC_INTERVAL", "5"))

# Initialize services
config_service = ConfigService(CONFIG_DIR)
db_service = DatabaseService(DB_CONNECTION)
health_service = HealthService(db_service)
kafka_service = KafkaService(KAFKA_BROKERS, db_service)
monitoring_service = MonitoringService(db_service, config_service)
scheduler = AsyncIOScheduler()

# Initialize Kafka consumer
consumer = KafkaConfigConsumer(
    KAFKA_BROKERS, 
    db_service,
    config_service
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: start the scheduler and Kafka consumer
    logger.info("Starting BAS Gateway Service")
    
    try:
        scheduler.add_job(
            monitoring_service.check_inactive_data_apps,
            'interval',
            seconds=HEARTBEAT_CHECK_INTERVAL,  
            id='monitor_data_apps'
        )
        scheduler.add_job(
            monitoring_service.sync_berth_configs,
            'interval',
            seconds=CONFIG_SYNC_INTERVAL,
            id='sync_berth_configs'
        )
        
        logger.info("Starting scheduler")
        scheduler.start()
        
        logger.info("Starting Kafka consumer")
        consumer.start()
        
        logger.info("BAS Gateway Service started successfully")
        yield
    
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}", exc_info=True)
        raise
    finally:
        # Shutdown: clean up resources
        logger.info("Shutting down BAS Gateway Service")
        scheduler.shutdown()
        consumer.running = False  # Stop the Kafka consumer
        logger.info("BAS Gateway Service shutdown complete")

app = FastAPI(
    title="BAS Gateway Service", 
    description="API Gateway for Berth Assignment System",
    version="1.0.0",
    lifespan=lifespan
)

# Register middleware and exception handlers
app.middleware("http")(request_middleware)
register_exception_handlers(app)

# Add health check endpoint
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "version": "1.0.0"
    }

@app.get("/data-app/heartbeat")
@handle_exceptions
async def data_app_heartbeat(
    code: str,
    last_timestamp: Optional[float] = 0
):
    try:
        status = await health_service.update_heartbeat(code)
        pending_configs = config_service.get_pending_configs(code, last_timestamp)
        
        return {
            "status": status, 
            "config_updates": pending_configs,
            "timestamp": time.time()
        }
    except HTTPException as e:
        if e.status_code == 404:
            raise NotFoundError(f"Data app with code {code} not found")
        raise

@app.post("/data-app/sensor-data")
@handle_exceptions
async def receive_sensor_data(data: MessageRequest):
    topic, message, code = data.topic, data.message, data.code
    
    if code not in config_service.config_cache:
        raise ValidationError(f"Data app with code {code} not configured")
    
    await health_service.update_active(code)
    
    try:
        result = await kafka_service.send_sensor_data(topic, message)
        return result
    except Exception as e:
        raise KafkaError(f"Failed to send sensor data: {str(e)}")

@app.post("/data-app/transition/{code}")
@handle_exceptions
async def receive_vessel_transition(code: str, transition: TransitionRequest):
    logger.info(f"Received transition request for {code}: {transition.fromState} -> {transition.toState}")
    
    # Validate the data app code
    if code != transition.dataAppCode:
        raise ValidationError(
            f"Code mismatch: URL code '{code}' doesn't match payload code '{transition.dataAppCode}'",
            details={"url_code": code, "payload_code": transition.dataAppCode}
        )
    
    # Process and store the transition
    try:
        result = await kafka_service.process_transition(transition)
        
        return {
            "status": "success",
            "message": f"Transition from {transition.fromState} to {transition.toState} recorded",
            "details": result
        }
    except Exception as e:
        raise KafkaError(f"Failed to process transition: {str(e)}")

@app.post("/data-app/config/{code}")
@handle_exceptions
async def update_data_app_config(code: str, config: dict):
    # Validate data app
    data_app = await db_service.get_data_app(code)
    if not data_app:
        raise NotFoundError(f"Data app with code {code} not found")
    
    config_service.add_config(code, config)
    
    return {
        "status": "success",
        "message": f"Configuration updated for data app {code}",
        "timestamp": time.time()
    }

@app.get("/data-app/config/{code}")
@handle_exceptions
async def get_data_app_config(code: str):
    try:
        await health_service.update_heartbeat(code)
    except HTTPException:
        # Ignore heartbeat errors when just fetching config
        pass
    
    config = config_service.get_config(code)
    
    if config is None:
        raise NotFoundError(f"Configuration not found for code: {code}")
    
    return config

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "22222"))
    uvicorn.run(app, host="0.0.0.0", port=port)