# BAS Gateway Service

A microservice that acts as a central gateway for the Berth Assignment System (BAS), managing data app configurations, sensor data, and vessel transitions.

## Features

- **Data App Management**: Register and monitor data collection applications at berths
- **Configuration Distribution**: Centralized configuration management with automatic propagation
- **Sensor Data Handling**: Process and forward sensor data to Kafka for real-time analytics
- **Vessel State Transitions**: Track vessel states through their lifecycle at berths
- **Health Monitoring**: Automatically detect and report disconnected data apps
- **Berth Configuration Sync**: Ensure all data apps at the same berth share consistent configurations

## Architecture

The BAS Gateway Service follows a microservice architecture with these components:

- **API Endpoints**: FastAPI-based REST API for client interactions
- **Service Layer**: Business logic encapsulated in service classes
- **Data Access Layer**: Database interactions via SQLAlchemy
- **Messaging Layer**: Kafka integration for event-driven communication
- **Configuration Management**: File-based configuration with in-memory cache
- **Monitoring**: Scheduled tasks for system health and synchronization

## Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL 13+
- Kafka 2.8+
- Docker (optional, for containerized deployment)

### Configuration

The service is configured through environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://root:rootReccotech@localhost:56432/bas_db` |
| `KAFKA_BROKERS` | Kafka broker list | `localhost:29092` |
| `CONFIG_DIR` | Directory for data app configs | `data_app_config` |
| `PORT` | API server port | `22222` |
| `HEARTBEAT_CHECK_INTERVAL` | Interval for monitoring disconnected apps (seconds) | `10` |
| `CONFIG_SYNC_INTERVAL` | Interval for syncing berth configs (seconds) | `5` |
| `LOG_LEVEL` | Logging level | `INFO` |

### Running the Service

Start the service:

```bash
python main_gateway.py
```

For development with auto-reload:

```bash
uvicorn main_gateway:app --reload --port 22222
```

## API Documentation

Once the service is running, access the API documentation at:

```
http://localhost:22222/docs
```

### Key Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Service health check |
| `/data-app/heartbeat` | GET | Data app heartbeat and config sync |
| `/data-app/sensor-data` | POST | Receive sensor data from data apps |
| `/data-app/transition/{code}` | POST | Process vessel state transitions |
| `/data-app/config/{code}` | GET/POST | Retrieve or update data app config |

## Deployment

### Docker Compose

For a complete development environment with Kafka and PostgreSQL:

```bash
docker-compose up -d
```

## Architecture Diagram

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Data App 1 │     │  Data App 2 │     │  Data App n │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       │                   │                   │
       │                   ▼                   │
       │           ┌─────────────┐            │
       └──────────►│BAS Gateway  │◄────────────┘
                   │   Service   │
                   └──────┬──────┘
                          │
          ┌───────────────┼───────────────┐
          │               │               │
          ▼               ▼               ▼
   ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
   │  Database   │ │    Kafka    │ │Config Files │
   └─────────────┘ └─────────────┘ └─────────────┘
```

## Development

### Project Structure

```
/
├── data_app_config/     # Data app configuration files
├── models/              # Data models and schemas
├── services/            # Business logic services
├── utils/               # Utility modules
├── main_gateway.py      # Application entry point
├── requirements.txt     # Dependencies
├── Dockerfile           # Docker build file
└── docker-compose.yml   # Development environment
```
