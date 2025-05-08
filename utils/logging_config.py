# utils/logging_config.py
import logging
import logging.config
import os
from pathlib import Path
import time
import json
from datetime import datetime

# Create logs directory if it doesn't exist
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Get log level from environment variable
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


class JSONFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings after parsing the log record.
    """
    def __init__(self, **kwargs):
        self.json_fields = kwargs.pop("json_fields", [])
        self.timestamp_field = kwargs.pop("timestamp_field", "timestamp")
        super().__init__(**kwargs)

    def format(self, record):
        log_record = {}
        
        # Add timestamps
        now = datetime.utcnow()
        log_record[self.timestamp_field] = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        # Add log metadata
        log_record["level"] = record.levelname
        log_record["logger"] = record.name
        log_record["module"] = record.module
        log_record["thread_id"] = record.thread
        log_record["process_id"] = record.process
        
        # Add formatted message
        log_record["message"] = super().format(record)
        
        # Add exceptions
        if record.exc_info:
            log_record["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info)
            }
        
        # Add extra fields
        for field in self.json_fields:
            if hasattr(record, field):
                log_record[field] = getattr(record, field)
                
        # Add extra dict
        if hasattr(record, "extra"):
            for key, value in record.extra.items():
                log_record[key] = value
                
        # Ensure all JSON values are serializable
        return json.dumps(sanitize_log_record(log_record))


def sanitize_log_record(record):
    """
    Ensure all values in the record are JSON serializable
    """
    for key, value in list(record.items()):
        if isinstance(value, (datetime, time.struct_time)):
            record[key] = value.isoformat() if hasattr(value, 'isoformat') else str(value)
        elif isinstance(value, Exception):
            record[key] = str(value)
        elif not isinstance(value, (str, int, float, bool, list, dict, type(None))):
            record[key] = str(value)
        elif isinstance(value, dict):
            record[key] = sanitize_log_record(value)
        elif isinstance(value, list):
            record[key] = [
                item.isoformat() if hasattr(item, 'isoformat')
                else str(item) if not isinstance(item, (str, int, float, bool, list, dict, type(None)))
                else item
                for item in value
            ]
    
    return record


def configure_logging():
    """
    Configure logging with console and file handlers
    """
    # Define logging configuration
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
            "json": {
                "()": JSONFormatter,
                "json_fields": ["request_id", "method", "path", "status_code", "elapsed_time", "details"]
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": LOG_LEVEL,
                "formatter": "standard",
                "stream": "ext://sys.stdout"
            },
            "file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "level": LOG_LEVEL,
                "formatter": "json",
                "filename": os.path.join(LOGS_DIR, "bas-gateway.log"),
                "when": "midnight",
                "interval": 1,
                "backupCount": 30,
                "encoding": "utf-8"
            },
            "error_file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "level": "ERROR",
                "formatter": "json",
                "filename": os.path.join(LOGS_DIR, "error.log"),
                "when": "midnight",
                "interval": 1,
                "backupCount": 30,
                "encoding": "utf-8"
            }
        },
        "loggers": {
            "": {  # Root logger
                "handlers": ["console", "file", "error_file"],
                "level": LOG_LEVEL,
                "propagate": True
            },
            "bas_gateway": {
                "handlers": ["console", "file", "error_file"],
                "level": LOG_LEVEL,
                "propagate": False
            },
            "uvicorn": {
                "handlers": ["console", "file"],
                "level": LOG_LEVEL,
                "propagate": False
            },
            "uvicorn.access": {
                "handlers": ["console", "file"],
                "level": LOG_LEVEL,
                "propagate": False
            }
        }
    }
    
    # Apply configuration
    logging.config.dictConfig(logging_config)
    
    # Return root logger
    return logging.getLogger("bas_gateway")