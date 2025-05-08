# utils/error_handler.py
import logging
import traceback
import functools
import time
import uuid
import inspect
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from typing import Dict, Any, Callable, Optional, Union, Awaitable

# Configure logging
logger = logging.getLogger("bas_gateway")

# Create response type variable
T = TypeVar('T')

class BaseError(Exception):
    """Base class for all application exceptions"""
    status_code: int = 500
    error_code: str = "INTERNAL_ERROR"
    
    def __init__(self, message: str = "An unexpected error occurred", details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

class ConfigurationError(BaseError):
    """Error related to configuration handling"""
    status_code = 400
    error_code = "CONFIGURATION_ERROR"

class DatabaseError(BaseError):
    """Error related to database operations"""
    status_code = 500
    error_code = "DATABASE_ERROR"

class KafkaError(BaseError):
    """Error related to Kafka operations"""
    status_code = 503
    error_code = "KAFKA_ERROR"

class ValidationError(BaseError):
    """Error related to data validation"""
    status_code = 400
    error_code = "VALIDATION_ERROR"

class NotFoundError(BaseError):
    """Error when a resource is not found"""
    status_code = 404
    error_code = "NOT_FOUND"

class RequestContext:
    """Context for tracking request information"""
    _data = {}
    
    @classmethod
    def init_request(cls, request_id: Optional[str] = None):
        """Initialize the request context with a new request ID"""
        if request_id is None:
            request_id = str(uuid.uuid4())
        cls._data["request_id"] = request_id
        cls._data["start_time"] = time.time()
        return request_id
    
    @classmethod
    def get_request_id(cls) -> Optional[str]:
        """Get the current request ID"""
        return cls._data.get("request_id")
    
    @classmethod
    def get_elapsed_time(cls) -> Optional[float]:
        """Get the elapsed time since the request started"""
        start_time = cls._data.get("start_time")
        if start_time is not None:
            return time.time() - start_time
        return None
    
    @classmethod
    def clear(cls):
        """Clear the request context"""
        cls._data = {}

def handle_exceptions(func: Callable[..., Union[T, Awaitable[T]]]) -> Callable[..., Union[T, Awaitable[T]]]:
    """
    Decorator for handling exceptions in service and endpoint methods.
    Works with both synchronous and asynchronous functions.
    """
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs) -> T:
        try:
            result = func(*args, **kwargs)
            # Check if the result is a coroutine (for async functions)
            if inspect.iscoroutine(result):
                # Await the coroutine
                return await result
            return result
        except BaseError as e:
            # Log the error with the request ID
            logger.error(
                f"Error [{e.error_code}] - Request ID: {RequestContext.get_request_id()}: {e.message}", 
                extra={"details": e.details}
            )
            # Re-raise to be handled by the FastAPI exception handler
            raise
        except Exception as e:
            # Convert to internal error and log
            logger.error(
                f"Unexpected error - Request ID: {RequestContext.get_request_id()}: {str(e)}", 
                exc_info=True
            )
            # Re-raise as a BaseError
            internal_error = BaseError(message=str(e))
            internal_error.details = {"traceback": traceback.format_exc()}
            raise internal_error
    
    # Choose the appropriate wrapper based on whether the function is async or not
    if inspect.iscoroutinefunction(func):
        return async_wrapper
    return async_wrapper  # For simplicity, we'll use the async wrapper for all functions

async def request_middleware(request: Request, call_next: Callable):
    """Middleware for request handling and logging"""
    # Set up the request context
    request_id = request.headers.get("X-Request-ID")
    request_id = RequestContext.init_request(request_id)
    
    # Log the request
    logger.info(f"Request {request.method} {request.url.path} - ID: {request_id}")
    
    try:
        # Process the request
        response = await call_next(request)
        
        # Log the response
        logger.info(f"Response {response.status_code} - Time: {RequestContext.get_elapsed_time():.3f}s - ID: {request_id}")
        
        # Add request ID to response headers
        response.headers["X-Request-ID"] = request_id
        
        return response
    except Exception as exc:
        # Log the error
        logger.error(f"Request failed - ID: {request_id}", exc_info=True)
        raise exc
    finally:
        # Clear the request context
        RequestContext.clear()

async def exception_handler(request: Request, exc: BaseError):
    """Handle application exceptions and convert to JSON response"""
    status_code = getattr(exc, "status_code", 500)
    error_code = getattr(exc, "error_code", "INTERNAL_ERROR")
    
    response = {
        "error": True,
        "code": error_code,
        "message": str(exc),
        "request_id": RequestContext.get_request_id(),
    }
    
    # Add details if available
    if hasattr(exc, "details") and exc.details:
        response["details"] = exc.details
    
    return JSONResponse(status_code=status_code, content=response)

def register_exception_handlers(app):
    """Register exception handlers with the FastAPI app"""
    # Register handlers for specific exception types
    app.add_exception_handler(BaseError, exception_handler)
    app.add_exception_handler(ConfigurationError, exception_handler)
    app.add_exception_handler(DatabaseError, exception_handler)
    app.add_exception_handler(KafkaError, exception_handler)
    app.add_exception_handler(ValidationError, exception_handler)
    app.add_exception_handler(NotFoundError, exception_handler)
    
    # Register handler for HTTPException
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": True,
                "code": "HTTP_ERROR",
                "message": exc.detail,
                "request_id": RequestContext.get_request_id(),
            },
        )
    
    # Register handler for generic exceptions
    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception):
        logger.error(f"Unhandled exception - ID: {RequestContext.get_request_id()}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "error": True,
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred",
                "request_id": RequestContext.get_request_id(),
            },
        )