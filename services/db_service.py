# services/db_service.py
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from contextlib import contextmanager, asynccontextmanager
from models.data_app import DataApp
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging
import asyncio
from utils.error_handler import DatabaseError, handle_exceptions

logger = logging.getLogger("bas_gateway.db")

class DatabaseService:
    def __init__(
        self, 
        connection_string: str,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 1800  # Recycle connections after 30 minutes
    ):
        self.connection_string = connection_string
        
        try:
            # Initialize sync engine for compatibility
            self.engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle
            )
            self.Session = scoped_session(sessionmaker(bind=self.engine))
            
            logger.info("Database connection initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}", exc_info=True)
            raise DatabaseError(f"Failed to initialize database connection: {str(e)}")
    
    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error: {str(e)}", exc_info=True)
            raise DatabaseError(f"Database error: {str(e)}")
        finally:
            session.close()

    def check_connection(self) -> bool:
        """Check if database connection is working"""
        try:
            with self.session_scope() as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database connection check failed: {str(e)}")
            return False

    @handle_exceptions
    async def get_data_app(self, code: str) -> Optional[DataApp]:
        """
        Get a data app by its code
        
        Args:
            code: The unique code of the data app
            
        Returns:
            DataApp object if found, None otherwise
        """
        return self.get_data_app_sync(code)
    
    @handle_exceptions
    async def get_data_app_by_berth(self, org_id: int, berth_id: int) -> Optional[DataApp]:
        """
        Get a data app by organization ID and berth ID
        
        Args:
            org_id: The organization ID
            berth_id: The berth ID
            
        Returns:
            DataApp object if found, None otherwise
        """
        return self.get_data_app_by_berth_sync(org_id, berth_id)
    
    @handle_exceptions
    async def get_all_data_apps(self) -> List[DataApp]:
        """
        Get all data apps
        
        Returns:
            List of DataApp objects
        """
        with self.session_scope() as session:
            data_apps = session.query(DataApp).all()
            # Detach from session
            session.expunge_all()
            return data_apps
    
    @handle_exceptions
    async def get_data_apps_by_berth_mapping(self) -> Dict[str, List[str]]:
        """
        Get a mapping of berth keys to data app codes
        
        Returns:
            Dictionary mapping "orgId_berthId" to a list of data app codes
        """
        with self.session_scope() as session:
            data_apps = session.query(DataApp).filter(DataApp.berthId.isnot(None)).all()
            
            berth_mappings = {}
            for app in data_apps:
                berth_key = f"{app.orgId}_{app.berthId}"
                if berth_key not in berth_mappings:
                    berth_mappings[berth_key] = []
                berth_mappings[berth_key].append(app.code)
            
            return berth_mappings

    @handle_exceptions
    async def update_data_app_heartbeat(self, code: str) -> None:
        """
        Update the last heartbeat timestamp for a data app
        
        Args:
            code: The unique code of the data app
        """
        self.update_data_app_heartbeat_sync(code)
    
    @handle_exceptions
    async def update_data_app_active(self, code: str, status: str) -> None:
        """
        Update the status and last active timestamp for a data app
        
        Args:
            code: The unique code of the data app
            status: The new status for the data app
        """
        self.update_data_app_active_sync(code, status)
    
    @handle_exceptions
    async def update_data_app_status(self, code: str, status: str) -> None:
        """
        Update the status for a data app
        
        Args:
            code: The unique code of the data app
            status: The new status for the data app
        """
        self.update_data_app_status_sync(code, status)
    
    @handle_exceptions
    async def update_inactive_data_apps(self, cutoff_time: datetime) -> List[str]:
        """
        Update status to DISCONNECTED for data apps that haven't sent data recently
        
        Args:
            cutoff_time: The cutoff time for considering a data app inactive
            
        Returns:
            List of codes for data apps that were updated
        """
        return self.update_inactive_data_apps_sync(cutoff_time)
    
    @handle_exceptions
    async def create_data_app(self, data_app: Dict[str, Any]) -> DataApp:
        """
        Create a new data app
        
        Args:
            data_app: Dictionary with data app properties
            
        Returns:
            The created DataApp object
        """
        with self.session_scope() as session:
            # Create DataApp object
            new_app = DataApp(
                code=data_app.get('code'),
                orgId=data_app.get('orgId'),
                berthId=data_app.get('berthId'),
                status=data_app.get('status', 'NORMAL'),
                displayName=data_app.get('displayName', f'App {data_app.get("code")}'),
                lastHeartbeat=datetime.now() - timedelta(hours=7),
                lastDataActive=datetime.now() - timedelta(hours=7),
                createdAt=datetime.now() - timedelta(hours=7)
            )
            
            # Add to session
            session.add(new_app)
            session.flush()
            
            # Detach from session
            session.expunge(new_app)
            
            logger.info(f"Created new data app with code {new_app.code}")
            return new_app
            
    # Synchronous methods
    
    def get_data_app_sync(self, code: str) -> Optional[DataApp]:
        """Get a data app by its code"""
        with self.session_scope() as session:
            data_app = session.query(DataApp).filter_by(code=code).first()
            if data_app:
                # Detach from session
                session.expunge(data_app)
            return data_app
    
    def get_data_app_by_berth_sync(self, org_id: int, berth_id: int) -> Optional[DataApp]:
        """Get a data app by organization ID and berth ID"""
        with self.session_scope() as session:
            data_app = session.query(DataApp).filter_by(
                orgId=org_id,
                berthId=berth_id
            ).first()
            if data_app:
                # Detach from session
                session.expunge(data_app)
            return data_app
    
    def update_data_app_heartbeat_sync(self, code: str) -> None:
        """Update the last heartbeat timestamp for a data app"""
        with self.session_scope() as session:
            now = datetime.now() - timedelta(hours=7)
            session.execute(
                text("UPDATE bas.\"DataApp\" SET \"lastHeartbeat\" = :now WHERE code = :code"),
                {"now": now, "code": code}
            )
            logger.debug(f"Updated heartbeat for data app {code}")

    def update_data_app_active_sync(self, code: str, status: str) -> None:
        """Update the status and last active timestamp for a data app"""
        with self.session_scope() as session:
            now = datetime.now() - timedelta(hours=7)
            session.execute(
                text("UPDATE bas.\"DataApp\" SET status = :status, \"lastDataActive\" = :now WHERE code = :code"),
                {"status": status, "now": now, "code": code}
            )
            logger.debug(f"Updated active status for data app {code} to {status}")
                
    def update_data_app_status_sync(self, code: str, status: str) -> None:
        """Update the status for a data app"""
        with self.session_scope() as session:
            session.execute(
                text("UPDATE bas.\"DataApp\" SET status = :status WHERE code = :code"),
                {"status": status, "code": code}
            )
            logger.debug(f"Updated status for data app {code} to {status}")
                
    def update_inactive_data_apps_sync(self, cutoff_time: datetime) -> List[str]:
        """Update status to DISCONNECTED for data apps that haven't sent data recently"""
        with self.session_scope() as session:
            query = text("""
                UPDATE bas."DataApp"
                SET status = 'DISCONNECTED'
                WHERE "lastDataActive" < :cutoff_time
                AND status != 'DISCONNECTED'
                RETURNING code
            """)
            
            result = session.execute(query, {"cutoff_time": cutoff_time})
            updated_codes = [row[0] for row in result]
            
            if updated_codes:
                logger.info(f"Updated status to DISCONNECTED for data apps: {updated_codes}")
            
            return updated_codes