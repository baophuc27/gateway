# services/health_service.py
from fastapi import HTTPException
from services.db_service import DatabaseService
import logging

logger = logging.getLogger("bas_gateway.health")

class HealthService:
    def __init__(self, db_service: DatabaseService):
        self.db_service = db_service

    async def update_heartbeat(self, code: str) -> str:
        """
        Update the heartbeat timestamp for a data app
        
        Args:
            code: The unique code of the data app
            
        Returns:
            The current status of the data app
        """
        data_app = await self.db_service.get_data_app(code)
        if not data_app:
            logger.warning(f"Data app not found for code: {code}")
            raise HTTPException(status_code=404, detail="Data app not found")
        
        await self.db_service.update_data_app_heartbeat(code)
        logger.debug(f"Updated heartbeat for data app {code}")
        
        return data_app.status

    async def update_active(self, code: str) -> str:
        """
        Update the active status and timestamp for a data app
        
        Args:
            code: The unique code of the data app
            
        Returns:
            The new status of the data app
        """
        data_app = await self.db_service.get_data_app(code)
        if not data_app:
            logger.warning(f"Data app not found for code: {code}")
            raise HTTPException(status_code=404, detail="Data app not found")

        status = 'NORMAL'
        
        await self.db_service.update_data_app_active(code, status)
        await self.db_service.update_data_app_heartbeat(code)
        
        logger.debug(f"Updated active status for data app {code} to {status}")
        return status