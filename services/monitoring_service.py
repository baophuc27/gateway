# services/monitoring_service.py
from datetime import datetime, timedelta
from services.db_service import DatabaseService
from services.config_service import ConfigService
import logging
import asyncio

logger = logging.getLogger("bas_gateway.monitoring")

class MonitoringService:
    def __init__(self, db_service: DatabaseService, config_service: ConfigService):
        self.db_service = db_service
        self.config_service = config_service
        self._event_loop = None

    @property
    def event_loop(self):
        """Get the current event loop or create a new one if needed"""
        if self._event_loop is None or self._event_loop.is_closed():
            try:
                self._event_loop = asyncio.get_event_loop()
            except RuntimeError:
                # If we're not in an event loop (e.g., in a thread), create a new one
                self._event_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._event_loop)
        return self._event_loop

    def check_inactive_data_apps(self):
        """
        Check for data apps that haven't sent data in the last 5 minutes
        and update their status to DISCONNECTED
        """
        try:
            # Calculate cutoff time (5 minutes ago)
            five_mins_ago = datetime.now() - timedelta(hours=7) - timedelta(minutes=5)
            logger.debug(f"Checking for inactive data apps (cutoff: {five_mins_ago})")
            
            # Run the update synchronously since this is called from the scheduler
            updated_codes = self.db_service.update_inactive_data_apps_sync(five_mins_ago)
            
            if updated_codes:
                logger.info(f"Updated status to DISCONNECTED for {len(updated_codes)} data apps: {updated_codes}")
        except Exception as e:
            logger.error(f"Error checking inactive data apps: {str(e)}", exc_info=True)

    def sync_berth_configs(self):
        """
        Query all data apps and their berth assignments, updating config cache mappings
        """
        try:
            # Use run_sync to safely run async code from a sync context
            berth_mappings = self.run_sync(self.db_service.get_data_apps_by_berth_mapping())
            
            # Update config cache with new mappings
            self.config_service.update_berth_mappings(berth_mappings)
            
            logger.debug(f"Updated berth-to-code mappings with {len(berth_mappings)} berths")
        except Exception as e:
            logger.error(f"Error syncing berth configs: {str(e)}", exc_info=True)
            
    def run_sync(self, coroutine):
        """
        Run a coroutine synchronously, handling the event loop appropriately
        """
        loop = self.event_loop
        
        if loop.is_running():
            # If the loop is already running (e.g., inside the application),
            # use run_coroutine_threadsafe
            future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            return future.result()
        else:
            # Otherwise, run the coroutine directly
            return loop.run_until_complete(coroutine)