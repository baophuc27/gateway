# services/monitoring_service.py
from datetime import datetime, timedelta
from services.db_service import DatabaseService
from services.config_service import ConfigService
import logging
import asyncio
import concurrent.futures

logger = logging.getLogger("bas_gateway.monitoring")

class MonitoringService:
    def __init__(self, db_service: DatabaseService, config_service: ConfigService):
        self.db_service = db_service
        self.config_service = config_service
        # Create a thread pool for running sync tasks
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        # Store the main event loop during initialization
        try:
            self.main_event_loop = asyncio.get_event_loop()
        except RuntimeError:
            # Create a new event loop if one doesn't exist
            self.main_event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.main_event_loop)

    def check_inactive_data_apps(self):
        """
        Check for data apps that haven't sent data in the last 5 minutes
        and update their status to DISCONNECTED
        """
        try:
            # Calculate cutoff time (5 minutes ago)
            five_mins_ago = datetime.now() - timedelta(hours=7) - timedelta(minutes=5)
            logger.debug(f"Checking for inactive data apps (cutoff: {five_mins_ago})")
            
            # Use the sync version directly since this is a scheduled task
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
            # Create a new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Run the coroutine in this new event loop
            berth_mappings = loop.run_until_complete(self.db_service.get_data_apps_by_berth_mapping())
            
            # Check if update_berth_mappings is an async function
            if asyncio.iscoroutinefunction(self.config_service.update_berth_mappings):
                # If it's async, run it in this thread's event loop
                loop.run_until_complete(self.config_service.update_berth_mappings(berth_mappings))
            else:
                # If it's not async, call it directly
                self.config_service.update_berth_mappings(berth_mappings)
            
            logger.debug(f"Updated berth-to-code mappings with {len(berth_mappings)} berths")
            
            # Clean up
            loop.close()
        except Exception as e:
            logger.error(f"Error syncing berth configs: {str(e)}", exc_info=True)
            
    def run_in_executor(self, func, *args):
        """
        Run a synchronous function in a thread pool
        """
        return self.executor.submit(func, *args).result()