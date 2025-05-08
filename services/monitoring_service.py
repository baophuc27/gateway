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
            # Get mapping of berth keys to data app codes - must run in event loop
            berth_mappings_future = asyncio.run_coroutine_threadsafe(
                self.db_service.get_data_apps_by_berth_mapping(),
                asyncio.get_event_loop()
            )
            berth_mappings = berth_mappings_future.result()
            
            # Update config cache with new mappings
            self.config_service.update_berth_mappings(berth_mappings)
            
            logger.debug(f"Updated berth-to-code mappings with {len(berth_mappings)} berths")
        except Exception as e:
            logger.error(f"Error syncing berth configs: {str(e)}", exc_info=True)