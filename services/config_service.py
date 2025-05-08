# services/config_service.py
import json
import os
import time
import logging
import threading
from typing import Dict, Optional, List, Any
from models.data_app import ConfigUpdate
from utils.error_handler import ConfigurationError, handle_exceptions

logger = logging.getLogger("bas_gateway.config")

class ConfigService:
    def __init__(self, config_dir: str = "data_app_config"):
        self.config_dir = config_dir
        self.config_cache: Dict[str, ConfigUpdate] = {}
        self.berth_mappings: Dict[str, List[str]] = {}  # Maps "orgId_berthId" to list of data app codes
        self.config_lock = threading.RLock()  # Reentrant lock for thread safety
        
        # Create config directory if it doesn't exist
        os.makedirs(config_dir, exist_ok=True)
        
        # Load existing configurations
        self._load_configs()
        logger.info(f"Initialized ConfigService with {len(self.config_cache)} configurations")
    
    @handle_exceptions
    def _load_configs(self) -> None:
        """
        Load all configuration files from the config directory
        """
        with self.config_lock:
            count = 0
            for filename in os.listdir(self.config_dir):
                if filename.endswith('.json'):
                    try:
                        code = filename.replace('.json', '')
                        filepath = os.path.join(self.config_dir, filename)
                        
                        with open(filepath) as f:
                            config = json.load(f)
                            
                            # Validate config structure
                            if not isinstance(config, dict) or not all(k in config for k in ["config", "timestamp", "version"]):
                                logger.warning(f"Invalid config structure in {filename}, skipping")
                                continue
                                
                            # Load the config
                            self.config_cache[code] = ConfigUpdate(**config)
                            count += 1
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse JSON in {filename}, skipping")
                    except Exception as e:
                        logger.error(f"Error loading config for {filename}: {str(e)}", exc_info=True)
            
            logger.info(f"Loaded {count} configurations from {self.config_dir}")

    @handle_exceptions
    def get_pending_configs(self, code: str, last_timestamp: float) -> Optional[Dict]:
        """
        Get pending configuration updates for a data app
        
        Args:
            code: The unique code of the data app
            last_timestamp: The timestamp of the last configuration update received by the data app
            
        Returns:
            The configuration if there's a newer version, None otherwise
        """
        with self.config_lock:
            if code not in self.config_cache:
                return None
                
            config = self.config_cache[code]
            return config.config if config.timestamp > last_timestamp else None

    @handle_exceptions
    def get_config(self, code: str) -> Optional[Dict]:
        """
        Get the current configuration for a data app
        
        Args:
            code: The unique code of the data app
            
        Returns:
            The configuration if it exists, None otherwise
        """
        with self.config_lock:
            if code not in self.config_cache:
                return None
                
            config = self.config_cache[code]
            
            # Normalize naming conventions if needed
            result = config.config.copy()
            if 'orgId' not in result and 'org_id' in result:
                result['orgId'] = result['org_id']
            if 'berthId' not in result and 'berth_id' in result:
                result['berthId'] = result['berth_id']
                
            return result

    @handle_exceptions
    def add_config(self, code: str, config: Dict) -> ConfigUpdate:
        """
        Add or update a configuration for a data app
        
        Args:
            code: The unique code of the data app
            config: The configuration to add or update
            
        Returns:
            The updated configuration
        """
        with self.config_lock:
            # Generate version and timestamp
            version = 1
            if code in self.config_cache:
                version = self.config_cache[code].version + 1
            
            # Create the update
            update = ConfigUpdate(
                config=config,
                timestamp=time.time(),
                version=version
            )
            
            # Update the cache
            self.config_cache[code] = update
            
            # Save to disk
            self._save_config(code)
            
            logger.info(f"Added/updated config for {code} (version {version})")
            
            # Propagate to other data apps at the same berth if applicable
            self._propagate_config(code, config)
            
            return update

    @handle_exceptions
    def _propagate_config(self, source_code: str, config: Dict) -> None:
        """
        Propagate a configuration to other data apps at the same berth
        
        Args:
            source_code: The code of the data app that received the update
            config: The configuration to propagate
        """
        # Extract berth information
        org_id = config.get('orgId') or config.get('org_id')
        berth_id = config.get('berthId') or config.get('berth_id')
        
        if org_id is None or berth_id is None:
            logger.debug(f"Config for {source_code} doesn't have org_id or berth_id, not propagating")
            return
            
        # Create berth key
        berth_key = f"{org_id}_{berth_id}"
        
        # Check if we have mappings for this berth
        if berth_key in self.berth_mappings:
            propagated = 0
            
            # Propagate to all data apps at this berth
            for other_code in self.berth_mappings[berth_key]:
                if other_code != source_code:  # Don't update the source
                    try:
                        self.add_config(other_code, config)
                        propagated += 1
                    except Exception as e:
                        logger.error(f"Failed to propagate config from {source_code} to {other_code}: {str(e)}")
            
            if propagated > 0:
                logger.info(f"Propagated config from {source_code} to {propagated} other data apps at berth {berth_key}")

    @handle_exceptions
    def update_berth_mappings(self, mappings: Dict[str, List[str]]) -> None:
        """
        Update the berth-to-data-app mappings and synchronize configurations
        
        Args:
            mappings: Dictionary mapping "orgId_berthId" to list of data app codes
        """
        with self.config_lock:
            # Get current codes with configurations
            all_codes = set(self.config_cache.keys())
            
            # Get all codes that now belong to berths
            codes_with_berths = set()
            for codes in mappings.values():
                codes_with_berths.update(codes)
                
            # Identify codes to remove (no longer associated with any berth)
            codes_to_remove = all_codes - codes_with_berths
            if codes_to_remove:
                logger.info(f"Removing configurations for {len(codes_to_remove)} data apps no longer at any berth")
            
            # Update berth mappings
            self.berth_mappings = mappings
            
            # For each berth, find the latest config and propagate it
            for berth_key, codes in mappings.items():
                latest_config = None
                latest_timestamp = 0
                latest_code = None
                
                # Find the most recent config for this berth
                for code in all_codes:
                    if code in self.config_cache:
                        config = self.config_cache[code]
                        config_org_id = config.config.get('orgId') or config.config.get('org_id')
                        config_berth_id = config.config.get('berthId') or config.config.get('berth_id')
                        
                        if config_org_id is not None and config_berth_id is not None:
                            key_from_config = f"{config_org_id}_{config_berth_id}"
                            
                            if berth_key == key_from_config and config.timestamp > latest_timestamp:
                                latest_config = config.config
                                latest_timestamp = config.timestamp
                                latest_code = code
                
                # If we found a config, ensure all codes at this berth have it
                if latest_config and latest_code:
                    logger.debug(f"Found latest config from {latest_code} for berth {berth_key}")
                    
                    for code in codes:
                        if code == latest_code:
                            # Skip the source code
                            continue
                            
                        if code not in self.config_cache:
                            # Code doesn't have a config yet, add it
                            self.add_config(code, latest_config)
                            logger.info(f"Added config for new code {code} at berth {berth_key}")
                        else:
                            current_config = self.config_cache[code]
                            if current_config.timestamp < latest_timestamp:
                                # Code has older config, update it
                                self.add_config(code, latest_config)
                                logger.info(f"Updated config for {code} with latest berth config")
            
            # Remove configurations for codes no longer at any berth
            for code in codes_to_remove:
                self._remove_config(code)

    @handle_exceptions
    def _save_config(self, code: str) -> None:
        """
        Save a configuration to disk
        
        Args:
            code: The unique code of the data app
        """
        try:
            filepath = os.path.join(self.config_dir, f'{code}.json')
            with open(filepath, 'w') as f:
                if hasattr(self.config_cache[code], "dict"):
                    # Pydantic v1
                    json.dump(self.config_cache[code].dict(), f, indent=2)
                else:
                    # Pydantic v2
                    json.dump(self.config_cache[code].model_dump(), f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save config for {code}: {str(e)}", exc_info=True)
            raise ConfigurationError(f"Failed to save configuration for {code}: {str(e)}")

    @handle_exceptions
    def _remove_config(self, code: str) -> None:
        """
        Remove a configuration from cache and disk
        
        Args:
            code: The unique code of the data app
        """
        try:
            # Remove from cache
            if code in self.config_cache:
                del self.config_cache[code]
            
            # Remove from disk
            filepath = os.path.join(self.config_dir, f'{code}.json')
            if os.path.exists(filepath):
                os.remove(filepath)
                
            logger.info(f"Removed config for code {code}")
        except Exception as e:
            logger.error(f"Failed to remove config for {code}: {str(e)}", exc_info=True)

    @handle_exceptions
    def get_config_version(self, code: str) -> Optional[int]:
        """
        Get the current version of a configuration
        
        Args:
            code: The unique code of the data app
            
        Returns:
            The version number if the configuration exists, None otherwise
        """
        with self.config_lock:
            if code not in self.config_cache:
                return None
            return self.config_cache[code].version

    @handle_exceptions
    def get_config_history(self, code: str) -> List[Dict[str, Any]]:
        """
        This is a placeholder for future implementation of config history
        
        Args:
            code: The unique code of the data app
            
        Returns:
            List of historical configurations (currently just returns the current one)
        """
        with self.config_lock:
            if code not in self.config_cache:
                return []
                
            # In a real implementation, this would retrieve historical versions
            # For now, just return the current version
            config = self.config_cache[code]
            return [{
                "version": config.version,
                "timestamp": config.timestamp,
                "config": config.config
            }]