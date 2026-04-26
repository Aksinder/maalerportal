"""History and statistics sensors for Målerportal integration."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import logging
import re
from typing import Any, Optional

import aiohttp
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from homeassistant.components.recorder.models import StatisticData

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorStateClass,
)
from homeassistant.const import (
    UnitOfEnergy,
    UnitOfVolume,
)
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.restore_state import RestoreEntity

from ..const import DOMAIN
from ..coordinator import MaalerportalCoordinator
from ..reconcile import compute_swap_offset
from .base import MaalerportalPollingSensor

_LOGGER = logging.getLogger(__name__)


class MaalerportalConsumptionSensor(MaalerportalPollingSensor, RestoreEntity):
    """Consumption sensor that shows virtual cumulative meter reading.
    
    For consumption-type meters (readingType=consumption), this sensor tracks
    a virtual cumulative meter reading by summing all consumption values.
    This is useful for display purposes while the StatisticSensor handles
    Energy Dashboard integration.
    """

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the consumption sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        counter_type = counter.get("counterType", "")
        is_primary = counter.get("isPrimary", False)
        
        # Get installation type to distinguish hot vs cold water
        # (counter_type may always be "ColdWater" for water meters)
        installation_type = installation.get("installationType", "").lower()
        
        # Set translation key based on counter type - these show virtual meter reading
        if counter_type == "ColdWater" or counter_type == "HotWater":
            # Use installation type to determine if it's hot or cold water
            if installation_type == "hotwater":
                self._attr_translation_key = "virtual_hot_water_meter"
            else:
                self._attr_translation_key = "virtual_cold_water_meter"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
        elif counter_type == "ElectricityFromGrid":
            self._attr_translation_key = "virtual_electricity_meter"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        elif counter_type == "ElectricityToGrid":
            self._attr_translation_key = "virtual_electricity_export_meter"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        elif counter_type == "Heat":
            self._attr_translation_key = "virtual_heat_meter"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        else:
            self._attr_translation_key = "virtual_meter"
            self._attr_device_class = None
            self._attr_native_unit_of_measurement = counter.get("unit")
        
        # Create unique ID based on counter type and primary/secondary status
        suffix = "primary" if is_primary else "secondary"
        self._attr_unique_id = f"{self._installation_id}_{counter_type.lower()}_consumption_{suffix}"
        
        # Use TOTAL_INCREASING for cumulative virtual meter reading
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_icon = "mdi:counter"
        
        # Virtual cumulative meter reading
        self._cumulative_sum: float = 0.0
        self._last_processed_timestamp: Optional[datetime] = None
        self._initialized: bool = False

    @property
    def native_value(self) -> Optional[float]:
        """Return the virtual cumulative meter reading."""
        if self._cumulative_sum > 0:
            return round(self._cumulative_sum, 3)
        return None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes for persistence."""
        attrs = super().extra_state_attributes
        if self._cumulative_sum > 0:
            attrs["cumulative_sum"] = self._cumulative_sum
        if self._last_processed_timestamp:
            attrs["last_processed_timestamp"] = self._last_processed_timestamp.isoformat()
        attrs["virtual_meter"] = True
        return attrs

    async def async_added_to_hass(self) -> None:
        """Restore previous state when entity is added to hass."""
        await super().async_added_to_hass()
        
        # Restore previous state
        last_state = await self.async_get_last_state()
        if last_state is not None:
            # Restore cumulative sum from state
            if last_state.state not in (None, "unknown", "unavailable"):
                try:
                    self._cumulative_sum = float(last_state.state)
                    _LOGGER.debug(
                        "Restored virtual meter reading for %s: %s",
                        self._attr_unique_id,
                        self._cumulative_sum
                    )
                except (ValueError, TypeError):
                    pass
            
            # Restore last processed timestamp from attributes
            if last_state.attributes:
                timestamp_str = last_state.attributes.get("last_processed_timestamp")
                if timestamp_str:
                    try:
                        self._last_processed_timestamp = datetime.fromisoformat(timestamp_str)
                    except (ValueError, TypeError):
                        pass
        
        self._initialized = True
        # Fetch initial data
        await self._fetch_and_accumulate()

    async def async_update(self) -> None:
        """Fetch new consumption data and update cumulative sum."""
        # Instance-based throttle: skip if updated recently
        now = datetime.now()
        if self._last_successful_update is not None:
            elapsed = now - self._last_successful_update
            if elapsed < self._polling_interval:
                return

        # If installation is unavailable, only do periodic availability checks
        if not self._installation_available:
            await self._check_installation_availability()
            return
        
        if self._initialized:
            await self._fetch_and_accumulate()
            # Mark successful update
            self._last_successful_update = now

    async def _fetch_and_accumulate(self) -> None:
        """Fetch historical consumption data and accumulate into virtual meter reading."""
        try:
            counter_id = self._counter.get("meterCounterId")
            if not counter_id:
                _LOGGER.warning("No meterCounterId available for consumption sensor")
                return
            
            now = datetime.now(timezone.utc)
            self._last_contact = now
            
            # Calculate date range - fetch data since last processed, or last 31 days if first time
            # (API limit is 31 days per request)
            if self._last_processed_timestamp:
                start_date = self._last_processed_timestamp
            else:
                start_date = now - timedelta(days=31)
            
            end_date = now
            
            # Format as ISO datetime strings for the API
            start_date_iso = start_date.strftime("%Y-%m-%dT00:00:00Z")
            end_date_iso = end_date.strftime("%Y-%m-%dT23:59:59Z")
            
            _LOGGER.debug("Fetching consumption for counter %s from %s to %s", 
                          counter_id, start_date_iso, end_date_iso)
            
            session = async_get_clientsession(self.hass)
            # Use POST endpoint with JSON body
            response = await session.post(
                f"{self._smarthome_base_url}/installations/{self._installation_id}/readings/historical",
                json={"from": start_date_iso, "to": end_date_iso},
                headers={"ApiKey": self._api_key, "Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30),
            )
            
            if response.status == 429:
                _LOGGER.warning("Rate limit exceeded for historical data, will retry later")
                return
            
            # Handle 404/403 - installation no longer accessible
            if response.status in (404, 403):
                _LOGGER.warning(
                    "Installation %s no longer accessible (HTTP %s)",
                    self._installation_id,
                    response.status
                )
                await self._handle_installation_unavailable()
                return
            
            if not response.ok:
                _LOGGER.error("Historical data request failed: HTTP %s", response.status)
                return
            
            historical_data = await response.json()
            
            # Process readings for this specific counter
            readings = historical_data.get("readings", [])
            new_consumption = 0.0
            valid_readings = 0
            latest_timestamp: Optional[datetime] = None
            
            target_id = str(counter_id)
            for reading in readings:
                # Filter by counter ID
                if str(reading.get("meterCounterId") or "") != target_id:
                    continue
                
                # Parse timestamp
                timestamp_str = reading.get("timestamp")
                if not timestamp_str:
                    continue
                
                try:
                    # Handle both Z suffix and explicit timezone offsets
                    if timestamp_str.endswith("Z"):
                        reading_timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    else:
                        reading_timestamp = datetime.fromisoformat(timestamp_str)
                    # Convert to UTC for consistency
                    reading_timestamp = reading_timestamp.astimezone(timezone.utc)
                except (ValueError, TypeError):
                    continue
                
                # Skip if already processed
                if self._last_processed_timestamp and reading_timestamp <= self._last_processed_timestamp:
                    continue
                
                # Use the value field (consumption is None for grid operator data)
                consumption = reading.get("value")
                if consumption is not None:
                    try:
                        if isinstance(consumption, (int, float)):
                            numeric_value = float(consumption)
                        elif isinstance(consumption, str):
                            cleaned_value = consumption.strip()
                            cleaned_value = re.sub(r'[^\d.-]', '', cleaned_value)
                            numeric_value = float(cleaned_value)
                        else:
                            continue
                        
                        if numeric_value >= 0:  # Only add positive values
                            new_consumption += numeric_value
                            valid_readings += 1
                            
                            # Track latest timestamp
                            if latest_timestamp is None or reading_timestamp > latest_timestamp:
                                latest_timestamp = reading_timestamp
                    except (ValueError, TypeError):
                        continue
            
            if valid_readings > 0:
                self._cumulative_sum += new_consumption
                if latest_timestamp:
                    self._last_processed_timestamp = latest_timestamp
                
                _LOGGER.debug(
                    "Updated virtual meter for %s: added %s, total now %s %s (from %d new readings)",
                    self._counter.get("counterType"),
                    new_consumption,
                    self._cumulative_sum,
                    self._attr_native_unit_of_measurement,
                    valid_readings
                )
            else:
                _LOGGER.debug("No valid readings found for consumption calculation")
                    
        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout fetching historical data for consumption sensor")
        except aiohttp.ClientError as err:
            _LOGGER.error("Connection error fetching historical data: %s", err)
        except Exception as err:
            _LOGGER.exception("Unexpected error fetching historical data: %s", err)

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Not used for consumption sensor - we fetch historical data directly."""
        pass


class MaalerportalStatisticSensor(MaalerportalPollingSensor, RestoreEntity):
    """Statistics sensor for electricity meters - inserts historical data into HA's long-term statistics.
    
    This sensor is designed for meters where data is delayed (1-3 days from grid operator).
    It uses async_add_external_statistics to insert historical readings into Home Assistant's
    statistics database, making it compatible with the Energy Dashboard.
    
    For consumption-type meters (readingType="consumption"), this sensor also maintains a
    virtual cumulative meter reading by summing all historical consumption values. This
    virtual meter is restored across restarts using RestoreEntity.
    """

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the statistics sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        counter_type = counter.get("counterType", "")
        is_primary = counter.get("isPrimary", False)
        suffix = "primary" if is_primary else "secondary"
        
        # Get installation type to distinguish hot vs cold water
        # (counter_type may always be "ColdWater" for water meters)
        installation_type = installation.get("installationType", "").lower()
        
        # Set attributes based on counter type
        if counter_type == "ColdWater" or counter_type == "HotWater":
            # Use installation type to determine if it's hot or cold water
            if installation_type == "hotwater":
                self._attr_translation_key = "hot_water_statistic"
                self._attr_icon = "mdi:water-thermometer"
            else:
                self._attr_translation_key = "cold_water_statistic"
                self._attr_icon = "mdi:water"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
            self._unit_class = "volume"
        elif counter_type == "ElectricityFromGrid":
            self._attr_translation_key = "electricity_statistic" if is_primary else "electricity_import_statistic"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_icon = "mdi:transmission-tower-import"
            self._unit_class = "energy"
        elif counter_type == "ElectricityToGrid":
            self._attr_translation_key = "electricity_export_statistic"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_icon = "mdi:transmission-tower-export"
            self._unit_class = "energy"
        elif counter_type == "Heat":
            self._attr_translation_key = "heat_statistic"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_icon = "mdi:fire"
            self._unit_class = "energy"
        else:
            self._attr_translation_key = "energy_statistic"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_icon = "mdi:chart-line-variant"
            self._unit_class = "energy"
        
        self._attr_unique_id = f"{self._installation_id}_{counter_type.lower()}_statistic_{suffix}"
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        # Sensor returns native_value=None on purpose (stats-only entity).
        # Hide from the default device card so users don't see "Unknown" —
        # it stays available for Energy Dashboard selection.
        self._attr_entity_registry_visible_default = False
        
        # Reading type determines how we process data
        # "counter" = cumulative meter reading (use value directly)
        # "consumption" = per-period consumption (need to accumulate)
        self._reading_type = counter.get("readingType", "counter").lower()
        
        # Statistics metadata - will be set to entity_id after entity is added to HA
        self._statistic_id: Optional[str] = None  # Will be set in async_added_to_hass
        self._stat_unit = self._attr_native_unit_of_measurement
        self._last_stats_update: Optional[datetime] = None
        
        # Track last inserted timestamp and cumulative sum for consumption types
        self._last_inserted_timestamp: Optional[datetime] = None
        self._cumulative_sum: float = 0.0

        # Per-counter offset to keep displayed sum continuous across meter
        # swaps. Loaded from the per-entry offset store on add-to-hass.
        self._meter_offset: float = 0.0
        # Heuristic: any reading where raw_value < prev_raw_value * THRESHOLD
        # is treated as the swap point when a swap is pending.
        self._swap_drop_threshold = 0.5

    @property
    def native_value(self) -> Optional[float]:
        """Return None - this sensor is only for importing statistics to Energy Dashboard.
        
        The virtual cumulative sum is tracked internally for statistics import,
        but the sensor itself shows 'unknown' to avoid confusion.
        """
        return None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes for persistence."""
        attrs = {
            "reading_type": self._reading_type,
        }
        if self._statistic_id:
            attrs["statistic_id"] = self._statistic_id
        if self._cumulative_sum > 0:
            attrs["cumulative_sum"] = self._cumulative_sum
        if self._last_inserted_timestamp:
            attrs["last_inserted_timestamp"] = self._last_inserted_timestamp.isoformat()
        return attrs

    async def async_added_to_hass(self) -> None:
        """Run when entity is added to hass - restore state and fetch data."""
        await super().async_added_to_hass()

        # Set statistic_id to the sensor's entity_id (e.g., "sensor.xxx")
        # This allows async_import_statistics to insert data into this sensor's statistics
        self._statistic_id = self.entity_id
        _LOGGER.debug("Set statistic_id to entity_id: %s", self._statistic_id)

        # Load any persisted offset for this counter (counter-type only —
        # consumption-type accumulates from incremental values and never
        # sees raw-meter resets).
        if self._reading_type != "consumption":
            entry_id = self._get_entry_id()
            counter_id = self._counter.get("meterCounterId")
            if entry_id and counter_id:
                offset_store = self.hass.data.get(DOMAIN, {}).get(entry_id, {}).get(
                    "offset_store"
                )
                if offset_store is not None:
                    self._meter_offset = offset_store.get(self._installation_id, counter_id)
                    if self._meter_offset:
                        _LOGGER.debug(
                            "Loaded meter offset for %s: %.4f",
                            self._statistic_id,
                            self._meter_offset,
                        )

        # Restore previous state for consumption-type meters
        if self._reading_type == "consumption":
            last_state = await self.async_get_last_state()
            if last_state is not None:
                # Restore cumulative sum from state
                if last_state.state not in (None, "unknown", "unavailable"):
                    try:
                        self._cumulative_sum = float(last_state.state)
                        _LOGGER.debug(
                            "Restored cumulative sum for %s: %s",
                            self._attr_unique_id,
                            self._cumulative_sum
                        )
                    except (ValueError, TypeError):
                        pass
                
                # Restore last inserted timestamp from attributes
                if last_state.attributes:
                    timestamp_str = last_state.attributes.get("last_inserted_timestamp")
                    if timestamp_str:
                        try:
                            self._last_inserted_timestamp = datetime.fromisoformat(timestamp_str)
                            _LOGGER.debug(
                                "Restored last_inserted_timestamp for %s: %s",
                                self._attr_unique_id,
                                self._last_inserted_timestamp
                            )
                        except (ValueError, TypeError):
                            pass
        
        # Schedule initial statistics update – always fetch full history on startup
        # to ensure completeness even after re-installation (old stats may linger in DB).
        # async_import_statistics handles duplicates gracefully.
        self._history_task = self.hass.async_create_task(
            self._async_update_statistics(force_full_fetch=True)
        )

    async def async_will_remove_from_hass(self) -> None:
        """Run when entity is removed from hass."""
        if hasattr(self, "_history_task") and self._history_task:
            self._history_task.cancel()
        await super().async_will_remove_from_hass()

    def _get_entry_id(self) -> Optional[str]:
        """Best-effort lookup of this sensor's config entry id."""
        platform = getattr(self, "platform", None)
        config_entry = getattr(platform, "config_entry", None) if platform else None
        if config_entry is not None:
            return config_entry.entry_id
        # Fallback: locate by matching installation_id in hass.data
        for entry_id, store in self.hass.data.get(DOMAIN, {}).items():
            if not isinstance(store, dict):
                continue
            for inst in store.get("installations", []):
                if inst.get("installationId") == self._installation_id:
                    return entry_id
        return None

    def _find_main_entity_id(self) -> Optional[str]:
        """Look up the Main sensor's entity_id for this installation.

        The Main sensor (Vattenmätaravläsning) is created with
        unique_id ``{installation_id}_main`` for primary counter-type
        meters. Returns None for consumption-type or secondary counters
        where no Main sensor exists.
        """
        if self._reading_type == "consumption":
            return None
        if self._counter and not self._counter.get("isPrimary", False):
            return None
        try:
            registry = er.async_get(self.hass)
        except Exception:  # noqa: BLE001
            return None
        target_unique_id = f"{self._installation_id}_main"
        for entity in registry.entities.values():
            if entity.platform != DOMAIN:
                continue
            if entity.unique_id == target_unique_id:
                return entity.entity_id
        return None

    def _mirror_statistics_to_main(self, statistics: list) -> None:
        """Mirror imported statistics onto the Main sensor's statistic_id.

        Without this, only the dedicated stats-only entity carries the
        backfilled history. That entity has ``state=unknown`` by design,
        which breaks history-graph cards and confuses users browsing
        their water meter device card. Mirroring lets users use the
        natural Vattenmätaravläsning entity in any chart card.

        Skipped silently when there is no Main sensor (consumption-type
        meters, secondary counters) or when the entity registry lookup
        fails.
        """
        if not statistics:
            return
        main_entity_id = self._find_main_entity_id()
        if main_entity_id is None:
            return
        from homeassistant.components.recorder.models import (
            StatisticMeanType,
            StatisticMetaData,
        )
        from homeassistant.components.recorder.statistics import (
            async_import_statistics,
        )
        main_metadata = StatisticMetaData(
            has_mean=False,
            has_sum=True,
            mean_type=StatisticMeanType.NONE,
            # name=None lets HA pick up the entity's friendly name on render
            name=None,
            source="recorder",
            statistic_id=main_entity_id,
            unit_of_measurement=self._stat_unit,
            unit_class=self._unit_class,
        )
        async_import_statistics(self.hass, main_metadata, statistics)
        _LOGGER.info(
            "Mirrored %d historical stats to Main sensor %s",
            len(statistics),
            main_entity_id,
        )

    async def async_update(self) -> None:
        """Update statistics from historical API data."""
        # Instance-based throttle: skip if updated recently
        now = datetime.now()
        if self._last_successful_update is not None:
            elapsed = now - self._last_successful_update
            if elapsed < self._polling_interval:
                return

        # If installation is unavailable, only do periodic availability checks
        if not self._installation_available:
            await self._check_installation_availability()
            return
        
        await self._async_update_statistics()
        # Mark successful update
        self._last_successful_update = now

    async def _async_update_statistics(self, force_full_fetch: bool = False) -> None:
        """Fetch historical data and insert into Home Assistant statistics.

        Args:
            force_full_fetch: If True, fetch up to 1 year of history regardless
                of existing statistics. Used on initial setup to ensure complete
                history even after re-installation (where old stats may linger
                in the recorder DB).
        """
        from homeassistant.components.recorder import get_instance
        from homeassistant.components.recorder.models import (
            StatisticData,
            StatisticMeanType,
            StatisticMetaData,
        )
        from homeassistant.components.recorder.statistics import (
            async_import_statistics,
            get_last_statistics,
        )
        try:
            # Ensure we have a statistic_id (entity_id) set
            if not self._statistic_id:
                _LOGGER.debug("Skipping statistics update: entity_id not yet available")
                return

            counter_id = self._counter.get("meterCounterId")
            if not counter_id:
                _LOGGER.warning("No meterCounterId available for statistics sensor")
                return

            _LOGGER.debug("Updating statistics for counter: %s (statistic_id: %s)",
                          counter_id, self._statistic_id)

            end_date = datetime.now(timezone.utc)

            # Check if we have existing statistics
            last_stats = await get_instance(self.hass).async_add_executor_job(
                get_last_statistics,
                self.hass,
                1,
                self._statistic_id,
                True,
                {"state", "sum"},
            )

            has_existing_stats = bool(last_stats and self._statistic_id in last_stats)

            if force_full_fetch:
                # Initial setup / restart / manual refresh: always fetch up to
                # 1 year of history. async_import_statistics replaces duplicate
                # timestamps, so re-fetching already-imported data is safe.
                start_date = end_date - timedelta(days=365)
                # Always reset the in-memory cursor so the per-reading skip
                # logic doesn't filter out the older data we just asked for.
                # Without this, consumption-type meters (which restore
                # _last_inserted_timestamp from RestoreEntity) would silently
                # discard everything older than the previous run's last
                # imported timestamp — i.e. no historical backfill ever.
                self._last_inserted_timestamp = None
                if self._reading_type == "consumption":
                    # Rebuild the running sum from scratch so re-imported
                    # interval values aren't double-counted on top of the
                    # previously accumulated total.
                    self._cumulative_sum = 0.0
                _LOGGER.info(
                    "Full history fetch for %s: fetching up to 1 year of "
                    "history in 31-day chunks",
                    self._statistic_id,
                )
            elif has_existing_stats:
                # Periodic update: just fetch recent data to keep statistics current.
                # Historical backfill is handled by the "Fetch 30 more days" button.
                start_date = end_date - timedelta(days=7)
                _LOGGER.debug(
                    "Periodic update: fetching last 7 days",
                )
            else:
                # No existing stats and not initial fetch – fetch up to 1 year
                # (API limit is 31 days per request; _fetch_historical_chunked handles splitting)
                start_date = end_date - timedelta(days=365)
                self._last_inserted_timestamp = None
                _LOGGER.info(
                    "No existing statistics, fetching up to 1 year of history in 31-day chunks"
                )

            readings = await self._fetch_historical_chunked(start_date, end_date)
            if not readings and not has_existing_stats:
                _LOGGER.debug("No historical readings returned for installation %s",
                              self._installation_id)
                return
            
            _LOGGER.info("Historical API returned %d readings for installation %s",
                         len(readings), self._installation_id)
            
            # Debug: Log sample reading to understand structure
            if readings and self._reading_type == "consumption":
                sample = readings[0]
                matching_samples = [r for r in readings[:5] if r.get("meterCounterId") == counter_id]
                _LOGGER.debug("Sample reading keys: %s", list(sample.keys()))
                _LOGGER.debug("Looking for meterCounterId=%s, found %d matches in first 5", 
                              counter_id, len(matching_samples))
                if matching_samples:
                    _LOGGER.debug("Matching sample: %s", matching_samples[0])
            
            # Filter readings for this counter
            target_id = str(counter_id)
            counter_readings = [
                r for r in readings 
                if str(r.get("meterCounterId") or "") == target_id and r.get("value") is not None
            ]
            
            _LOGGER.info("Filtered to %d readings for counter %s (reading_type=%s)",
                         len(counter_readings), counter_id, self._reading_type)
            
            if not counter_readings:
                _LOGGER.debug("No readings found for counter %s (reading_type=%s)", counter_id, self._reading_type)
                return
            
            # Sort by timestamp
            counter_readings.sort(key=lambda x: x.get("timestamp", ""))
            
            # For counter-type meters without existing statistics:
            # Use the first reading as a baseline and subtract it from all values
            # This prevents the Energy Dashboard from showing massive consumption (0 to current meter value)
            # Instead, consumption starts from 0 and only shows the delta from the first reading
            counter_baseline = None
            if self._reading_type != "consumption" and not has_existing_stats:
                # Get the first reading value as baseline
                if counter_readings:
                    first_value = counter_readings[0].get("value")
                    if first_value is not None:
                        if isinstance(first_value, str):
                            counter_baseline = float(re.sub(r'[^\d.-]', '', first_value.strip()))
                        else:
                            counter_baseline = float(first_value)
                        _LOGGER.debug(
                            "Using first counter reading as baseline: %s (will subtract from all values)",
                            counter_baseline
                        )

            # Meter-swap offset bookkeeping (counter-type only).
            # `prev_raw_value` and `prev_displayed_sum` track the last seen
            # raw API value and last persisted displayed sum so we can spot
            # a sudden drop (the swap point) and re-anchor the offset.
            entry_id = self._get_entry_id()
            from .. import is_swap_pending  # local import avoids circular at module load
            swap_pending = (
                self._reading_type != "consumption"
                and entry_id is not None
                and is_swap_pending(self.hass, entry_id, self._installation_id)
            )
            prev_raw_value: Optional[float] = None
            prev_displayed_sum: Optional[float] = None
            if has_existing_stats and last_stats and self._statistic_id in last_stats:
                last_row = last_stats[self._statistic_id][0]
                prev_raw_value = last_row.get("state")
                prev_displayed_sum = last_row.get("sum")
                if prev_raw_value is not None:
                    try:
                        prev_raw_value = float(prev_raw_value)
                    except (TypeError, ValueError):
                        prev_raw_value = None
                if prev_displayed_sum is not None:
                    try:
                        prev_displayed_sum = float(prev_displayed_sum)
                    except (TypeError, ValueError):
                        prev_displayed_sum = None
            
            # For consumption type, continue accumulating from where we left
            # off — but only on periodic updates. On force_full_fetch we
            # explicitly want to rebuild from 0 (cumulative_sum was reset
            # above), otherwise re-imported intervals would stack on top of
            # the existing total.
            if (
                self._reading_type == "consumption"
                and not force_full_fetch
                and self._cumulative_sum == 0.0
            ):
                try:
                    existing_stats = await get_instance(self.hass).async_add_executor_job(
                        get_last_statistics,
                        self.hass,
                        1,
                        self._statistic_id,
                        True,
                        {"state", "sum"},
                    )
                    if existing_stats and self._statistic_id in existing_stats:
                        last_stat = existing_stats[self._statistic_id][0]
                        self._cumulative_sum = last_stat.get("sum", 0.0) or 0.0
                        _LOGGER.debug("Loaded existing cumulative sum: %s", self._cumulative_sum)
                except Exception as err:
                    _LOGGER.debug("Could not load existing statistics: %s", err)
            
            # Build statistics data
            statistics: list[StatisticData] = []
            cumulative_sum = self._cumulative_sum
            
            for reading in counter_readings:
                try:
                    timestamp_str = reading.get("timestamp")
                    if not timestamp_str:
                        continue
                    
                    # Parse timestamp - handle both Z suffix and explicit timezone offsets
                    if timestamp_str.endswith("Z"):
                        timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    else:
                        timestamp = datetime.fromisoformat(timestamp_str)
                    
                    # Convert to UTC for Home Assistant statistics
                    timestamp = timestamp.astimezone(timezone.utc)
                    
                    # Round to the start of the hour (in UTC)
                    timestamp = timestamp.replace(minute=0, second=0, microsecond=0)
                    
                    # For counter-type meters, the API timestamp represents WHEN the reading was taken
                    # (end of measurement period). We need to attribute it to the PREVIOUS hour.
                    # Example: reading at 00:05 represents the meter state at end of 23:00-00:00 hour
                    #          so it should be stored with start=23:00, not 00:00
                    # For consumption-type, the timestamp is already correct from the API.
                    if self._reading_type != "consumption":
                        timestamp = timestamp - timedelta(hours=1)
                    
                    # Skip if we already have this timestamp
                    if self._last_inserted_timestamp and timestamp <= self._last_inserted_timestamp:
                        continue
                    
                    if self._reading_type == "consumption":
                        # For consumption type: accumulate interval values to create virtual meter
                        # Note: API puts interval consumption in "value" field, not "consumption"
                        interval_value = reading.get("value")
                        if interval_value is None:
                            continue
                        
                        # Parse interval value
                        if isinstance(interval_value, str):
                            interval_value = float(re.sub(r'[^\d.-]', '', interval_value.strip()))
                        else:
                            interval_value = float(interval_value)
                        
                        # Only add positive consumption
                        if interval_value > 0:
                            cumulative_sum += interval_value
                        
                        statistics.append(
                            StatisticData(
                                start=timestamp,
                                state=interval_value,  # Current period consumption
                                sum=cumulative_sum,    # Virtual meter reading (accumulated)
                            )
                        )
                    else:
                        # For counter type: value is already cumulative
                        value = reading.get("value")
                        if value is None:
                            continue

                        # Parse value
                        if isinstance(value, str):
                            value = float(re.sub(r'[^\d.-]', '', value.strip()))
                        else:
                            value = float(value)

                        # Detect a meter swap: sudden drop in raw value within
                        # this batch of readings. A swap is recognised when:
                        #   - we have a previous raw value (existing stats or
                        #     earlier reading in this batch), AND
                        #   - the new raw value is significantly lower
                        #     (< prev * threshold), AND
                        #   - either reconciliation has flagged a pending
                        #     swap, OR we have no flag but the drop is
                        #     unmistakable (defensive fallback).
                        if (
                            prev_raw_value is not None
                            and prev_displayed_sum is not None
                            and value < prev_raw_value * self._swap_drop_threshold
                            and (swap_pending or value < prev_raw_value * 0.1)
                        ):
                            new_offset = compute_swap_offset(
                                prev_displayed_sum, value
                            )
                            _LOGGER.warning(
                                "Meter swap detected for %s at %s: raw value "
                                "dropped %.4f -> %.4f. Re-anchoring offset "
                                "%.4f -> %.4f to keep accumulated total continuous.",
                                self._statistic_id,
                                timestamp.isoformat(),
                                prev_raw_value,
                                value,
                                self._meter_offset,
                                new_offset,
                            )
                            self._meter_offset = new_offset
                            swap_pending = False
                            # Persist immediately so a crash mid-batch doesn't
                            # lose the offset.
                            counter_id_for_persist = self._counter.get("meterCounterId")
                            if entry_id and counter_id_for_persist:
                                offset_store = self.hass.data.get(DOMAIN, {}).get(
                                    entry_id, {}
                                ).get("offset_store")
                                if offset_store is not None:
                                    await offset_store.async_set(
                                        self._installation_id,
                                        counter_id_for_persist,
                                        new_offset,
                                    )
                                from .. import consume_swap_pending
                                consume_swap_pending(
                                    self.hass, entry_id, self._installation_id
                                )

                        # Calculate displayed sum:
                        #  - First-time install: subtract baseline (legacy
                        #    behaviour, makes Energy Dashboard start near 0)
                        #  - Otherwise: apply meter-swap offset (0 if no
                        #    swap has happened)
                        if counter_baseline is not None:
                            displayed_sum = value - counter_baseline
                        else:
                            displayed_sum = value + self._meter_offset

                        statistics.append(
                            StatisticData(
                                start=timestamp,
                                state=value,  # Raw meter reading
                                sum=displayed_sum,  # User-facing accumulated total
                            )
                        )

                        prev_raw_value = value
                        prev_displayed_sum = displayed_sum

                except (ValueError, TypeError) as err:
                    _LOGGER.debug("Error parsing reading: %s - %s", reading, err)
                    continue
            
            # Update cumulative sum for next time
            if self._reading_type == "consumption":
                self._cumulative_sum = cumulative_sum
            
            if not statistics:
                _LOGGER.debug("No new statistics to insert for counter %s", counter_id)
                return
            
            # Import statistics into the sensor's history using its entity_id
            # This allows the Energy Dashboard to see the historical data
            if not self._statistic_id:
                _LOGGER.warning("Cannot insert statistics: entity_id not set yet")
                return
            
            # Create metadata - use "recorder" as source for sensor statistics
            metadata = StatisticMetaData(
                has_mean=False,
                has_sum=True,
                mean_type=StatisticMeanType.NONE,
                name=self.name or f"{self._base_device_name}",
                source="recorder",
                statistic_id=self._statistic_id,
                unit_of_measurement=self._stat_unit,
                unit_class=self._unit_class,
            )
            
            # Insert statistics using async_import_statistics
            # This imports into the sensor's existing statistics
            async_import_statistics(self.hass, metadata, statistics)

            # Mirror onto the Main sensor's statistic_id so users see the
            # same history on the user-friendly entity (Vattenmätaravläsning)
            # — its native_value is "live" and Statistics graph + history
            # cards render naturally there. The dedicated Stats sensor
            # remains as a stable Energy Dashboard target.
            self._mirror_statistics_to_main(statistics)

            # Update last inserted timestamp
            if statistics:
                self._last_inserted_timestamp = statistics[-1]["start"]

            self._last_stats_update = datetime.now(timezone.utc)
            _LOGGER.info(
                "Inserted %d statistics records for %s (from %s to %s)",
                len(statistics),
                self._statistic_id,
                statistics[0]["start"].isoformat() if statistics else "N/A",
                statistics[-1]["start"].isoformat() if statistics else "N/A",
            )
            
            # Update entity state for consumption-type meters to reflect new cumulative sum
            if self._reading_type == "consumption":
                self.async_write_ha_state()
                
        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout fetching statistics data for sensor")
        except aiohttp.ClientError as err:
            _LOGGER.error("Connection error fetching statistics data: %s", err)
        except Exception as err:
            _LOGGER.exception("Unexpected error updating statistics: %s", err)

    async def _fetch_historical_chunked(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """Fetch historical readings in ≤31-day chunks to respect the API limit.

        The API returns HTTP 500 with "Date range cannot exceed 31 days" for
        larger windows, so we split automatically and return all readings merged.
        """
        MAX_CHUNK_DAYS = 31
        session = async_get_clientsession(self.hass)
        all_readings: list[dict] = []
        chunk_end = end_date

        while chunk_end > start_date:
            chunk_start = max(chunk_end - timedelta(days=MAX_CHUNK_DAYS), start_date)
            from_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            to_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")

            try:
                response = await session.post(
                    f"{self._smarthome_base_url}/installations/{self._installation_id}/readings/historical",
                    json={"from": from_str, "to": to_str},
                    headers={"ApiKey": self._api_key, "Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=60),
                )

                if response.status == 429:
                    _LOGGER.warning("Rate limit exceeded during chunked history fetch, stopping early")
                    break

                if response.status in (404, 403):
                    _LOGGER.warning(
                        "Installation %s no longer accessible (HTTP %s)",
                        self._installation_id, response.status,
                    )
                    await self._handle_installation_unavailable()
                    return all_readings

                if not response.ok:
                    _LOGGER.error(
                        "Chunked history request failed: HTTP %s (%s to %s)",
                        response.status, from_str, to_str,
                    )
                    break

                data = await response.json()
                chunk_readings = data.get("readings", [])
                all_readings.extend(chunk_readings)
                if chunk_readings:
                    _LOGGER.info(
                        "Chunk %s → %s: %d readings", from_str, to_str, len(chunk_readings)
                    )
                else:
                    _LOGGER.debug(
                        "Chunk %s → %s: 0 readings", from_str, to_str
                    )

            except asyncio.TimeoutError:
                _LOGGER.warning("Timeout fetching chunk %s to %s", from_str, to_str)
                break
            except aiohttp.ClientError as err:
                _LOGGER.error("Connection error fetching chunk: %s", err)
                break

            chunk_end = chunk_start - timedelta(seconds=1)

        return all_readings

    async def async_fetch_older_history(self, from_days_ago: int, to_days_ago: int) -> int:
        """Fetch older historical data for a specific date range and insert into statistics."""
        if not self._statistic_id:
            _LOGGER.warning("Cannot fetch older history: entity_id not set yet")
            return 0
            
        from homeassistant.components.recorder.models import (
            StatisticData,
            StatisticMeanType,
            StatisticMetaData,
        )
        from homeassistant.components.recorder.statistics import async_import_statistics
        
        counter_id = self._counter.get("meterCounterId")
        if not counter_id:
            _LOGGER.warning("No meterCounterId available for older history fetch")
            return 0
        
        try:
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=from_days_ago)
            fetch_end_date = end_date - timedelta(days=to_days_ago)
            
            start_date_iso = start_date.strftime("%Y-%m-%dT00:00:00Z")
            end_date_iso = fetch_end_date.strftime("%Y-%m-%dT23:59:59Z")
            
            _LOGGER.info(
                "Fetching older history for %s: %d to %d days ago (%s to %s)",
                self._statistic_id, from_days_ago, to_days_ago,
                start_date_iso, end_date_iso
            )

            readings = await self._fetch_historical_chunked(start_date, fetch_end_date)
            _LOGGER.debug("Older history: %d total readings across all chunks", len(readings))
            
            _LOGGER.debug("Older history API returned %d readings", len(readings))
            
            # Filter readings for this counter
            target_id = str(counter_id)
            counter_readings = [
                r for r in readings
                if str(r.get("meterCounterId") or "") == target_id and r.get("value") is not None
            ]
            
            if not counter_readings:
                _LOGGER.info("No older readings found for counter %s", counter_id)
                return 0
            
            # Sort by timestamp
            counter_readings.sort(key=lambda x: x.get("timestamp", ""))
            
            # For counter-type: use the first reading as baseline
            counter_baseline = None
            if self._reading_type != "consumption":
                first_value = counter_readings[0].get("value")
                if first_value is not None:
                    if isinstance(first_value, str):
                        counter_baseline = float(re.sub(r'[^\d.-]', '', first_value.strip()))
                    else:
                        counter_baseline = float(first_value)
            
            # For consumption type, start cumulative sum at 0 for this batch
            cumulative_sum = 0.0
            
            # Build statistics data
            statistics: list[StatisticData] = []
            
            for reading in counter_readings:
                try:
                    timestamp_str = reading.get("timestamp")
                    if not timestamp_str:
                        continue
                    
                    if timestamp_str.endswith("Z"):
                        timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    else:
                        timestamp = datetime.fromisoformat(timestamp_str)
                    
                    timestamp = timestamp.astimezone(timezone.utc)
                    timestamp = timestamp.replace(minute=0, second=0, microsecond=0)
                    
                    if self._reading_type != "consumption":
                        timestamp = timestamp - timedelta(hours=1)
                    
                    if self._reading_type == "consumption":
                        interval_value = reading.get("value")
                        if interval_value is None:
                            continue
                        if isinstance(interval_value, str):
                            interval_value = float(re.sub(r'[^\d.-]', '', interval_value.strip()))
                        else:
                            interval_value = float(interval_value)
                        if interval_value > 0:
                            cumulative_sum += interval_value
                        statistics.append(
                            StatisticData(
                                start=timestamp,
                                state=interval_value,
                                sum=cumulative_sum,
                            )
                        )
                    else:
                        value = reading.get("value")
                        if value is None:
                            continue
                        if isinstance(value, str):
                            value = float(re.sub(r'[^\d.-]', '', value.strip()))
                        else:
                            value = float(value)
                        relative_sum = value - counter_baseline if counter_baseline is not None else value
                        statistics.append(
                            StatisticData(
                                start=timestamp,
                                state=value,
                                sum=relative_sum,
                            )
                        )
                except (ValueError, TypeError) as err:
                    _LOGGER.debug("Error parsing older reading: %s - %s", reading, err)
                    continue
            
            if not statistics:
                _LOGGER.info("No new older statistics to insert for counter %s", counter_id)
                return 0
            
            # Create metadata
            metadata = StatisticMetaData(
                has_mean=False,
                has_sum=True,
                mean_type=StatisticMeanType.NONE,
                name=self.name or f"{self._base_device_name}",
                source="recorder",
                statistic_id=self._statistic_id,
                unit_of_measurement=self._stat_unit,
                unit_class=self._unit_class,
            )
            
            # Insert older statistics
            async_import_statistics(self.hass, metadata, statistics)
            self._mirror_statistics_to_main(statistics)

            _LOGGER.info(
                "Inserted %d older statistics records for %s (from %s to %s)",
                len(statistics),
                self._statistic_id,
                statistics[0]["start"].isoformat() if statistics else "N/A",
                statistics[-1]["start"].isoformat() if statistics else "N/A",
            )
            
            return len(statistics)
            
        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout fetching older history")
        except aiohttp.ClientError as err:
            _LOGGER.error("Connection error fetching older history: %s", err)
        except Exception as err:
            _LOGGER.exception("Unexpected error fetching older history: %s", err)
        
        return 0
