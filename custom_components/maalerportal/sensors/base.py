"""Base sensor classes for Målerportal integration."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import logging
import re
from typing import Any, Optional

import aiohttp
from homeassistant.components.sensor import SensorEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from ..const import DOMAIN
from ..coordinator import MaalerportalCoordinator

_LOGGER = logging.getLogger(__name__)

class MaalerportalBaseSensor(SensorEntity):
    """Base class for all Målerportal sensors (common attributes)."""
    
    _attr_has_entity_name = True
    
    # Translation map for installation/meter types
    METER_TYPE_TRANSLATIONS = {
        "Apartment": "Lejlighed",
        "House": "Hus",
        "SummerHouse": "Sommerhus",
        "Business": "Erhverv",
        "School": "Skole",
        "Institution": "Institution", 
        "Other": "Andet",
        "Heat": "Varmemåler",
        "ColdWater": "Koldtvandsmåler", 
        "HotWater": "Varmtvarsmåler",
        "Electricity": "Elmåler",
        "Gas": "Gasmåler",
    }
    
    def __init__(
        self,
        installation: dict,
        api_key: str, 
        smarthome_base_url: str,
        counter: dict = None,
    ) -> None:
        """Initialize the base sensor."""
        self._installation = installation
        self._api_key = api_key
        self._smarthome_base_url = smarthome_base_url
        self._installation_id = installation["installationId"]
        self._installation_type = installation["installationType"]
        self._counter = counter
        
        # Create base device name
        self._base_device_name = f"{installation['address']} - {installation['meterSerial']}"
        if installation.get("nickname"):
            self._base_device_name += f" ({installation['nickname']})"


    def _get_translated_meter_type(self) -> str:
        """Get translated meter type for device model."""
        return self.METER_TYPE_TRANSLATIONS.get(self._installation_type, self._installation_type)

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information."""
        return DeviceInfo(
            identifiers={(DOMAIN, self._installation_id)},
            name=self._base_device_name,
            manufacturer=self._installation.get("utilityName", "Unknown"),
            model=self._get_translated_meter_type(),
            serial_number=self._installation.get("meterSerial"),
        )
        
    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        attrs: dict[str, Any] = {
            "installation_id": self._installation_id,
        }
        if self._counter:
            attrs["counter_type"] = self._counter.get("counterType", "")
            attrs["meter_counter_id"] = self._counter.get("meterCounterId", "")
        return attrs

    def _parse_counter_value(self, counter: dict) -> float | None:
        """Parse and validate counter value."""
        latest_value = counter.get("latestValue")
        if latest_value is None:
            _LOGGER.debug("latestValue is None for counter %s", counter.get("meterCounterId"))
            return None
            
        try:
            if isinstance(latest_value, (int, float)):
                numeric_value = float(latest_value)
            elif isinstance(latest_value, str):
                # Clean the string value - remove any non-numeric characters except decimal point and minus sign
                cleaned_value = latest_value.strip()
                _LOGGER.debug("Raw string value for counter %s: '%s'", counter.get("meterCounterId"), cleaned_value)
                cleaned_value = re.sub(r'[^\d.-]', '', cleaned_value)
                if not cleaned_value:
                    _LOGGER.warning("Value '%s' resulted in empty string after cleaning", latest_value)
                    return None
                numeric_value = float(cleaned_value)
            else:
                _LOGGER.error("Unexpected value type for %s: %s", latest_value, type(latest_value))
                return None
                
            # Validate the number
            if not (isinstance(numeric_value, (int, float)) and numeric_value == numeric_value):  # Check for NaN
                _LOGGER.error("Invalid numeric value for %s: %s", counter.get("meterCounterId"), latest_value)
                return None
                
            _LOGGER.debug("Parsed value for %s: %s -> %s", counter.get("meterCounterId"), latest_value, numeric_value)
            return numeric_value
            
        except (ValueError, TypeError) as err:
            _LOGGER.error("Error parsing meter value '%s': %s", latest_value, err)
            return None


class MaalerportalCoordinatorSensor(CoordinatorEntity[MaalerportalCoordinator], MaalerportalBaseSensor):
    """Sensor that updates via MaalerportalCoordinator."""
    
    def __init__(
        self,
        coordinator: MaalerportalCoordinator,
        counter: dict = None,
    ) -> None:
        """Initialize coordinator sensor."""
        # Initialize CoordinatorEntity
        super().__init__(coordinator)
        # Initialize BaseSensor
        MaalerportalBaseSensor.__init__(
            self,
            coordinator.installation,
            coordinator.api_key,
            coordinator.base_url,
            counter
        )
        
    async def async_added_to_hass(self) -> None:
        """When entity is added to hass."""
        await super().async_added_to_hass()
        if self.coordinator.data:
            self._handle_coordinator_update()

    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        # Get meter counters from coordinator data
        if not self.coordinator.data:
            _LOGGER.debug("No coordinator data available for %s", self.entity_id)
            return

        meter_counters = self.coordinator.data.get("meterCounters", [])
        _LOGGER.debug("Updating %s with %d counters", self.entity_id, len(meter_counters))
        
        # Find our counter and update
        if self._counter:
            our_id = self._counter.get("meterCounterId")
            _LOGGER.debug(
                "Entity %s (tracking counter %s) updating from %d counters",
                self.entity_id,
                our_id,
                len(meter_counters)
            )
            self._update_from_meter_counters(meter_counters)
            self.async_write_ha_state()
        else:
            _LOGGER.debug("Entity %s has no tracking counter", self.entity_id)

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update sensor state from counter list (implemented by subclasses)."""
        pass


class MaalerportalPollingSensor(MaalerportalBaseSensor):
    """Sensor that manages its own polling (e.g. historical data)."""
    
    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict = None,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the polling sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter)
        self._polling_interval = polling_interval
        
        # Instance-based throttle tracking
        self._last_successful_update: Optional[datetime] = None
        
        # Rate limiting
        self._rate_limit_delay = 2000  # 2 seconds between requests
        
        # Last contact tracking
        self._last_contact: Optional[datetime] = None
        self._last_reading_timestamp: Optional[str] = None
        
        # Installation availability tracking (for 404 handling)
        self._installation_available: bool = True
        self._last_availability_check: Optional[datetime] = None
        self._unavailable_since: Optional[datetime] = None
        self._availability_check_count: int = 0
        self._base_check_interval = timedelta(minutes=15)
        self._max_check_interval = timedelta(hours=24)
        self._max_unavailable_days: int = 30

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        attrs = super().extra_state_attributes
        if self._last_contact:
            attrs["last_contact"] = self._last_contact.isoformat()
        if self._last_reading_timestamp:
            attrs["last_reading_timestamp"] = self._last_reading_timestamp
        return attrs

    async def async_update(self) -> None:
        """Fetch data from API."""
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
        
        try:
            _LOGGER.debug("Fetching meter readings for installation: %s", self._installation_id)
            
            session = async_get_clientsession(self.hass)
            # Get latest readings
            response = await session.get(
                f"{self._smarthome_base_url}/installations/{self._installation_id}/readings/latest",
                headers={"ApiKey": self._api_key},
                timeout=aiohttp.ClientTimeout(total=30),
            )
            
            if response.status == 429:
                _LOGGER.warning("Rate limit exceeded, backing off")
                # Simple backoff handled by interval
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
                _LOGGER.error("Error fetching meter readings: %s", response.status)
                return
            
            # Reset availability on success
            if not self._installation_available:
                _LOGGER.info("Installation %s is available again", self._installation_id)
                self._installation_available = True
                self._unavailable_since = None
                self._availability_check_count = 0
            
            readings_data = await response.json()
            _LOGGER.debug("Received %d meter counters", len(readings_data.get("meterCounters", [])))

            if readings_data.get("meterCounters"):
                self._update_from_meter_counters(readings_data["meterCounters"])
            else:
                _LOGGER.debug("No meter counters found in API response")
            
            # Mark successful update for throttle tracking
            self._last_successful_update = now
            self._last_contact = now

        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout fetching meter readings for installation: %s", self._installation_id)
        except Exception as err:
            _LOGGER.error("Error updating meter readings for installation %s: %s", 
                         self._installation_id, err)

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update sensor state from meter counter data - to be implemented by subclasses."""
        pass
        
    async def _handle_installation_unavailable(self) -> None:
        """Handle installation being unavailable (404/403)."""
        if self._installation_available:
            self._installation_available = False
            self._unavailable_since = datetime.now(timezone.utc)
            _LOGGER.warning("Marking installation %s as unavailable", self._installation_id)
        
        # Calculate next check interval with exponential backoff
        self._availability_check_count += 1
        backoff_minutes = min(
            self._base_check_interval.total_seconds() / 60 * (2 ** (self._availability_check_count - 1)),
            self._max_check_interval.total_seconds() / 60
        )
        self._next_availability_check = datetime.now() + timedelta(minutes=backoff_minutes)
        _LOGGER.debug(
            "Next availability check for %s in %.1f minutes", 
            self._installation_id, backoff_minutes
        )

    async def _check_installation_availability(self) -> None:
        """Check if a previously unavailable installation is back."""
        # Don't check too often
        now = datetime.now(timezone.utc)
        if hasattr(self, "_next_availability_check") and now < self._next_availability_check:
            return
            
        # Check if we should give up
        if self._unavailable_since:
            unavailable_days = (now - self._unavailable_since).days
            if unavailable_days > self._max_unavailable_days:
                return

        try:
            session = async_get_clientsession(self.hass)
            response = await session.get(
                f"{self._smarthome_base_url}/installations/{self._installation_id}/addresses",
                headers={"ApiKey": self._api_key},
                timeout=aiohttp.ClientTimeout(total=10),
            )
            
            if response.status == 200:
                _LOGGER.info("Installation %s is back online!", self._installation_id)
                self._installation_available = True
                self._unavailable_since = None
                self._availability_check_count = 0
                # Trigger update
                await self.async_update()
            else:
                # Still unavailable, schedule next check
                await self._handle_installation_unavailable()
                
        except Exception as err:
            await self._handle_installation_unavailable()

    def _get_current_check_interval(self) -> timedelta:
        """Calculate current check interval using exponential backoff."""
        multiplier = 2 ** self._availability_check_count
        interval = self._base_check_interval * multiplier
        return min(interval, self._max_check_interval)
