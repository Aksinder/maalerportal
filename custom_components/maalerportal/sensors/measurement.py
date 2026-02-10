"""Standard measurement sensors for Målerportal integration."""
from __future__ import annotations

import logging
from datetime import timedelta

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorStateClass,
)
from homeassistant.const import (
    UnitOfEnergy,
    UnitOfVolume,
    UnitOfTemperature,
    UnitOfTime,
    UnitOfFrequency,
    UnitOfPower,
)

from ..const import DOMAIN
from ..coordinator import MaalerportalCoordinator
from .base import (
    MaalerportalCoordinatorSensor,
    MaalerportalPollingSensor,
)

_LOGGER = logging.getLogger(__name__)

# Import event firing function - will be available after __init__ loads
# We need to define the event name here or import it
EVENT_METER_UPDATED = f"{DOMAIN}_meter_updated"


class MaalerportalMainSensor(MaalerportalCoordinatorSensor):
    """Main sensor for primary meter counter."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the main sensor."""
        super().__init__(coordinator, counter)
        
        self._attr_unique_id = f"{self._installation_id}_main"
        
        # Set attributes based on counter type
        counter_type = counter.get("counterType", "").lower()
        if counter_type in ["coldwater", "hotwater"]:
            self._attr_translation_key = "meter_reading_water"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif counter_type in ["electricityfromgrid", "electricitytogrid"]:
            self._attr_translation_key = "meter_reading_electricity"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif counter_type == "heat":
            self._attr_translation_key = "meter_reading_heat"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        else:
            self._attr_translation_key = "meter_reading"
            self._attr_device_class = None
            self._attr_native_unit_of_measurement = counter.get("unit")
            self._attr_state_class = SensorStateClass.MEASUREMENT

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update main sensor from primary counter."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    # Ensure positive value for meters
                    if value < 0:
                        _LOGGER.warning("Negative meter value received, taking absolute value: %s", value)
                        value = abs(value)
                    
                    # Check if value changed before firing event
                    old_value = self._attr_native_value
                    self._attr_native_value = value
                    _LOGGER.debug("Updated main sensor value: %s %s", value, counter.get("unit", ""))
                    
                    # Fire event if value changed and hass is available
                    if self.hass and (old_value is None or old_value != value):
                        self.hass.bus.fire(EVENT_METER_UPDATED, {
                            "installation_id": self._installation_id,
                            "meter_value": value,
                            "unit": counter.get("unit", ""),
                            "counter_type": counter.get("counterType", ""),
                            "timestamp": counter.get("latestTimestamp"),
                        })
                        _LOGGER.debug("Fired %s event for installation %s", EVENT_METER_UPDATED, self._installation_id)
                break


class MaalerportalBasicSensor(MaalerportalPollingSensor):
    """Basic fallback sensor when meter data is not available."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the basic sensor."""
        super().__init__(installation, api_key, smarthome_base_url, polling_interval=polling_interval)
        
        self._attr_unique_id = f"{self._installation_id}_basic"
        
        # Set default attributes based on installation type
        installation_type = self._installation_type.lower()
        if installation_type in ["coldwater", "hotwater"]:
            self._attr_translation_key = "meter_reading_water"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif installation_type == "electricity":
            self._attr_translation_key = "meter_reading_electricity"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif installation_type == "heat":
            self._attr_translation_key = "meter_reading_heat"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        else:
            self._attr_translation_key = "meter_reading"
            self._attr_device_class = None
            self._attr_native_unit_of_measurement = None
            self._attr_state_class = SensorStateClass.MEASUREMENT

    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update basic sensor from first available counter."""
        if meter_counters:
            # Find primary counter or use first one
            primary_counter = None
            for counter in meter_counters:
                if counter.get("isPrimary", False):
                    primary_counter = counter
                    break
            
            if not primary_counter:
                primary_counter = meter_counters[0]
            
            value = self._parse_counter_value(primary_counter)
            if value is not None:
                if value < 0:
                    value = abs(value)
                
                self._attr_native_value = value
                _LOGGER.debug("Updated basic sensor value: %s %s", value, primary_counter.get("unit", ""))


class MaalerportalBatterySensor(MaalerportalCoordinatorSensor):
    """Battery days remaining sensor."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the battery sensor."""
        super().__init__(coordinator, counter)
        
        self._attr_translation_key = "battery_days"
        self._attr_unique_id = f"{self._installation_id}_battery_days"
        self._attr_device_class = SensorDeviceClass.DURATION
        self._attr_native_unit_of_measurement = UnitOfTime.DAYS
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:battery"

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update battery sensor."""
        for counter in meter_counters:
            if counter.get("counterType") == "BatteryDaysRemaining":
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = int(value)
                    _LOGGER.debug("Updated battery days: %s", value)
                break


class MaalerportalTemperatureSensor(MaalerportalCoordinatorSensor):
    """Ambient temperature sensor."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the temperature sensor."""
        super().__init__(coordinator, counter)
        
        counter_type = counter.get("counterType", "")
        if "Max" in counter_type:
            self._attr_translation_key = "max_ambient_temperature"
            self._attr_unique_id = f"{self._installation_id}_temp_ambient_max"
        else:
            self._attr_translation_key = "min_ambient_temperature"
            self._attr_unique_id = f"{self._installation_id}_temp_ambient_min"
            
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update temperature sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 1)
                    _LOGGER.debug("Updated ambient temperature: %s°C", value)
                break


class MaalerportalWaterTemperatureSensor(MaalerportalCoordinatorSensor):
    """Water temperature sensor."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the water temperature sensor."""
        super().__init__(coordinator, counter)
        
        counter_type = counter.get("counterType", "")
        if "Max" in counter_type:
            self._attr_translation_key = "max_water_temperature"
            self._attr_unique_id = f"{self._installation_id}_temp_water_max"
        else:
            self._attr_translation_key = "min_water_temperature"
            self._attr_unique_id = f"{self._installation_id}_temp_water_min"
            
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update water temperature sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 1)
                    _LOGGER.debug("Updated water temperature: %s°C", value)
                break


class MaalerportalFlowSensor(MaalerportalCoordinatorSensor):
    """Flow sensor."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the flow sensor."""
        super().__init__(coordinator, counter)
        
        counter_type = counter.get("counterType", "")
        if "Max" in counter_type:
            self._attr_translation_key = "max_flow"
            self._attr_unique_id = f"{self._installation_id}_flow_max"
        else:
            self._attr_translation_key = "min_flow"
            self._attr_unique_id = f"{self._installation_id}_flow_min"
            
        self._attr_native_unit_of_measurement = "L/h"
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:water-pump"

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update flow sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 3)
                    _LOGGER.debug("Updated flow: %s L/h", value)
                break


class MaalerportalNoiseSensor(MaalerportalCoordinatorSensor):
    """Acoustic noise sensor."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the noise sensor."""
        super().__init__(coordinator, counter)
        
        self._attr_translation_key = "acoustic_noise"
        self._attr_unique_id = f"{self._installation_id}_acoustic_noise"
        self._attr_native_unit_of_measurement = UnitOfFrequency.HERTZ
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:volume-high"

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update noise sensor."""
        for counter in meter_counters:
            if counter.get("counterType") == "AcousticNoise":
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = int(value)
                    _LOGGER.debug("Updated acoustic noise: %s Hz", value)
                break


class MaalerportalSecondarySensor(MaalerportalCoordinatorSensor):
    """Secondary meter sensor."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the secondary sensor."""
        super().__init__(coordinator, counter)
        
        counter_type = counter.get("counterType", "")
        
        # Set translation key based on counter type
        if counter_type == "ColdWater":
            self._attr_translation_key = "cold_water"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
        elif counter_type == "HotWater":
            self._attr_translation_key = "hot_water"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
        elif counter_type == "ElectricityFromGrid":
            self._attr_translation_key = "electricity_import"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        elif counter_type == "ElectricityToGrid":
            self._attr_translation_key = "electricity_export"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        elif counter_type == "Heat":
            self._attr_translation_key = "heat"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        else:
            self._attr_translation_key = "meter_reading"
            self._attr_device_class = None
            self._attr_native_unit_of_measurement = counter.get("unit")
        
        self._attr_unique_id = f"{self._installation_id}_{counter_type.lower()}_secondary"
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update secondary sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    if value < 0:
                        value = abs(value)
                    
                    # Check if value changed before firing event
                    old_value = self._attr_native_value
                    self._attr_native_value = value
                    _LOGGER.debug("Updated secondary sensor %s: %s %s", 
                                self._counter.get("counterType"), value, counter.get("unit", ""))
                    
                    # Fire event if value changed and hass is available
                    if self.hass and (old_value is None or old_value != value):
                        self.hass.bus.fire(EVENT_METER_UPDATED, {
                            "installation_id": self._installation_id,
                            "meter_value": value,
                            "unit": counter.get("unit", ""),
                            "counter_type": counter.get("counterType", ""),
                            "timestamp": counter.get("latestTimestamp"),
                        })
                break


class MaalerportalSupplyTempSensor(MaalerportalCoordinatorSensor):
    """Supply/flow temperature sensor for heat meters."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the supply temperature sensor."""
        super().__init__(coordinator, counter)
        
        self._attr_translation_key = "supply_temperature"
        self._attr_unique_id = f"{self._installation_id}_temp_supply"
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:thermometer-chevron-up"

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update supply temperature sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 1)
                    _LOGGER.debug("Updated supply temperature: %s°C", value)
                break


class MaalerportalReturnTempSensor(MaalerportalCoordinatorSensor):
    """Return temperature sensor for heat meters."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the return temperature sensor."""
        super().__init__(coordinator, counter)
        
        self._attr_translation_key = "return_temperature"
        self._attr_unique_id = f"{self._installation_id}_temp_return"
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:thermometer-chevron-down"

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update return temperature sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 1)
                    _LOGGER.debug("Updated return temperature: %s°C", value)
                break


class MaalerportalTempDiffSensor(MaalerportalCoordinatorSensor):
    """Temperature difference sensor for heat meters."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the temperature difference sensor."""
        super().__init__(coordinator, counter)
        
        self._attr_translation_key = "temperature_difference"
        self._attr_unique_id = f"{self._installation_id}_temp_diff"
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:thermometer-lines"

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update temperature difference sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 1)
                    _LOGGER.debug("Updated temperature difference: %s°C", value)
                break


class MaalerportalHeatPowerSensor(MaalerportalCoordinatorSensor):
    """Heat power/effect sensor for heat meters."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the heat power sensor."""
        super().__init__(coordinator, counter)
        
        self._attr_translation_key = "heat_power"
        self._attr_unique_id = f"{self._installation_id}_heat_power"
        self._attr_device_class = SensorDeviceClass.POWER
        self._attr_native_unit_of_measurement = UnitOfPower.KILO_WATT
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:fire"

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update heat power sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 2)
                    _LOGGER.debug("Updated heat power: %s kW", value)
                break


class MaalerportalHeatVolumeSensor(MaalerportalCoordinatorSensor):
    """Heat volume sensor for heat meters."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the heat volume sensor."""
        super().__init__(coordinator, counter)
        
        self._attr_translation_key = "heat_volume"
        self._attr_unique_id = f"{self._installation_id}_heat_volume"
        self._attr_device_class = SensorDeviceClass.WATER
        self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_icon = "mdi:water-thermometer"

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update heat volume sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 3)
                    _LOGGER.debug("Updated heat volume: %s m³", value)
                break
