"""Platform for Målerportal sensor integration."""

import logging
from datetime import timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.entity import SensorEntity

from .const import DOMAIN, DEFAULT_POLLING_INTERVAL
from .coordinator import MaalerportalCoordinator
from .sensors import (
    MaalerportalMainSensor,
    MaalerportalStatisticSensor,
    MaalerportalConsumptionSensor,
    MaalerportalPriceSensor,
    MaalerportalBatterySensor,
    MaalerportalTemperatureSensor,
    MaalerportalWaterTemperatureSensor,
    MaalerportalFlowSensor,
    MaalerportalNoiseSensor,
    MaalerportalSupplyTempSensor,
    MaalerportalReturnTempSensor,
    MaalerportalTempDiffSensor,
    MaalerportalHeatPowerSensor,
    MaalerportalHeatVolumeSensor,
    MaalerportalSecondarySensor,
)

_LOGGER = logging.getLogger(__name__)


def get_polling_interval(config: ConfigEntry) -> timedelta:
    """Get polling interval from config options."""
    interval = config.options.get("polling_interval", DEFAULT_POLLING_INTERVAL)
    return timedelta(minutes=interval)


async def async_setup_entry(
    hass: HomeAssistant, config: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up the sensor platform."""
    config_data = hass.data[DOMAIN][config.entry_id]
    
    # Get coordinators
    coordinators = config_data.get("coordinators", {})
    sensors = []
    
    for installation_id, coordinator in coordinators.items():
        # Get data directly from coordinator
        meter_counters = []
        if coordinator.data:
            meter_counters = coordinator.data.get("meterCounters", [])
            
        # Create sensors based on available meter counters
        installation_sensors = create_sensors_from_counters(
            coordinator, meter_counters
        )
        sensors.extend(installation_sensors)
        
    async_add_entities(sensors)
    
    # Store sensor references so services can access them
    config_data["sensors"] = sensors


def create_sensors_from_counters(
    coordinator: MaalerportalCoordinator, meter_counters: list[dict]
) -> list[SensorEntity]:
    """Create appropriate sensors based on available meter counters."""
    sensors = []
    
    # Consumable counter types:
    # - For readingType="counter": Only meter reading sensor (direct from meter)
    # - For readingType="consumption": Statistics sensor (Energy Dashboard) + Consumption sensor
    consumable_counter_types = [
        "ColdWater", "HotWater", "ElectricityFromGrid", "ElectricityToGrid", "Heat"
    ]
    
    # Heat meter counter types
    supply_temp_types = ["SupplyTemp", "FlowTemp", "T1", "SupplyTemperature"]
    return_temp_types = ["ReturnTemp", "T2", "ReturnTemperature"]
    temp_diff_types = ["TempDiff", "DeltaT", "TemperatureDifference"]
    heat_power_types = ["Power", "Effect", "HeatPower"]
    heat_volume_types = ["Volume", "V1", "HeatVolume"]
    
    # Find primary counter for main sensor
    primary_counter = None
    for counter in meter_counters:
        if counter.get("isPrimary", False):
            primary_counter = counter
            break
    
    if not primary_counter and meter_counters:
        primary_counter = meter_counters[0]
    
    # Helper to unpack coordinator for PollingSensors
    installation = coordinator.installation
    api_key = coordinator.api_key
    base_url = coordinator.base_url
    interval = coordinator.update_interval

    # Create sensors for primary counter based on reading type
    if primary_counter:
        reading_type = primary_counter.get("readingType", "counter").lower()
        counter_type = primary_counter.get("counterType", "")
        
        if reading_type == "consumption":
            # For consumption-based meters (e.g., electricity from grid operator):
            # Create statistics sensor for Energy Dashboard + consumption sensor for 30-day usage
            if counter_type in consumable_counter_types:
                sensors.append(MaalerportalStatisticSensor(
                    installation, api_key, base_url, primary_counter, interval
                ))
                sensors.append(MaalerportalConsumptionSensor(
                    installation, api_key, base_url, primary_counter, interval
                ))
        else:
            # For counter-based meters (cumulative readings):
            # Create main sensor showing current meter reading
            sensors.append(MaalerportalMainSensor(
                coordinator, primary_counter
            ))
            # Also add statistics sensor to load historical data (last 30 days)
            if counter_type in consumable_counter_types:
                sensors.append(MaalerportalStatisticSensor(
                    installation, api_key, base_url, primary_counter, interval
                ))
        
        # Add price sensor for primary counter if price is available
        if primary_counter.get("pricePerUnit") is not None:
            sensors.append(MaalerportalPriceSensor(
                coordinator, primary_counter
            ))
    
    # Create additional sensors for other counter types
    for counter in meter_counters:
        counter_type = counter.get("counterType", "")
        # is_primary = counter.get("isPrimary", False) # Unused
        
        if counter_type == "BatteryDaysRemaining":
            sensors.append(MaalerportalBatterySensor(
                coordinator, counter
            ))
        elif counter_type in ["DailyMaxAmbientTemp", "DailyMinAmbientTemp"]:
            sensors.append(MaalerportalTemperatureSensor(
                coordinator, counter
            ))
        elif counter_type in ["DailyMaxWaterTemp", "DailyMinWaterTemp"]:
            sensors.append(MaalerportalWaterTemperatureSensor(
                coordinator, counter
            ))
        elif counter_type in ["DailyMaxFlow1", "DailyMinFlow1"]:
            sensors.append(MaalerportalFlowSensor(
                coordinator, counter
            ))
        elif counter_type == "AcousticNoise":
            sensors.append(MaalerportalNoiseSensor(
                coordinator, counter
            ))
        # Heat meter specific sensors
        elif counter_type in supply_temp_types:
            sensors.append(MaalerportalSupplyTempSensor(
                coordinator, counter
            ))
        elif counter_type in return_temp_types:
            sensors.append(MaalerportalReturnTempSensor(
                coordinator, counter
            ))
        elif counter_type in temp_diff_types:
            sensors.append(MaalerportalTempDiffSensor(
                coordinator, counter
            ))
        elif counter_type in heat_power_types:
            sensors.append(MaalerportalHeatPowerSensor(
                coordinator, counter
            ))
        elif counter_type in heat_volume_types:
            sensors.append(MaalerportalHeatVolumeSensor(
                coordinator, counter
            ))
        elif counter != primary_counter and counter_type in consumable_counter_types:
            # Secondary meters - check if already handled as primary (loop logic covers it?)
            if counter == primary_counter:
                continue

            # Secondary meters - sensors depend on reading type
            reading_type = counter.get("readingType", "counter").lower()
            
            if reading_type == "consumption":
                # For consumption-based: statistics sensor + consumption sensor
                sensors.append(MaalerportalStatisticSensor(
                    installation, api_key, base_url, counter, interval
                ))
                sensors.append(MaalerportalConsumptionSensor(
                    installation, api_key, base_url, counter, interval
                ))
            else:
                # For counter-based: meter reading sensor + statistics for historical data
                sensors.append(MaalerportalSecondarySensor(
                    coordinator, counter
                ))
                sensors.append(MaalerportalStatisticSensor(
                    installation, api_key, base_url, counter, interval
                ))
    
    return sensors
