"""Standard measurement sensors for Målerportal integration."""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorStateClass,
)
from homeassistant.helpers.entity import EntityCategory
from homeassistant.util import dt as dt_util
from homeassistant.const import (
    UnitOfEnergy,
    UnitOfVolume,
    UnitOfVolumeFlowRate,
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

_SV_WEEKDAYS = ["mån", "tis", "ons", "tors", "fre", "lör", "sön"]


def _parse_api_timestamp(timestamp: str | None) -> datetime | None:
    """Parse an API timestamp into an aware datetime."""
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _localize_api_timestamp(timestamp: str | None) -> dict[str, str]:
    """Return local timestamp fields for user-facing table attributes."""
    parsed = _parse_api_timestamp(timestamp)
    if parsed is None:
        return {}
    local = dt_util.as_local(parsed)
    return {
        "timestamp": local.isoformat(),
        "timestamp_utc": parsed.astimezone(timezone.utc).isoformat(),
        "date": local.strftime("%Y-%m-%d"),
        "time": local.strftime("%H:%M"),
        "timezone": local.tzname() or "",
    }


def _numeric_value(value: Any) -> float | None:
    try:
        return float(str(value).replace(",", ".").strip())
    except (TypeError, ValueError):
        return None


def _delta_to_liters(delta: float, unit: str | None) -> float:
    normalized = (unit or "").lower()
    if normalized in {"m3", "m³", "m^3", "cubic_meter", "cubic_meters"}:
        return delta * 1000
    return delta


def _dashboard_usage_summary(readings: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Build app-style water usage summaries from cumulative readings."""
    rows: list[tuple[datetime, float, str]] = []
    for reading in readings:
        timestamp = _parse_api_timestamp(reading.get("timestamp"))
        value = _numeric_value(reading.get("value"))
        if timestamp is None or value is None:
            continue
        rows.append((timestamp, value, str(reading.get("unit") or "")))

    rows.sort(key=lambda row: row[0])
    if len(rows) < 2:
        return {}

    daily: dict[str, float] = defaultdict(float)
    hourly: dict[str, float] = defaultdict(float)
    for previous, current in zip(rows, rows[1:]):
        previous_ts, previous_value, _ = previous
        current_ts, current_value, unit = current
        if current_ts <= previous_ts:
            continue
        delta = current_value - previous_value
        if delta <= 0:
            continue
        liters = _delta_to_liters(delta, unit)
        local = dt_util.as_local(current_ts)
        local_hour = local.replace(minute=0, second=0, microsecond=0)
        daily[local.date().isoformat()] += liters
        hourly[local_hour.isoformat()] += liters

    if not daily:
        return {}

    today = dt_util.now().date()
    yesterday = today - timedelta(days=1)
    daily_bars = []
    for offset in range(13, -1, -1):
        day = today - timedelta(days=offset)
        value = round(daily.get(day.isoformat(), 0))
        daily_bars.append(
            {
                "date": day.isoformat(),
                "weekday": _SV_WEEKDAYS[day.weekday()],
                "day_month": f"{day.day}/{day.month}",
                "liters": value,
            }
        )

    latest_day = max(datetime.fromisoformat(key).date() for key in daily)
    hourly_bars = []
    for hour in range(24):
        local_hour = datetime(
            latest_day.year,
            latest_day.month,
            latest_day.day,
            hour,
            tzinfo=dt_util.DEFAULT_TIME_ZONE,
        )
        hourly_bars.append(
            {
                "hour": f"{hour:02d}",
                "liters": round(hourly.get(local_hour.isoformat(), 0)),
            }
        )

    today_liters = round(daily.get(today.isoformat(), 0))
    yesterday_liters = round(daily.get(yesterday.isoformat(), 0))
    last_7_liters = round(
        sum(daily.get((today - timedelta(days=offset)).isoformat(), 0) for offset in range(7))
    )
    previous_7_liters = round(
        sum(daily.get((today - timedelta(days=offset)).isoformat(), 0) for offset in range(7, 14))
    )
    day_diff = today_liters - yesterday_liters
    week_diff = last_7_liters - previous_7_liters

    return {
        "today_liters": today_liters,
        "yesterday_liters": yesterday_liters,
        "today_vs_yesterday_delta_liters": day_diff,
        "today_vs_yesterday_direction": "down" if day_diff < 0 else "up" if day_diff > 0 else "flat",
        "today_vs_yesterday_text": (
            "Samma som igår"
            if day_diff == 0
            else f"{'Mindre' if day_diff < 0 else 'Mer'} {_SV_WEEKDAYS[today.weekday()]} än {_SV_WEEKDAYS[yesterday.weekday()]}"
        ),
        "last_7_days_liters": last_7_liters,
        "previous_7_days_liters": previous_7_liters,
        "last_7_days_delta_liters": week_diff,
        "last_7_days_direction": "down" if week_diff < 0 else "up" if week_diff > 0 else "flat",
        "last_7_days_text": (
            "Samma som föregående 7 dagar"
            if week_diff == 0
            else f"{'Mindre' if week_diff < 0 else 'Mer'} än föregående 7 dagar"
        ),
        "daily_consumption": daily_bars,
        "hourly_consumption": hourly_bars,
        "daily_max_liters": max((item["liters"] for item in daily_bars), default=0),
        "hourly_max_liters": max((item["liters"] for item in hourly_bars), default=0),
    }


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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update main sensor from primary counter."""
        our_id = str(self._counter.get("meterCounterId") or "")
        for counter in meter_counters:
            counter_id = str(counter.get("meterCounterId") or "")
            _LOGGER.debug(
                "Updating %s: comparing counter %s with expected %s",
                self.entity_id,
                counter_id,
                our_id
            )
            if our_id and counter_id == our_id:
                _LOGGER.debug("ID match found for %s", counter_id)
                value = self._parse_counter_value(counter)
                if value is not None:
                    # Ensure positive value for meters
                    if value < 0:
                        _LOGGER.warning("Negative meter value received, taking absolute value: %s", value)
                        value = abs(value)
                    
                    # Check if value changed before firing event
                    old_value = self._attr_native_value
                    self._attr_native_value = value
                    _LOGGER.debug("Updated main sensor %s value: %s %s", self.entity_id, value, counter.get("unit", ""))
                    
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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
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
                _LOGGER.debug("Updated basic sensor %s value: %s %s", self.entity_id, value, primary_counter.get("unit", ""))


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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update battery sensor."""
        our_id = str(self._counter.get("meterCounterId") or "")
        for counter in meter_counters:
            counter_id = str(counter.get("meterCounterId") or "")
            counter_type = counter.get("counterType")
            
            _LOGGER.debug(
                "Checking counter %s for battery matching: type %s (expected BatteryDaysRemaining)",
                counter_id,
                counter_type
            )
            
            if counter_type == "BatteryDaysRemaining":
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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update temperature sensor."""
        our_id = str(self._counter.get("meterCounterId") or "")
        for counter in meter_counters:
            counter_id = str(counter.get("meterCounterId") or "")
            if our_id and counter_id == our_id:
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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update water temperature sensor."""
        our_id = str(self._counter.get("meterCounterId") or "")
        for counter in meter_counters:
            counter_id = str(counter.get("meterCounterId") or "")
            if our_id and counter_id == our_id:
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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update flow sensor."""
        our_id = str(self._counter.get("meterCounterId") or "")
        for counter in meter_counters:
            counter_id = str(counter.get("meterCounterId") or "")
            if our_id and counter_id == our_id:
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 3)
                    _LOGGER.debug("Updated flow: %s L/h", value)
                break


class MaalerportalCurrentFlowSensor(MaalerportalCoordinatorSensor):
    """Instantaneous flow rate (Flow1/Flow2) — current L/h passing the meter.

    Distinct from MaalerportalFlowSensor which surfaces only the daily
    min/max aggregates (DailyMinFlow1, DailyMaxFlow1). This one reflects
    the value right now: 0 means nothing is being used, non-zero means
    water is currently flowing somewhere downstream of the meter.
    """

    def __init__(
        self,
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        super().__init__(coordinator, counter)

        counter_type = counter.get("counterType", "")
        # Flow1 = primary inlet, Flow2 = secondary (e.g. hot/cold split)
        suffix = counter_type.lower()  # flow1 / flow2
        self._attr_translation_key = "current_flow"
        self._attr_unique_id = f"{self._installation_id}_current_{suffix}"

        self._attr_device_class = SensorDeviceClass.VOLUME_FLOW_RATE
        self._attr_native_unit_of_measurement = UnitOfVolumeFlowRate.LITERS_PER_HOUR
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:water-sync"

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update from coordinator data."""
        our_id = str(self._counter.get("meterCounterId") or "")
        for counter in meter_counters:
            counter_id = str(counter.get("meterCounterId") or "")
            if our_id and counter_id == our_id:
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 3)
                break


class MaalerportalLastReadingSensor(MaalerportalCoordinatorSensor):
    """User-facing sensor that surfaces the upstream meter's last-reading
    timestamp.

    HA renders this as a relative time ("3 hours ago") thanks to
    ``device_class=timestamp`` — letting users tell at a glance whether
    the meter is fresh or has gone quiet. Originally placed under
    Diagnostic; promoted to the main sensor section because it's a
    primary signal, not technical detail. (See migration in __init__
    that clears entity_category for legacy installs.)

    Surfaces the freshest timestamp seen across all counters on the
    installation; per-counter timestamps are exposed in attributes.
    """

    _attr_device_class = SensorDeviceClass.TIMESTAMP
    _attr_translation_key = "last_reading"
    _attr_icon = "mdi:clock-check-outline"

    def __init__(
        self,
        coordinator: MaalerportalCoordinator,
        recent_readings_count: int = 30,
    ) -> None:
        # No specific counter — we surface the freshest timestamp seen
        # across all counters on the installation.
        super().__init__(coordinator, counter=None)
        self._attr_unique_id = f"{self._installation_id}_last_reading"
        # How many recent raw readings to surface in the
        # recent_readings attribute. Configurable per integration entry
        # via Options → Settings.
        self._recent_readings_count = recent_readings_count

    def _handle_coordinator_update(self) -> None:
        """Force a state write on every coordinator refresh.

        The base class's _handle_coordinator_update only writes state when
        ``self._counter`` is set; this sensor passes counter=None because
        it aggregates timestamps across ALL counters. Without this
        override the entity's native_value property would still compute
        the current timestamp correctly, but HA never re-reads it — so
        the state appears frozen at whatever the first refresh produced.
        """
        self.async_write_ha_state()

    @property
    def native_value(self) -> datetime | None:
        if not self.coordinator.data:
            return None
        latest: datetime | None = None
        for counter in self.coordinator.data.get("meterCounters", []):
            ts = counter.get("latestTimestamp")
            if not ts:
                continue
            parsed = _parse_api_timestamp(ts)
            if parsed is None:
                continue
            if latest is None or parsed > latest:
                latest = parsed
        return latest

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Per-counter timestamps + recent_readings for table-style cards.

        ``recent_readings`` exposes the last 30 primary-counter readings
        with their original API timestamps so users can render a table
        of "date · time · value" via a markdown card without writing
        custom Python or pulling the CSV manually. Sourced from the
        per-installation ReadingsLog's in-memory buffer.
        """
        attrs: dict[str, Any] = {"installation_id": self._installation_id}
        if not self.coordinator.data:
            return attrs
        per_counter: dict[str, str] = {}
        primary_counter_id: str | None = None
        for counter in self.coordinator.data.get("meterCounters", []):
            counter_type = counter.get("counterType")
            ts = counter.get("latestTimestamp")
            if counter_type and ts:
                per_counter[counter_type] = _localize_api_timestamp(ts).get("timestamp", ts)
            if counter.get("isPrimary"):
                primary_counter_id = counter.get("meterCounterId")
        if per_counter:
            attrs["per_counter_timestamp"] = per_counter

        # Recent raw readings of the primary counter (e.g. ColdWater for
        # a water meter). Limited to 30 to keep state-history payload
        # reasonable; users wanting more can read the CSV directly.
        if primary_counter_id and self.hass:
            store = self.hass.data.get(DOMAIN, {})
            for entry_data in store.values():
                if not isinstance(entry_data, dict):
                    continue
                rl = entry_data.get("readings_logs", {}).get(self._installation_id)
                if rl is None:
                    continue
                summary_recent = rl.recent_readings(
                    counter_id=primary_counter_id,
                    n=1500,
                )
                if summary_recent:
                    attrs.update(_dashboard_usage_summary(summary_recent))
                recent = rl.recent_readings(
                    counter_id=primary_counter_id,
                    n=self._recent_readings_count,
                )
                if recent:
                    attrs["recent_readings"] = [
                        {
                            **_localize_api_timestamp(r.get("timestamp")),
                            "value": r.get("value"),
                            "unit": r.get("unit"),
                        }
                        for r in recent
                    ]
                break
        return attrs


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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
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

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update heat volume sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 3)
                    _LOGGER.debug("Updated heat volume: %s m³", value)
                break
