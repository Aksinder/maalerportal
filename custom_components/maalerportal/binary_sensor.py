"""Binary sensor platform for Målerportal — leak-detection alarm."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Optional

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import MaalerportalCoordinator

_LOGGER = logging.getLogger(__name__)

DEFAULT_NOISE_THRESHOLD_HZ = 30
DEFAULT_SUSTAINED_HOURS = 6
ACOUSTIC_NOISE_COUNTER_TYPE = "AcousticNoise"

CONF_NOISE_THRESHOLD = "acoustic_noise_threshold"
CONF_SUSTAINED_HOURS = "acoustic_noise_sustained_hours"
CONF_NOTIFY_ENABLED = "leak_notify_enabled"
CONF_NOTIFY_SERVICE = "leak_notify_service"
DEFAULT_NOTIFY_SERVICE = "persistent_notification.create"


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create one leak-alarm binary sensor per installation that exposes
    an AcousticNoise counter."""
    config_data = hass.data[DOMAIN][entry.entry_id]
    coordinators: dict[str, MaalerportalCoordinator] = config_data["coordinators"]

    threshold = float(
        entry.options.get(CONF_NOISE_THRESHOLD, DEFAULT_NOISE_THRESHOLD_HZ)
    )
    sustained_hours = float(
        entry.options.get(CONF_SUSTAINED_HOURS, DEFAULT_SUSTAINED_HOURS)
    )

    entities: list[BinarySensorEntity] = []
    for coordinator in coordinators.values():
        if not coordinator.data:
            continue
        for counter in coordinator.data.get("meterCounters", []):
            if counter.get("counterType") == ACOUSTIC_NOISE_COUNTER_TYPE:
                entities.append(
                    MaalerportalLeakAlarmSensor(
                        hass=hass,
                        entry=entry,
                        coordinator=coordinator,
                        counter=counter,
                        threshold_hz=threshold,
                        sustained_hours=sustained_hours,
                    )
                )
                break  # one alarm sensor per installation

    if entities:
        async_add_entities(entities)


class MaalerportalLeakAlarmSensor(
    CoordinatorEntity[MaalerportalCoordinator], BinarySensorEntity, RestoreEntity
):
    """Turns ON when acoustic noise has stayed at or above ``threshold_hz``
    for at least ``sustained_hours`` consecutive hours.

    Acoustic noise on a Kamstrup-style ultrasonic water meter rises when
    there is continuous flow somewhere downstream of the meter — i.e.,
    a leak. Brief spikes from normal usage drop the sensor back to OFF
    immediately.
    """

    _attr_device_class = BinarySensorDeviceClass.PROBLEM
    _attr_translation_key = "leak_alarm"
    _attr_has_entity_name = True

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        coordinator: MaalerportalCoordinator,
        counter: dict,
        threshold_hz: float,
        sustained_hours: float,
    ) -> None:
        super().__init__(coordinator)
        self._hass = hass
        self._entry = entry
        self._counter_id: str = counter["meterCounterId"]
        self._threshold_hz = threshold_hz
        self._sustained = timedelta(hours=sustained_hours)

        self._installation_id: str = coordinator.installation["installationId"]
        self._attr_unique_id = f"{self._installation_id}_leak_alarm"

        # Timestamp of the first elevated reading in the current run of
        # consecutively-elevated values. Reset to None when the noise
        # drops back below the threshold.
        self._first_elevated: Optional[datetime] = None
        self._latest_value: Optional[float] = None
        # Tracks the last observed alarm state so we can fire a one-shot
        # notification on the OFF -> ON transition (no spam on every poll).
        self._was_on: bool = False

        installation = coordinator.installation
        device_name = f"{installation['address']} - {installation['meterSerial']}"
        if installation.get("nickname"):
            device_name += f" ({installation['nickname']})"
        self._device_name = device_name

    @property
    def device_info(self) -> DeviceInfo:
        """Attach the alarm to the same device as the meter sensors."""
        return DeviceInfo(
            identifiers={(DOMAIN, self._installation_id)},
            name=self._device_name,
            manufacturer=self.coordinator.installation.get("utilityName"),
            serial_number=self.coordinator.installation.get("meterSerial"),
        )

    @property
    def is_on(self) -> Optional[bool]:
        """ON when elevated for the configured sustained duration."""
        if self._first_elevated is None:
            return False
        elapsed = datetime.now(timezone.utc) - self._first_elevated
        return elapsed >= self._sustained

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        attrs: dict[str, Any] = {
            "threshold_hz": self._threshold_hz,
            "sustained_hours": self._sustained.total_seconds() / 3600,
        }
        if self._latest_value is not None:
            attrs["current_value_hz"] = self._latest_value
        if self._first_elevated is not None:
            attrs["elevated_since"] = self._first_elevated.isoformat()
            elapsed = datetime.now(timezone.utc) - self._first_elevated
            attrs["elevated_for_hours"] = round(
                elapsed.total_seconds() / 3600, 2
            )
        return attrs

    async def async_added_to_hass(self) -> None:
        """Restore the elevated-since timestamp across restarts so a
        long-running leak isn't reset by every HA restart."""
        await super().async_added_to_hass()
        last_state = await self.async_get_last_state()
        if last_state is not None:
            if last_state.attributes:
                ts = last_state.attributes.get("elevated_since")
                if ts:
                    try:
                        self._first_elevated = datetime.fromisoformat(ts)
                    except (TypeError, ValueError):
                        self._first_elevated = None
            # Remember prior alarm state so a HA restart while the alarm
            # is already ON doesn't trigger a duplicate notification.
            self._was_on = last_state.state == "on"
        if self.coordinator.data:
            self._update_from_coordinator()

    @callback
    def _handle_coordinator_update(self) -> None:
        self._update_from_coordinator()
        is_on_now = bool(self.is_on)
        if is_on_now and not self._was_on:
            self._hass.async_create_task(self._async_fire_notification())
        self._was_on = is_on_now
        self.async_write_ha_state()

    async def _async_fire_notification(self) -> None:
        """Send a one-shot notification on OFF -> ON transition.

        Opt-in via the integration's options. The user picks a service
        like ``persistent_notification.create`` (default, always works
        without setup) or ``notify.mobile_app_<device>`` for push.
        Failures here are logged but never propagate — a misconfigured
        notify service must not break the alarm itself.
        """
        if not self._entry.options.get(CONF_NOTIFY_ENABLED, False):
            return
        target = self._entry.options.get(CONF_NOTIFY_SERVICE, DEFAULT_NOTIFY_SERVICE)
        domain, _, service = target.partition(".")
        if not domain or not service:
            _LOGGER.warning(
                "Invalid leak notify service %r — expected 'domain.service'", target
            )
            return

        title = "Misstänkt vattenläcka"
        elevated_for = ""
        if self._first_elevated is not None:
            hours = (
                datetime.now(timezone.utc) - self._first_elevated
            ).total_seconds() / 3600
            elevated_for = f" (förhöjt brus i {hours:.1f} h)"
        message = (
            f"{self._device_name}: akustiskt brus {self._latest_value} Hz "
            f"≥ tröskel {self._threshold_hz} Hz{elevated_for}. "
            "Kontrollera om något läcker."
        )

        service_data: dict[str, Any] = {"title": title, "message": message}
        if domain == "persistent_notification" and service == "create":
            # Stable id so re-firing replaces instead of stacking up.
            service_data["notification_id"] = self._attr_unique_id

        try:
            await self._hass.services.async_call(
                domain, service, service_data, blocking=False
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning(
                "Could not send leak notification via %s.%s: %s",
                domain,
                service,
                err,
            )

    def _update_from_coordinator(self) -> None:
        if not self.coordinator.data:
            return
        for counter in self.coordinator.data.get("meterCounters", []):
            if counter.get("meterCounterId") != self._counter_id:
                continue
            raw = counter.get("latestValue")
            if raw is None:
                return
            try:
                value = float(raw)
            except (TypeError, ValueError):
                return
            self._latest_value = value
            if value >= self._threshold_hz:
                if self._first_elevated is None:
                    self._first_elevated = datetime.now(timezone.utc)
            else:
                if self._first_elevated is not None:
                    _LOGGER.debug(
                        "Acoustic noise on %s dropped below threshold "
                        "(%.1f < %.1f Hz) — clearing elevated state.",
                        self._installation_id,
                        value,
                        self._threshold_hz,
                    )
                self._first_elevated = None
            return
