"""Price sensor for Målerportal integration."""
from __future__ import annotations

import logging

from homeassistant.components.sensor import SensorStateClass

from ..coordinator import MaalerportalCoordinator
from .base import MaalerportalCoordinatorSensor

_LOGGER = logging.getLogger(__name__)


class MaalerportalPriceSensor(MaalerportalCoordinatorSensor):
    """Price per unit sensor for primary counters."""

    def __init__(
        self, 
        coordinator: MaalerportalCoordinator,
        counter: dict,
    ) -> None:
        """Initialize the price sensor."""
        super().__init__(coordinator, counter)
        
        unit = counter.get("unit", "unit")
        self._attr_translation_key = "price_per_unit"
        self._attr_unique_id = f"{self._installation_id}_price_per_unit"
        self._attr_native_unit_of_measurement = f"kr/{unit}"
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:currency-usd"

    def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update price sensor."""
        for counter in meter_counters:
            if (counter.get("meterCounterId") == self._counter.get("meterCounterId") and 
                counter.get("isPrimary", False)):
                price_per_unit = counter.get("pricePerUnit")
                if price_per_unit is not None:
                    # Convert from øre to kroner (divide by 100)
                    price_in_kroner = price_per_unit / 100
                    self._attr_native_value = round(price_in_kroner, 4)
                    _LOGGER.debug("Updated price per unit: %s kr/%s", 
                                price_in_kroner, counter.get("unit", ""))
                break
