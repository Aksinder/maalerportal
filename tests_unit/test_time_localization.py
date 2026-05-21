"""Unit tests for local-time bucketing and dashboard summaries."""
from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
SWEDEN = ZoneInfo("Europe/Stockholm")


def _install_homeassistant_stubs() -> None:
    def enum_stub(**values):
        return type("EnumStub", (), values)

    homeassistant = types.ModuleType("homeassistant")
    components = types.ModuleType("homeassistant.components")
    sensor = types.ModuleType("homeassistant.components.sensor")
    sensor.SensorDeviceClass = enum_stub(
        WATER="water",
        ENERGY="energy",
        DURATION="duration",
        TEMPERATURE="temperature",
        VOLUME_FLOW_RATE="volume_flow_rate",
        TIMESTAMP="timestamp",
    )
    sensor.SensorStateClass = enum_stub(
        TOTAL_INCREASING="total_increasing",
        MEASUREMENT="measurement",
    )

    helpers = types.ModuleType("homeassistant.helpers")
    entity = types.ModuleType("homeassistant.helpers.entity")
    entity.EntityCategory = enum_stub(DIAGNOSTIC="diagnostic")
    aiohttp_client = types.ModuleType("homeassistant.helpers.aiohttp_client")
    aiohttp_client.async_get_clientsession = lambda hass: None
    restore_state = types.ModuleType("homeassistant.helpers.restore_state")
    restore_state.RestoreEntity = type("RestoreEntity", (), {})
    entity_registry = types.ModuleType("homeassistant.helpers.entity_registry")
    entity_registry.async_get = lambda hass: None

    const = types.ModuleType("homeassistant.const")
    const.UnitOfEnergy = enum_stub(KILO_WATT_HOUR="kWh")
    const.UnitOfVolume = enum_stub(CUBIC_METERS="m³")
    const.UnitOfVolumeFlowRate = enum_stub(LITERS_PER_HOUR="L/h")
    const.UnitOfTemperature = enum_stub(CELSIUS="°C")
    const.UnitOfTime = enum_stub(DAYS="d")
    const.UnitOfFrequency = enum_stub(HERTZ="Hz")
    const.UnitOfPower = enum_stub(WATT="W")

    util = types.ModuleType("homeassistant.util")
    dt = types.ModuleType("homeassistant.util.dt")
    dt.DEFAULT_TIME_ZONE = SWEDEN
    dt.as_local = lambda value: value.astimezone(SWEDEN)
    dt.now = lambda: datetime(2026, 5, 20, 12, 0, tzinfo=SWEDEN)

    sys.modules.update(
        {
            "homeassistant": homeassistant,
            "homeassistant.components": components,
            "homeassistant.components.sensor": sensor,
            "homeassistant.helpers": helpers,
            "homeassistant.helpers.entity": entity,
            "homeassistant.helpers.entity_registry": entity_registry,
            "homeassistant.helpers.aiohttp_client": aiohttp_client,
            "homeassistant.helpers.restore_state": restore_state,
            "homeassistant.const": const,
            "homeassistant.util": util,
            "homeassistant.util.dt": dt,
        }
    )

    aiohttp = types.ModuleType("aiohttp")
    aiohttp.ClientError = Exception
    aiohttp.ClientTimeout = lambda total=None: None
    sys.modules["aiohttp"] = aiohttp


def _install_package_stubs() -> None:
    custom_components = types.ModuleType("custom_components")
    maalerportal = types.ModuleType("custom_components.maalerportal")
    sensors = types.ModuleType("custom_components.maalerportal.sensors")
    custom_components.__path__ = [str(ROOT / "custom_components")]
    maalerportal.__path__ = [str(ROOT / "custom_components" / "maalerportal")]
    sensors.__path__ = [str(ROOT / "custom_components" / "maalerportal" / "sensors")]

    coordinator = types.ModuleType("custom_components.maalerportal.coordinator")
    coordinator.MaalerportalCoordinator = object

    base = types.ModuleType("custom_components.maalerportal.sensors.base")
    base.MaalerportalCoordinatorSensor = type("MaalerportalCoordinatorSensor", (), {})
    base.MaalerportalPollingSensor = type("MaalerportalPollingSensor", (), {})

    sys.modules.update(
        {
            "custom_components": custom_components,
            "custom_components.maalerportal": maalerportal,
            "custom_components.maalerportal.sensors": sensors,
            "custom_components.maalerportal.coordinator": coordinator,
            "custom_components.maalerportal.sensors.base": base,
        }
    )


def _load_module(name: str, path: Path):
    _install_homeassistant_stubs()
    _install_package_stubs()
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_api_timestamps_are_exposed_in_swedish_winter_and_summer_time():
    measurement = _load_module(
        "custom_components.maalerportal.sensors.measurement",
        ROOT / "custom_components" / "maalerportal" / "sensors" / "measurement.py",
    )

    winter = measurement._localize_api_timestamp("2026-01-15T10:00:00Z")
    summer = measurement._localize_api_timestamp("2026-05-11T10:00:00Z")

    assert winter["time"] == "11:00"
    assert winter["timezone"] == "CET"
    assert summer["time"] == "12:00"
    assert summer["timezone"] == "CEST"


def test_statistics_hour_start_buckets_by_local_wall_clock_then_returns_utc():
    history = _load_module(
        "custom_components.maalerportal.sensors.history",
        ROOT / "custom_components" / "maalerportal" / "sensors" / "history.py",
    )
    timestamp = datetime.fromisoformat("2026-05-11T10:05:00+00:00")

    bucket = history._statistics_hour_start(timestamp, "counter")

    assert bucket.isoformat() == "2026-05-11T09:00:00+00:00"


def test_dashboard_summary_builds_liter_deltas_by_local_day():
    measurement = _load_module(
        "custom_components.maalerportal.sensors.measurement",
        ROOT / "custom_components" / "maalerportal" / "sensors" / "measurement.py",
    )
    readings = [
        {"timestamp": "2026-05-18T22:00:00Z", "value": "5.000", "unit": "m³"},
        {"timestamp": "2026-05-19T10:00:00Z", "value": "5.100", "unit": "m³"},
        {"timestamp": "2026-05-19T22:00:00Z", "value": "5.100", "unit": "m³"},
        {"timestamp": "2026-05-20T10:00:00Z", "value": "5.159", "unit": "m³"},
    ]

    summary = measurement._dashboard_usage_summary(readings)

    assert summary["today_liters"] == 59
    assert summary["yesterday_liters"] == 100
    assert summary["today_vs_yesterday_direction"] == "down"
    assert summary["daily_consumption"][-1]["liters"] == 59
    assert summary["daily_consumption"][-2]["liters"] == 100
