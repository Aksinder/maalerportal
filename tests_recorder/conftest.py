"""Fixtures for tests that use a real recorder.

The integration targets a recent Home Assistant (``StatisticMeanType`` and the
``mean_type`` / ``unit_class`` statistics metadata keys), but the version
pinned in ``requirements_test.txt`` predates them. The fixture below adapts the
old recorder so these tests can still exercise the real import path. What they
check is *which rows and sums are imported*, not Home Assistant's own metadata
handling.
"""
from __future__ import annotations

import enum

import pytest

_NEW_METADATA_KEYS = ("mean_type", "unit_class")


@pytest.fixture(autouse=True)
def _recorder_compat(monkeypatch: pytest.MonkeyPatch) -> None:
    from homeassistant.components.recorder import models, statistics

    if not hasattr(models, "StatisticMeanType"):
        monkeypatch.setattr(
            models,
            "StatisticMeanType",
            enum.Enum("StatisticMeanType", {"NONE": 0, "ARITHMETIC": 1, "CIRCULAR": 2}),
            raising=False,
        )

    original = statistics.async_import_statistics

    def _import(hass, metadata, stats, *args, **kwargs):
        legacy = {k: v for k, v in metadata.items() if k not in _NEW_METADATA_KEYS}
        return original(hass, legacy, stats, *args, **kwargs)

    monkeypatch.setattr(statistics, "async_import_statistics", _import)
