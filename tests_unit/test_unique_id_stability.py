"""Guard: entity unique_ids must not depend on meter serial / counter id.

History — both short-term states and long-term statistics — is keyed on
entity_id, which Home Assistant derives from each entity's unique_id. A meter
swap changes the meterSerial and every meterCounterId, but the installation
(address) stays the same. If any unique_id embedded the serial or a counter
id, a swap would register brand-new entities and orphan all history for that
address.

This is a source-level guard (no Home Assistant dependency, so it lives in the
committed tests_unit suite): every ``self._attr_unique_id`` assignment must be
scoped to the installation and must never reference a meter-specific
identifier. The full behavioural counterpart lives in tests/test_sensor.py
(``test_unique_ids_stable_across_meter_swap``), which runs under the HA test
harness.

Run with:
    pytest tests_unit/
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

_PKG = Path(__file__).resolve().parent.parent / "custom_components" / "maalerportal"
_SOURCE_FILES = [_PKG / "binary_sensor.py", *sorted((_PKG / "sensors").glob("*.py"))]

# Matches a real assignment `self._attr_unique_id = <expr>` (not reads, where
# _attr_unique_id appears on the right-hand side).
_ASSIGN_RE = re.compile(r"self\._attr_unique_id\s*=\s*(?P<expr>.+)")

# Identifiers that change on a meter swap and must never appear in a unique_id.
_FORBIDDEN = (
    "meterSerial",
    "meter_serial",
    "meterCounterId",
    "counter_id",
    "counterId",
    "serial",
)


def _unique_id_assignments() -> list[tuple[str, int, str]]:
    found: list[tuple[str, int, str]] = []
    for path in _SOURCE_FILES:
        if not path.exists():
            continue
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            match = _ASSIGN_RE.search(line)
            if match:
                found.append((path.name, lineno, match.group("expr").strip()))
    return found


def test_unique_id_assignments_exist() -> None:
    """Sanity: the guard below actually has something to check."""
    assert _unique_id_assignments(), "No self._attr_unique_id assignments found"


@pytest.mark.parametrize(
    "filename,lineno,expr",
    _unique_id_assignments(),
    ids=lambda v: v if isinstance(v, str) else str(v),
)
def test_unique_id_is_installation_scoped(filename: str, lineno: int, expr: str) -> None:
    assert "_installation_id" in expr, (
        f"{filename}:{lineno}: unique_id must be scoped to the installation, "
        f"got: {expr}"
    )
    for token in _FORBIDDEN:
        assert token not in expr, (
            f"{filename}:{lineno}: unique_id must not depend on '{token}' "
            f"(it changes on a meter swap and would orphan history): {expr}"
        )
