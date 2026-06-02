"""Global fixtures for Målerportal integration tests."""
import threading
from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Thread name patterns that are harmless daemon threads (platform-specific)
_ALLOWED_DAEMON_THREAD_PATTERNS = ("_run_safe_shutdown_loop",)


class _MockResponse:
    """Stand-in for an aiohttp response.

    The integration calls the session both ways — ``resp = await session.x()``
    and ``async with session.x() as resp`` — so this object is awaitable *and*
    an async context manager, yielding itself either way.
    """

    def __init__(self, json_data):
        self.status = 200
        self.ok = True
        self.json = AsyncMock(return_value=json_data)
        self.text = AsyncMock(return_value="")
        self.raise_for_status = MagicMock()

    def __await__(self):
        async def _yield_self():
            return self

        return _yield_self().__await__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_teardown(item, nextitem):
    """Filter harmless daemon threads from threading.enumerate during teardown.

    On macOS + Python 3.12, asyncio shutdown may leave behind a daemon thread
    called _run_safe_shutdown_loop. The pytest-homeassistant-custom-component
    verify_cleanup fixture rejects any unexpected thread. We wrap enumerate()
    so these daemon threads are invisible to the check.
    """
    original_enumerate = threading.enumerate

    def _filtered_enumerate():
        return [
            t
            for t in original_enumerate()
            if not (
                t.daemon
                and any(p in t.name for p in _ALLOWED_DAEMON_THREAD_PATTERNS)
            )
        ]

    threading.enumerate = _filtered_enumerate
    try:
        yield
    finally:
        threading.enumerate = original_enumerate


# This fixture enables loading custom integrations in all tests.
# Usually to enable custom integrations, you need to add this fixture
# or use pytest-homeassistant-custom-component.
@pytest.fixture(autouse=True)
async def auto_enable_custom_integrations(enable_custom_integrations):
    """Enable custom integrations defined in the test dir."""
    yield


# Prepare a bypass for authentication calls
@pytest.fixture(name="bypass_auth")
def bypass_auth_fixture():
    """Skip authentication calls to Målerportal API."""
    with (
        patch(
            "custom_components.maalerportal.config_flow.check_auth_methods",
            new_callable=AsyncMock,
        ),
        patch(
            "custom_components.maalerportal.config_flow.attempt_login",
            new_callable=AsyncMock,
        ),
        patch(
            "custom_components.maalerportal.config_flow.get_api_key",
            new_callable=AsyncMock,
        ),
        patch(
            "custom_components.maalerportal.config_flow.test_api_connection",
            new_callable=AsyncMock,
        ),
    ):
        yield


@pytest.fixture(name="mock_aiohttp", autouse=True)
def mock_aiohttp_fixture():
    """Mock aiohttp clientsession."""
    with (
        patch(
            "homeassistant.helpers.aiohttp_client.async_get_clientsession"
        ) as mock_hass_session,
        patch("aiohttp.ClientSession") as mock_session,
    ):
        # The only un-mocked endpoint hit in the suite is GET /addresses
        # (coordinators are patched/mocked in the tests that need them).
        # It must return a list of address dicts each carrying an
        # `installations` list — the shape `_fetch_fresh_installations`
        # and the config flow both consume. Installation "123" matches the
        # configured installation in test_sensor so reconciliation keeps it.
        mock_response = _MockResponse(
            [
                {
                    "address": "Test Address",
                    "timezone": "Europe/Copenhagen",
                    "installations": [
                        {
                            "installationId": "123",
                            "installationType": "Electricity",
                            "utilityName": "Test Util",
                            "meterSerial": "M123",
                            "nickname": "",
                        }
                    ],
                }
            ]
        )

        # get/post are plain MagicMocks returning the response synchronously;
        # _MockResponse itself handles both the `await session.x(...)` and
        # `async with session.x(...)` call styles the integration uses.
        session = MagicMock()
        session.get = MagicMock(return_value=mock_response)
        session.post = MagicMock(return_value=mock_response)

        # Support context manager for the session itself
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)

        mock_hass_session.return_value = session
        mock_session.return_value = session
        yield session


@pytest.fixture(autouse=True)
def mock_recorder():
    """Mock recorder."""
    with (
        patch(
            "homeassistant.components.recorder.get_instance"
        ) as mock_get_instance,
        patch(
            "homeassistant.components.recorder.statistics.get_last_statistics"
        ),
        patch(
            "homeassistant.components.recorder.statistics.async_import_statistics"
        ),
    ):
        mock_instance = MagicMock()
        mock_instance.async_add_executor_job = AsyncMock()
        mock_get_instance.return_value = mock_instance
        yield
