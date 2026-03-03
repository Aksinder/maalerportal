"""Global fixtures for Målerportal integration tests."""
import asyncio
import datetime
import threading
from collections.abc import Generator

import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from homeassistant.util import dt as dt_util

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
    with patch("custom_components.maalerportal.config_flow.check_auth_methods", new_callable=AsyncMock), \
         patch("custom_components.maalerportal.config_flow.attempt_login", new_callable=AsyncMock), \
         patch("custom_components.maalerportal.config_flow.get_api_key", new_callable=AsyncMock), \
         patch("custom_components.maalerportal.config_flow.test_api_connection", new_callable=AsyncMock):
        yield

@pytest.fixture(name="mock_aiohttp", autouse=True)
def mock_aiohttp_fixture():
    """Mock aiohttp clientsession."""
    with patch("homeassistant.helpers.aiohttp_client.async_get_clientsession") as mock_hass_session, \
         patch("aiohttp.ClientSession") as mock_session:

        # Create response as AsyncMock to support await response.json()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.ok = True
        mock_response.json = AsyncMock(return_value={"readings": []})

        # Support context manager protocol: async with session.get(...) as response:
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        # Create session as MagicMock.
        # session.get must be a regular MagicMock (not AsyncMock) because some code uses
        # `async with session.get(...) as resp:` which requires a synchronous call returning
        # an async context manager (not a coroutine).
        # session.post is AsyncMock because it's used as `resp = await session.post(...)`.
        session = MagicMock()
        session.get.return_value = mock_response
        session.post = AsyncMock(return_value=mock_response)

        # Also support context manager for the session itself
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)

        mock_hass_session.return_value = session
        mock_session.return_value = session
        yield session


@pytest.fixture(autouse=True)
def verify_cleanup(
    event_loop: asyncio.AbstractEventLoop,
    expected_lingering_tasks: bool,
    expected_lingering_timers: bool,
) -> Generator[None]:
    """Override plugin's verify_cleanup to also allow daemon threads.

    The default verify_cleanup rejects any non-DummyThread / non-waitpid thread.
    On macOS + Python 3.12, event loop shutdown can leave behind a daemon thread
    called ``_run_safe_shutdown_loop`` which is harmless.
    """
    threads_before = frozenset(threading.enumerate())
    tasks_before = asyncio.all_tasks(event_loop)
    yield

    event_loop.run_until_complete(event_loop.shutdown_default_executor())

    # Warn and clean-up lingering tasks and timers
    tasks = asyncio.all_tasks(event_loop) - tasks_before
    for task in tasks:
        if expected_lingering_tasks:
            pass
        else:
            pytest.fail(f"Lingering task after test {task!r}")
        task.cancel()
    if tasks:
        event_loop.run_until_complete(asyncio.wait(tasks))

    # Verify no non-daemon threads were left behind.
    threads = frozenset(threading.enumerate()) - threads_before
    for thread in threads:
        if thread.daemon:
            continue  # Daemon threads are harmless
        assert isinstance(thread, threading._DummyThread) or thread.name.startswith(
            "waitpid-"
        )

    try:
        assert dt_util.DEFAULT_TIME_ZONE is datetime.UTC
    finally:
        dt_util.DEFAULT_TIME_ZONE = datetime.UTC


@pytest.fixture(autouse=True)
def mock_recorder():
    """Mock recorder."""
    with patch("homeassistant.components.recorder.get_instance") as mock_get_instance, \
         patch("homeassistant.components.recorder.statistics.get_last_statistics"), \
         patch("homeassistant.components.recorder.statistics.async_import_statistics"):

        # Configure instance to support awaited methods
        mock_instance = MagicMock()
        mock_instance.async_add_executor_job = AsyncMock()
        mock_get_instance.return_value = mock_instance

        yield
