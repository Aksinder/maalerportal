"""Global fixtures for Målerportal integration tests."""
import pytest
from unittest.mock import patch

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
    with patch("custom_components.maalerportal.config_flow.check_auth_methods"), \
         patch("custom_components.maalerportal.config_flow.attempt_login"), \
         patch("custom_components.maalerportal.config_flow.get_api_key"), \
         patch("custom_components.maalerportal.config_flow.test_api_connection"):
        yield
