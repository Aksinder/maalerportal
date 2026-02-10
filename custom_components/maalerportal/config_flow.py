"""Config flow for Målerportal integration."""
from __future__ import annotations

import logging
from typing import Any
import aiohttp

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv

from .const import (
    DOMAIN, 
    AUTH_BASE_URL, 
    ME_BASE_URL, 
    SMARTHOME_BASE_URL,
    DEFAULT_POLLING_INTERVAL,
    MIN_POLLING_INTERVAL,
    MAX_POLLING_INTERVAL,
)

_LOGGER = logging.getLogger(__name__)

# Data schemas
STEP_EMAIL_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): str,
    }
)

STEP_PASSWORD_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_PASSWORD): str,
    }
)

STEP_2FA_CODE_SCHEMA = vol.Schema(
    {
        vol.Required("code"): str,
    }
)

# Meter type translations (Danish as default, since it's a Danish product)
METER_TYPE_TRANSLATIONS = {
    "ColdWater": "Koldt vand",
    "HotWater": "Varmt vand",
    "Electricity": "El",
    "Heat": "Varme",
}


def get_meter_type_label(meter_type: str) -> str:
    """Get translated label for meter type."""
    return METER_TYPE_TRANSLATIONS.get(meter_type, meter_type)


async def check_auth_methods(
    session: aiohttp.ClientSession,
    email: str,
) -> dict[str, Any]:
    """Check which authentication methods are available for a user.
    
    Returns dict with:
    - {"exists": bool, "hasPassword": bool, "hasPasskey": bool}
    """
    response = await session.post(
        f"{AUTH_BASE_URL}/check-auth-methods",
        json={"emailAddress": email},
        headers={"Content-Type": "application/json"},
    )
    
    if not response.ok:
        # If endpoint doesn't exist or errors, assume user has password
        return {"exists": True, "hasPassword": True, "hasPasskey": False}
    
    return await response.json()


async def attempt_login(
    session: aiohttp.ClientSession,
    email: str,
    password: str,
    totp_code: str | None = None,
    two_factor_method: str | None = None,
) -> dict[str, Any]:
    """Attempt to login and return result dict.
    
    Returns dict with either:
    - {"success": True, "token": "...", "refresh_token": "..."}
    - {"requires_2fa": True, "available_methods": {"totp": bool, "email": bool}}
    - {"email_sent": True} for email OTP trigger
    - Raises InvalidAuth or CannotConnect on error
    """
    login_payload: dict[str, Any] = {
        "emailAddress": email,
        "password": password,
        "platform": "smarthome",
    }
    
    if totp_code:
        login_payload["token"] = totp_code
    if two_factor_method:
        login_payload["twoFactorMethod"] = two_factor_method
    
    _LOGGER.debug("Attempting login with payload keys: %s", list(login_payload.keys()))
    
    login_response = await session.post(
        f"{AUTH_BASE_URL}/login",
        json=login_payload,
        headers={"Content-Type": "application/json"},
    )
    
    _LOGGER.debug("Login response status: %s", login_response.status)
    
    # Try to parse response
    try:
        response_data = await login_response.json()
    except (ValueError, aiohttp.ContentTypeError):
        response_data = {}
    
    # Check if we got a token (success)
    if response_data.get("token"):
        return {
            "success": True,
            "token": response_data["token"],
            "refresh_token": response_data.get("refreshToken"),
        }
    
    # Check for 2FA required (HTTP 428)
    if login_response.status == 428:
        if response_data.get("twoFactorRequired"):
            return {
                "requires_2fa": True,
                "available_methods": response_data.get("availableMethods", {"totp": False, "email": False}),
            }
        raise InvalidAuth("Two-factor authentication required but no methods available")
    
    # Check for email OTP sent (HTTP 202)
    if login_response.status == 202:
        return {"email_sent": True}
    
    # Check for other errors
    if not login_response.ok:
        error_msg = response_data.get("message", "") or response_data.get("error", "")
        errors_list = response_data.get("errors", [])
        
        # Check for wrong password
        if "Wrong password" in str(errors_list) or "Wrong password" in error_msg:
            raise InvalidAuth("wrong_password")
        
        # Legacy TOTP required check
        if "OTP" in error_msg or "TOTP" in error_msg:
            return {
                "requires_2fa": True,
                "available_methods": {"totp": True, "email": False},
            }
        
        raise InvalidAuth(f"Login failed: HTTP {login_response.status} - {error_msg}")
    
    # Fallback - unexpected response
    raise InvalidAuth("Unexpected response from login endpoint")


async def get_api_key(session: aiohttp.ClientSession, jwt_token: str) -> str:
    """Get or create smarthome API key."""
    api_key_response = await session.post(
        f"{ME_BASE_URL}/smarthome-apikey",
        json={"description": "Home Assistant Integration"},
        headers={
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json",
        },
    )

    if not api_key_response.ok:
        raise InvalidAuth(f"Failed to get API key: HTTP {api_key_response.status}")

    api_key_data = await api_key_response.json()
    api_key = api_key_data.get("apiKey")

    if not api_key:
        raise InvalidAuth("No API key received")
    
    return api_key


async def test_api_connection(session: aiohttp.ClientSession, api_key: str) -> bool:
    """Test the API connection."""
    test_response = await session.get(
        f"{SMARTHOME_BASE_URL}/addresses",
        headers={"ApiKey": api_key},
    )

    if test_response.status == 429:
        raise CannotConnect("Rate limit exceeded. Please try again later.")
    elif not test_response.ok:
        error_text = await test_response.text()
        raise CannotConnect(f"API test failed: HTTP {test_response.status} - {error_text}")
    
    return True


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Målerportal."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._email: str = ""
        self._password: str = ""
        self._user_auth_methods: dict[str, Any] = {}
        self._available_2fa_methods: dict[str, bool] = {}
        self._selected_2fa_method: str = ""

    @staticmethod
    def async_get_options_flow(config_entry: config_entries.ConfigEntry) -> "OptionsFlow":
        """Get the options flow for this handler."""
        return OptionsFlow(config_entry)

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step - email input."""
        errors: dict[str, str] = {}
        
        if user_input is not None:
            self._email = user_input[CONF_EMAIL].strip()
            
            if not self._email:
                errors["base"] = "invalid_email"
            else:
                try:
                    async with aiohttp.ClientSession() as session:
                        # Check which auth methods are available
                        self._user_auth_methods = await check_auth_methods(session, self._email)
                        
                        if not self._user_auth_methods.get("exists", True):
                            errors["base"] = "user_not_found"
                        elif not self._user_auth_methods.get("hasPassword", True):
                            # User only has passkey - not supported in HA
                            errors["base"] = "passkey_only"
                        else:
                            # User has password - set unique ID and check for duplicates
                            await self.async_set_unique_id(self._email.lower())
                            self._abort_if_unique_id_configured()
                            # Proceed to password step
                            return await self.async_step_password()
                            
                except aiohttp.ClientError:
                    errors["base"] = "cannot_connect"
                except Exception:
                    _LOGGER.exception("Unexpected exception during auth method check")
                    errors["base"] = "unknown"

        return self.async_show_form(
            step_id="user", 
            data_schema=STEP_EMAIL_SCHEMA, 
            errors=errors,
            description_placeholders={"email": self._email} if self._email else None,
        )

    async def async_step_password(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the password input step."""
        errors: dict[str, str] = {}
        
        if user_input is not None:
            self._password = user_input[CONF_PASSWORD]
            
            try:
                async with aiohttp.ClientSession() as session:
                    # Attempt login with email and password
                    result = await attempt_login(session, self._email, self._password)
                    
                    if result.get("requires_2fa"):
                        # 2FA required - store available methods and proceed
                        self._available_2fa_methods = result.get("available_methods", {})
                        
                        has_totp = self._available_2fa_methods.get("totp", False)
                        has_email = self._available_2fa_methods.get("email", False)
                        
                        _LOGGER.debug("2FA required. TOTP: %s, Email: %s", has_totp, has_email)
                        
                        if has_totp and has_email:
                            # Both methods available - let user choose
                            return await self.async_step_2fa_choice()
                        elif has_totp:
                            # Only TOTP - go directly to TOTP code input
                            self._selected_2fa_method = "totp"
                            return await self.async_step_2fa_totp()
                        elif has_email:
                            # Only Email - trigger email send and go to email code input
                            self._selected_2fa_method = "email"
                            try:
                                await attempt_login(
                                    session, self._email, self._password,
                                    two_factor_method="email"
                                )
                            except Exception:
                                # Email send might return 428/202 which is expected
                                pass
                            return await self.async_step_2fa_email()
                        else:
                            errors["base"] = "no_2fa_methods"
                    elif result.get("success"):
                        # Login successful - get API key
                        jwt_token = result["token"]
                        api_key = await get_api_key(session, jwt_token)
                        await test_api_connection(session, api_key)
                        
                        # Clear password from memory after successful use
                        self._password = ""
                        
                        self.context.update({"apikey": api_key})
                        return await self.async_step_entity_selection()
                        
            except InvalidAuth as err:
                if "wrong_password" in str(err):
                    errors["base"] = "invalid_auth"
                else:
                    errors["base"] = "invalid_auth"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except aiohttp.ClientError:
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected exception during login")
                errors["base"] = "unknown"

        return self.async_show_form(
            step_id="password", 
            data_schema=STEP_PASSWORD_SCHEMA, 
            errors=errors,
            description_placeholders={"email": self._email},
        )

    async def async_step_2fa_choice(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle 2FA method selection menu."""
        # Show method choice menu - the menu options call async_step_totp or async_step_email
        return self.async_show_menu(
            step_id="2fa_choice",
            menu_options=["totp", "email"],
        )

    async def async_step_totp(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle TOTP selection from menu."""
        self._selected_2fa_method = "totp"
        return await self.async_step_2fa_totp()

    async def async_step_email(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle Email OTP selection from menu."""
        self._selected_2fa_method = "email"
        
        # Trigger email OTP send
        try:
            async with aiohttp.ClientSession() as session:
                await attempt_login(
                    session, self._email, self._password,
                    two_factor_method="email"
                )
        except Exception:
            # Email send might return 428/202 which is expected
            pass
        
        return await self.async_step_2fa_email()

    async def async_step_2fa_totp(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle TOTP code verification."""
        errors: dict[str, str] = {}
        
        if user_input is not None:
            code = user_input.get("code", "").strip()
            
            if len(code) != 6 or not code.isdigit():
                errors["base"] = "invalid_code_format"
            else:
                try:
                    async with aiohttp.ClientSession() as session:
                        # Verify TOTP code
                        result = await attempt_login(
                            session, self._email, self._password,
                            totp_code=code,
                            two_factor_method="totp"
                        )
                        
                        if result.get("success"):
                            # 2FA verified - get API key
                            jwt_token = result["token"]
                            api_key = await get_api_key(session, jwt_token)
                            await test_api_connection(session, api_key)
                            
                            self.context.update({"apikey": api_key})
                            return await self.async_step_entity_selection()
                        else:
                            errors["base"] = "invalid_code"
                            
                except InvalidAuth:
                    errors["base"] = "invalid_code"
                except CannotConnect:
                    errors["base"] = "cannot_connect"
                except aiohttp.ClientError:
                    errors["base"] = "cannot_connect"
                except Exception:
                    _LOGGER.exception("Unexpected exception during TOTP verification")
                    errors["base"] = "unknown"

        return self.async_show_form(
            step_id="2fa_totp",
            data_schema=STEP_2FA_CODE_SCHEMA,
            errors=errors,
        )

    async def async_step_2fa_email(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle Email OTP code verification."""
        errors: dict[str, str] = {}
        
        if user_input is not None:
            code = user_input.get("code", "").strip()
            
            if len(code) != 6 or not code.isdigit():
                errors["base"] = "invalid_code_format"
            else:
                try:
                    async with aiohttp.ClientSession() as session:
                        # Verify email OTP code
                        result = await attempt_login(
                            session, self._email, self._password,
                            totp_code=code,
                            two_factor_method="email"
                        )
                        
                        if result.get("success"):
                            # 2FA verified - get API key
                            jwt_token = result["token"]
                            api_key = await get_api_key(session, jwt_token)
                            await test_api_connection(session, api_key)
                            
                            self.context.update({"apikey": api_key})
                            return await self.async_step_entity_selection()
                        else:
                            errors["base"] = "invalid_code"
                            
                except InvalidAuth:
                    errors["base"] = "invalid_code"
                except CannotConnect:
                    errors["base"] = "cannot_connect"
                except aiohttp.ClientError:
                    errors["base"] = "cannot_connect"
                except Exception:
                    _LOGGER.exception("Unexpected exception during email OTP verification")
                    errors["base"] = "unknown"

        return self.async_show_form(
            step_id="2fa_email",
            data_schema=STEP_2FA_CODE_SCHEMA,
            errors=errors,
            description_placeholders={"email": self._email},
        )

    async def async_step_entity_selection(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle selection of installations step."""
        errors: dict[str, str] = {}
        if user_input is not None:
            installations: list[dict] = self.context["installationdata"]
            save_data: dict[str, Any] = {}
            save_data["installations"] = []
            for k in user_input["entity_selection"]:
                for installation in installations:
                    if installation["installationId"] == k:
                        save_data["installations"].append(installation)
                        break
            save_data["api_key"] = self.context["apikey"]
            save_data["smarthome_base_url"] = SMARTHOME_BASE_URL
            save_data["email"] = self._email
            return self.async_create_entry(title=f"Målerportal ({self._email})", data=save_data)

        entities_with_labels: dict[str, str] = {}
        try:
            async with aiohttp.ClientSession() as session:
                # Get addresses and installations
                addresses_response = await session.get(
                    f"{SMARTHOME_BASE_URL}/addresses",
                    headers={"ApiKey": self.context["apikey"]},
                )

                if addresses_response.status == 429:
                    errors["base"] = "rate_limit"
                elif not addresses_response.ok:
                    errors["base"] = "cannot_connect"
                else:
                    addresses = await addresses_response.json()
                    installations = []

                    for address in addresses:
                        for installation in address.get("installations", []):
                            installation_id = installation.get("installationId")
                            utility_name = installation.get("utilityName", "Unknown")
                            installation_type = installation.get("installationType", "Unknown")
                            meter_serial = installation.get("meterSerial", "Unknown")
                            nickname = installation.get("nickname", "")

                            # Create device name: Address - SerialNumber (+ nickname if available)
                            device_name = f"{address.get('address', 'Unknown')} - {meter_serial}"
                            if nickname:
                                device_name += f" ({nickname})"

                            # Get translated meter type label
                            meter_type_label = get_meter_type_label(installation_type)
                            entities_with_labels[installation_id] = f"{device_name} [{meter_type_label}]"
                            
                            # Store full installation data
                            installations.append({
                                "installationId": installation_id,
                                "address": address.get("address"),
                                "timezone": address.get("timezone"),
                                "installationType": installation_type,
                                "utilityName": utility_name,
                                "meterSerial": meter_serial,
                                "nickname": nickname,
                            })

                    self.context["installationdata"] = installations
                    
                    # Check if no meters were found
                    if not entities_with_labels:
                        errors["base"] = "no_meters"

        except aiohttp.ClientError:
            errors["base"] = "cannot_connect"
        except Exception:
            _LOGGER.exception("Unexpected error fetching installations")
            errors["base"] = "unknown"

        # If no meters found, show the form with error
        if errors.get("base") == "no_meters":
            return self.async_show_form(
                step_id="entity_selection",
                data_schema=vol.Schema({}),
                errors=errors,
            )

        return self.async_show_form(
            step_id="entity_selection",
            data_schema=vol.Schema(
                {
                    vol.Optional("entity_selection"): cv.multi_select(
                        entities_with_labels
                    )
                },
                extra=vol.ALLOW_EXTRA,
            ),
            errors=errors,
        )


class OptionsFlow(config_entries.OptionsFlow):
    """Handle options flow for Målerportal."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        self._config_entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Show the options menu."""
        current_days = self._config_entry.options.get("history_fetched_days", 30)
        return self.async_show_menu(
            step_id="init",
            menu_options=["settings", "fetch_more_history"],
            description_placeholders={"days": str(current_days)},
        )

    async def async_step_settings(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage polling interval settings."""
        if user_input is not None:
            # Preserve history_fetched_days when saving other settings
            new_options = dict(self._config_entry.options)
            new_options.update(user_input)
            return self.async_create_entry(title="", data=new_options)

        # Get current values
        current_interval = self._config_entry.options.get(
            "polling_interval", DEFAULT_POLLING_INTERVAL
        )

        return self.async_show_form(
            step_id="settings",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        "polling_interval",
                        default=current_interval,
                    ): vol.All(
                        vol.Coerce(int),
                        vol.Range(min=MIN_POLLING_INTERVAL, max=MAX_POLLING_INTERVAL),
                    ),
                }
            ),
        )

    async def async_step_fetch_more_history(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Fetch more historical data."""
        # Call the service to fetch more history
        await self.hass.services.async_call(
            DOMAIN, "fetch_more_history", {}
        )

        # Read persisted value from options (updated by the service)
        days_fetched = self._config_entry.options.get("history_fetched_days", 30)

        return self.async_abort(
            reason="fetch_more_history_done",
            description_placeholders={"days": str(days_fetched)},
        )


class CannotConnect(HomeAssistantError):
    """Error to indicate we cannot connect."""


class InvalidAuth(HomeAssistantError):
    """Error to indicate there is invalid auth."""
