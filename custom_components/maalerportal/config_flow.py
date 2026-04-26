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
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    DOMAIN,
    AUTH_BASE_URL,
    ME_BASE_URL,
    SMARTHOME_BASE_URL,
    DEFAULT_POLLING_INTERVAL,
    MIN_POLLING_INTERVAL,
    MAX_POLLING_INTERVAL,
    CONF_CURRENCY,
    DEFAULT_CURRENCY,
    SUPPORTED_CURRENCIES,
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

# Meter type translations per language
_METER_TYPE_TRANSLATIONS: dict[str, dict[str, str]] = {
    "da": {
        "ColdWater": "Koldt vand",
        "HotWater": "Varmt vand",
        "Electricity": "El",
        "Heat": "Varme",
    },
    "sv": {
        "ColdWater": "Kallvatten",
        "HotWater": "Varmvatten",
        "Electricity": "El",
        "Heat": "Värme",
    },
    "no": {
        "ColdWater": "Kaldt vann",
        "HotWater": "Varmt vann",
        "Electricity": "Strøm",
        "Heat": "Varme",
    },
    "en": {
        "ColdWater": "Cold water",
        "HotWater": "Hot water",
        "Electricity": "Electricity",
        "Heat": "Heat",
    },
}


def get_meter_type_label(meter_type: str, language: str = "da") -> str:
    """Get translated label for meter type based on HA language."""
    translations = _METER_TYPE_TRANSLATIONS.get(language) or _METER_TYPE_TRANSLATIONS["da"]
    return translations.get(meter_type, meter_type)


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
                    session = async_get_clientsession(self.hass)
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
                session = async_get_clientsession(self.hass)
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
            session = async_get_clientsession(self.hass)
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
                    session = async_get_clientsession(self.hass)
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
                    session = async_get_clientsession(self.hass)
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

    async def _async_fetch_installations(
        self, api_key: str
    ) -> tuple[list[dict[str, Any]], dict[str, str], str | None]:
        """Fetch installations from /addresses.

        Returns (installations, labels_by_id, error_key) where error_key
        is None on success, or one of "rate_limit"/"cannot_connect"/"no_meters".
        """
        try:
            session = async_get_clientsession(self.hass)
            async with session.get(
                f"{SMARTHOME_BASE_URL}/addresses",
                headers={"ApiKey": api_key},
            ) as addresses_response:
                if addresses_response.status == 429:
                    return [], {}, "rate_limit"
                if not addresses_response.ok:
                    return [], {}, "cannot_connect"
                addresses = await addresses_response.json()
        except aiohttp.ClientError:
            return [], {}, "cannot_connect"
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception("Unexpected error fetching installations: %s", err)
            return [], {}, "cannot_connect"

        installations: list[dict[str, Any]] = []
        labels: dict[str, str] = {}
        for address in addresses:
            for installation in address.get("installations", []):
                installation_id = installation.get("installationId")
                utility_name = installation.get("utilityName", "Unknown")
                installation_type = installation.get("installationType", "Unknown")
                meter_serial = installation.get("meterSerial", "Unknown")
                nickname = installation.get("nickname", "")

                device_name = f"{address.get('address', 'Unknown')} - {meter_serial}"
                if nickname:
                    device_name += f" ({nickname})"

                meter_type_label = get_meter_type_label(
                    installation_type, self.hass.config.language
                )
                labels[installation_id] = f"{device_name} [{meter_type_label}]"

                installations.append(
                    {
                        "installationId": installation_id,
                        "address": address.get("address"),
                        "timezone": address.get("timezone"),
                        "installationType": installation_type,
                        "utilityName": utility_name,
                        "meterSerial": meter_serial,
                        "nickname": nickname,
                    }
                )

        if not labels:
            return installations, labels, "no_meters"
        return installations, labels, None

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
            save_data[CONF_CURRENCY] = user_input.get(CONF_CURRENCY, DEFAULT_CURRENCY)
            return self.async_create_entry(title=f"Målerportal ({self._email})", data=save_data)

        installations, entities_with_labels, error_key = await self._async_fetch_installations(
            self.context["apikey"]
        )
        if error_key:
            errors["base"] = error_key

        self.context["installationdata"] = installations

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
                    vol.Optional(
                        "entity_selection",
                        default=list(entities_with_labels.keys()),
                    ): cv.multi_select(entities_with_labels),
                    vol.Optional(
                        CONF_CURRENCY,
                        default=DEFAULT_CURRENCY,
                    ): vol.In(SUPPORTED_CURRENCIES),
                },
                extra=vol.ALLOW_EXTRA,
            ),
            errors=errors,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Reconfigure flow — change selected installations and currency
        without re-authenticating."""
        from homeassistant.helpers import issue_registry as ir
        from .const import DOMAIN

        entry = self.hass.config_entries.async_get_entry(self.context["entry_id"])
        if entry is None:
            return self.async_abort(reason="reconfigure_failed")

        errors: dict[str, str] = {}

        if user_input is not None:
            selected_ids: list[str] = list(user_input.get("entity_selection", []))
            fresh_installations: list[dict[str, Any]] = self.context.get(
                "installationdata", []
            )
            fresh_by_id = {i["installationId"]: i for i in fresh_installations}
            saved_by_id = {
                i["installationId"]: i for i in entry.data.get("installations", [])
            }

            new_installations: list[dict[str, Any]] = []
            for inst_id in selected_ids:
                # Prefer fresh data; fall back to saved if user kept a
                # missing-upstream installation in the selection.
                if inst_id in fresh_by_id:
                    new_installations.append(fresh_by_id[inst_id])
                elif inst_id in saved_by_id:
                    new_installations.append(saved_by_id[inst_id])

            removed_ids = set(saved_by_id) - set(selected_ids)
            for removed_id in removed_ids:
                # Clear any Repairs issue we created for this installation.
                ir.async_delete_issue(
                    self.hass, DOMAIN, f"missing_installation_{removed_id}"
                )

            new_data = {
                **entry.data,
                "installations": new_installations,
                CONF_CURRENCY: user_input.get(
                    CONF_CURRENCY,
                    entry.data.get(CONF_CURRENCY, DEFAULT_CURRENCY),
                ),
            }
            self.hass.config_entries.async_update_entry(entry, data=new_data)
            await self.hass.config_entries.async_reload(entry.entry_id)
            return self.async_abort(reason="reconfigure_successful")

        # Build the form. Pre-select all currently saved installations
        # (including ones that are missing upstream so the user can keep
        # or drop them).
        api_key = entry.data["api_key"]
        fresh_installations, fresh_labels, error_key = await self._async_fetch_installations(
            api_key
        )
        if error_key and error_key != "no_meters":
            errors["base"] = error_key

        saved_by_id = {
            i["installationId"]: i for i in entry.data.get("installations", [])
        }
        fresh_by_id = {i["installationId"]: i for i in fresh_installations}

        labels: dict[str, str] = dict(fresh_labels)
        # Surface saved-but-missing installations with a clear marker so
        # the user knows what to uncheck.
        for saved_id, saved_inst in saved_by_id.items():
            if saved_id in fresh_by_id:
                continue
            address = saved_inst.get("address") or "Unknown"
            serial = saved_inst.get("meterSerial") or "Unknown"
            labels[saved_id] = f"{address} - {serial} (no longer in account)"

        self.context["installationdata"] = fresh_installations

        if not labels:
            return self.async_show_form(
                step_id="reconfigure",
                data_schema=vol.Schema({}),
                errors={"base": "no_meters"},
            )

        default_selection = list(saved_by_id.keys())
        current_currency = entry.options.get(
            CONF_CURRENCY,
            entry.data.get(CONF_CURRENCY, DEFAULT_CURRENCY),
        )

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        "entity_selection",
                        default=default_selection,
                    ): cv.multi_select(labels),
                    vol.Optional(
                        CONF_CURRENCY,
                        default=current_currency,
                    ): vol.In(SUPPORTED_CURRENCIES),
                },
                extra=vol.ALLOW_EXTRA,
            ),
            errors=errors,
            description_placeholders={
                "email": entry.data.get("email", ""),
            },
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
        current_days = self._config_entry.options.get("history_fetched_days", 7)
        return self.async_show_menu(
            step_id="init",
            menu_options=["settings", "fetch_more_history", "debug_logging"],
            description_placeholders={"days": str(current_days)},
        )

    async def async_step_settings(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage polling, currency, and leak-alarm settings."""
        from .binary_sensor import (
            CONF_NOISE_THRESHOLD,
            CONF_NOTIFY_ENABLED,
            CONF_NOTIFY_SERVICE,
            CONF_SUSTAINED_HOURS,
            DEFAULT_NOISE_THRESHOLD_HZ,
            DEFAULT_NOTIFY_SERVICE,
            DEFAULT_SUSTAINED_HOURS,
        )

        if user_input is not None:
            # Preserve history_fetched_days when saving other settings
            new_options = dict(self._config_entry.options)
            new_options.update(user_input)
            return self.async_create_entry(title="", data=new_options)

        # Get current values
        current_interval = self._config_entry.options.get(
            "polling_interval", DEFAULT_POLLING_INTERVAL
        )
        current_currency = self._config_entry.options.get(
            CONF_CURRENCY,
            self._config_entry.data.get(CONF_CURRENCY, DEFAULT_CURRENCY),
        )
        current_threshold = self._config_entry.options.get(
            CONF_NOISE_THRESHOLD, DEFAULT_NOISE_THRESHOLD_HZ
        )
        current_sustained = self._config_entry.options.get(
            CONF_SUSTAINED_HOURS, DEFAULT_SUSTAINED_HOURS
        )
        current_notify_enabled = self._config_entry.options.get(
            CONF_NOTIFY_ENABLED, False
        )
        current_notify_service = self._config_entry.options.get(
            CONF_NOTIFY_SERVICE, DEFAULT_NOTIFY_SERVICE
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
                    vol.Optional(
                        CONF_CURRENCY,
                        default=current_currency,
                    ): vol.In(SUPPORTED_CURRENCIES),
                    vol.Optional(
                        CONF_NOISE_THRESHOLD,
                        default=current_threshold,
                    ): vol.All(vol.Coerce(float), vol.Range(min=1, max=1000)),
                    vol.Optional(
                        CONF_SUSTAINED_HOURS,
                        default=current_sustained,
                    ): vol.All(vol.Coerce(float), vol.Range(min=0.5, max=72)),
                    vol.Optional(
                        CONF_NOTIFY_ENABLED,
                        default=current_notify_enabled,
                    ): bool,
                    vol.Optional(
                        CONF_NOTIFY_SERVICE,
                        default=current_notify_service,
                    ): str,
                }
            ),
        )

    async def async_step_fetch_more_history(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Fetch more historical data."""
        entry_data = self.hass.data.get(DOMAIN, {}).get(
            self._config_entry.entry_id, {}
        )
        sensors = entry_data.get("sensors", [])
        # Find statistic sensors that support history fetching
        history_sensors = [
            s for s in sensors if hasattr(s, "async_fetch_older_history")
        ]

        if not history_sensors:
            _LOGGER.warning("No history sensors found for config entry %s", self._config_entry.entry_id)
            return self.async_abort(reason="no_history_sensors")

        days_fetched = entry_data.get("history_fetched_days", 7)
        from_days = days_fetched + 30
        to_days = days_fetched

        # Directly call fetch on each sensor (avoids service schema issues)
        for sensor in history_sensors:
            await sensor.async_fetch_older_history(from_days, to_days)

        # Store days counter in hass.data (NOT in options, to avoid triggering a reload)
        entry_data["history_fetched_days"] = from_days

        return self.async_abort(
            reason="fetch_more_history_done",
            description_placeholders={"days": str(from_days)},
        )

    async def async_step_debug_logging(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Enable or disable debug logging for the integration."""
        if user_input is not None:
            enable_debug = user_input.get("enable_debug", False)
            log_level_str = user_input.get("log_level", "DEBUG")

            # Map string to logging level
            level_map = {
                "DEBUG": logging.DEBUG,
                "INFO": logging.INFO,
                "WARNING": logging.WARNING,
            }
            level = level_map.get(log_level_str, logging.DEBUG)

            # Get the root logger for this integration
            integration_logger = logging.getLogger("custom_components.maalerportal")

            if enable_debug:
                integration_logger.setLevel(level)
                _LOGGER.info(
                    "Debug logging ENABLED for Målerportal (level: %s)", log_level_str
                )
                return self.async_abort(
                    reason="debug_logging_enabled",
                    description_placeholders={"level": log_level_str},
                )
            else:
                # Reset to default (WARNING level, or let HA manage it)
                integration_logger.setLevel(logging.WARNING)
                _LOGGER.warning("Debug logging DISABLED for Målerportal (reset to WARNING)")
                return self.async_abort(reason="debug_logging_disabled")

        # Determine current state
        integration_logger = logging.getLogger("custom_components.maalerportal")
        current_level = integration_logger.getEffectiveLevel()
        is_debug_enabled = current_level <= logging.DEBUG

        # Map current level to string for default
        level_name_map = {
            logging.DEBUG: "DEBUG",
            logging.INFO: "INFO",
            logging.WARNING: "WARNING",
        }
        current_level_str = level_name_map.get(current_level, "DEBUG")

        return self.async_show_form(
            step_id="debug_logging",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        "enable_debug",
                        default=is_debug_enabled,
                    ): bool,
                    vol.Optional(
                        "log_level",
                        default=current_level_str,
                    ): vol.In(["DEBUG", "INFO", "WARNING"]),
                }
            ),
        )


class CannotConnect(HomeAssistantError):
    """Error to indicate we cannot connect."""


class InvalidAuth(HomeAssistantError):
    """Error to indicate there is invalid auth."""
