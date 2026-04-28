"""Constants for the Målerportal integration."""

DOMAIN = "maalerportal"
AUTH_BASE_URL = "https://api.gateway.meterportal.eu/v1/auth"
ME_BASE_URL = "https://api.gateway.meterportal.eu/v1/me"
SMARTHOME_BASE_URL = "https://api.gateway.meterportal.eu/v1/smarthome"

# Default polling interval in minutes
DEFAULT_POLLING_INTERVAL = 30
MIN_POLLING_INTERVAL = 15
MAX_POLLING_INTERVAL = 120

# Currency
CONF_CURRENCY = "currency"
DEFAULT_CURRENCY = "SEK"
SUPPORTED_CURRENCIES = ["SEK", "DKK", "NOK", "EUR"]

SERVICE_FETCH_MORE_HISTORY = "fetch_more_history"

# How many recent raw readings to expose on the Senaste avläsning sensor
# via the recent_readings attribute. Bounded by the in-memory buffer size
# in readings_log.py (currently 200).
CONF_RECENT_READINGS_COUNT = "recent_readings_count"
DEFAULT_RECENT_READINGS_COUNT = 30
MIN_RECENT_READINGS_COUNT = 1
MAX_RECENT_READINGS_COUNT = 200
