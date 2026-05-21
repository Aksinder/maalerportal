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

# Last N rows ReadingsLog keeps in memory for cheap "recent readings"
# lookups. The CSV is the canonical archive — this in-memory ring is just
# a fast view, also used to build the dashboard usage summaries.
RECENT_BUFFER_SIZE = 1500

# How many recent raw readings to expose on the Senaste avläsning sensor
# via the recent_readings attribute. Bounded by RECENT_BUFFER_SIZE.
CONF_RECENT_READINGS_COUNT = "recent_readings_count"
DEFAULT_RECENT_READINGS_COUNT = 30
MIN_RECENT_READINGS_COUNT = 1
MAX_RECENT_READINGS_COUNT = 500
