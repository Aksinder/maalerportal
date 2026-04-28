# Målerportal

Custom Home Assistant integration for Målerportal — surfaces water,
electricity and heat meter readings from Region Gotland (and other
utilities on the same backend) as native HA sensors with full
long-term statistics, leak detection, stale-data alerts and
configurable cross-installation meter-swap migration.

## Prerequisites

Ensure that HACS is installed.

- Guide: https://www.hacs.xyz/docs/use/configuration/basic/#to-set-up-the-hacs-integration
- Guide docker: https://www.simplysmart.house/blog/how-to-install-HACS-on-home-assistant-Docker

Minimum Home Assistant: **2024.10.0** (required for the
async_step_reconfigure flow).

## Installation

1. Go to "HACS" on the left-hand side of the Home Assistant dashboard
2. Click the button in the top right corner:
    ![alt text](documentation/image.png)
3. Add this repository (https://github.com/maalerportal/maalerportal) as a custom repository:
    ![alt text](documentation/image2.png)
4. After clicking the add button, press the X button
5. Search for and download the "Målerportal" integration.
6. Restart Home Assistant.
7. After the restart go to **Settings** → **Devices & Services** → **Add Integration**
8. Search and click on "Målerportal"
9. You will now be prompted to log in with your Målerportal credentials
10. All meters found on your account will be pre-selected — uncheck any you don't want to add
11. Select your currency (SEK, DKK, NOK or EUR)
12. **Wait 1-2 minutes** for the integration to fetch your historical meter data

## Features

### Multiple Addresses / Installations
If you have multiple addresses or meters on a single Målerportal
account, the integration finds all of them automatically. All
installations are pre-selected during setup — simply uncheck any you
don't want to include. Each meter appears as its own device in Home
Assistant, labeled with its address and meter serial number.

### Multiple Accounts
If you have meters across different Målerportal accounts, you can
add the integration multiple times — once per account. Each account
will appear as a separate entry in **Settings → Devices & Services**,
labeled with the account email.

### Currency Selection
During setup (and later via **Configure → Settings**) you can choose
which currency to use for price sensors: **SEK**, **DKK**, **NOK** or
**EUR**. The default is SEK.

### Automatic Reconciliation on Startup
On every startup the integration calls `/addresses` and reconciles
your saved installations against what the account actually contains:

- Updated `meterSerial`, `address` or `nickname` in the upstream
  account is mirrored to the device card automatically
- Installations that vanish upstream are surfaced as a Repairs
  issue (see [Repairs](#repairs-issues) below) — entities go
  unavailable rather than continuing to display stale data
- Installations newly appearing upstream are logged for the user to
  add via the Reconfigure flow

### Reconfigure Flow
You can change which meters are active and which currency is used
without removing the integration:

1. **Settings** → **Devices & Services** → **Målerportal**
2. Three-dot menu → **"Reconfigure"**
3. Tick / untick installations and pick a currency
4. Save — the integration reloads with the new selection

### Automatic Historical Data
When the integration is first set up, or after a forced re-fetch, it
walks `/readings/historical` in 31-day chunks back to 1 year and
imports every reading into Home Assistant's long-term statistics so
the Energy Dashboard has a full chart from day one. The same data
is also mirrored onto the user-friendly `Vattenmätaravläsning`
sensor so the Statistics tab and `statistics-graph` cards work on
either entity.

### Forced Re-Fetch of Last Year
If your Energy Dashboard has gaps or you've reset the recorder:

1. Settings → Devices & Services → Målerportal → **Configure**
2. Choose **"Re-fetch last year of history"**
3. The integration re-imports up to 1 year of statistics for every
   StatisticSensor in the background. Idempotent — duplicate
   timestamps are replaced.

### Manual Older-History Fetch
For periods further back than 1 year, or to backfill specific gaps:

**Configure menu** → **"Fetch 30 more days of history"** advances
the historical window by 30 days each time it's pressed.

Or via service call:

**Service:** `maalerportal.fetch_more_history`

**Parameters:**
- **Statistic Sensor**: e.g. `sensor.<address>_kallvatten_energi_dashboard`
- **From Days Ago**: start of window (default 60)
- **To Days Ago**: end of window (default 30)

### Configurable Polling Interval
By default the integration polls every 30 minutes. Adjust under
**Configure** → **Settings** → **Update interval** (15-120 minutes).

### Null-Latest Fallback
Some Målerportal installations expose `latestValue: null` in the
`/readings/latest` endpoint even when `/readings/historical` has
perfectly good data. The coordinator now backfills the latest value
from the historical endpoint automatically (cached per counter)
so the entity isn't stuck on "Unknown" indefinitely. Marked with
`isFallback: true` on the underlying counter for diagnostics.

### Meter-Swap Handling
Two distinct scenarios are handled:

**Same `installationId`, new physical meter** (most common): the
reconciliation step picks up the new `meterSerial` automatically
and the StatisticSensor detects the value drop in the historical
data. An offset is computed and persisted so the displayed
cumulative total stays continuous across the swap. Stored in
`<config>/.storage/maalerportal.meter_offsets.<entry_id>`.

**Different `installationId`** (Region Gotland sometimes assigns
fresh installation IDs): a manual migration step lets you bridge
the orphan entity's history to the new active entity:

1. **Configure** → **"Migrate previous meter (after meter swap)"**
2. Pick the source (orphan) and target (current active) entities
3. Submit — the integration computes the offset and triggers a
   re-import so the chart continues seamlessly

### Leak Detection (Acoustic Noise)
For meters that expose an `AcousticNoise` counter (modern Kamstrup
ultrasonic water meters), the integration creates a binary sensor
**`Misstänkt vattenläcka`** that turns ON when noise stays at or
above a threshold for a sustained duration. Defaults: **30 Hz / 6 h**.

Tunable via **Configure** → **Settings**:
- *Leak alarm threshold (Hz)* — raise to reduce false alarms
- *Leak alarm sustained duration (hours)* — filters out brief spikes

State translations: "Ingen avvikelse" / "Misstänkt läcka!" (Swedish),
"No anomaly" / "Suspected leak!" (English).

### Optional Leak Notifications
Opt in via **Settings**:
- *Send notification on leak alarm* — toggles a one-shot
  notification on the OFF→ON transition
- *Notification service* — defaults to
  `persistent_notification.create`; set to e.g.
  `notify.mobile_app_<your_device>` for push to your phone

The previous alarm state is restored from RestoreEntity so a HA
restart while in alarm doesn't double-notify, and the first
post-restart coordinator update can confirm but not clear an
already-elevated state.

### Stale-Data Alerts (Auto-Tuned)
Each installation gets a Repairs issue when the meter goes silent
for noticeably longer than its observed cadence. The threshold is
auto-tuned from `/readings/historical` — median delta between real
upstream timestamps × a configurable multiplier (default 3).

For an hourly water meter the threshold lands around 3-6 h. For a
daily LPWAN meter it lands at 2-3 days. No per-meter manual config
needed; values cached in `<config>/.storage/maalerportal.stale_monitor.<entry_id>`
and refreshed weekly.

Tunable via **Configure** → **Settings**:
- *Stale-data threshold multiplier* (default 3.0)
- *Stale-data fallback hours* (default 12; used until cadence is
  learned)

### Append-Only CSV Archive
Every reading the integration receives is written to an append-only
CSV at:

```
<config>/maalerportal/<installation_id>.csv
```

Format:

```csv
timestamp,counter_type,meter_counter_id,value,unit,source
2026-04-25T22:00:00.000+02:00,ColdWater,966e9613-...,1.229,m³,historical
2026-04-27T08:33:00.000Z,ColdWater,966e9613-...,1.937,m³,latest
```

The `source` column distinguishes:
- `latest` — from `/readings/latest`
- `fallback` — backfilled by the coordinator from
  `/readings/historical` because `/readings/latest` returned null
- `historical` — from a deliberate historical fetch (StatisticSensor
  force-fetch, fetch-more-history button, or fallback's history
  scan)

Deduplicated on `(meter_counter_id, timestamp)` so re-fetches over
the same period don't grow the file. Useful for archival, external
analysis, grep, or importing into a spreadsheet.

### Repairs Issues
Surfaced via **Settings → System → Repairs** so they don't pollute
the error log:

| Issue | Trigger | Action |
|---|---|---|
| **Installation no longer in account** | Reconciliation finds an installation that vanished upstream | Reconfigure to remove it |
| **Meter has not reported recently** | Stale-data monitor detects a silence longer than the auto-tuned threshold | Check upstream / adjust threshold |

Auto-clears as soon as the underlying condition resolves (data
arrives, installation re-appears, etc.).

## Sensors Created Per Meter

The integration creates the following sensors for each installation:

| Sensor | Type | Purpose |
|---|---|---|
| **Vattenmätaravläsning** | sensor (m³, total_increasing) | Current cumulative meter reading. Use this in Energy Dashboard, history-graph, statistics-graph cards. |
| **Kallvatten (Energi-dashboard)** | sensor (m³, total_increasing, hidden by default) | Stats-only entity used as a backfill target. Hidden — Vattenmätaravläsning is the primary user-facing entity. |
| **Aktuellt flöde** | sensor (L/h, measurement) | Instant flow rate from the meter (Flow1/Flow2 counters). 0 when no water is flowing. |
| **Akustiskt brus** | sensor (Hz, measurement) | Live acoustic noise level reported by the meter. Used as input to the leak alarm. |
| **Misstänkt vattenläcka** | binary_sensor (problem) | ON when noise has stayed at or above threshold for the configured duration. |
| **Senaste avläsning** | sensor (timestamp) | When the meter itself last recorded a value (from API), rendered as relative time. Promoted out of Diagnostic — primary at-a-glance signal. |

Heat / electricity meters produce a similar set tailored to their
counter types (Heat statistic, Supply/Return temperature, Heat
power, Heat volume etc.).

### Useful Attributes

Every coordinator-based sensor exposes:

| Attribute | Meaning |
|---|---|
| `last_reading_at` | Original ISO timestamp from the API — when the meter recorded the value |
| `reading_age_minutes` | Minutes between `last_reading_at` and now (updates on every state read) |
| `report_lag_minutes` | Minutes between meter recording and our integration first observing the value |
| `counter_type` | API counter type (`ColdWater`, `Flow1`, `AcousticNoise`, `Heat`, …) |
| `meter_counter_id` | Stable counter UUID — useful when correlating across files / API |
| `installation_id` | The Målerportal installation this sensor belongs to |

These let you build automations like:

```yaml
trigger:
  - platform: numeric_state
    entity_id: sensor.<address>_<serial>_vattenmataravlasning
    attribute: reading_age_minutes
    above: 360   # 6 hours
action:
  - service: notify.mobile_app_xxx
    data:
      title: "Vattenmätare tystnar"
      message: >-
        Mätaren har inte rapporterat på {{
        trigger.to_state.attributes.reading_age_minutes | int }} min
```

## Adding Meters to Your Energy Dashboard

### Electricity Meters
1. Go to **Settings** → **Dashboards** → **Energy**
2. Under "Electricity grid", click **Add consumption**
3. Select the sensor ending with "El (Energy Dashboard)" or
   "Electricity (Energy Dashboard)"
4. For solar/export: Click **Add return to grid** and select the
   export sensor
5. Click **Save**

### Water Meters
1. Go to **Settings** → **Dashboards** → **Energy**
2. Scroll down to "Water consumption"
3. Click **Add water source**
4. Select **Vattenmätaravläsning** for the relevant address (the
   primary sensor — both Vattenmätaravläsning and Kallvatten
   (Energi-dashboard) work, but Vattenmätaravläsning has a live
   state value)
5. Click **Save**

### Heat Meters
1. Go to **Settings** → **Dashboards** → **Energy**
2. Under "Gas consumption", click **Add gas source**
3. Select the heat sensor ending with "(Energy Dashboard)"
4. Click **Save**

After saving, click **"Show me my energy dashboard!"** to see your
consumption data.

## Useful Card Snippets

> Replace `sensor.your_meter_*` with your actual entity IDs — typically
> formatted as `sensor.<address_slug>_<serial>_<type>` (e.g.
> `sensor.example_street_12345_vattenmataravlasning`).

### Meter status overview (compact, two-line)
```yaml
type: entities
title: Vattenmätare
entities:
  - entity: sensor.your_meter_vattenmataravlasning
    name: Avläsning
    secondary_info: last-changed
  - entity: sensor.your_meter_senaste_avlasning
    name: Mätaren rapporterade
```

### Markdown card with full timestamp story
Shows current value plus both timestamps (when meter recorded the
value and when HA picked it up) so the lag between upstream and
your integration is visible.

```yaml
type: markdown
content: >-
  ## Vatten — {{ states('sensor.your_meter_vattenmataravlasning') }} m³
  
  {% set s = states.sensor.your_meter_vattenmataravlasning %}
  - Mätaren rapporterade {{ relative_time(s.attributes.last_reading_at | as_datetime) }}
  - Synkat till HA {{ relative_time(s.last_changed) }}
  - Lag: {{ ((s.last_changed - (s.attributes.last_reading_at | as_datetime)).total_seconds() / 60) | int }} min
```

### Recent readings table (date · time · value · delta)
Renders the last N raw readings (configurable via
**Configure → Settings → Number of recent readings**, default 30) as
a table with weekday and consumption delta between rows. Newest
first.

```yaml
type: markdown
title: Senaste avläsningar
content: >-
  | # | Datum | Veckodag | Tid | Värde | Δ |
  |---|---|---|---|---|---|
  {%- set readings = state_attr('sensor.your_meter_senaste_avlasning', 'recent_readings') | reverse | list %}
  {%- set ns = namespace(prev=none) %}
  {%- for r in readings %}
  {%- set ts = r.timestamp | as_datetime %}
  {%- set wd = ['Måndag','Tisdag','Onsdag','Torsdag','Fredag','Lördag','Söndag'][ts.weekday()] %}
  {%- set delta = '' if ns.prev is none else '+' ~ ((ns.prev - r.value) | round(3)) ~ ' m³' %}
  | {{ loop.index }} | {{ ts.strftime('%Y-%m-%d') }} | {{ wd }} | {{ ts.strftime('%H:%M') }} | **{{ r.value }} {{ r.unit }}** | {{ delta }} |
  {%- set ns.prev = r.value %}
  {%- endfor %}
```

### Recent readings — minimal version
Same data, fewer columns. Good for sparse meters or compact
sidebars.

```yaml
type: markdown
content: >-
  ## Avläsningar
  
  | Datum | Tid | Värde |
  |---|---|---|
  {%- for r in state_attr('sensor.your_meter_senaste_avlasning', 'recent_readings') | reverse %}
  | {{ r.timestamp[:10] }} | {{ r.timestamp[11:16] }} | {{ r.value }} {{ r.unit }} |
  {%- endfor %}
```

### Statistics graph (long-term cumulative + daily change)
Best HA-native way to chart historical data. The `state` series
shows the meter's cumulative reading; `change` shows daily
consumption.

```yaml
type: statistics-graph
entities:
  - sensor.your_meter_vattenmataravlasning
chart_type: line
period: day
days_to_show: 90
stat_types:
  - state
  - change
```

### Multi-installation status grid (glance)
For accounts with multiple meters at different addresses:

```yaml
type: glance
columns: 2
entities:
  - entity: sensor.location_a_vattenmataravlasning
    name: Sommarstuga
  - entity: sensor.location_b_vattenmataravlasning
    name: Hus
  - entity: sensor.location_a_senaste_avlasning
    name: Sommarstuga rapport
  - entity: sensor.location_b_senaste_avlasning
    name: Hus rapport
```

### Leak detection card with current acoustic noise
Combines the binary alarm sensor with the live noise reading and
threshold setting. Useful for tuning the threshold.

```yaml
type: entities
title: Läckdetektering
entities:
  - entity: binary_sensor.your_meter_misstankt_vattenlacka
    name: Larm
  - entity: sensor.your_meter_akustiskt_brus
    name: Aktuellt brus
    secondary_info: last-updated
  - type: attribute
    entity: binary_sensor.your_meter_misstankt_vattenlacka
    attribute: threshold_hz
    name: Tröskel
    suffix: ' Hz'
  - type: attribute
    entity: binary_sensor.your_meter_misstankt_vattenlacka
    attribute: sustained_hours
    name: Krävd duration
    suffix: ' h'
```

### Live flow + total reading (mini-stats)
Side-by-side gauges or numbers for the dashboard hero card.

```yaml
type: glance
title: Vatten just nu
entities:
  - entity: sensor.your_meter_vattenmataravlasning
    name: Mätarställning
  - entity: sensor.your_meter_aktuellt_flode
    name: Just nu
  - entity: sensor.your_meter_akustiskt_brus
    name: Brus
```

### Stale-data status overview
Quick at-a-glance for whether each meter is fresh. Combines the
last-reading time with a template that shows hours since.

```yaml
type: markdown
content: >-
  ## Mätarstatus
  
  {% set meters = [
    ('Sommarstuga', 'sensor.location_a_senaste_avlasning'),
    ('Hus', 'sensor.location_b_senaste_avlasning'),
  ] %}
  | Mätare | Senast rapporterat | Status |
  |---|---|---|
  {%- for name, eid in meters %}
  {%- set ts = states(eid) | as_datetime %}
  {%- set hours = ((now() - ts).total_seconds() / 3600) | round(1) %}
  {%- set status = '🟢 OK' if hours < 6 else ('🟡 Försenad' if hours < 24 else '🔴 Tyst') %}
  | {{ name }} | {{ relative_time(ts) }} | {{ status }} |
  {%- endfor %}
```

## Troubleshooting

### Sensor shows "Unknown"
Most often means `/readings/latest` returns `null` AND the
historical fallback is empty (e.g. the meter has been silent
upstream). Check **Settings → System → Repairs** for a "Meter has
not reported recently" issue. The CSV under
`<config>/maalerportal/` shows whether we've ever observed any
data for the counter.

### Energy Dashboard chart is empty / has gaps
Run **Configure** → **"Re-fetch last year of history"** to force a
clean re-import from upstream. Statistics persist across HA restarts
but can be pruned by recorder maintenance — re-fetch is idempotent.

### Repairs warning fires too quickly / not quickly enough
Adjust the **Stale-data threshold multiplier** under Settings.
Default 3.0 = alarm at ~3× the median observed cadence. Raise to
reduce false alarms, lower to be more sensitive.

### Meter swap not detected
If both `installationId` AND `meterSerial` change at the same time
(Region Gotland sometimes does this), use **Configure → "Migrate
previous meter"** to manually link the old and new entities.

## Development

The integration is structured as:

| Module | Purpose |
|---|---|
| `__init__.py` | Setup/unload, reconciliation, migrations, services |
| `coordinator.py` | API polling, null-value fallback, first-observed tracking, readings_log integration |
| `config_flow.py` | Initial setup, reconfigure, options menu (settings, fetch-more-history, migrate-meter, debug) |
| `reconcile.py` | Pure functions for installation reconciliation + meter-swap offset math |
| `stale_monitor.py` | Auto-tuned cadence calculation + Repairs issue management |
| `readings_log.py` | Append-only CSV per installation |
| `binary_sensor.py` | Leak-detection alarm |
| `sensors/` | All measurement / history / price sensors |
| `tests_unit/` | Pure unit tests for reconcile + offset logic (no HA mocks needed) |

Run the unit tests with:

```bash
pip install pytest
pytest tests_unit/ -v
```

Versioning is CalVer with HHMM suffix (`YYYY.M.D.HHMM`) — auto-bumped
by the pre-commit hook in `.githooks/pre-commit`. Activate locally with:

```bash
git config core.hooksPath .githooks
```
