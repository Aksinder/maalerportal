# Målerportal

This is a custom component for Home Assistant to integrate Målerportal.

## Prerequisites

Ensure that HACS is installed.

- Guide: https://www.hacs.xyz/docs/use/configuration/basic/#to-set-up-the-hacs-integration
- Guide docker: https://www.simplysmart.house/blog/how-to-install-HACS-on-home-assistant-Docker


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
10. Select which meters you want to add to Home Assistant (you can always add more later)
11. **Wait 1-2 minutes** for the integration to fetch your historical meter data

## Features

### Multiple Accounts
If you have meters across different Målerportal accounts, you can add the integration multiple times — once per account. Each account will appear as a separate entry in **Settings → Devices & Services**, labeled with the account email.

### Configurable Polling Interval
By default, the integration fetches new data every 30 minutes. You can adjust this interval:
1. Go to **Settings** → **Devices & Services** → **Målerportal**
2. Click **Configure**
3. Select **Settings**
4. Adjust the **Update interval** (15-120 minutes)

### Manual History Fetch
Sometimes there may be gaps in your historical data, or you want to fetch extensive history (e.g., last 3 months) for the Energy Dashboard.
You can manually trigger a fetch via `Developer Tools` -> `Services`:

**Service:** `maalerportal.fetch_more_history`

**Parameters:**
- **Statistic Sensor**: The specific sensor (e.g., `sensor.electricity_statistic`)
- **From Days Ago**: Start fetching from this many days ago (default: 60)
- **To Days Ago**: Stop fetching at this many days ago (default: 30)

Example: To fill data for the month before last, set `From Days` to 60 and `To Days` to 30.

## Sensor Types

The integration creates the following sensors for each meter:

| Meter Type | Sensors Created |
|------------|-----------------|
| **Water (Cold/Hot)** | Meter Reading, Energy Dashboard sensor |
| **Electricity** | Energy Dashboard sensor, Virtual Meter Reading (for consumption-type) |
| **Heat** | Meter Reading, Energy Dashboard sensor |

**Note:** Sensors marked "(Energy Dashboard)" or "(Energi-dashboard)" are specifically designed for use with Home Assistant's Energy Dashboard and contain historical data.

## Add meters to your Energy Dashboard

### Electricity Meters
1. Go to **Settings** → **Dashboards** → **Energy**
2. Under "Electricity grid", click **Add consumption**
3. Select the sensor ending with "El (Energy Dashboard)" or "Electricity (Energy Dashboard)"
4. For solar/export: Click **Add return to grid** and select the export sensor
5. Click **Save**

### Water Meters
1. Go to **Settings** → **Dashboards** → **Energy**
2. Scroll down to "Water consumption"
3. Click **Add water source**
4. Select the sensor ending with "(Energy Dashboard)" - choose the correct one for cold or hot water
5. Click **Save**

### Heat Meters
1. Go to **Settings** → **Dashboards** → **Energy**
2. Under "Gas consumption", click **Add gas source**
3. Select the heat sensor ending with "(Energy Dashboard)"
4. Click **Save**

After saving, click **"Show me my energy dashboard!"** to see your consumption data.