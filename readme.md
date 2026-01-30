# Målerportal

This is a custom component for Home Assistant to integrate Målerportal.

## Prerequisits:

Ensure that HACS is installed.

Guide: https://www.hacs.xyz/docs/use/configuration/basic/#to-set-up-the-hacs-integration<br/>
Guide docker: https://www.simplysmart.house/blog/how-to-install-HACS-on-home-assistant-Docker


## Installation:
- Go to "HACS" on the left-hand side of the Home Assistant dashboard
- Click the button in the top right corner <br/>
    ![alt text](documentation/image.png)
- Add this repository (https://github.com/maalerportal/maalerportal) as a custom repository<br/>
![alt text](documentation/image2.png)
- After clicking the add button
- Press the X button
- Search for and download the "Målerportal" integration.
- Restart Home Assistant.
- After the restart go to "Settings" → "Devices & Services" → "Add Integration"
- Search and click on "Målerportal"         
- You will now be prompted to log in with your Målerportal credentials
- Select which meters you want to add to Home Assistant (you can always add more later)
- **Wait 1-2 minutes** for the integration to fetch your historical meter data

**Note:** The first data fetch may take a few minutes as it retrieves up to 30 days of historical readings for the Energy Dashboard.
 
## Sensor Types

The integration creates the following sensors for each meter:

| Meter Type | Sensors Created |
|------------|-----------------|
| **Water (Cold/Hot)** | Meter Reading, Energy Dashboard sensor |
| **Electricity** | Energy Dashboard sensor, Virtual Meter Reading (for consumption-type) |
| **Heat** | Meter Reading, Energy Dashboard sensor |

**Note:** Sensors marked "(Energy Dashboard)" or "(Energi-dashboard)" are specifically designed for use with Home Assistant's Energy Dashboard and contain historical data.

## Add meters to your Energy Dashboard:

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