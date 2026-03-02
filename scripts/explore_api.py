"""
Målerportal API Explorer
========================
Loggar in och utforskar alla tillgängliga API-endpoints.
Laddar credentials från .env i projektets rot.

Kör med:
    pip install aiohttp python-dotenv
    python scripts/explore_api.py
"""
import asyncio
import json
import os
import sys
from pathlib import Path

import aiohttp

# Load .env from project root
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

AUTH_BASE_URL = "https://api.gateway.meterportal.eu/v1/auth"
ME_BASE_URL = "https://api.gateway.meterportal.eu/v1/me"
SMARTHOME_BASE_URL = "https://api.gateway.meterportal.eu/v1/smarthome"


def print_section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def print_response(status: int, data: object) -> None:
    print(f"HTTP {status}")
    print(json.dumps(data, indent=2, ensure_ascii=False, default=str))


async def check_auth_methods(session: aiohttp.ClientSession, email: str) -> dict:
    print_section("1. POST /v1/auth/check-auth-methods")
    print(f"Payload: {{emailAddress: {email}}}")
    async with session.post(
        f"{AUTH_BASE_URL}/check-auth-methods",
        json={"emailAddress": email},
        headers={"Content-Type": "application/json"},
    ) as resp:
        try:
            data = await resp.json()
        except Exception:
            data = {"raw": await resp.text()}
        print_response(resp.status, data)
        return data if resp.ok else {"exists": True, "hasPassword": True, "hasPasskey": False}


async def login(
    session: aiohttp.ClientSession,
    email: str,
    password: str,
    totp_code: str | None = None,
    two_factor_method: str | None = None,
) -> dict:
    payload: dict = {
        "emailAddress": email,
        "password": password,
        "platform": "smarthome",
    }
    if totp_code:
        payload["token"] = totp_code
    if two_factor_method:
        payload["twoFactorMethod"] = two_factor_method

    label = "2. POST /v1/auth/login"
    if two_factor_method:
        label += f" (2FA: {two_factor_method})"
    print_section(label)
    print(f"Payload keys: {list(payload.keys())}")

    async with session.post(
        f"{AUTH_BASE_URL}/login",
        json=payload,
        headers={"Content-Type": "application/json"},
    ) as resp:
        try:
            data = await resp.json()
        except Exception:
            data = {"raw": await resp.text()}

        # Mask token in output for readability
        display_data = dict(data) if isinstance(data, dict) else data
        if isinstance(display_data, dict) and "token" in display_data:
            display_data = {**display_data, "token": display_data["token"][:20] + "..."}
        if isinstance(display_data, dict) and "refreshToken" in display_data:
            display_data = {**display_data, "refreshToken": display_data["refreshToken"][:20] + "..."}

        print_response(resp.status, display_data)
        return {"status": resp.status, "data": data}


async def get_api_key(session: aiohttp.ClientSession, jwt_token: str) -> str:
    print_section("3. POST /v1/me/smarthome-apikey")
    async with session.post(
        f"{ME_BASE_URL}/smarthome-apikey",
        json={"description": "API Explorer Script"},
        headers={
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json",
        },
    ) as resp:
        try:
            data = await resp.json()
        except Exception:
            data = {"raw": await resp.text()}

        display_data = dict(data) if isinstance(data, dict) else data
        if isinstance(display_data, dict) and "apiKey" in display_data:
            display_data = {**display_data, "apiKey": display_data["apiKey"][:10] + "..."}

        print_response(resp.status, display_data)
        if not resp.ok:
            raise RuntimeError(f"Failed to get API key: HTTP {resp.status}")

        api_key = data.get("apiKey") if isinstance(data, dict) else None
        if not api_key:
            raise RuntimeError("No apiKey in response")
        return api_key


async def fetch_addresses(session: aiohttp.ClientSession, api_key: str) -> list:
    print_section("4. GET /v1/smarthome/addresses")
    async with session.get(
        f"{SMARTHOME_BASE_URL}/addresses",
        headers={"ApiKey": api_key},
    ) as resp:
        try:
            data = await resp.json()
        except Exception:
            data = {"raw": await resp.text()}
        print_response(resp.status, data)
        return data if resp.ok and isinstance(data, list) else []


async def fetch_latest_readings(
    session: aiohttp.ClientSession, api_key: str, installation_id: str, label: str
) -> None:
    print_section(f"GET /v1/smarthome/installations/{installation_id}/readings/latest")
    print(f"Installation: {label}")
    async with session.get(
        f"{SMARTHOME_BASE_URL}/installations/{installation_id}/readings/latest",
        headers={"ApiKey": api_key},
    ) as resp:
        try:
            data = await resp.json()
        except Exception:
            data = {"raw": await resp.text()}
        print_response(resp.status, data)


async def fetch_history(
    session: aiohttp.ClientSession, api_key: str, installation_id: str, label: str
) -> None:
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    from_date = (now - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00Z")
    to_date = now.strftime("%Y-%m-%dT23:59:59Z")

    print_section(f"POST /v1/smarthome/installations/{installation_id}/readings/historical")
    print(f"Installation: {label}")
    print(f"Body: {{from: {from_date}, to: {to_date}}}")
    async with session.post(
        f"{SMARTHOME_BASE_URL}/installations/{installation_id}/readings/historical",
        json={"from": from_date, "to": to_date},
        headers={"ApiKey": api_key, "Content-Type": "application/json"},
    ) as resp:
        try:
            data = await resp.json()
        except Exception:
            data = {"raw": await resp.text()}
        print_response(resp.status, data)


async def do_login_flow(session: aiohttp.ClientSession, email: str, password: str) -> str:
    """Full login flow, returns JWT token."""
    await check_auth_methods(session, email)

    result = await login(session, email, password)
    status = result["status"]
    data = result["data"]

    if isinstance(data, dict) and data.get("token"):
        return data["token"]

    # 2FA required
    if status == 428 and isinstance(data, dict) and data.get("twoFactorRequired"):
        methods = data.get("availableMethods", {})
        has_totp = methods.get("totp", False)
        has_email = methods.get("email", False)

        print(f"\n2FA krävs. Tillgängliga metoder: totp={has_totp}, email={has_email}")

        if has_totp and has_email:
            choice = input("Välj 2FA-metod (totp/email): ").strip().lower()
        elif has_totp:
            choice = "totp"
        elif has_email:
            choice = "email"
        else:
            raise RuntimeError("Inga 2FA-metoder tillgängliga")

        if choice == "email":
            print("Skickar e-post OTP...")
            await login(session, email, password, two_factor_method="email")

        code = input(f"Ange 6-siffrig {choice.upper()}-kod: ").strip()
        result2 = await login(session, email, password, totp_code=code, two_factor_method=choice)
        data2 = result2["data"]
        if isinstance(data2, dict) and data2.get("token"):
            return data2["token"]
        raise RuntimeError("2FA misslyckades – kontrollera koden")

    # HTTP 202 = email OTP sent automatically
    if status == 202:
        code = input("E-post OTP skickat. Ange 6-siffrig kod: ").strip()
        result2 = await login(session, email, password, totp_code=code, two_factor_method="email")
        data2 = result2["data"]
        if isinstance(data2, dict) and data2.get("token"):
            return data2["token"]
        raise RuntimeError("E-post OTP misslyckades")

    raise RuntimeError(f"Inloggning misslyckades: HTTP {status} – {data}")


async def main() -> None:
    email = os.environ.get("EMAIL", "")
    password = os.environ.get("PASSWORD", "")

    if not email or not password or email == "din@epost.se":
        print("Fel: Fyll i EMAIL och PASSWORD i .env-filen")
        sys.exit(1)

    print(f"Loggar in som: {email}")

    async with aiohttp.ClientSession() as session:
        jwt_token = await do_login_flow(session, email, password)
        api_key = await get_api_key(session, jwt_token)

        addresses = await fetch_addresses(session, api_key)

        installations = []
        for address in addresses:
            addr_str = address.get("address", "Okänd adress")
            for inst in address.get("installations", []):
                inst_id = inst.get("installationId")
                inst_type = inst.get("installationType", "")
                serial = inst.get("meterSerial", "")
                label = f"{addr_str} - {serial} [{inst_type}]"
                installations.append((inst_id, label))

        if not installations:
            print("\nInga installationer hittades.")
            return

        print(f"\nHittade {len(installations)} installation(er):")
        for inst_id, label in installations:
            print(f"  - {label} (id: {inst_id})")

        for inst_id, label in installations:
            await fetch_latest_readings(session, api_key, inst_id, label)
            await fetch_history(session, api_key, inst_id, label)

    print_section("Klar!")


if __name__ == "__main__":
    asyncio.run(main())
