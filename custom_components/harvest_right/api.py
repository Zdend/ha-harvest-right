"""REST API client for Harvest Right."""

from __future__ import annotations

import logging
import re
import time
from typing import Any

import aiohttp

from .const import API_BASE

_LOGGER = logging.getLogger(__name__)

# Fields _store_auth() requires in an auth response.
_REQUIRED_AUTH_FIELDS = (
    "accessToken",
    "refreshToken",
    "refreshAfter",
    "customerId",
)


class HarvestRightAuthError(Exception):
    """Raised when authentication fails and re-auth is required."""


class HarvestRightApiError(Exception):
    """Raised when an API call fails for a non-auth reason."""


# Re-registering a dryer in the Harvest Right app does not reuse its record. The
# backend dedupes against the serial still held by the old one and hands back
# `<SERIAL>-1` (then -2, -3...) for the new one. Because the serial is what this
# integration builds unique IDs and device identifiers from, that reissue looks
# like a brand new machine: Home Assistant creates a parallel set of entities,
# the originals go `unavailable` forever, and every dashboard, automation and
# statistic silently keeps pointing at the dead half.
#
# The physical machine has not changed, so neither should its identity. A base
# of at least 6 characters keeps this away from short placeholder serials, and
# the suffix is capped at 3 digits so a serial that genuinely ends in a hyphen
# and a long number is left alone.
_DUPLICATE_SERIAL_RE = re.compile(r"^(?P<base>.{6,}?)-(?P<copy>\d{1,3})$")


def canonical_serial(serial: str) -> str:
    """Strip a re-registration suffix so a re-added dryer keeps its identity."""
    match = _DUPLICATE_SERIAL_RE.match(serial)
    if match is None:
        return serial
    return match.group("base")


def normalize_dryer(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw dryer dict from the API into a known shape.

    The Harvest Right API is reverse-engineered and field names have
    shifted between versions. This maps whatever it returns into a
    stable dict and fails loudly if the one truly mandatory field
    (a numeric id) is missing.
    """
    dryer_id = raw.get("id")
    if dryer_id is None:
        raise HarvestRightApiError(
            f"Dryer record is missing an 'id' field: {sorted(raw)}"
        )

    # Serial is used for unique IDs / device identifiers. Fall back to the
    # numeric id so entities still get stable unique IDs if it is absent.
    serial = (
        raw.get("serial")
        or raw.get("serialNumber")
        or raw.get("cpuSerial")
        or str(dryer_id)
    )

    serial = str(serial)
    # `serial` is identity; `serial_reported` is what the cloud actually said, so
    # diagnostics can still show the reissue.
    reported = serial
    serial = canonical_serial(serial)

    name = str(
        raw.get("dryer_name") or raw.get("dryerName") or raw.get("name") or serial
    )
    # An unnamed dryer is named after its serial by the cloud, so a reissue drags
    # the suffix into the device name too — and with `_attr_has_entity_name` the
    # device name is what every entity_id is slugified from. Left alone, a fresh
    # install after a reissue would build `sensor.<serial>_1_vacuum_pressure`.
    if name == reported:
        name = serial

    return {
        "id": int(dryer_id),
        "serial": serial,
        "serial_reported": reported,
        "name": name,
        "model": raw.get("model") or raw.get("dryerModel"),
        "firmware": raw.get("firmware") or raw.get("firmwareVersion"),
        "hardware": raw.get("hardware") or raw.get("hardwareVersion"),
        "raw": raw,
    }


class HarvestRightApi:
    """Client for the Harvest Right REST API."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        email: str,
        password: str | None = None,
        refresh_token: str | None = None,
    ) -> None:
        """Initialize the client.

        Either ``password`` (for a fresh login) or ``refresh_token`` (to
        resume an existing session) must be provided.
        """
        self._session = session
        self._email = email
        self._password = password
        self._access_token: str | None = None
        self._refresh_token: str | None = refresh_token
        self._refresh_after: float = 0
        self._customer_id: int | None = None
        self._user_id: int | None = None

    @property
    def email(self) -> str:
        """Return the account email."""
        return self._email

    @property
    def access_token(self) -> str | None:
        """Return the current access token."""
        return self._access_token

    @property
    def refresh_token_value(self) -> str | None:
        """Return the current refresh token (persisted across restarts)."""
        return self._refresh_token

    @property
    def customer_id(self) -> int | None:
        """Return the account customer id."""
        return self._customer_id

    @property
    def user_id(self) -> int | None:
        """Return the account user id."""
        return self._user_id

    @property
    def refresh_after(self) -> float:
        """Return the unix time after which the token should be refreshed."""
        return self._refresh_after

    def _store_auth(self, data: dict) -> None:
        """Validate and store tokens and IDs from an auth response."""
        missing = [f for f in _REQUIRED_AUTH_FIELDS if data.get(f) is None]
        if missing:
            raise HarvestRightAuthError(
                f"Auth response missing required fields: {', '.join(missing)}"
            )
        self._access_token = data["accessToken"]
        self._refresh_token = data["refreshToken"]
        self._refresh_after = float(data["refreshAfter"])
        self._customer_id = int(data["customerId"])
        # userId is informational only; tolerate its absence.
        user_id = data.get("userId")
        self._user_id = int(user_id) if user_id is not None else None
        _LOGGER.debug("Stored auth for customer %s", self._customer_id)

    async def login(self) -> dict:
        """Authenticate with email and password."""
        if not self._password:
            raise HarvestRightAuthError("No password available for login")
        try:
            resp = await self._session.post(
                f"{API_BASE}/auth/v1",
                json={
                    "username": self._email,
                    "password": self._password,
                    "rememberme": True,
                },
            )
        except aiohttp.ClientError as err:
            raise HarvestRightApiError(f"Connection error: {err}") from err

        if resp.status == 401:
            raise HarvestRightAuthError("Invalid email or password")

        if resp.status != 200:
            raise HarvestRightApiError(f"Login failed with status {resp.status}")

        data = await resp.json()
        if data.get("error"):
            raise HarvestRightAuthError(str(data["error"]))

        self._store_auth(data)
        _LOGGER.debug("Logged in as customer %s", self._customer_id)
        return data

    async def refresh_token(self) -> dict:
        """Refresh the access token using the refresh token.

        Falls back to a full login only when a password is available;
        otherwise raises HarvestRightAuthError so the caller can trigger
        the Home Assistant re-auth flow.
        """
        if not self._refresh_token:
            return await self._login_or_reauth()

        try:
            resp = await self._session.post(
                f"{API_BASE}/auth/v1/refresh-token",
                headers={
                    "Authorization": f"Bearer {self._refresh_token}",
                    "Content-Type": "application/json",
                },
            )
        except aiohttp.ClientError as err:
            _LOGGER.warning("Token refresh connection error: %s", err)
            return await self._login_or_reauth()

        if resp.status == 401:
            _LOGGER.warning("Refresh token rejected (401)")
            return await self._login_or_reauth()

        if resp.status != 200:
            _LOGGER.warning("Token refresh returned %s, falling back", resp.status)
            return await self._login_or_reauth()

        data = await resp.json()
        if data.get("error"):
            _LOGGER.warning("Token refresh error: %s", data["error"])
            return await self._login_or_reauth()

        self._store_auth(data)
        _LOGGER.debug("Token refreshed for customer %s", self._customer_id)
        return data

    async def _login_or_reauth(self) -> dict:
        """Fall back to a password login, or demand re-auth if impossible."""
        if self._password:
            return await self.login()
        raise HarvestRightAuthError(
            "Session expired and no password is stored; re-authentication required"
        )

    async def ensure_valid_token(self) -> None:
        """Refresh the token if it is at or past its refresh time."""
        if self._access_token is None or time.time() >= self._refresh_after:
            await self.refresh_token()

    async def get_freeze_dryers(self) -> list[dict]:
        """Fetch the list of registered freeze dryers, normalized."""
        await self.ensure_valid_token()
        resp = await self._get_dryers_raw()

        if resp.status == 401:
            _LOGGER.warning("Dryer fetch got 401, refreshing token and retrying")
            await self.refresh_token()
            resp = await self._get_dryers_raw()

        if resp.status == 401:
            raise HarvestRightAuthError("Unauthorized fetching freeze dryers")

        if resp.status != 200:
            raise HarvestRightApiError(f"Failed to fetch dryers (status {resp.status})")

        payload = await resp.json()
        if not isinstance(payload, list):
            raise HarvestRightApiError(
                f"Unexpected dryer-list payload type: {type(payload).__name__}"
            )
        dryers = [normalize_dryer(item) for item in payload if isinstance(item, dict)]

        # Collapsing the suffix must never merge two machines. If it would, the
        # suffix was load-bearing after all — keep every reported serial as-is.
        serials = [d["serial"] for d in dryers]
        if len(set(serials)) != len(serials):
            _LOGGER.warning(
                "Canonical serials collide (%s); keeping the reported serials",
                ", ".join(sorted(set(serials))),
            )
            for dryer in dryers:
                dryer["serial"] = dryer["serial_reported"]
        else:
            for dryer in dryers:
                if dryer["serial"] != dryer["serial_reported"]:
                    _LOGGER.info(
                        "Dryer %s was reissued as %s; keeping its original identity",
                        dryer["serial"],
                        dryer["serial_reported"],
                    )

        return dryers

    async def _get_dryers_raw(self) -> aiohttp.ClientResponse:
        """Issue the freeze-dryer GET, mapping connection errors to API errors."""
        try:
            return await self._session.get(
                f"{API_BASE}/freeze-dryer/v1",
                headers={"Authorization": f"Bearer {self._access_token}"},
            )
        except aiohttp.ClientError as err:
            raise HarvestRightApiError(f"Connection error: {err}") from err
