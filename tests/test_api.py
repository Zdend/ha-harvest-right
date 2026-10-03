"""Tests for the REST API client."""

import time

from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
import pytest

from custom_components.harvest_right.api import (
    HarvestRightApi,
    HarvestRightApiError,
    HarvestRightAuthError,
    canonical_serial,
    normalize_dryer,
)

from .conftest import API_BASE, EMAIL, PASSWORD, auth_response

# ── normalize_dryer ──────────────────────────────────────────────────────


def test_normalize_dryer_missing_id_raises() -> None:
    """A dryer record without an id is rejected."""
    with pytest.raises(HarvestRightApiError):
        normalize_dryer({"serial": "HR-1"})


def test_normalize_dryer_serial_fallback_to_id() -> None:
    """Serial falls back to the numeric id when absent."""
    dryer = normalize_dryer({"id": 7})
    assert dryer["serial"] == "7"
    assert dryer["id"] == 7


def test_normalize_dryer_accepts_alternate_key_spellings() -> None:
    """camelCase field variants are mapped to the normalized shape."""
    dryer = normalize_dryer(
        {"id": 3, "serialNumber": "SN3", "dryerName": "Garage", "model": "Large"}
    )
    assert dryer["serial"] == "SN3"
    assert dryer["name"] == "Garage"
    assert dryer["model"] == "Large"


@pytest.mark.parametrize(
    ("reported", "expected"),
    [
        # The case this exists for: a re-add in the app reissues the serial.
        ("2LG4BA325BBC00026-1", "2LG4BA325BBC00026"),
        ("2LG4BA325BBC00026-12", "2LG4BA325BBC00026"),
        # Untouched: no suffix at all.
        ("2LG4BA325BBC00026", "2LG4BA325BBC00026"),
        # Untouched: the base is too short to be a real serial, so the hyphen is
        # part of the name rather than a reissue marker.
        ("HR-1", "HR-1"),
        # Untouched: four digits is past anything the dedupe produces, so this
        # is a serial that genuinely ends that way.
        ("2LG4BA325BBC00026-1234", "2LG4BA325BBC00026-1234"),
        # Untouched: the suffix must be digits.
        ("2LG4BA325BBC00026-A", "2LG4BA325BBC00026-A"),
    ],
)
def test_canonical_serial(reported: str, expected: str) -> None:
    """A re-registration suffix is stripped; anything else is left alone."""
    assert canonical_serial(reported) == expected


def test_normalize_dryer_keeps_identity_across_a_reissue() -> None:
    """A reissued serial normalizes back onto the original identity."""
    before = normalize_dryer({"id": 11, "serial": "2LG4BA325BBC00026"})
    after = normalize_dryer({"id": 12, "serial": "2LG4BA325BBC00026-1"})

    # Identity — what unique_ids and device identifiers are built from.
    assert after["serial"] == before["serial"]
    # The device name drives entity_id slugs, so it must not carry the suffix.
    assert after["name"] == before["name"] == "2LG4BA325BBC00026"
    # ...but what the cloud actually said is still recoverable.
    assert after["serial_reported"] == "2LG4BA325BBC00026-1"
    assert before["serial_reported"] == "2LG4BA325BBC00026"


def test_normalize_dryer_keeps_a_real_name_over_the_serial() -> None:
    """A dryer the user named keeps that name, suffix or not."""
    dryer = normalize_dryer(
        {"id": 13, "serial": "2LG4BA325BBC00026-1", "name": "Garage"}
    )
    assert dryer["name"] == "Garage"
    assert dryer["serial"] == "2LG4BA325BBC00026"


# ── login ────────────────────────────────────────────────────────────────


async def test_login_success_stores_tokens(hass: HomeAssistant, aioclient_mock) -> None:
    """A successful login stores tokens and the customer id."""
    aioclient_mock.post(f"{API_BASE}/auth/v1", json=auth_response())
    api = HarvestRightApi(async_get_clientsession(hass), EMAIL, password=PASSWORD)
    await api.login()
    assert api.access_token == "access-1"
    assert api.refresh_token_value == "refresh-1"
    assert api.customer_id == 12345


async def test_login_invalid_credentials_raises_auth_error(
    hass: HomeAssistant, aioclient_mock
) -> None:
    """A 401 from the auth endpoint raises HarvestRightAuthError."""
    aioclient_mock.post(f"{API_BASE}/auth/v1", status=401)
    api = HarvestRightApi(async_get_clientsession(hass), EMAIL, password=PASSWORD)
    with pytest.raises(HarvestRightAuthError):
        await api.login()


async def test_login_missing_field_raises_auth_error(
    hass: HomeAssistant, aioclient_mock
) -> None:
    """An auth response missing a required field is rejected loudly."""
    bad = auth_response()
    del bad["refreshToken"]
    aioclient_mock.post(f"{API_BASE}/auth/v1", json=bad)
    api = HarvestRightApi(async_get_clientsession(hass), EMAIL, password=PASSWORD)
    with pytest.raises(HarvestRightAuthError):
        await api.login()


# ── refresh_token ────────────────────────────────────────────────────────


async def test_refresh_without_token_and_no_password_raises(
    hass: HomeAssistant,
) -> None:
    """With neither a refresh token nor a password, re-auth is demanded."""
    api = HarvestRightApi(async_get_clientsession(hass), EMAIL)
    with pytest.raises(HarvestRightAuthError):
        await api.refresh_token()


async def test_refresh_rejected_falls_back_to_login(
    hass: HomeAssistant, aioclient_mock
) -> None:
    """A 401 on refresh falls back to a password login when possible."""
    aioclient_mock.post(f"{API_BASE}/auth/v1/refresh-token", status=401)
    aioclient_mock.post(f"{API_BASE}/auth/v1", json=auth_response(access="fresh"))
    api = HarvestRightApi(
        async_get_clientsession(hass),
        EMAIL,
        password=PASSWORD,
        refresh_token="stale",
    )
    await api.refresh_token()
    assert api.access_token == "fresh"


async def test_refresh_rejected_without_password_raises(
    hass: HomeAssistant, aioclient_mock
) -> None:
    """A 401 on refresh with no password triggers re-auth."""
    aioclient_mock.post(f"{API_BASE}/auth/v1/refresh-token", status=401)
    api = HarvestRightApi(async_get_clientsession(hass), EMAIL, refresh_token="stale")
    with pytest.raises(HarvestRightAuthError):
        await api.refresh_token()


# ── get_freeze_dryers ────────────────────────────────────────────────────


async def test_get_freeze_dryers_normalizes(
    hass: HomeAssistant, aioclient_mock
) -> None:
    """The dryer list is returned in normalized form."""
    aioclient_mock.post(f"{API_BASE}/auth/v1/refresh-token", json=auth_response())
    aioclient_mock.get(
        f"{API_BASE}/freeze-dryer/v1",
        json=[{"id": 9, "serial": "HR-9", "dryer_name": "Shed"}],
    )
    api = HarvestRightApi(async_get_clientsession(hass), EMAIL, refresh_token="r")
    dryers = await api.get_freeze_dryers()
    assert len(dryers) == 1
    assert dryers[0]["id"] == 9
    assert dryers[0]["name"] == "Shed"


async def test_get_freeze_dryers_collapses_a_reissued_serial(
    hass: HomeAssistant, aioclient_mock
) -> None:
    """A reissued dryer keeps the identity of the machine it replaced."""
    aioclient_mock.post(f"{API_BASE}/auth/v1/refresh-token", json=auth_response())
    aioclient_mock.get(
        f"{API_BASE}/freeze-dryer/v1",
        json=[{"id": 42, "serial": "2LG4BA325BBC00026-1"}],
    )
    api = HarvestRightApi(async_get_clientsession(hass), EMAIL, refresh_token="r")
    dryers = await api.get_freeze_dryers()
    assert dryers[0]["serial"] == "2LG4BA325BBC00026"
    assert dryers[0]["serial_reported"] == "2LG4BA325BBC00026-1"


async def test_get_freeze_dryers_keeps_serials_that_would_collide(
    hass: HomeAssistant, aioclient_mock
) -> None:
    """Collapsing must never merge two machines onto one identity.

    If both the original and its reissue are on the account at once, the suffix
    is the only thing telling them apart, so every reported serial is kept.
    """
    aioclient_mock.post(f"{API_BASE}/auth/v1/refresh-token", json=auth_response())
    aioclient_mock.get(
        f"{API_BASE}/freeze-dryer/v1",
        json=[
            {"id": 1, "serial": "2LG4BA325BBC00026"},
            {"id": 2, "serial": "2LG4BA325BBC00026-1"},
        ],
    )
    api = HarvestRightApi(async_get_clientsession(hass), EMAIL, refresh_token="r")
    dryers = await api.get_freeze_dryers()
    assert [d["serial"] for d in dryers] == [
        "2LG4BA325BBC00026",
        "2LG4BA325BBC00026-1",
    ]


async def test_get_freeze_dryers_bad_payload_raises(
    hass: HomeAssistant, aioclient_mock
) -> None:
    """A non-list dryer payload raises HarvestRightApiError."""
    aioclient_mock.post(f"{API_BASE}/auth/v1/refresh-token", json=auth_response())
    aioclient_mock.get(f"{API_BASE}/freeze-dryer/v1", json={"oops": True})
    api = HarvestRightApi(async_get_clientsession(hass), EMAIL, refresh_token="r")
    with pytest.raises(HarvestRightApiError):
        await api.get_freeze_dryers()


async def test_ensure_valid_token_refreshes_when_expired(
    hass: HomeAssistant, aioclient_mock
) -> None:
    """ensure_valid_token refreshes once the refresh time has passed."""
    aioclient_mock.post(f"{API_BASE}/auth/v1/refresh-token", json=auth_response())
    api = HarvestRightApi(async_get_clientsession(hass), EMAIL, refresh_token="r")
    api._refresh_after = time.time() - 1  # already due
    await api.ensure_valid_token()
    assert api.access_token == "access-1"
