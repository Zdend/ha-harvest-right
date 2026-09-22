"""Tests for the MQTT topic parsing logic."""

from unittest.mock import MagicMock

from custom_components.harvest_right.const import (
    MQTT_RC_BANNED,
    ONLINE_PAYLOAD_CONTINUE,
    ONLINE_PAYLOAD_START,
)
from custom_components.harvest_right.mqtt_client import (
    SUBSCRIBE_MSG_TYPES,
    HarvestRightMqttClient,
)


def _make_client(on_message) -> HarvestRightMqttClient:
    """Build a client with a mocked hass and the given message callback."""
    return HarvestRightMqttClient(
        MagicMock(),
        customer_id=100,
        email="e@x.com",
        access_token="tok",
        on_message=on_message,
        client_suffix="abc123",
    )


def _msg(topic: str, payload: bytes):
    """Build a fake paho MQTTMessage."""
    m = MagicMock()
    m.topic = topic
    m.payload = payload
    return m


def test_telemetry_message_dispatched() -> None:
    """A well-formed telemetry topic dispatches the parsed payload."""
    cb = MagicMock()
    client = _make_client(cb)
    msg = _msg("act/100/ed/42/m/telemetry", b'{"temp":5}')
    client._on_mqtt_message(None, None, msg)
    cb.assert_called_once_with(42, "telemetry", {"temp": 5})


def test_batch_summary_is_subscribed() -> None:
    """batch-summary is among the subscribed message types."""
    assert "batch-summary" in SUBSCRIBE_MSG_TYPES


def test_online_topic_ignored() -> None:
    """The plain-string /on topic does not dispatch a message."""
    cb = MagicMock()
    client = _make_client(cb)
    client._on_mqtt_message(None, None, _msg("act/100/on", b"on"))
    cb.assert_not_called()


def test_bad_json_ignored() -> None:
    """A non-JSON payload is dropped without dispatching."""
    cb = MagicMock()
    client = _make_client(cb)
    client._on_mqtt_message(None, None, _msg("act/100/ed/42/m/telemetry", b"not json"))
    cb.assert_not_called()


def test_invalid_dryer_id_ignored() -> None:
    """A non-numeric dryer id in the topic is dropped."""
    cb = MagicMock()
    client = _make_client(cb)
    client._on_mqtt_message(None, None, _msg("act/100/ed/xx/m/telemetry", b"{}"))
    cb.assert_not_called()


def test_short_topic_ignored() -> None:
    """A topic that does not match the expected shape is dropped."""
    cb = MagicMock()
    client = _make_client(cb)
    client._on_mqtt_message(None, None, _msg("act/100/weird", b"{}"))
    cb.assert_not_called()


def test_subscribe_dryer_records_id() -> None:
    """Registering a dryer adds it to the subscription set."""
    client = _make_client(MagicMock())
    client.subscribe_dryer(7)
    assert 7 in client._subscribed_dryers


# ── Client identity and the broker ban ───────────────────────────────────────


def test_client_id_uses_the_sanctioned_pattern() -> None:
    """The client ID must not use the blocked `-ha-device.` shape.

    Harvest Right's broker blocks `<customer_id>-ha-device.*` outright — a
    brand-new install on an unrelated account was refused Banned on its first
    ever connection. `<customer_id>-home-assist.*` is the pattern they
    sanctioned in its place. See issue #4.
    """
    client = _make_client(MagicMock())
    built = client._build_client()
    client_id = built._client_id.decode()

    assert client_id == "100-home-assist.abc123"
    assert "-ha-device." not in client_id


def test_client_id_is_stable_across_reconnects() -> None:
    """Rebuilding the client must not mint a new identity.

    The original bug: the suffix was a fresh uuid4 per _build_client(), so a
    reconnect loop presented the broker a stream of distinct clients.
    """
    client = _make_client(MagicMock())
    first = client._build_client()._client_id.decode()
    second = client._build_client()._client_id.decode()
    assert first == second


def test_publish_online_defaults_to_continue() -> None:
    """The periodic heartbeat must not ask for a resend."""
    client = _make_client(MagicMock())
    paho = MagicMock()
    paho.is_connected.return_value = True
    client._client = paho

    client.publish_online()

    topic, payload = paho.publish.call_args[0]
    assert topic == "act/100/on"
    assert payload == ONLINE_PAYLOAD_CONTINUE


def test_publish_online_can_request_a_resend() -> None:
    """"on" is still available for connect / refresh / re-arm."""
    client = _make_client(MagicMock())
    paho = MagicMock()
    paho.is_connected.return_value = True
    client._client = paho

    client.publish_online(ONLINE_PAYLOAD_START)

    assert paho.publish.call_args[0][1] == ONLINE_PAYLOAD_START


def test_banned_connack_latches_and_notifies() -> None:
    """0x8A latches, fires the ban callback, and is not an auth failure."""
    client = _make_client(MagicMock())
    on_banned = MagicMock()
    on_fail = MagicMock()
    client.set_on_banned(on_banned)
    client.set_on_connect_fail(on_fail)

    client._on_connect(MagicMock(), None, None, MQTT_RC_BANNED)

    assert client.banned is True
    on_banned.assert_called_once()
    # A ban is terminal, not a token problem — refreshing the token and
    # retrying is exactly the loop that hammers the block.
    on_fail.assert_not_called()


def test_force_reconnect_honours_the_ban_latch() -> None:
    """force_reconnect is the path that kept re-presenting a banned client.

    The token-refresh loop calls update_token -> force_reconnect every ~12h,
    and that path never consulted the latch.
    """
    client = _make_client(MagicMock())
    client._banned = True
    existing = MagicMock()
    client._client = existing

    client.force_reconnect(new_token="fresh")

    # Nothing was re-presented to the broker: the existing client was neither
    # torn down nor replaced, and the fresh token was not adopted.
    assert client._client is existing
    assert client._access_token == "tok"
    existing.disconnect.assert_not_called()


def test_ordinary_connect_failure_still_reports_auth_failure() -> None:
    """A non-ban failure keeps the existing token-refresh behaviour."""
    client = _make_client(MagicMock())
    on_fail = MagicMock()
    client.set_on_connect_fail(on_fail)

    client._on_connect(MagicMock(), None, None, 5)

    assert client.banned is False
    on_fail.assert_called_once()
