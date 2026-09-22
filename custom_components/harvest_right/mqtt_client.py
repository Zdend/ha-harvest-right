"""MQTT client for Harvest Right using native TCP with TLS."""

from __future__ import annotations

from collections.abc import Callable
import json
import logging
import ssl
import threading
import time

from homeassistant.core import HomeAssistant
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from .const import (
    MQTT_BROKER,
    MQTT_CLIENT_ID_PREFIX,
    MQTT_KEEPALIVE,
    MQTT_PORT,
    MQTT_RC_BANNED,
    MQTT_SESSION_EXPIRY,
    ONLINE_PAYLOAD_CONTINUE,
    ONLINE_PAYLOAD_START,
)

_LOGGER = logging.getLogger(__name__)

# Message types to subscribe to per dryer.
SUBSCRIBE_MSG_TYPES = [
    "telemetry",
    "system",
    "name-update",
    "batch-summary",
]

MessageCallback = Callable[[int, str, dict], None]


class HarvestRightMqttClient:
    """MQTT client for Harvest Right freeze dryers (native TCP + TLS).

    paho runs its network loop on its own thread and invokes the
    on_connect / on_message / on_disconnect callbacks from that thread.
    All mutation of the client object (_init_client / _connect / disconnect
    / force_reconnect) is serialized with ``_lock`` so a reconnect cannot
    race a callback firing on the old client.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        customer_id: int,
        email: str,
        access_token: str,
        on_message: MessageCallback,
        client_suffix: str,
    ) -> None:
        """Initialize the client wrapper (does not connect)."""
        self._hass = hass
        self._customer_id = customer_id
        self._email = email
        self._access_token = access_token
        self._on_message = on_message
        # Persisted by the coordinator in the config entry, so this install
        # presents the SAME client ID for its whole life. Regenerating it per
        # connection — as this client used to — makes a reconnect loop look
        # like a swarm of distinct clients to the broker, which is the
        # behaviour Harvest Right asked to see the back of.
        self._client_suffix = client_suffix
        # Latched on CONNACK 0x8A. Terminal for every reconnect path.
        self._banned = False
        self._subscribed_dryers: set[int] = set()
        self._last_message_time: float = 0.0
        self._on_connect_fail: Callable[[], None] | None = None
        self._on_banned: Callable[[], None] | None = None
        self._client: mqtt.Client | None = None
        self._lock = threading.Lock()

        self._connect_props = Properties(PacketTypes.CONNECT)
        self._connect_props.SessionExpiryInterval = MQTT_SESSION_EXPIRY

    # ── Properties ────────────────────────────────────────────────────────

    @property
    def last_message_time(self) -> float:
        """Return the monotonic time of the last received message."""
        return self._last_message_time

    @property
    def is_connected(self) -> bool:
        """Return True if the MQTT client is currently connected."""
        client = self._client
        return client is not None and client.is_connected()

    @property
    def banned(self) -> bool:
        """Return True if the broker has refused this client as Banned."""
        return self._banned

    def set_on_connect_fail(self, callback: Callable[[], None]) -> None:
        """Set a callback invoked on connection authentication failure."""
        self._on_connect_fail = callback

    def set_on_banned(self, callback: Callable[[], None]) -> None:
        """Set a callback invoked once, when the broker reports Banned."""
        self._on_banned = callback

    # ── Client construction ───────────────────────────────────────────────

    def _build_client(self) -> mqtt.Client:
        """Create and configure a fresh paho client (blocking)."""
        client_id = (
            f"{self._customer_id}-{MQTT_CLIENT_ID_PREFIX}.{self._client_suffix}"
        )

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv5,
        )
        # PROTOCOL_TLS_CLIENT enables certificate + hostname verification.
        client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
        client.username_pw_set(self._email, self._access_token)
        client.reconnect_delay_set(min_delay=1, max_delay=120)
        if _LOGGER.isEnabledFor(logging.DEBUG):
            client.enable_logger(_LOGGER)

        client.on_connect = self._on_connect
        client.on_message = self._on_mqtt_message
        client.on_disconnect = self._on_disconnect
        return client

    def _teardown_client(self, client: mqtt.Client | None) -> None:
        """Cleanly stop a client and detach its callbacks (blocking).

        Detaching the callbacks first guarantees a late on_disconnect /
        on_connect from this (now stale) client cannot touch our state or
        trigger another reconnect.
        """
        if client is None:
            return
        client.on_connect = None
        client.on_message = None
        client.on_disconnect = None
        # disconnect() before loop_stop() so the DISCONNECT packet is flushed
        # by the still-running network loop.
        try:
            client.disconnect()
        except Exception:
            _LOGGER.debug("disconnect() raised during teardown", exc_info=True)
        try:
            client.loop_stop()
        except Exception:
            _LOGGER.debug("loop_stop() raised during teardown", exc_info=True)

    # ── Connection lifecycle (all blocking — call from executor) ──────────

    async def connect(self) -> None:
        """Connect to the MQTT broker (non-blocking on the event loop)."""
        _LOGGER.info("Connecting to MQTT broker %s:%s", MQTT_BROKER, MQTT_PORT)
        await self._hass.async_add_executor_job(self._connect_sync)

    def _connect_sync(self) -> None:
        """Build the client and start an async connect (runs on executor)."""
        with self._lock:
            self._teardown_client(self._client)
            self._client = self._build_client()
            self._last_message_time = time.monotonic()
            self._client.connect_async(
                MQTT_BROKER,
                MQTT_PORT,
                MQTT_KEEPALIVE,
                properties=self._connect_props,
            )
            self._client.loop_start()

    async def disconnect(self) -> None:
        """Disconnect from the MQTT broker and release the client."""
        _LOGGER.debug("Disconnecting from MQTT broker")
        await self._hass.async_add_executor_job(self._disconnect_sync)

    def _disconnect_sync(self) -> None:
        """Tear down the client (runs on executor)."""
        with self._lock:
            self._teardown_client(self._client)
            self._client = None

    def force_reconnect(self, new_token: str | None = None) -> None:
        """Force a full MQTT reconnect, optionally with a new token.

        Tears the old client down completely (callbacks detached) and
        builds a fresh one. Serialized with ``_lock``; safe to call from
        any thread.
        """
        if self._banned:
            # The path benspinks found still re-presenting a banned client every
            # 12 hours: the token-refresh loop calls update_token ->
            # force_reconnect, which never consulted the latch. It does now.
            _LOGGER.debug("Skipping forced reconnect: client is banned")
            return
        _LOGGER.info("Forcing MQTT reconnect")
        with self._lock:
            if new_token is not None:
                self._access_token = new_token
            self._teardown_client(self._client)
            # Reset the watchdog timer so the new connection gets time to
            # establish before another reconnect is triggered.
            self._last_message_time = time.monotonic()
            self._client = self._build_client()
            self._client.connect_async(
                MQTT_BROKER,
                MQTT_PORT,
                MQTT_KEEPALIVE,
                properties=self._connect_props,
            )
            self._client.loop_start()

    def update_token(self, access_token: str) -> None:
        """Update the access token, reconnecting only if it changed."""
        if access_token == self._access_token:
            _LOGGER.debug("Token unchanged, skipping reconnect")
            return
        self.force_reconnect(new_token=access_token)

    # ── Subscriptions / publishing ────────────────────────────────────────

    def subscribe_dryer(self, dryer_id: int) -> None:
        """Register a dryer for topic subscription.

        Actual subscribing happens in _on_connect once connected.
        """
        with self._lock:
            self._subscribed_dryers.add(dryer_id)

    def _subscribe_dryer_topics(self, client: mqtt.Client, dryer_id: int) -> None:
        """Subscribe to all MQTT topics for one dryer."""
        for msg_type in SUBSCRIBE_MSG_TYPES:
            topic = f"act/{self._customer_id}/ed/{dryer_id}/m/{msg_type}"
            result, _ = client.subscribe(topic, qos=0)
            if result != mqtt.MQTT_ERR_SUCCESS:
                _LOGGER.warning("Subscribe to %s failed (rc=%s)", topic, result)
            else:
                _LOGGER.debug("Subscribed to %s", topic)

    def publish_online(self, payload: str = ONLINE_PAYLOAD_CONTINUE) -> None:
        """Publish to the online topic to keep telemetry flowing.

        The dryer's WiFi adapter only sends telemetry while it knows a client
        is listening. Defaults to "continue" — the cheap heartbeat. Pass
        ONLINE_PAYLOAD_START ("on") only when a resend is actually wanted:
        on connect, on the ~24h `system` refresh, or to re-arm a dryer that
        has been silent. See the note in const.py.
        """
        client = self._client
        if client is None or not client.is_connected():
            return
        topic = f"act/{self._customer_id}/on"
        info = client.publish(topic, payload, qos=0)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            _LOGGER.debug(
                "Publish %r to %s failed (rc=%s)", payload, topic, info.rc
            )

    # ── paho callbacks (run on paho's network thread) ─────────────────────

    def _on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        """Handle MQTT connection (paho thread)."""
        if rc == 0:
            self._last_message_time = time.monotonic()
            _LOGGER.info("Connected to MQTT broker successfully")
            for dryer_id in list(self._subscribed_dryers):
                self._subscribe_dryer_topics(client, dryer_id)
            # Signal the dryer(s) to start sending telemetry. "on" is correct
            # HERE and only here (plus the 24h refresh) — the periodic
            # heartbeat uses "continue".
            client.publish(
                f"act/{self._customer_id}/on", ONLINE_PAYLOAD_START, qos=0
            )
        elif getattr(rc, "value", rc) == MQTT_RC_BANNED:
            # 0x8A is terminal. Every retry from here is a client hammering a
            # block the broker has already applied, which is precisely what got
            # accounts flagged in the first place. Latch, tell the user, stop.
            self._banned = True
            _LOGGER.error(
                "MQTT broker refused this client as Banned (0x8A). Not "
                "retrying: reconnecting would only hammer a block that is "
                "already in place. See "
                "https://github.com/itsdrewmiller/ha-harvest-right/issues/4"
            )
            try:
                client.disconnect()
            except Exception:
                _LOGGER.debug("disconnect() raised on ban", exc_info=True)
            if self._on_banned is not None:
                self._on_banned()
        else:
            _LOGGER.error("MQTT connection failed with code %s", rc)
            # Stop paho's auto-reconnect — the coordinator handles reconnection
            # with a fresh token via force_reconnect.
            try:
                client.disconnect()
            except Exception:
                _LOGGER.debug("disconnect() raised in _on_connect", exc_info=True)
            if self._on_connect_fail is not None:
                self._on_connect_fail()

    def _on_mqtt_message(self, client, userdata, msg) -> None:
        """Handle an incoming MQTT message (paho thread)."""
        self._last_message_time = time.monotonic()

        # The online/offline topic sends plain strings ("on", "continue").
        if msg.topic.endswith("/on"):
            _LOGGER.debug(
                "Online status: %s",
                msg.payload.decode("utf-8", errors="replace"),
            )
            return

        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            _LOGGER.warning("Failed to decode MQTT message on %s", msg.topic)
            return

        # Topic shape: act/{custId}/ed/{dryerId}/m/{msgType}
        parts = msg.topic.split("/")
        if len(parts) >= 6 and parts[2] == "ed" and parts[4] == "m":
            try:
                dryer_id = int(parts[3])
            except ValueError:
                _LOGGER.warning("Invalid dryer ID in topic %s", msg.topic)
                return
            self._on_message(dryer_id, parts[5], payload)
        else:
            _LOGGER.debug("Unhandled topic: %s", msg.topic)

    def _on_disconnect(self, client, userdata, flags, rc, properties=None) -> None:
        """Handle MQTT disconnection (paho thread)."""
        if rc != 0:
            _LOGGER.warning(
                "Unexpected MQTT disconnect (code %s), will attempt reconnect",
                rc,
            )
        else:
            _LOGGER.debug("MQTT disconnected cleanly")
