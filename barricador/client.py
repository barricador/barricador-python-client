"""Synchronous Barricador server SDK client."""
from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any, Optional

from . import evaluation
from .context import UserContext
from .store import FlagStore, MetricsBuffer
from .transport import Transport

logger = logging.getLogger("barricador")


class BarricadorClient:
    """Server SDK client.

    Evaluation methods are synchronous, in-memory, and never perform I/O or raise. A daemon thread
    keeps the cache fresh, and another daemon thread flushes aggregated telemetry every
    ``metrics_flush_interval`` seconds. Use as a context manager or call :meth:`close`.

    By default the cache is refreshed by a conditional poll every ``poll_interval`` seconds, so an
    unchanged ruleset costs one 304. Pass ``streaming_enabled=True`` for near-instant propagation via
    SSE — that holds a connection open, which is billed as continuous backend instance time, so it is
    opt-in rather than the default.
    """

    def __init__(
        self,
        sdk_key: str,
        base_url: str = "https://app.barricador.com",
        *,
        streaming_enabled: bool = False,
        poll_interval: float = 30.0,
        metrics_enabled: bool = True,
        metrics_flush_interval: float = 30.0,
        bootstrap_timeout: float = 5.0,
        initial_reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 60.0,
    ) -> None:
        if not sdk_key:
            raise ValueError("sdk_key is required")
        self._transport = Transport(base_url, sdk_key)
        self._store = FlagStore()
        self._metrics = MetricsBuffer()
        self._metrics_enabled = metrics_enabled
        self._metrics_flush_interval = metrics_flush_interval
        self._bootstrap_timeout = bootstrap_timeout
        self._poll_interval = poll_interval
        self._etag: Optional[str] = None
        self._initial_reconnect_delay = initial_reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay

        self._closed = threading.Event()
        self._sse_conn = None  # type: Optional[Any]

        self._safe_bootstrap()
        if streaming_enabled:
            self._sse_thread = threading.Thread(target=self._stream_loop, name="barricador-sse", daemon=True)
            self._sse_thread.start()
        else:
            self._poll_thread = threading.Thread(target=self._poll_loop, name="barricador-poll", daemon=True)
            self._poll_thread.start()
        if metrics_enabled:
            self._flush_thread = threading.Thread(target=self._flush_loop, name="barricador-metrics", daemon=True)
            self._flush_thread.start()

    # --- public evaluation API ---

    def is_enabled(self, flag_key: str, user: UserContext, default: bool = False) -> bool:
        value = self._evaluate(flag_key, user, default).value
        return value if isinstance(value, bool) else default

    def bool_variation(self, flag_key: str, user: UserContext, default: bool) -> bool:
        return self.is_enabled(flag_key, user, default)

    def string_variation(self, flag_key: str, user: UserContext, default: str) -> str:
        value = self._evaluate(flag_key, user, default).value
        return default if value is None else str(value)

    def number_variation(self, flag_key: str, user: UserContext, default: float) -> float:
        value = self._evaluate(flag_key, user, default).value
        try:
            return float(value) if value is not None else default
        except (TypeError, ValueError):
            return default

    def json_variation(self, flag_key: str, user: UserContext, default: Any) -> Any:
        return self._evaluate(flag_key, user, default).value

    @property
    def initialized(self) -> bool:
        return self._store.initialized

    def _evaluate(self, flag_key: str, user: UserContext, fallback: Any) -> evaluation.EvaluationResult:
        flag = self._store.get(flag_key)
        result = evaluation.evaluate(flag, user, fallback)
        if self._metrics_enabled:
            self._metrics.record(flag_key, result.variation_id, result.is_defaulted)
        return result

    # --- lifecycle ---

    def _safe_bootstrap(self) -> None:
        try:
            _nm, etag, resp = self._transport.bootstrap_conditional(None, timeout=self._bootstrap_timeout)
            resp = resp or {}
            flags = {f["key"]: f for f in (resp.get("flags") or [])}
            self._store.replace_all(flags, int(resp.get("rulesVersion", 0)))
            self._etag = etag
            logger.debug("Barricador bootstrap: %d flags (v%s)", len(flags), resp.get("rulesVersion"))
        except Exception as exc:  # noqa: BLE001 - never fatal
            logger.warning("Barricador bootstrap failed (%s); serving cached/defaults", exc)

    def _poll_loop(self) -> None:
        """Refresh the ruleset on a fixed interval using a conditional GET.

        This is the default sync mode. It trades propagation latency (up to one interval) for cost:
        an open SSE stream bills backend instance time continuously, whereas an unchanged poll is a
        304 that occupies the server for milliseconds. A failed poll leaves the cache intact.
        """
        while not self._closed.wait(self._jittered_interval()):
            try:
                not_modified, etag, resp = self._transport.bootstrap_conditional(
                    self._etag, timeout=self._bootstrap_timeout
                )
                if not_modified or not resp:
                    continue
                flags = {f["key"]: f for f in (resp.get("flags") or [])}
                self._store.replace_all(flags, int(resp.get("rulesVersion", 0)))
                self._etag = etag
                logger.debug("Barricador poll: %d flags (v%s)", len(flags), resp.get("rulesVersion"))
            except Exception as exc:  # noqa: BLE001 - never fatal
                logger.debug("Barricador poll failed (%s); keeping cached ruleset", exc)

    def _jittered_interval(self) -> float:
        """Spread polls across a fleet so N processes don't hit the backend in lockstep."""
        return self._poll_interval + random.uniform(0, self._poll_interval / 10)

    def _stream_loop(self) -> None:
        delay = self._initial_reconnect_delay
        while not self._closed.is_set():
            try:
                conn = self._transport.open_stream()
                self._sse_conn = conn
                # Reconnected: re-bootstrap to recover any deltas missed while offline.
                self._safe_bootstrap()
                delay = self._initial_reconnect_delay
                for evt in conn.events():
                    if self._closed.is_set():
                        break
                    self._apply_event(evt)
            except Exception as exc:  # noqa: BLE001
                if self._closed.is_set():
                    break
                logger.debug("SSE disconnected (%s); reconnecting in %.1fs", exc, delay)
                self._sleep_with_jitter(delay)
                delay = min(delay * 2, self._max_reconnect_delay)

    def _apply_event(self, evt: dict) -> None:
        if evt.get("event") != "flag-change":
            return
        try:
            import json

            payload = json.loads(evt["data"])
            if payload.get("type") == "DELETE":
                self._store.remove(payload.get("flagKey"))
            else:
                flag = payload.get("flag")
                if flag:
                    self._store.upsert(flag)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Failed to apply SSE delta: %s", exc)

    def _flush_loop(self) -> None:
        while not self._closed.wait(self._metrics_flush_interval):
            self.flush()

    def flush(self) -> None:
        try:
            events = self._metrics.drain()
            if events:
                self._transport.flush_metrics(events)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Metrics flush failed: %s", exc)

    def _sleep_with_jitter(self, base: float) -> None:
        self._closed.wait(base + random.uniform(0, base / 2))

    def close(self) -> None:
        self._closed.set()
        if self._sse_conn is not None:
            self._sse_conn.close()
        self.flush()

    def __enter__(self) -> "BarricadorClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
