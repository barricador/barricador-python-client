"""Asyncio-native Barricador server SDK client.

Evaluation stays synchronous (it is a pure in-memory O(1) lookup — there is nothing to await), but
all network I/O is non-blocking: the initial bootstrap and periodic flush run via ``asyncio.to_thread``
and an ``asyncio`` task, while the blocking SSE iteration runs on a dedicated daemon thread that feeds
the shared, thread-safe store.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import threading
from typing import Any, Optional

from . import evaluation
from .context import UserContext
from .store import FlagStore, MetricsBuffer
from .transport import Transport

logger = logging.getLogger("barricador.aio")


class AsyncBarricadorClient:
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
        self._streaming_enabled = streaming_enabled
        self._metrics_enabled = metrics_enabled
        self._metrics_flush_interval = metrics_flush_interval
        self._bootstrap_timeout = bootstrap_timeout
        self._poll_interval = poll_interval
        self._etag: Optional[str] = None
        self._initial_reconnect_delay = initial_reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay

        self._closed = threading.Event()
        self._sse_conn: Optional[Any] = None
        self._sse_thread: Optional[threading.Thread] = None
        self._flush_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None

    @classmethod
    async def create(cls, sdk_key: str, base_url: str = "https://app.barricador.com", **kwargs: Any) -> "AsyncBarricadorClient":
        client = cls(sdk_key, base_url, **kwargs)
        await client.start()
        return client

    async def start(self) -> None:
        await self._safe_bootstrap()
        if self._streaming_enabled:
            self._sse_thread = threading.Thread(target=self._stream_loop, name="barricador-sse", daemon=True)
            self._sse_thread.start()
        else:
            self._poll_task = asyncio.create_task(self._poll_loop())
        if self._metrics_enabled:
            self._flush_task = asyncio.create_task(self._flush_loop())

    # --- synchronous, in-memory evaluation (no await needed) ---

    def is_enabled(self, flag_key: str, user: UserContext, default: bool = False) -> bool:
        value = self._evaluate(flag_key, user, default).value
        return value if isinstance(value, bool) else default

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
        result = evaluation.evaluate(self._store.get(flag_key), user, fallback)
        if self._metrics_enabled:
            self._metrics.record(flag_key, result.variation_id, result.is_defaulted)
        return result

    # --- lifecycle ---

    async def _safe_bootstrap(self) -> None:
        try:
            _nm, etag, resp = await asyncio.to_thread(
                self._transport.bootstrap_conditional, None, self._bootstrap_timeout
            )
            resp = resp or {}
            flags = {f["key"]: f for f in (resp.get("flags") or [])}
            self._store.replace_all(flags, int(resp.get("rulesVersion", 0)))
            self._etag = etag
        except Exception as exc:  # noqa: BLE001
            logger.warning("Async bootstrap failed (%s); serving cached/defaults", exc)

    async def _poll_loop(self) -> None:
        """Refresh the ruleset on a fixed interval using a conditional GET.

        The default sync mode. An unchanged poll is a 304 that occupies the backend for
        milliseconds, whereas an open SSE stream bills backend instance time for its whole lifetime.
        """
        try:
            while not self._closed.is_set():
                # Jitter so N processes in a fleet don't poll in lockstep.
                await asyncio.sleep(self._poll_interval + random.uniform(0, self._poll_interval / 10))
                if self._closed.is_set():
                    return
                try:
                    not_modified, etag, resp = await asyncio.to_thread(
                        self._transport.bootstrap_conditional, self._etag, self._bootstrap_timeout
                    )
                    if not_modified or not resp:
                        continue
                    flags = {f["key"]: f for f in (resp.get("flags") or [])}
                    self._store.replace_all(flags, int(resp.get("rulesVersion", 0)))
                    self._etag = etag
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Async poll failed (%s); keeping cached ruleset", exc)
        except asyncio.CancelledError:
            raise

    def _stream_loop(self) -> None:
        delay = self._initial_reconnect_delay
        while not self._closed.is_set():
            try:
                conn = self._transport.open_stream()
                self._sse_conn = conn
                delay = self._initial_reconnect_delay
                for evt in conn.events():
                    if self._closed.is_set():
                        break
                    self._apply_event(evt)
            except Exception as exc:  # noqa: BLE001
                if self._closed.is_set():
                    break
                logger.debug("SSE disconnected (%s); reconnecting in %.1fs", exc, delay)
                self._closed.wait(delay + random.uniform(0, delay / 2))
                delay = min(delay * 2, self._max_reconnect_delay)

    def _apply_event(self, evt: dict) -> None:
        if evt.get("event") != "flag-change":
            return
        try:
            payload = json.loads(evt["data"])
            if payload.get("type") == "DELETE":
                self._store.remove(payload.get("flagKey"))
            elif payload.get("flag"):
                self._store.upsert(payload["flag"])
        except Exception as exc:  # noqa: BLE001
            logger.debug("Failed to apply SSE delta: %s", exc)

    async def _flush_loop(self) -> None:
        try:
            while not self._closed.is_set():
                await asyncio.sleep(self._metrics_flush_interval)
                await self.flush()
        except asyncio.CancelledError:
            pass

    async def flush(self) -> None:
        events = self._metrics.drain()
        if events:
            try:
                await asyncio.to_thread(self._transport.flush_metrics, events)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Async metrics flush failed: %s", exc)

    async def aclose(self) -> None:
        self._closed.set()
        if self._flush_task is not None:
            self._flush_task.cancel()
        if self._poll_task is not None:
            self._poll_task.cancel()
        if self._sse_conn is not None:
            self._sse_conn.close()
        await self.flush()

    async def __aenter__(self) -> "AsyncBarricadorClient":
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()
