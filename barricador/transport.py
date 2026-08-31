"""HTTP transport using only the standard library (urllib)."""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Dict, Iterator, List, Optional, Tuple


class Transport:
    """Bootstrap + metrics flush (request/response) and SSE line streaming, over urllib."""

    def __init__(self, base_url: str, sdk_key: str, connect_timeout: float = 10.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._sdk_key = sdk_key
        self._connect_timeout = connect_timeout

    def _headers(self, accept: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._sdk_key}", "Accept": accept}

    def bootstrap(self, timeout: float = 5.0) -> Dict[str, Any]:
        """Fetch the full ruleset unconditionally."""
        _not_modified, _etag, body = self.bootstrap_conditional(None, timeout=timeout)
        return body or {}

    def bootstrap_conditional(
        self, etag: Optional[str], timeout: float = 5.0
    ) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
        """Fetch the ruleset, sending ``If-None-Match`` when ``etag`` is given.

        Returns ``(not_modified, etag, body)``. On a 304 the body is ``None`` and the cached ruleset
        is still current — that response costs the backend no flag reads, which is what makes polling
        cheaper than holding an SSE stream open (an open stream bills server instance time for its
        entire lifetime).
        """
        headers = self._headers("application/json")
        if etag:
            headers["If-None-Match"] = etag
        req = urllib.request.Request(
            f"{self._base_url}/api/v1/flags/bootstrap",
            headers=headers,
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return False, resp.headers.get("ETag"), json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 304:
                return True, etag, None
            raise

    def flush_metrics(self, events: List[Dict[str, Any]], timeout: float = 10.0) -> bool:
        body = json.dumps({"events": events}).encode("utf-8")
        headers = self._headers("application/json")
        headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            f"{self._base_url}/api/v1/metrics/flush",
            data=body,
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return 200 <= resp.status < 300
        except urllib.error.HTTPError:
            return False

    def open_stream(self) -> "SseConnection":
        """Open the SSE stream. Caller iterates events and must close the connection."""
        req = urllib.request.Request(
            f"{self._base_url}/api/v1/flags/stream",
            headers=self._headers("text/event-stream"),
            method="GET",
        )
        resp = urllib.request.urlopen(req, timeout=self._connect_timeout)
        return SseConnection(resp)


class SseConnection:
    """Parses a Server-Sent Events byte stream into (event, data) tuples."""

    def __init__(self, response: Any) -> None:
        self._response = response

    def events(self) -> Iterator[Dict[str, Optional[str]]]:
        event: Optional[str] = None
        data_parts: List[str] = []
        for raw in self._response:
            line = raw.decode("utf-8").rstrip("\n").rstrip("\r")
            if line == "":
                if event is not None and data_parts:
                    yield {"event": event, "data": "".join(data_parts)}
                event, data_parts = None, []
            elif line.startswith(":"):
                continue  # heartbeat/comment
            elif line.startswith("event:"):
                event = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data_parts.append(line[len("data:"):].strip())

    def close(self) -> None:
        try:
            self._response.close()
        except Exception:  # noqa: BLE001
            pass
