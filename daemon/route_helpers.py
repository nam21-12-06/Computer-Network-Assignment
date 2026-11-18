"""Ho tro goi handler va tao HTTP response cho StreapRous."""

import asyncio
import inspect
import json
from http import HTTPStatus


class RouteHelpers:
    @staticmethod
    def _serialize_hook_response(result):
        """Convert a route result into a compact HTTP response.

        Route handlers may return a value, ``(value, status)``, or
        ``(value, status, headers)``. Dictionaries and lists are encoded as JSON.
        """
        status = 200
        extra_headers = {}
        payload = result
        if isinstance(result, tuple):
            if len(result) == 2:
                payload, status = result
            elif len(result) == 3:
                payload, status, extra_headers = result
            else:
                raise ValueError("route handlers must return value, (value, status), or (value, status, headers)")

        if isinstance(payload, (dict, list)):
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            content_type = "application/json; charset=utf-8"
        elif isinstance(payload, bytes):
            body = payload
            content_type = "application/octet-stream"
        elif payload is None:
            body = b""
            content_type = "text/plain; charset=utf-8"
        else:
            body = str(payload).encode("utf-8")
            content_type = "text/plain; charset=utf-8"

        try:
            phrase = HTTPStatus(int(status)).phrase
        except ValueError as error:
            raise ValueError("route handler returned an invalid HTTP status") from error

        headers = {
            "Content-Type": content_type,
            "Content-Length": str(len(body)),
            "Connection": "close",
        }
        headers.update(extra_headers or {})
        header_bytes = "".join(
            [f"HTTP/1.1 {int(status)} {phrase}\r\n"]
            + [f"{key}: {value}\r\n" for key, value in headers.items()]
            + ["\r\n"]
        ).encode("utf-8")
        return header_bytes + body

    def _route_headers(self, req):
        """Expose the observed peer address to routes without changing their signature."""
        headers = dict(req.headers or {})
        if self.conn_info is not None:
            headers["x-remote-ip"] = self.conn_info.client_ip
            headers["x-remote-port"] = str(self.conn_info.client_port)
        elif self.connaddr:
            headers["x-remote-ip"] = self.connaddr[0]
            headers["x-remote-port"] = str(self.connaddr[1])
        return headers

    def _run_route(self, req):
        """Run a sync or async registered route and produce an HTTP response."""
        result = req.hook(headers=self._route_headers(req), body=req.body or "")
        if inspect.isawaitable(result):
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(result)
            finally:
                loop.close()
        return self._serialize_hook_response(result)
