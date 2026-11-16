"""Tests for parsing and dispatching a JSON REST route."""

import json
import unittest

from daemon.httpadapter import HttpAdapter
from daemon.request import Request


class HttpRouteTests(unittest.TestCase):
    def test_request_body_reaches_route_and_returns_json(self) -> None:
        def echo(headers, body):
            return {"value": json.loads(body)["value"], "remote_ip": headers["x-remote-ip"]}, 201

        request = Request()
        request.prepare(
            "POST /echo HTTP/1.1\r\n"
            "Host: tracker.local\r\n"
            "Content-Type: application/json\r\n"
            "\r\n"
            '{"value":"hello"}',
            {("POST", "/echo"): echo},
        )
        adapter = HttpAdapter("0.0.0.0", 2027, None, ("192.168.56.50", 9100), {})

        response = adapter._run_route(request)
        header_bytes, body = response.split(b"\r\n\r\n", 1)

        self.assertIn(b"HTTP/1.1 201 Created", header_bytes)
        self.assertEqual(json.loads(body), {"value": "hello", "remote_ip": "192.168.56.50"})


if __name__ == "__main__":
    unittest.main()
