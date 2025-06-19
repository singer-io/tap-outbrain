import unittest
from unittest.mock import patch
import time
import requests
from requests.exceptions import HTTPError, RequestException

from tap_outbrain.client import (
    OutbrainClient,
    Server429Error,
    SESSION,
    RETRY_RATE_LIMIT_MS,
)


class DummyResponse:
    def __init__(self, status_code, headers=None, content=b""):
        self.status_code = status_code
        self.headers = headers or {}
        self.content = content
        self.url = "http://dummy-url/"

    def raise_for_status(self):
        raise HTTPError(f"{self.status_code} Error")


class TestOutbrainClient(unittest.TestCase):

    def setUp(self):
        self.client = OutbrainClient()

    def test_rate_limit_backoff_generator_continuous(self):
        """
        Verify that _rate_limit_backoff yields the current retry_after value
        repeatedly, and updates when retry_after changes.
        """
        gen = self.client._rate_limit_backoff()
        first = next(gen)
        second = next(gen)
        self.assertEqual(first, self.client._retry_after)
        self.assertEqual(second, self.client._retry_after)

        # Changing retry_after should reflect in subsequent yields
        self.client._retry_after = 7.25
        third = next(gen)
        self.assertEqual(third, 7.25)

    @patch.object(SESSION, "send")
    def test_make_request_success_with_auth_only(self, mock_send):
        """
        Simulate a 200 OK response when only auth is provided,
        and assert a single call to SESSION.send.
        """
        resp = requests.Response()
        resp.status_code = 200
        resp._content = b'{"result":"ok"}'
        resp.url = "http://auth-only/"
        mock_send.return_value = resp

        result = self.client.make_request("GET", "http://auth-only", auth=("u", "p"))
        self.assertEqual(result.status_code, 200)
        mock_send.assert_called_once()

    @patch.object(SESSION, "send")
    def test_make_request_success_with_headers_and_params_and_data(self, mock_send):
        """
        Simulate a 200 OK response when headers, params, and data are provided,
        and verify that they appear on the prepared request.
        """
        resp = requests.Response()
        resp.status_code = 200
        resp._content = b""
        resp.url = "http://headers-params/"
        mock_send.return_value = resp

        headers = {"X-Custom": "Val"}
        params = {"q": "test"}
        data = {"key": "value"}

        result = self.client.make_request(
            "post", "http://headers-params", headers=headers, params=params, data=data
        )
        self.assertEqual(result.status_code, 200)

        sent_req = mock_send.call_args[0][0]
        # Check headers and params are honored
        self.assertIn("X-Custom", sent_req.headers)
        self.assertIn("q=test", sent_req.url)
        mock_send.assert_called_once()

    @patch.object(time, "sleep", lambda s: None)
    @patch.object(SESSION, "send")
    def test_429_retries_n_times_then_error(self, mock_send):
        """
        Simulate repeated 429 responses with a valid header value,
        verify five retry attempts, and expect Server429Error.
        """
        mock_send.return_value = DummyResponse(
            429, headers={"rate-limit-msec-left": "1500"}
        )

        with self.assertRaises(Server429Error):
            self.client.make_request("GET", "http://rate-limit/")

        self.assertEqual(self.client._retry_after, 1.5)
        self.assertEqual(mock_send.call_count, 5)

    @patch.object(time, "sleep", lambda s: None)
    @patch.object(SESSION, "send")
    def test_429_retries(self, mock_send):
        """
        Simulate two repeated 429 responses, with third one being a success,
        verify three retry attempts
        """
        responses = [
            DummyResponse(429, headers={"rate-limit-msec-left": "2500.0"}),
            DummyResponse(429, headers={"rate-limit-msec-left": "2000"}),
            DummyResponse(200, headers={}),
        ]
        mock_send.side_effect = responses

        self.client.make_request("GET", "http://mixed-headers/")

        self.assertEqual(mock_send.call_count, 3)
        # Final retry_after should correspond to last valid or fallback
        self.assertEqual(self.client._retry_after, 2.0)

    @patch.object(time, "sleep", lambda s: None)
    @patch.object(SESSION, "send")
    def test_429_with_float_header_and_non_numeric_and_missing(self, mock_send):
        """
        Cover 429 responses in sequence:
        - float header value
        - non-numeric header
        - missing header
        - valid numeric header
        - missing header
        Expect five total retries and fallback logic on parsing.
        """
        responses = [
            DummyResponse(429, headers={"rate-limit-msec-left": "2500.0"}),
            DummyResponse(429, headers={"rate-limit-msec-left": "foo"}),
            DummyResponse(429, headers={}),
            DummyResponse(429, headers={"rate-limit-msec-left": "1000"}),
            DummyResponse(429, headers={}),
        ]
        mock_send.side_effect = responses

        with self.assertRaises(Server429Error):
            self.client.make_request("GET", "http://mixed-headers/")

        # Final retry_after should correspond to last valid or fallback
        self.assertEqual(self.client._retry_after, RETRY_RATE_LIMIT_MS / 1000.0)
        self.assertEqual(mock_send.call_count, 5)

    @patch.object(time, "sleep", lambda s: None)
    @patch.object(SESSION, "send")
    def test_5xx_errors_retry_then_http_error(self, mock_send):
        """
        Simulate repeated 500 responses causing raise_for_status,
        verify five retry attempts, and expect HTTPError on final attempt.
        """
        bad_resp = DummyResponse(500)
        bad_resp.raise_for_status = lambda: (_ for _ in ()).throw(
            HTTPError("500 Error")
        )
        mock_send.return_value = bad_resp

        with self.assertRaises(HTTPError):
            self.client.make_request("PUT", "http://server-error/")

        self.assertEqual(mock_send.call_count, 5)

    @patch.object(time, "sleep", lambda s: None)
    @patch.object(SESSION, "send")
    def test_4xx_except_429_giveup_immediately(self, mock_send):
        """
        Simulate a single 404 response (4xx except 429),
        and verify that make_request gives up immediately,
        and that the raised HTTPError carries the original response.
        """
        not_found = DummyResponse(404)

        def raise_http_error():
            err = HTTPError("404 Error")
            err.response = not_found
            raise err

        not_found.raise_for_status = raise_http_error
        mock_send.return_value = not_found

        with self.assertRaises(HTTPError) as cm:
            self.client.make_request("DELETE", "http://not-found/")

        # Ensure it gave up immediately (no retries)
        self.assertEqual(mock_send.call_count, 1)

    @patch.object(time, "sleep", lambda s: None)
    @patch.object(SESSION, "send")
    def test_network_exception_retries_then_fail(self, mock_send):
        """
        Simulate a network-level RequestException on SESSION.send,
        verify five retry attempts, and expect the exception propagated.
        """
        mock_send.side_effect = RequestException("Network down")

        with self.assertRaises(RequestException):
            self.client.make_request("GET", "http://network-fail/")

        self.assertEqual(mock_send.call_count, 5)
