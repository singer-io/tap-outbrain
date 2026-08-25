import unittest
from unittest.mock import patch
import time
import requests
from requests.exceptions import HTTPError, RequestException

from tap_outbrain.client import (
    OutbrainClient,
    OutbrainForbiddenError,
    OutbrainUnauthorizedError,
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

    def test_dummy_response_raise_for_status(self):
        """DummyResponse.raise_for_status raises HTTPError containing the status code."""
        resp = DummyResponse(503)
        with self.assertRaises(HTTPError) as cm:
            resp.raise_for_status()
        self.assertIn("503", str(cm.exception))

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

        with self.assertRaises(Server429Error) as ob:
            self.client.make_request("GET", "http://rate-limit/")

        self.assertEqual(self.client._retry_after, 1.5)
        self.assertEqual(mock_send.call_count, 5)
        self.assertEqual("Rate limit exceeded", str(ob.exception))

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

    @patch.object(SESSION, "send")
    def test_403_forbidden_error(self, mock_send):
        """
        Simulate a 403 Forbidden response and verify that
        OutbrainForbiddenError is raised with the correct message.
        """
        forbidden_resp = DummyResponse(403, content=b"Access denied")
        mock_send.return_value = forbidden_resp

        with self.assertRaises(OutbrainForbiddenError) as cm:
            self.client.make_request("GET", "http://forbidden/")

        self.assertIn("403", str(cm.exception))
        self.assertIn("Access denied", str(cm.exception))
        self.assertEqual(mock_send.call_count, 1)

    @patch.object(SESSION, "send")
    def test_401_unauthorized_error(self, mock_send):
        """
        Simulate a 401 Unauthorized response and verify that
        OutbrainUnauthorizedError is raised with the correct message.
        """
        unauthorized_resp = DummyResponse(401, content=b"Invalid token")
        mock_send.return_value = unauthorized_resp

        with self.assertRaises(OutbrainUnauthorizedError) as cm:
            self.client.make_request("GET", "http://unauthorized/")

        self.assertIn("401", str(cm.exception))
        self.assertIn("Invalid token", str(cm.exception))
        self.assertEqual(mock_send.call_count, 1)

    def test_check_credentials_requires_access_token(self):
        client = OutbrainClient(config={"account_id": "acct1"})

        with self.assertRaises(ValueError) as cm:
            client.check_credentials()

        self.assertIn("access_token", str(cm.exception))

    def test_check_credentials_requires_account_id(self):
        client = OutbrainClient(config={"access_token": "tok"})

        with self.assertRaises(ValueError) as cm:
            client.check_credentials()

        self.assertIn("account_id", str(cm.exception))

    @patch.object(OutbrainClient, "make_request")
    def test_check_credentials_success(self, mock_make_request):
        client = OutbrainClient(config={"access_token": "tok", "account_id": "acct1"})

        returned_token = client.check_credentials()

        mock_make_request.assert_called_once_with(
            "GET",
            "https://api.outbrain.com/amplify/v0.1/marketers/acct1",
            headers={"OB-TOKEN-V1": "tok"},
        )
        self.assertEqual(returned_token, "tok")

    @patch.object(OutbrainClient, "make_request")
    def test_check_credentials_raises_unauthorized_error(self, mock_make_request):
        mock_make_request.side_effect = OutbrainUnauthorizedError(
            "HTTP-error-code: 401, Error: b'Invalid token'"
        )
        client = OutbrainClient(config={"access_token": "tok", "account_id": "acct1"})

        with self.assertRaises(OutbrainUnauthorizedError) as cm:
            client.check_credentials()

        self.assertIn("401 Unauthorized", str(cm.exception))

    @patch('tap_outbrain.generate_token')
    @patch.object(OutbrainClient, "make_request")
    def test_check_credentials_refreshes_token_on_unauthorized(
        self, mock_make_request, mock_generate_token
    ):
        mock_make_request.side_effect = [
            OutbrainUnauthorizedError("HTTP-error-code: 401, Error: b'Invalid token'"),
            None,
        ]
        mock_generate_token.return_value = "fresh-token"
        client = OutbrainClient(
            config={
                "access_token": "stale-token",
                "account_id": "acct1",
                "username": "user",
                "password": "pass",
            }
        )

        returned_token = client.check_credentials()

        mock_generate_token.assert_called_once_with("user", "pass")
        self.assertEqual(client.config["access_token"], "fresh-token")
        self.assertEqual(returned_token, "fresh-token")
        self.assertEqual(mock_make_request.call_count, 2)
        first_call = mock_make_request.call_args_list[0]
        second_call = mock_make_request.call_args_list[1]
        self.assertEqual(first_call.kwargs["headers"], {"OB-TOKEN-V1": "stale-token"})
        self.assertEqual(second_call.kwargs["headers"], {"OB-TOKEN-V1": "fresh-token"})

    @patch('tap_outbrain.generate_token')
    @patch.object(OutbrainClient, "make_request")
    def test_check_credentials_raises_when_token_refresh_fails(
        self, mock_make_request, mock_generate_token
    ):
        mock_make_request.side_effect = OutbrainUnauthorizedError(
            "HTTP-error-code: 401, Error: b'Invalid token'"
        )
        mock_generate_token.return_value = None
        client = OutbrainClient(
            config={
                "access_token": "stale-token",
                "account_id": "acct1",
                "username": "user",
                "password": "pass",
            }
        )

        with self.assertRaises(OutbrainUnauthorizedError) as cm:
            client.check_credentials()

        self.assertIn("token refresh failed", str(cm.exception))

    @patch('tap_outbrain.generate_token')
    @patch.object(OutbrainClient, "make_request")
    def test_check_credentials_wraps_token_refresh_exception(
        self, mock_make_request, mock_generate_token
    ):
        mock_make_request.side_effect = OutbrainUnauthorizedError(
            "HTTP-error-code: 401, Error: b'Invalid token'"
        )
        mock_generate_token.side_effect = RuntimeError("login endpoint failed")
        client = OutbrainClient(
            config={
                "access_token": "stale-token",
                "account_id": "acct1",
                "username": "user",
                "password": "pass",
            }
        )

        with self.assertRaises(OutbrainUnauthorizedError) as cm:
            client.check_credentials()

        self.assertIn("token refresh failed", str(cm.exception))

    @patch('tap_outbrain.generate_token')
    @patch.object(OutbrainClient, "make_request")
    def test_check_credentials_raises_when_refreshed_token_is_rejected(
        self, mock_make_request, mock_generate_token
    ):
        mock_make_request.side_effect = [
            OutbrainUnauthorizedError("HTTP-error-code: 401, Error: b'Invalid token'"),
            OutbrainUnauthorizedError("HTTP-error-code: 401, Error: b'Invalid refreshed token'"),
        ]
        mock_generate_token.return_value = "fresh-token"
        client = OutbrainClient(
            config={
                "access_token": "stale-token",
                "account_id": "acct1",
                "username": "user",
                "password": "pass",
            }
        )

        with self.assertRaises(OutbrainUnauthorizedError) as cm:
            client.check_credentials()

        self.assertIn("refreshed token was also rejected", str(cm.exception))

    @patch.object(OutbrainClient, "make_request")
    def test_check_credentials_raises_forbidden_error(self, mock_make_request):
        mock_make_request.side_effect = OutbrainForbiddenError(
            "HTTP-error-code: 403, Error: b'Access denied'"
        )
        client = OutbrainClient(config={"access_token": "tok", "account_id": "acct1"})

        with self.assertRaises(OutbrainForbiddenError) as cm:
            client.check_credentials()

        self.assertIn("403 Forbidden", str(cm.exception))
        self.assertIn("acct1", str(cm.exception))

    @patch('tap_outbrain.client.LOGGER')
    @patch.object(time, "sleep", lambda s: None)
    @patch.object(SESSION, "send")
    def test_4xx_error_logs_and_raises(self, mock_send, mock_logger):
        """
        Simulate a 400 Bad Request response and verify that:
        1. LOGGER.error is called with the request details
        2. HTTPError is raised via raise_for_status()
        """
        bad_req_resp = DummyResponse(400, content=b"Bad request")
        bad_req_resp.url = "http://bad-request/"

        def raise_http_error():
            err = HTTPError("400 Error")
            err.response = bad_req_resp
            raise err

        bad_req_resp.raise_for_status = raise_http_error
        mock_send.return_value = bad_req_resp

        with self.assertRaises(HTTPError):
            self.client.make_request("POST", "http://bad-request/", data={"key": "value"})

        # Verify logger.error was called
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args[0][0]
        self.assertIn("400", call_args)
        self.assertEqual(mock_send.call_count, 1)

    @patch('tap_outbrain.client.LOGGER')
    @patch.object(time, "sleep", lambda s: None)
    @patch.object(SESSION, "send")
    def test_5xx_error_logs_and_retries(self, mock_send, mock_logger):
        """
        Simulate a 503 Service Unavailable response and verify that:
        1. LOGGER.error is called
        2. Request is retried 5 times before giving up
        """
        server_error_resp = DummyResponse(503, content=b"Service unavailable")
        server_error_resp.url = "http://server-error/"

        def raise_http_error():
            err = HTTPError("503 Error")
            err.response = server_error_resp
            raise err

        server_error_resp.raise_for_status = raise_http_error
        mock_send.return_value = server_error_resp

        with self.assertRaises(HTTPError):
            self.client.make_request("GET", "http://server-error/")

        # Verify logger.error was called
        self.assertGreater(mock_logger.error.call_count, 0)
        # Verify retries occurred
        self.assertEqual(mock_send.call_count, 5)
