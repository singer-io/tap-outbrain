import json

import backoff
import requests
import singer

from singer.requests import giveup_on_http_4xx_except_429

RETRY_RATE_LIMIT_MS = 360000

LOGGER = singer.get_logger()
SESSION = requests.Session()

OUTBRAIN_API_BASE = 'https://api.outbrain.com/amplify/v0.1'


class Server429Error(Exception):
    pass


class OutbrainUnauthorizedError(Exception):
    pass


class OutbrainForbiddenError(Exception):
    pass


class OutbrainClient:

    def __init__(self, config=None, config_path=None):
        self._retry_after = RETRY_RATE_LIMIT_MS / 1000.0  # Conversion to seconds
        self.config = config or {}
        self.config_path = config_path

    def _rate_limit_backoff(self):
        """
        Bound wait‐generator: on each retry backoff will call next()
        and sleep for self._retry_after seconds.
        """
        while True:
            yield self._retry_after

    def make_request(
        self, method, url, headers=None, params=None, auth=None, json=None, data=None
    ):
        @backoff.on_exception(
            self._rate_limit_backoff,
            Server429Error,
            max_tries=5,
            jitter=None,
        )
        @backoff.on_exception(
            backoff.constant,
            (requests.exceptions.RequestException),
            jitter=backoff.random_jitter,
            max_tries=5,
            giveup=giveup_on_http_4xx_except_429,
            interval=30,
        )
        def _call():
            LOGGER.debug(f"Making request: {method} {url} params={params or {}} data={data or {}}")

            req = requests.Request(
                method,
                url,
                headers=headers,
                params=params,
                auth=auth,
                json=json,
                data=data,
            ).prepare()
            LOGGER.debug(f"Prepared {method} URL: {req.url}")
            resp = SESSION.send(req)

            LOGGER.debug(f"Received {resp.status_code} for {method} {req.url}")

            if resp.status_code == 429:
                try:
                    # Reference: https://amplifyv01.docs.apiary.io/#reference/rate-limits
                    self._retry_after = int(
                        float(resp.headers.get("rate-limit-msec-left", RETRY_RATE_LIMIT_MS))
                    )
                except (TypeError, ValueError):
                    self._retry_after = RETRY_RATE_LIMIT_MS
                self._retry_after /= 1000.0  # For miliseconds conversion to seconds
                raise Server429Error("Rate limit exceeded")
            elif resp.status_code == 401:
                raise OutbrainUnauthorizedError(
                    f"HTTP-error-code: 401, Error: {resp.content!r}"
                )
            elif resp.status_code == 403:
                raise OutbrainForbiddenError(
                    f"HTTP-error-code: 403, Error: {resp.content!r}"
                )
            elif resp.status_code >= 400:
                LOGGER.error(
                    f"{method} {req.url} [{resp.status_code} – {resp.content!r}]"
                )
                resp.raise_for_status()

            return resp

        return _call()

    def check_credentials(self):
        access_token = self.config.get("access_token")
        account_id = self.config.get("account_id")
        username = self.config.get("username")
        password = self.config.get("password")

        if not access_token:
            raise ValueError("access_token is required to validate Outbrain credentials")
        if not account_id:
            raise ValueError("account_id is required to validate Outbrain credentials")

        url = f"{OUTBRAIN_API_BASE}/marketers/{account_id}"

        def _check_with_token(token):
            self.make_request("GET", url, headers={"OB-TOKEN-V1": token})

        def _persist_access_token(token):
            if not self.config_path:
                return

            persisted_config = dict(self.config)
            persisted_config["access_token"] = token
            with open(self.config_path, "w", encoding="utf-8") as config_file:
                json.dump(persisted_config, config_file, indent=4)
                config_file.write("\n")

        try:
            _check_with_token(access_token)
        except OutbrainUnauthorizedError as exc:
            if not username or not password:
                raise OutbrainUnauthorizedError(
                    "Invalid Outbrain credentials: access token was rejected with 401 Unauthorized."
                ) from exc

            LOGGER.info("Credential check returned 401. Attempting to generate a new token.")
            import tap_outbrain

            refreshed_token = tap_outbrain.generate_token(username, password)
            if not refreshed_token:
                raise OutbrainUnauthorizedError(
                    "Invalid Outbrain credentials: access token was rejected with 401 Unauthorized, "
                    "and token refresh failed."
                ) from exc

            self.config["access_token"] = refreshed_token

            try:
                _check_with_token(refreshed_token)
            except OutbrainUnauthorizedError as refreshed_exc:
                raise OutbrainUnauthorizedError(
                    "Invalid Outbrain credentials: access token was rejected with 401 Unauthorized, "
                    "and the refreshed token was also rejected."
                ) from refreshed_exc

            _persist_access_token(refreshed_token)
        except OutbrainForbiddenError as exc:
            raise OutbrainForbiddenError(
                "Outbrain credentials are valid but do not have access to the configured account "
                f"'{account_id}' (403 Forbidden)."
            ) from exc

        return self.config["access_token"]
