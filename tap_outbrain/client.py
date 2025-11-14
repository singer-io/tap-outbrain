import backoff
import requests
import singer

from singer.requests import giveup_on_http_4xx_except_429

RETRY_RATE_LIMIT_MS = 360000

LOGGER = singer.get_logger()
SESSION = requests.Session()


class Server429Error(Exception):
    pass


class OutbrainClient:

    def __init__(self):
        self._retry_after = RETRY_RATE_LIMIT_MS / 1000.0  # Conversion to seconds

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
            LOGGER.info(f"Making request: {method} {url} params={params or {}} data={data or {}}")

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

            LOGGER.info(f"Received {resp.status_code} for {method} {req.url}")

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
            elif resp.status_code >= 400:
                LOGGER.error(
                    f"{method} {req.url} [{resp.status_code} – {resp.content!r}]"
                )
                resp.raise_for_status()

            return resp

        return _call()
