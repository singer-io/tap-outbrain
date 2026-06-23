import singer

from tap_outbrain.client import OUTBRAIN_API_BASE, OutbrainForbiddenError

LOGGER = singer.get_logger()


class BaseStream:
    """
    Abstract base class for all tap-outbrain streams.

    Subclasses set class-level attributes describing the stream and may
    override ``check_access()`` to probe the appropriate API endpoint.
    Child streams (those with ``parent`` set) always return ``True`` from
    the default ``check_access()`` implementation; their accessibility is
    governed by their parent stream's check.
    """

    name = None
    key_properties = []
    replication_keys = None
    replication_method = None
    parent = None

    def __init__(self, client=None):
        self.client = client

    def get_probe_url(self):
        """
        Return the URL used by check_access() to probe API read access.
        Parent stream subclasses must override this to return their endpoint.
        """
        return None

    def check_access(self) -> bool:
        """
        Verify that the API credentials have read access to this stream.

        Child streams (those with ``parent`` set) always return True — their
        accessibility is governed by the parent stream's check, so no API
        probe is needed.

        Parent streams make a lightweight GET request to ``get_probe_url()``
        and return False when the API responds with HTTP 403 Forbidden.
        """
        if self.parent:
            return True

        url = self.get_probe_url()
        access_token = self.client.config.get("access_token")
        headers = {"OB-TOKEN-V1": access_token}
        try:
            self.client.make_request(
                "GET", url, headers=headers, params={"limit": 1}
            )
            return True
        except OutbrainForbiddenError as exc:
            LOGGER.warning(
                "Permission Error: Stream '%s' - %s",
                self.__class__.__name__,
                exc,
            )
            return False


class Campaign(BaseStream):
    name = "campaign"
    key_properties = ["id"]
    replication_keys = None
    replication_method = "FULL_TABLE"

    def get_probe_url(self):
        """Return the campaigns endpoint for this marketer account."""
        account_id = self.client.config.get("account_id")
        return f"{OUTBRAIN_API_BASE}/marketers/{account_id}/campaigns"


class CampaignPerformance(BaseStream):
    name = "campaign_performance"
    key_properties = ["campaignId", "fromDate"]
    bookmark_properties = ["fromDate"]
    replication_keys = "fromDate"
    replication_method = "INCREMENTAL"
    parent = "campaign"
    # check_access() in BaseStream returns True for child streams.
    # No override needed here.


STREAMS = {
    "campaign": Campaign,
    "campaign_performance": CampaignPerformance,
}
