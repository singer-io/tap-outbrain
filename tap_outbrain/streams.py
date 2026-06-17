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

    def check_access(self) -> bool:
        """
        Verify that the API credentials have read access to this stream.
        Returns True if accessible, False if a 403 Forbidden error is raised.
        Child streams always return True (access is governed by the parent check).
        """
        return True


class Campaign(BaseStream):
    name = "campaign"
    key_properties = ["id"]
    replication_keys = None
    replication_method = "FULL_TABLE"

    def check_access(self) -> bool:
        """
        Probe the campaigns endpoint to verify read access.
        Returns False when the API responds with HTTP 403 Forbidden.
        """
        account_id = self.client.config.get("account_id")
        access_token = self.client.config.get("access_token")
        headers = {"OB-TOKEN-V1": access_token}
        url = f"{OUTBRAIN_API_BASE}/marketers/{account_id}/campaigns"
        try:
            self.client.make_request("GET", url, headers=headers, params={"limit": 1})
            return True
        except OutbrainForbiddenError as exc:
            LOGGER.warning(
                "Permission Error: Stream '%s' - %s",
                self.__class__.__name__,
                exc,
            )
            return False


class CampaignPerformance(BaseStream):
    name = "campaign_performance"
    key_properties = ["campaignId", "fromDate"]
    bookmark_properties = ["fromDate"]
    replication_keys = "fromDate"
    replication_method = "INCREMENTAL"
    parent = "campaign"
    # check_access() inherited from BaseStream always returns True for child streams


STREAMS = {
    "campaign": Campaign,
    "campaign_performance": CampaignPerformance,
}
