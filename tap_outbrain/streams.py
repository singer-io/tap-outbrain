class Campaign:
    name = "campaign"
    key_properties = ["id"]
    replication_keys = None
    replication_method = "FULL_TABLE"


class CampaignPerformance:
    name = "campaign_performance"
    key_properties = ["campaignId", "fromDate"]
    bookmark_properties = ["fromDate"]
    replication_keys = "fromDate"
    replication_method = "INCREMENTAL"
    parent = "campaign"


STREAMS = {
    "campaign": Campaign,
    "campaign_performance": CampaignPerformance,
}
