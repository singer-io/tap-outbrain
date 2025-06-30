class Campaign:
    name = "campaign"
    key_properties = ["id"]


class CampaignPerformance:
    name = "campaign_performance"
    key_properties = ["campaignId", "fromDate"]
    replication_keys = "fromDate"
    replication_method = "INCREMENTAL"


STREAMS = {
    "campaign": Campaign,
    "campaign_performance": CampaignPerformance,
}
