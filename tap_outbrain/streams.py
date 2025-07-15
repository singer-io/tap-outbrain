class Campaign:
    name = "campaign"
    key_properties = ["id"]
    replication_keys = "lastModified"
    replication_method = "INCREMENTAL"


class CampaignPerformance:
    name = "campaign_performance"
    key_properties = ["campaignId", "fromDate"]
    bookmark_properties = ["fromDate"]
    replication_keys = "fromDate"
    replication_method = "INCREMENTAL"


STREAMS = {
    "campaign": Campaign,
    "campaign_performance": CampaignPerformance,
}


SUB_STREAMS = {
    'campaign': ['campaign_performance']
}
