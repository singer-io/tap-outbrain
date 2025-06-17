class Campaign:
    name = "campaign"
    key_properties = ["id"]
    replication_keys = "created_at"
    replication_method = "INCREMENTAL"


class CampaignPerformance:
    name = "campaign_performance"
    key_properties = ["campaignId", "fromDate"]
    replication_keys = "created_at"
    replication_method = "INCREMENTAL"


class Link:
    name = "link"
    key_properties = ["id"]
    replication_keys = "created_at"
    replication_method = "INCREMENTAL"


class LinkPerformance:
    name = "link_performance"
    key_properties = ["campaignId"]
    replication_keys = "created_at"
    replication_method = "INCREMENTAL"


STREAMS = {
    "campaign": Campaign,
    "campaign_performance": CampaignPerformance,
    "link": Link,
    "link_performance": LinkPerformance
}
