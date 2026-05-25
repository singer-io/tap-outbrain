# Changelog

## 1.2.0
  * Fix `id` in `campaign` schema — key property must be non-nullable [#31](https://github.com/singer-io/tap-outbrain/pull/31)
  * Fix `campaignId` and `fromDate` in `campaign_performance` schema — key/bookmark properties must be non-nullable
  * Fix `fromDate` in `campaign_performance` — convert API date strings (`YYYY-MM-DD`) to `YYYY-MM-DDT00:00:00Z` to match `date-time` schema, preventing NOT NULL violations
  * Upgrade `requests` to `2.34.2`

## 1.1.0
  * Upgrade Python to 3.12 in CircleCI [#30](https://github.com/singer-io/tap-outbrain/pull/30)
  * Upgrade `singer-python`, `requests` and `python-dateutil` to latest version
  * Add JSON schema validation and coverage reporting steps to CircleCI
  * Add unit tests for `discover`, `sync` utilities, `client` error handling, and bookmark/start-date logic
  * Add mock integration tests for discovery, bookmarks, all-fields, automatic-fields, start-date

## 1.0.0
  * Add support of discovery mode [#22](https://github.com/singer-io/tap-braintree/pull/22)