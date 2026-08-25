# Changelog

## 1.2.0
  * Exclude unauthorized streams from the catalog during discovery. [#34](https://github.com/singer-io/tap-outbrain/pull/34)

## 1.1.0
  * Upgrade Python to 3.12 in CircleCI [#30](https://github.com/singer-io/tap-outbrain/pull/30)
  * Upgrade `singer-python`, `requests` and `python-dateutil` to latest version
  * Add JSON schema validation and coverage reporting steps to CircleCI
  * Add unit tests for `discover`, `sync` utilities, `client` error handling, and bookmark/start-date logic
  * Add mock integration tests for discovery, bookmarks, all-fields, automatic-fields, start-date

## 1.0.0
  * Add support of discovery mode [#22](https://github.com/singer-io/tap-braintree/pull/22)