#!/usr/bin/env python3

import argparse
import base64
import copy
import datetime
import json
import os
import sys
import time
import dateutil.parser

import singer
from singer import utils

from tap_outbrain.client import OutbrainClient
from tap_outbrain.discover import discover
from tap_outbrain import streams
from typing import Dict
from requests.auth import HTTPBasicAuth


LOGGER = singer.get_logger()

BASE_URL = 'https://api.outbrain.com/amplify/v0.1'
CONFIG = {}

DEFAULT_STATE = {
    'campaign_performance': {}
}

DEFAULT_START_DATE = '2016-08-01'

# We can retrieve at most 2 campaigns per minute. We only have 5.5 hours
# to run so that works out to about 660 (120 campaigns per hour * 5.5 =
# 660) campaigns.
TAP_CAMPAIGN_COUNT_ERROR_CEILING = 660
MARKETERS_CAMPAIGNS_MAX_LIMIT = 50
# This is an arbitrary limit and can be tuned later down the road if we
# see need for it. (Tested with 200 at least)
REPORTS_MARKETERS_PERIODIC_MAX_LIMIT = 100


class StreamSelectionError(Exception):
    """Raised when required stream is not selected for sync."""
    pass


def get_abs_path(path):
    """
    Return full path for the current file
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def request(url, access_token, params):
    headers = {'OB-TOKEN-V1': access_token}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    return OutbrainClient().make_request('GET', url, headers=headers, params=params)


def generate_token(username, password):
    LOGGER.info("Generating new token using basic auth.")
    auth = HTTPBasicAuth(username, password)

    resp = OutbrainClient().make_request('GET', f'{BASE_URL}/login', auth=auth)
    return resp.json().get('OB-TOKEN-V1')


def parse_datetime(date_time):
    parsed_datetime = dateutil.parser.parse(date_time)
    # Normalize tz-aware datetimes to UTC before appending 'Z', so we never
    # produce the invalid '+00:00Z' double-encoding.
    if parsed_datetime.tzinfo is not None:
        parsed_datetime = parsed_datetime.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return parsed_datetime.isoformat('T') + 'Z'


def parse_performance(result, extra_fields):
    metrics = result.get('metrics', {})
    metadata = result.get('metadata', {})

    to_return = {
        'fromDate': metadata.get('fromDate'),
        'impressions': int(metrics.get('impressions', 0)),
        'clicks': int(metrics.get('clicks', 0)),
        'ctr': float(metrics.get('ctr', 0.0)),
        'spend': float(metrics.get('spend', 0.0)),
        'ecpc': float(metrics.get('ecpc', 0.0)),
        'conversions': int(metrics.get('conversions', 0)),
        'conversionRate': float(metrics.get('conversionRate', 0.0)),
        'cpa': float(metrics.get('cpa', 0.0)),
    }
    to_return.update(extra_fields)

    return to_return


def get_date_ranges(start, end, interval_in_days):
    if start > end:
        return []

    to_return = []
    interval_start = start

    while interval_start < end:
        to_return.append({
            'from_date': interval_start,
            'to_date': min(end,
                           (interval_start + datetime.timedelta(
                               days=interval_in_days-1)))
        })

        interval_start = interval_start + datetime.timedelta(
            days=interval_in_days)

    return to_return


def sync_campaign_performance(state, access_token, account_id, campaign_id, catalog=None):
    return sync_performance(
        state,
        access_token,
        account_id,
        'campaign_performance',
        campaign_id,
        {'campaignId': campaign_id},
        {'campaignId': campaign_id},
        catalog=catalog)


def sync_performance(state, access_token, account_id, table_name, state_sub_id,
                     extra_params, extra_persist_fields, catalog=None):
    """
    This function is heavily parameterized as it is used to sync performance
    both based on campaign ID alone, and by campaign ID and link ID.

    - `state`: state map
    - `access_token`: access token for Outbrain Amplify API
    - `account_id`: Outbrain marketer ID
    - `table_name`: the table name to use. At present:
      `campaign_performance`
    - `state_sub_id`: the id to use within the state map to identify this
                      sub-object. For example,

                        state['campaign_performance'][state_sub_id]

                      is used for the `campaign_performance` table.
    - `extra_params`: extra params sent to the Outbrain API
    - `extra_persist_fields`: extra fields pushed into the destination data.
                              For example:

                                {'campaignId': '000b...'}
    - `catalog`: singer Catalog for field filtering
    """
    # On initial sync, start exactly at configured start_date.
    # On resume, look back 2 days to account for late arriving data.
    stream_saved_date = state.get('bookmarks', {}).get(table_name, {}).get('fromDate')
    campaign_saved_date = state.get(table_name, {}).get(state_sub_id)
    # Prefer the campaign-level bookmark when available; fall back to the
    # stream-level bookmark for tap-tester compatibility.
    saved_date = campaign_saved_date or stream_saved_date
    if saved_date:
        from_date = datetime.datetime.strptime(saved_date, '%Y-%m-%d').date() - datetime.timedelta(days=2)
    else:
        from_date = datetime.datetime.strptime(DEFAULT_START_DATE, '%Y-%m-%d').date()

    to_date = datetime.date.today()

    interval_in_days = REPORTS_MARKETERS_PERIODIC_MAX_LIMIT

    date_ranges = get_date_ranges(from_date, to_date, interval_in_days)

    last_request_start = None

    for date_range in date_ranges:
        LOGGER.info(
            'Pulling {} for {} from {} to {}'
            .format(table_name,
                    extra_persist_fields,
                    date_range.get('from_date'),
                    date_range.get('to_date')))

        params = {
            'from': date_range.get('from_date'),
            'to': date_range.get('to_date'),
            'breakdown': 'daily',
            'limit': REPORTS_MARKETERS_PERIODIC_MAX_LIMIT,
            'sort': '+fromDate',
            'includeArchivedCampaigns': True,
        }
        params.update(extra_params)

        last_request_start = utils.now()
        response = request(
            '{}/reports/marketers/{}/periodic'.format(BASE_URL, account_id),
            access_token,
            params).json()
        if REPORTS_MARKETERS_PERIODIC_MAX_LIMIT < response.get('totalResults'):
            LOGGER.warning('More performance data (`{}`) than the tap can currently retrieve (`{}`)'.format(
                response.get('totalResults'), REPORTS_MARKETERS_PERIODIC_MAX_LIMIT))
        else:
            LOGGER.info('Syncing `{}` rows of performance data for campaign `{}`. Requested `{}`.'.format(
                response.get('totalResults'), state_sub_id, REPORTS_MARKETERS_PERIODIC_MAX_LIMIT))
        last_request_end = utils.now()

        LOGGER.info('Done in {} sec'.format(
            last_request_end.timestamp() - last_request_start.timestamp()))

        results = response.get('results') or []
        performance = [
            parse_performance(result, extra_persist_fields)
            for result in results]

        selected_fields = get_selected_fields(catalog, table_name)
        for record in performance:
            filtered_record = filter_record(record, selected_fields)
            singer.write_record(table_name, filtered_record, time_extracted=last_request_end)

        if performance:
            last_record = performance[-1]
            new_from_date = last_record.get('fromDate')
        else:
            new_from_date = date_range.get('to_date').strftime('%Y-%m-%d')

        state.setdefault(table_name, {})[state_sub_id] = new_from_date
        singer.write_state(state)

        from_date = new_from_date

        if last_request_start is not None and \
           (time.time() - last_request_end.timestamp()) < 30:
            to_sleep = 30 - (time.time() - last_request_end.timestamp())
            LOGGER.info(
                'Limiting to 2 requests per minute. Sleeping {} sec '
                'before making the next reporting request.'
                .format(to_sleep))
            time.sleep(to_sleep)


def get_selected_fields(catalog, stream_name):
    """Extract selected field names for a stream from the catalog metadata."""
    if catalog is None:
        return None
    
    for stream in catalog.streams:
        if stream.stream == stream_name or stream.tap_stream_id == stream_name:
            selected = set()
            for mdata_entry in stream.metadata:
                breadcrumb = mdata_entry.get('breadcrumb', [])
                metadata = mdata_entry.get('metadata', {})
                
                # Top-level stream metadata or property-level metadata
                if breadcrumb and len(breadcrumb) > 0 and breadcrumb[0] == 'properties':
                    field_name = breadcrumb[-1]
                    if metadata.get('selected') or metadata.get('inclusion') == 'automatic':
                        selected.add(field_name)
            
            return selected if selected else None
    
    return None


def filter_record(record, selected_fields):
    """Filter record to only include selected fields."""
    if selected_fields is None:
        return record
    return {k: v for k, v in record.items() if k in selected_fields}


def parse_campaign(campaign):
    live_status = campaign.get('liveStatus') or {}
    campaign['campaignOnAir'] = live_status.get('campaignOnAir')
    campaign['onAirReason'] = live_status.get('onAirReason')

    if campaign.get('budget') is not None:
        campaign['budget']['creationTime'] = parse_datetime(
            campaign.get('budget').get('creationTime'))
        campaign['budget']['lastModified'] = parse_datetime(
            campaign.get('budget').get('lastModified'))

    return campaign


def get_campaigns_page(account_id, access_token, offset):
    # NOTE: We probably should be more aggressive about ensuring that the
    # response was successful.
    return request(
        '{}/marketers/{}/campaigns'.format(BASE_URL, account_id),
        access_token, {'limit': MARKETERS_CAMPAIGNS_MAX_LIMIT,
                       'offset': offset}).json()


def get_campaign_pages(account_id, access_token):
    more_campaigns = True
    offset = 0

    while more_campaigns:
        LOGGER.info('Retrieving campaigns from offset `{}`'.format(
            offset))
        campaign_page = get_campaigns_page(account_id, access_token,
                                           offset)
        if TAP_CAMPAIGN_COUNT_ERROR_CEILING < campaign_page.get('totalCount'):
            msg = 'Tap found `{}` campaigns which is more than can be retrieved in the alloted time (`{}`).'.format(
                campaign_page.get('totalCount'), TAP_CAMPAIGN_COUNT_ERROR_CEILING)
            LOGGER.error(msg)
            raise Exception(msg)
        LOGGER.info('Retrieved offset `{}` campaigns out of `{}`'.format(
            offset, campaign_page.get('totalCount')))
        yield campaign_page
        if (offset + MARKETERS_CAMPAIGNS_MAX_LIMIT) < campaign_page.get('totalCount'):
            offset += MARKETERS_CAMPAIGNS_MAX_LIMIT
        else:
            more_campaigns = False

    LOGGER.info('Finished retrieving `{}` campaigns'.format(
        campaign_page.get('totalCount')))


def sync_campaign_page(state, access_token, account_id, campaign_page, selected_streams, catalog=None):
    campaigns = [parse_campaign(campaign) for campaign
                 in campaign_page.get('campaigns', [])]

    selected_fields = get_selected_fields(catalog, 'campaign')
    for campaign in campaigns:
        filtered_campaign = filter_record(campaign, selected_fields)
        singer.write_record('campaign', filtered_campaign,
                            time_extracted=utils.now())
        if streams.CampaignPerformance.name in selected_streams:
            sync_campaign_performance(state, access_token, account_id,
                                      campaign.get('id'), catalog=catalog)

    if streams.CampaignPerformance.name not in selected_streams:
        LOGGER.info("Skipping sync for campaign performance")


def sync_campaigns(state, access_token, account_id, selected_streams, catalog=None):
    LOGGER.info('Syncing campaigns.')

    for campaign_page in get_campaign_pages(account_id, access_token):
        sync_campaign_page(state, access_token, account_id, campaign_page, selected_streams, catalog=catalog)

    # Set stream-level bookmark once after all campaigns are synced so it
    # reflects overall progress, while keeping per-campaign bookmarks granular.
    campaign_performance_state = state.get('campaign_performance', {})
    if campaign_performance_state:
        stream_bookmark = max(campaign_performance_state.values())
        state.setdefault('bookmarks', {}).setdefault('campaign_performance', {})['fromDate'] = stream_bookmark
        singer.write_state(state)

    LOGGER.info('Done!')

def do_discover():
    LOGGER.info("Starting discovery")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")

def do_sync(catalog: singer.Catalog, config: Dict, state):
    #pylint: disable=global-statement
    global DEFAULT_START_DATE

    CONFIG.update(config)

    DEFAULT_START_DATE = config.get('start_date')[:10]

    access_token = config.get('access_token') or generate_token(config.get('username'), config.get('password'))
    if access_token is None:
        LOGGER.fatal("Failed to generate a new access token.")
        raise RuntimeError

    # NEVER RAISE THIS ABOVE DEBUG!
    LOGGER.debug('Using access token `{}`'.format(access_token))

    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info("selected_streams: {}".format(selected_streams))

    # Sync only for campaigns as Parent and campaign_performance as child
    if streams.Campaign.name in selected_streams:
        schema_path = get_abs_path(f"schemas/{streams.Campaign.name}.json")
        with open(schema_path) as f:
            campaign = json.load(f)
        singer.write_schema(streams.Campaign.name,
                            campaign,
                            key_properties=streams.Campaign.key_properties)
    else:
        msg = "Stream campaign is not selected for sync"
        LOGGER.error(msg)
        raise StreamSelectionError(msg)

    if streams.CampaignPerformance.name in selected_streams:
        schema_path = get_abs_path(f"schemas/{streams.CampaignPerformance.name}.json")
        with open(schema_path) as f:
            campaign_performance = json.load(f)
        singer.write_schema(streams.CampaignPerformance.name,
                            campaign_performance,
                            key_properties=streams.CampaignPerformance.key_properties,
                            bookmark_properties=streams.CampaignPerformance.bookmark_properties)

    sync_campaigns(state, access_token, config.get('account_id'), selected_streams, catalog=catalog)


def main_impl():
    args = singer.utils.parse_args(
        required_config_keys=[
            'account_id',
            'username',
            'password',
            'start_date'])

    if args.discover:
        do_discover()
    elif args.catalog:
        state = args.state or DEFAULT_STATE
        do_sync(args.catalog, args.config, state)


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()