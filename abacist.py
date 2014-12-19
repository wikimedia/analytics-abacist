# -*- coding: utf-8 -*-
"""
  Stats aggregator for Wikimedia's blog

  Subscribes to
  <https://meta.wikimedia.org/wiki/Schema:WikimediaBlogVisit> events
  from EventLogging and updates counters in redis.

  If there's a page view on blog.wikimedia.org/some-post, the following
  counters get incremented: lifetime total views of the post, hourly views
  for the post, weekly, monthly, yearly. apart from the lifetime totals, the
  other keys are rotated by having an expiration time set based on a retention
  policy.

  Copyright 2014 Ori Livneh <ori@wikimedia.org>
  
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
      http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

"""
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import argparse
import time

import zmq
import redis


# Keep counts for each of the following time resolutions, specified
# as (name, duration_in_seconds, number_of_entries_to_retain).
resolutions = (
    ('hour', 3600, 25),
    ('day', 86400, 31),
    ('month', 2592000, 13),
    ('year', 31557600, 5),
)

ap = argparse.ArgumentParser(description='Wikimedia blog stats aggregator')
ap.add_argument('publisher', help='address of EventLogging publisher')
ap.add_argument('--redis-server', default='localhost')
args = ap.parse_args()

context = zmq.Context.instance()
sock = context.socket(zmq.SUB)

sock.connect(args.publisher)
sock.subscribe = b''

r = redis.Redis(args.redis_server)

for meta in iter(sock.recv_json, None):
    if (meta['schema'] != 'WikimediaBlogVisit' or
            meta['webHost'] != 'blog.wikimedia.org'):
        continue

    timestamp = int(meta['timestamp'])
    event = meta['event']

    request_url = event['requestUrl']
    referrer_url = event.get('referrerUrl')

    pipe = r.pipeline()

    pipe.hincrby('hits:total', request_url, 1)
    if referrer_url is not None:
        pipe.hincrby('referrers:total', request_url, 1)

    for resolution, length, retain_count in resolutions:
        expiry = timestamp + (length * retain_count)
        ordinal = timestamp // length

        key = 'hits:%s:%d' % (resolution, ordinal)
        print key
        pipe.hincrby(key, request_url, 1)
        pipe.expireat(key, expiry)

        if referrer_url is not None:
            key = 'referrers:%s:%d' % (resolution, ordinal)
            print key
            pipe.hincrby(key, referrer_url, 1)
            pipe.expireat(key, expiry)

    pipe.execute()
