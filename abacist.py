# -*- coding: utf-8 -*-
"""
  abacist
  ~~~~~~~

  Web analytics aggregator for the Wikimedia Blog.

  Subscribes to [[meta:Schema:WikimediaBlogVisit]] events and updates
  counters in Redis.

  If there's a page view on blog.wikimedia.org/some-post, the following
  counters get incremented: lifetime total views of the post, hourly views
  for the post, weekly, monthly, yearly. apart from the lifetime totals,
  the other keys are rotated by having an expiration time set based on a
  retention policy.

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

import eventlogging
import redis


class Interval(object):
    """Represents a unit of time."""

    def __init__(self, name, duration, period):
        self.name = name
        self.duration = duration
        self.period = period

    def epoch_to_ordinal(self, epoch_time):
        """Convert a POSIX timestamp to an ordinal value. The ordinal value
        is the number of complete units that have elapsed since midnight,
        1 January 1970."""
        return int(epoch_time / self.duration)

    def ordinal_to_epoch(self, ordinal_time):
        """Convert an ordinal value to a POSIX timestamp."""
        return int(ordinal_time * self.duration)


SCHEMA = 'WikimediaBlogVisit'

INTERVALS = (
    Interval('hour', duration=(60*60), period=24),
    Interval('day', duration=(60*60*24), period=30),
    Interval('month', duration=(60*60*24*30), period=12),
    Interval('year', duration=(60*60*24*365.25), period=5),
    Interval('total', duration=float('inf'), period=1),
)


ap = argparse.ArgumentParser(description='Wikimedia blog stats aggregator')
ap.add_argument('endpoint', help='address of EventLogging publisher')
ap.add_argument('--redis-server', default='localhost')
args = ap.parse_args()

r = redis.Redis(args.redis_server)
events = eventlogging.connect(args.endpoint)

for meta in events.filter(schema=SCHEMA, webHost='blog.wikimedia.org'):
    with r.pipeline() as pipe:
        for field in ('requestUrl', 'referrerUrl'):
            value = meta['event'].get(field)
            if not value:
                continue
            for iv in INTERVALS:
                ordinal = iv.epoch_to_ordinal(meta['timestamp'])
                key = '%s:%s:%s:%d' % (SCHEMA, field, iv.name, ordinal)
                pipe.hincrby(key, value, 1)
                if iv.duration < float('inf'):
                    expires = iv.ordinal_to_epoch(ordinal + iv.period)
                    pipe.expireat(key, expires)
        pipe.execute()
