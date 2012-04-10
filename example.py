# Example usage of the Twisted Python OpenTSDB Client
# awf@google.com (Adam Fletcher)
#
#
# Copyright 2012 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Example use of the tostdb module

Creates a simple twisted event loop that adds metric data to an OpenTSDB instance
every 30 seconds.
"""

import calendar
import datetime
import sys
import logging
from random import randint
from tostdb import SendMetrics
from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet import task


def FakeStuff(metrics):
  metric = "example_metric"
  timestamp = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
  for i in range(1,11):
    tags = { "foo": "bar", "host" : "localhost", "thread": str(i) }
    noisy_value = (10000 / i) + randint(0, 100)
    metrics.append({ 'metric': metric, 'timestamp': timestamp, 'value': noisy_value, 'tags': tags})

@defer.inlineCallbacks
def SendTSDBMetrics(tsdburi, metrics):
  m_to_send = []

  for i in range(0, len(metrics)):
    m_to_send.append(metrics.pop())

  logger.info("Sending metrics to TSDB")
  if m_to_send:
    try:
      yield SendMetrics(tsdburi, m_to_send)
    except Exception, e:
      for i in m_to_send:
        metrics.append(i)
      m_to_send = []
      logger.debug("Failed to send metrics.")
      logger.error("%s (%s)" % (e.__class__, e,))

  logger.info("Done sending %s metrics to TSDB" % (len(m_to_send),))



logger = logging.getLogger('totsdb_example')
def main(argv):
  logger.addHandler(logging.StreamHandler())
  logger.setLevel(logging.DEBUG)

  tsdb_metrics = []

  fake_stuff_task = task.LoopingCall(
      lambda: FakeStuff(tsdb_metrics))
  reactor.callWhenRunning(fake_stuff_task.start, 10)

  tsdb_metric_task = task.LoopingCall(
      lambda: SendTSDBMetrics("localhost:4242", tsdb_metrics)
      .addErrback(logger.error))
  reactor.callWhenRunning(tsdb_metric_task.start, 30)

  reactor.run()
  return 0


if __name__ == '__main__':
  sys.exit(main(sys.argv))
