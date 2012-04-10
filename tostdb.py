# Twisted Python OpenTSDB Client
# awf@google.com (Adam Fletcher)
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

"""A Twisted Python client for OpenTSDB

This client takes a list of metrics and uses the simple
TSDB TCP protocol to put each metrics into a TSDB
instance.

Each metric is a dictionary:
  * "metric" is a string representing a TSDB metric
  * "timestamp" is a timestamp in epoch seconds
  * "value" is the numerical value to associate with the metric
  * "tags" is a dictionary of key=value TSDB tags

SendMetrics() should be imported from this module and used;
there's no need to use the other functions.

"""

from twisted.internet import reactor
from twisted.internet import utils
from twisted.internet import error
from twisted.internet import defer
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.python import failure

class TSDB(Protocol):
  def connectionMade(self):
    for metric in self.factory.metrics:
      metric_string = "put "
      metric_string += metric["metric"] + " " + str(metric["timestamp"]) + " " + str(metric["value"])
      for k in metric["tags"]:
        metric_string += " " + k + "=" + str(metric["tags"][k])
      try:
        self.transport.write(metric_string + "\r\n")
      except TypeError:
        pass # twisted hates unicode; casting to a str() is probably not enjoyed by TSDB

    self.transport.loseConnection()
    self.factory.deferred.callback(None)

class TSDBClientFactory(ClientFactory):
  protocol = TSDB
  def __init__(self, metrics, dfr):
    self.deferred = dfr
    self.metrics = metrics

  def clientConnectionFailed(self, connector, reason):
    self.deferred.errback(reason)

def SendMetrics(url, data):
  (host, port) = url.split(':')
  d = defer.Deferred()
  timeout = 1 # fail fast on a TCP connection timeout

  def HandleServerFailure(result):
    if isinstance(result, failure.Failure):
      result.trap(error.ConnectionDone)
      return None
    else:
      return result

  d.addErrback(HandleServerFailure)

  reactor.connectTCP(host, int(port), TSDBClientFactory(data, d), timeout)

  return d

if __name__ == '__main__':
  pass
