# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from functools import partial
import six
import sys
from threading import Thread
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import WhiteListRoundRobinPolicy

from tests.integration import PROTOCOL_VERSION


class ConcurrencyTests(unittest.TestCase):

    def test_connection_contention(self):
        if PROTOCOL_VERSION < 3:
            raise unittest.SkipTest("v3 protocol is required")

        policy = WhiteListRoundRobinPolicy("127.0.0.1")
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, load_balancing_policy=policy)
        session = cluster.connect('test1rf')

        session.execute("CREATE TABLE foo (a int PRIMARY KEY, b int)")
        prepared = session.prepare("INSERT INTO foo (a, b) VALUES (?, ?)")
        execute_concurrent_with_args(session, prepared, [(i, i) for i in range(100)])

        if six.PY2:
            original_interval = sys.getcheckinterval()
            sys.setcheckinterval(1)
        else:
            original_interval = sys.getswitchinterval()
            sys.setswitchinterval(1)

        prepared = session.prepare("SELECT * FROM foo WHERE a=?")

        def do_reads(thread_num):
            for i in range(10000):
                results = session.execute(prepared, [thread_num])
                self.assertEquals(1, len(results))
                self.assertEquals(thread_num, results[0].a)
                self.assertEquals(thread_num, results[0].b)

        try:
            threads = []
            for i in range(10):
                thread = Thread(target=partial(do_reads, i))
                threads.append(thread)

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()
        finally:
            if six.PY2:
                sys.setcheckinterval(original_interval)
            else:
                sys.setswitchinterval(original_interval)
            cluster.shutdown()
