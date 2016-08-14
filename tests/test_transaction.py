# Copyright 2016, Anton Frolov <frolov.anton@gmail.com>
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import rethinktx
from . import mocks

import unittest


class TransactionTestCase(unittest.TestCase):
    def setUp(self):
        super(TransactionTestCase, self).setUp()
        self.conn = mocks.get_connection()

    def tearDown(self):
        super(TransactionTestCase, self).tearDown()
        mocks.cleanup_connection(self.conn)

    def test_get_non_existent_raise_notfound(self):
        with rethinktx.Transaction(self.conn) as tx:
            with self.assertRaises(rethinktx.NotFound):
                tx.table('table1').get('key')

    def test_get_non_existent_return_default(self):
        with rethinktx.Transaction(self.conn) as tx:
            uniq_val = object()
            self.assertIs(uniq_val, tx.table('table1').get('key', uniq_val))

    def test_put_get(self):
        with rethinktx.Transaction(self.conn) as tx1:
            tx1.table('table1').put('key', 'data')

        with rethinktx.Transaction(self.conn) as tx2:
            self.assertEqual('data', tx2.table('table1').get('key'))

    def test_concurrent_non_overlapping(self):
        with rethinktx.Transaction(self.conn) as tx1, \
                rethinktx.Transaction(self.conn) as tx2:
            tx1.table('table1').put('key1', 'data1')
            tx2.table('table1').put('key2', 'data2')

        with rethinktx.Transaction(self.conn) as tx:
            self.assertEqual('data1', tx.table('table1').get('key1'))
            self.assertEqual('data2', tx.table('table1').get('key2'))

    def test_concurrent_overlapping(self):
        with rethinktx.Transaction(self.conn) as tx1, \
                rethinktx.Transaction(self.conn) as tx2:
            tx1.table('table1').put('key', 'data1')
            tx2.table('table1').put('key', 'data2')
            with self.assertRaises(rethinktx.OptimisticLockFailure):
                tx1.commit()

        # tx2 should win since it have greater XID/timestamp
        with rethinktx.Transaction(self.conn) as tx:
            self.assertEqual('data2', tx.table('table1').get('key'))

    def test_concurrent_overlapping_rollback(self):
        with rethinktx.Transaction(self.conn) as tx1, \
                rethinktx.Transaction(self.conn) as tx2:
            tx1.table('table1').put('key-1', 'data1')
            tx1.table('table1').put('key-2', 'data1')

            tx2.table('table1').put('key-1', 'data2')

            with self.assertRaises(rethinktx.OptimisticLockFailure):
                tx1.commit()

        # tx2 should win since it have greater XID/timestamp
        with rethinktx.Transaction(self.conn) as tx:
            self.assertEqual('data2', tx.table('table1').get('key-1'))
            with self.assertRaises(rethinktx.NotFound):
                print(tx.table('table1').get('key-2'))

    def test_write_to_changed_committed(self):
        with rethinktx.Transaction(self.conn) as tx:
            tx.table('table1').put('key-1', 'data1')

        with rethinktx.Transaction(self.conn) as tx1, \
                rethinktx.Transaction(self.conn) as tx2:
            # Read key-1 through tx2 in order to fix current value for tx2
            self.assertEqual('data1', tx2.table('table1').get('key-1'))

            # Write key-1 through tx1 and commit it, making value red by tx2
            # stale
            tx1.table('table1').put('key-1', 'modified data1')
            tx1.commit()

            # Now write to key-1 through tx2 should fail with optimisic lock
            # failure
            with self.assertRaises(rethinktx.OptimisticLockFailure):
                tx2.table('table1').put('key-1', 'what a failure')
                tx2.abort()
