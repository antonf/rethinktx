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

import logging
import random
import threading
import uuid

import rethinkdb
import rethinktx
import six
from . import mocks

import unittest

LOG = logging.getLogger(__name__)
NUM_ACCOUNTS = 10
NUM_ITERATIONS = 100
NUM_THREADS = 10


def perform_work(conn, account_ids):
    for _ in six.moves.range(NUM_ITERATIONS):
        acct_from_id, acct_to_id = random.sample(account_ids, 2)
        try:
            with rethinktx.Transaction(conn) as tx:
                accounts_tbl = tx.table('accounts')
                acct_from = accounts_tbl.get(acct_from_id)
                acct_to = accounts_tbl.get(acct_to_id)
                acct_from['balance'] -= 10
                acct_to['balance'] += 10
                accounts_tbl.put(acct_from_id, acct_from)
                accounts_tbl.put(acct_to_id, acct_to)
        except rethinktx.OptimisticLockFailure:
            pass
        except rethinkdb.ReqlAvailabilityError:
            pass


class WorkerThread(threading.Thread):
    def __init__(self, account_ids):
        super(WorkerThread, self).__init__()
        self.account_ids = account_ids

    def run(self):
        with mocks.get_connection() as conn:
            perform_work(conn, self.account_ids)


class ConcurrentTransactionsTestCase(unittest.TestCase):
    def setUp(self):
        super(ConcurrentTransactionsTestCase, self).setUp()
        with mocks.get_connection() as conn:
            if isinstance(conn, mocks.ConnectionMock):
                self.skipTest('Mocked connection not supported')
            self._ensure_provisioned(conn)
            self.account_ids = self._create_accounts(conn, NUM_ACCOUNTS)

    @staticmethod
    def _ensure_provisioned(conn):
        def ignore_exc(fn, *args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception:
                LOG.debug('Ignored exception', exc_info=True)

        ignore_exc(rethinkdb.db_create(conn.db).run, conn)
        ignore_exc(rethinkdb.table_create('accounts').run, conn)
        ignore_exc(rethinkdb.table_create('transactions').run, conn)
        rethinkdb.table('accounts').delete().run(conn)
        rethinkdb.table('transactions').delete().run(conn)

    @staticmethod
    def _create_accounts(conn, num_accounts):
        account_ids = []
        with rethinktx.Transaction(conn) as tx:
            accounts_tbl = tx.table('accounts')
            for i in six.moves.range(num_accounts):
                key = str(uuid.uuid4())
                account_ids.append(key)
                accounts_tbl.put(key, {'index': i, 'balance': 0})
        return account_ids

    def _total_balance(self):
        with mocks.get_connection() as conn:
            total_balance = 0
            with rethinktx.Transaction(conn) as tx:
                accounts_tbl = tx.table('accounts')
                for account_id in self.account_ids:
                    total_balance += accounts_tbl.get(account_id)['balance']
            return total_balance

    @staticmethod
    def _show_stats():
        with mocks.get_connection() as conn:
            num_committed = rethinkdb.table('transactions')\
                .filter({'status': 'committed'}).count().run(conn)
            num_aborted = rethinkdb.table('transactions')\
                .filter({'status': 'aborted'}).count().run(conn)
            LOG.info('Committed transactions: %d; Aborted transaction: %d',
                     num_committed, num_aborted)

    def test_concurrent_transactions(self):
        workers = []
        for _ in six.moves.range(NUM_THREADS):
            worker = WorkerThread(self.account_ids)
            workers.append(worker)
            worker.start()

        for worker in workers:
            worker.join()

        self._show_stats()
        self.assertEqual(0, self._total_balance())
