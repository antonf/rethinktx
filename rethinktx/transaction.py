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
import six

import rethinkdb

from rethinktx import exceptions
from rethinktx import low_level

LOG = logging.getLogger(__name__)
MISSING = object()
STATE_PENDING = 'pending'
STATE_COMMITTED = 'committed'
STATE_ABORTED = 'aborted'


class VersionedDocument(object):
    __slots__ = ('xid', 'doc')

    def __init__(self, xid, doc):
        self.xid = xid
        self.doc = doc

    def __repr__(self):
        return '{name}(xid={xid}, doc={doc})'.format(
            name=self.__class__.__name__, xid=self.xid, doc=self.doc)


class Table(object):
    def __init__(self, tx, name):
        self.tx = tx
        self.name = name
        self.table = rethinkdb.table(name)

    def _read(self, key, default=None):
        if self.tx.state is not STATE_PENDING:
            raise RuntimeError('Transaction in state "{state}" use is '
                               'prohibited'.format(state=self.tx.state))

        tx = self.tx
        vd = tx._lookup(self.name, key)
        if vd is None:
            xid, doc = low_level.read(tx.conn, self.table, key,
                                      default=MISSING)
            if doc is not MISSING:
                vd = VersionedDocument(xid, doc)
                tx._memoize(self.name, key, vd)
            else:
                return VersionedDocument(xid, default)
        return vd

    def _write(self, key, old_vd, new_doc):
        if self.tx.state is not STATE_PENDING:
            raise RuntimeError('Transaction in state "{state}" use is '
                               'prohibited'.format(state=self.tx.state))

        tx = self.tx
        low_level.write(tx.conn, tx.xid, self.table, key, old_vd.xid, new_doc)
        tx._memoize(self.name, key, VersionedDocument(tx.xid, new_doc))

    def get(self, key, default=MISSING):
        vd = self._read(key, default)
        if vd.doc is MISSING:
            raise exceptions.NotFound(key)
        else:
            return vd.doc

    def put(self, key, doc):
        old_vd = self._read(key)
        self._write(key, old_vd, doc)

    def update(self, key, data):
        old_vd = self._read(key, MISSING)
        if old_vd.doc is MISSING:
            raise exceptions.NotFound(self.name, key)
        doc = dict(old_vd.doc)
        doc.update(data)
        self._write(key, old_vd, doc)


class Transaction(object):
    def __init__(self, conn=None, db=None, host='localhost',
                 port=rethinkdb.DEFAULT_PORT):
        if conn is None:
            conn = rethinkdb.connect(host, port)
        if db is not None:
            conn.use(db)
        self.session = {}
        self.conn = conn
        self.xid = low_level.create_tx(conn)
        self.state = STATE_PENDING
        LOG.debug('Started transaction #%s', self.xid)

    def table(self, name):
        return Table(self, name)

    def _lookup(self, table_name, key):
        table = self.session.get(table_name)
        if table is None:
            return None
        return table.get(key)

    def _memoize(self, table_name, key, vd):
        self.session.setdefault(table_name, {})[key] = vd

    def commit(self):
        writes_keys = {}
        for table_name, data in six.iteritems(self.session):
            keys = set()
            for key, vd in six.iteritems(data):
                if vd.xid == self.xid:
                    keys.add(key)

            if keys:
                writes_keys[table_name] = keys

        LOG.debug('Committing transaction #%s: writes=%s', self.xid,
                  repr(writes_keys))

        if low_level.commit(self.conn, self.xid, writes_keys):
            self.state = STATE_COMMITTED
            for table_name, keys in six.iteritems(writes_keys):
                table = rethinkdb.table(table_name)
                low_level.clear(self.conn, self.xid, True, table, keys)
        else:
            self.abort()
            raise exceptions.OptimisticLockFailure(self.xid)

    def abort(self):
        LOG.debug('Aborting transaction #%s', self.xid)
        if low_level.abort(self.conn, self.xid):
            self.state = STATE_ABORTED
            for table_name, data in six.iteritems(self.session):
                table = rethinkdb.table(table_name)
                keys = six.iterkeys(data)
                low_level.clear(self.conn, self.xid, False, table, keys)
        else:
            raise exceptions.OptimisticLockFailure(self.xid)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.state == STATE_PENDING:
            if exc_val is not None:
                LOG.debug('Aborting transaction #%s due to exception',
                          self.xid, exc_info=(exc_type, exc_val, exc_tb))
                self.abort()
            else:
                self.commit()
