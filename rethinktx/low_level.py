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

import uuid

import rethinkdb

from rethinktx import exceptions

TX_TBL = rethinkdb.table('transactions')
INTENT_ROW = rethinkdb.row['intent']
XID_ROW = rethinkdb.row['xid']
STATUS_ROW = rethinkdb.row['status']


def run_query(query, conn):
    return query.run(conn, read_mode='majority')


def create_tx(conn):
    xid = str(uuid.uuid4())
    result = run_query(TX_TBL.insert({'id': xid,
                                      'status': 'pending',
                                      'timestamp': rethinkdb.now()},
                                     conflict='error'), conn)
    if result['inserted'] != 1:
        raise exceptions.DatabaseException(
            'Error creating transaction record: %s', result.get('error'))
    return xid


def write(conn, xid, table, key, old_xid, document):
    if old_xid is not None:
        result = run_query(table.get(key).update(
                               rethinkdb.branch(
                                   XID_ROW.eq(old_xid),
                                   {'xid': xid, 'intent': document},
                                   rethinkdb.error('write conflict'))),
                           conn)
    else:
        result = run_query(table.insert({'id': key, 'xid': xid,
                                         'intent': document},
                                        conflict='error'),
                           conn)
    if result['errors'] != 0:
        raise exceptions.OptimisticLockFailure(xid)


def read(conn, table, key, default=None):
    record = run_query(table.get(key), conn)
    if record is None:
        return None, default
    while record['intent'] is not None:
        record_xid = record['xid']
        tx = run_query(TX_TBL.get(record_xid), conn)
        if tx is None:
            tx_status = 'aborted'
        else:
            tx_status = tx['status']
        if tx_status == 'pending':
            if abort(conn, record_xid):
                tx_status = 'aborted'
            else:
                continue
        if tx_status == 'aborted':
            result = run_query(table.get(key).update(
                rethinkdb.branch(
                    XID_ROW.eq(record_xid) & INTENT_ROW.ne(None),
                    {'intent': None}, {}),
                return_changes='always'), conn)
            record = result['changes'][0]['new_val']
        elif tx['status'] == 'committed':
            result = run_query(table.get(key).update(
                rethinkdb.branch(
                    XID_ROW.eq(record_xid) & INTENT_ROW.ne(None),
                    {'intent': None, 'value': INTENT_ROW}, {}),
                return_changes='always'), conn)
            record = result['changes'][0]['new_val']
    return record['xid'], record.get('value', default)


def commit(conn, xid, changes):
    result = run_query(TX_TBL.get(xid).update(
        rethinkdb.branch(
            STATUS_ROW.eq('pending'),
            {'status': 'committed', 'changes': changes},
            rethinkdb.error('precondition failed'))), conn)
    return result['errors'] == 0


def abort(conn, xid):
    result = run_query(TX_TBL.get(xid).update(
        rethinkdb.branch(
            STATUS_ROW.eq('pending'),
            {'status': 'aborted'},
            {}),
        return_changes='always'), conn)
    if result['skipped'] == 1:
        return True
    else:
        return result['changes'][0]['new_val']['status'] == 'aborted'


def clear(conn, xid, committed, table, keys):
    update = {'intent': None}
    if committed:
        update['value'] = INTENT_ROW
    for key in keys:
        run_query(table.get(key).update(
            rethinkdb.branch(XID_ROW.eq(xid) & INTENT_ROW.ne(None),
                             update, {})), conn)
