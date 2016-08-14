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

import copy
import datetime
import os
import six

import rethinkdb


def get_arg(term, index):
    return term._args[index]


def get_args(term):
    return term._args


class ConnectionMock(object):
    def __init__(self):
        self.db = None
        self.tables = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def use(self, db):
        self.db = db

    def _start(self, term, **global_optargs):
        ctx = {
            'result': {
                'deleted': 0,
                'errors': 0,
                'inserted': 0,
                'replaced': 0,
                'skipped': 0,
                'unchanged': 0,
            }
        }
        self._affect_context_by_optargs(ctx, global_optargs)
        try:
            return self._eval_term(ctx, term)
        except RuntimeError as ex:
            result = ctx['result']
            result['errors'] += 1
            result['error'] = ex.args[0]
            return result

    def _affect_context_by_optargs(self, ctx, optargs):
        if 'return_changes' in optargs:
            ctx['return_changes'] = self._eval_term(ctx,
                                                    optargs['return_changes'])
            if ctx['return_changes']:
                ctx['result']['changes'] = []
        if 'conflict' in optargs:
            ctx['conflict'] = self._eval_term(ctx, optargs['conflict'])

    def _eval_term(self, ctx, term):
        if not isinstance(term, rethinkdb.ast.RqlQuery):
            return term
        local_ctx = copy.deepcopy(ctx)
        self._affect_context_by_optargs(local_ctx, term.optargs)
        if isinstance(term, rethinkdb.ast.Get):
            table = self._eval_term(local_ctx, get_arg(term, 0))
            key = self._eval_term(local_ctx, get_arg(term, 1))
            return table.get(key)
        elif isinstance(term, rethinkdb.ast.Insert):
            return self._eval_insert(local_ctx, term)
        elif isinstance(term, rethinkdb.ast.Update):
            return self._eval_update(local_ctx, term)
        elif isinstance(term, rethinkdb.ast.MakeObj):
            return {k: self._eval_term(local_ctx, v)
                    for k, v in six.iteritems(term.optargs)}
        elif isinstance(term, rethinkdb.ast.MakeArray):
            return [self._eval_term(local_ctx, x) for x in get_args(term)]
        elif isinstance(term, rethinkdb.ast.Datum):
            return term.data
        elif isinstance(term, rethinkdb.ast.Bracket):
            lhs = self._eval_term(local_ctx, get_arg(term, 0))
            rhs = self._eval_term(local_ctx, get_arg(term, 1))
            return lhs[rhs]
        elif isinstance(term, rethinkdb.ast.Add):
            lhs = self._eval_term(local_ctx, get_arg(term, 0))
            rhs = self._eval_term(local_ctx, get_arg(term, 1))
            return lhs + rhs
        elif isinstance(term, rethinkdb.ast.Eq):
            lhs = self._eval_term(local_ctx, get_arg(term, 0))
            rhs = self._eval_term(local_ctx, get_arg(term, 1))
            return lhs == rhs
        elif isinstance(term, rethinkdb.ast.Ne):
            lhs = self._eval_term(local_ctx, get_arg(term, 0))
            rhs = self._eval_term(local_ctx, get_arg(term, 1))
            return lhs != rhs
        elif isinstance(term, rethinkdb.ast.And):
            lhs = self._eval_term(local_ctx, get_arg(term, 0))
            rhs = self._eval_term(local_ctx, get_arg(term, 1))
            return lhs and rhs
        elif isinstance(term, rethinkdb.ast.ImplicitVar):
            return local_ctx['implicit_var']
        elif isinstance(term, rethinkdb.ast.Branch):
            if self._eval_term(local_ctx, get_arg(term, 0)):
                return self._eval_term(local_ctx, get_arg(term, 1))
            else:
                return self._eval_term(local_ctx, get_arg(term, 2))
        elif isinstance(term, rethinkdb.ast.Table):
            table_name = self._eval_term(local_ctx, get_arg(term, 0))
            return self.tables.setdefault(table_name, {})
        elif isinstance(term, rethinkdb.ast.Now):
            return datetime.datetime.now()
        elif isinstance(term, rethinkdb.ast.Func):
            # Just ignore function and eval the body
            return self._eval_term(local_ctx, get_arg(term, 1))
        elif isinstance(term, rethinkdb.ast.UserError):
            raise RuntimeError(self._eval_term(local_ctx, get_arg(term, 0)))
        else:
            raise AssertionError(
                'Term class ' + repr(term.__class__) + ' not supported')

    @staticmethod
    def _insert_value(ctx, table, key, value):
        result = ctx['result']
        table[key] = value
        result['inserted'] += 1
        if ctx.get('return_changes'):
            result['changes'].append({
                'old_val': None,
                'new_val': copy.deepcopy(value),
            })
        return result

    @staticmethod
    def _update_value(ctx, new_value, value):
        result = ctx['result']
        for k, v in six.iteritems(value):
            if k not in new_value:
                break
            if new_value[k] != v:
                break
        else:
            if ctx.get('return_changes') == 'always':
                old_value = copy.deepcopy(new_value)
                result['changes'].append({
                    'old_val': old_value,
                    'new_val': old_value,
                })
            result['unchanged'] += 1
            return result
        old_value = copy.deepcopy(new_value)
        new_value.update(value)
        if ctx.get('return_changes'):
            result['changes'].append({
                'old_val': old_value,
                'new_val': copy.deepcopy(new_value),
            })
        result['replaced'] += 1
        return result

    @staticmethod
    def _skip_value(ctx):
        result = ctx['result']
        result['skipped'] += 1
        return result

    def _eval_insert(self, local_ctx, term):
        table = self._eval_term(local_ctx, get_arg(term, 0))
        value = self._eval_term(local_ctx, get_arg(term, 1))
        key = value['id']
        conflict = local_ctx.get('conflict', 'error')
        if key not in table or conflict == 'replace':
            return self._insert_value(local_ctx, table, key, value)
        elif conflict == 'error':
            raise RuntimeError('conflict')
        elif conflict == 'update':
            return self._update_value(local_ctx, table[key], value)

    def _eval_update(self, local_ctx, term):
        curr_value = self._eval_term(local_ctx, get_arg(term, 0))
        if curr_value is None:
            return self._skip_value(local_ctx)
        else:
            local_ctx['implicit_var'] = curr_value
            updated_data = self._eval_term(local_ctx, get_arg(term, 1))
            return self._update_value(local_ctx, curr_value, updated_data)


def get_connection():
    port = int(os.environ.get('RDB_PORT', '0'))
    host = os.environ.get('RDB_HOST', '')
    db = os.environ.get('RDB_DB', '')
    if port and host and db:
        return rethinkdb.connect(host=host, port=port, db=db)
    else:
        return ConnectionMock()


def cleanup_connection(conn):
    if isinstance(conn, ConnectionMock):
        return
    for table_name in rethinkdb.table_list().run(conn):
        rethinkdb.table(table_name).delete().run(conn)
