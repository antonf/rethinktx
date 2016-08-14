RethinkTx: transaction protocol for RethinkDb
=============================================

RethinkTx provide transactions that guarantee that updates to documents will
only succeed if no other transaction concurrently update any of this documents.
Any of records red during transaction but not updated may change before
transaction will successfully commit.

Transactions are atomic in a sense that either all writes will succeed or
nothing will occur.

Transactions are optimistic in a sense that if some two transactions will try
to update same database record concurrently, then one of them will be aborted.

How it works
------------

First, let's introduce a way to store uncommitted changes and track which
transaction did it. In order to do this, let's modify the way we store values
in tables. Here is new table record schema:

<table>
  <tr>
    <th colspan=2>Table record object structure</th>
  </tr>
  <tr>
    <th>ID</th>
    <td>
      Record key.
    </td>
  </tr>
  <tr>
    <th>Value</th>
    <td>
      Committed record value. Contents of this field is what clients usually
      would see when they try to read data associated with record key.
    </td>
  </tr>
  <tr>
    <th>Intent</th>
    <td>
      When some record is updated by pending transaction, it will first stage
      the updated data by writing it to this field. Write intent will replace
      the value of record at some point in time after transaction commit. For
      objects that are not being modified by any transaction this value usually
      will be equal to <strong>None</strong>, but may contain actual value due
      to race conditions or incomplete transactions. This inconsistency will be
      fixed by implementing rather complex read procedure.
    </td>
  </tr>
  <tr>
    <th>XID</th>
    <td>
      Each time some transaction put data to intent field of the record,
      it will write it's identifier to this field so we can get transaction
      state using this identifier and check if transaction was committed
      if write intent isn't None.
      Also, this field could be considered as document version, since we use
      monotonically increasing integer values for transaction identifiers.
    </td>
  </tr>
</table>

Next, we need to introduce transaction objects which will track in what state
the transactions are, so that we can fix any inconsistencies introduced by
concurrent or incomplete transactions.

<table>
  <tr>
    <th colspan=2>Transaction object structure</th>
  </tr>
  <tr>
    <th>ID</th>
    <td>
      Numeric identifier that is always greater than identifiers of any
      previously created transactions. In case of concurrent write more recent
      transaction will take precedence over the older so the latter one will
      be aborted. This identifier will be used to decide which one is older.
    </td>
  </tr>
  <tr>
    <th>State</th>
    <td>
      Transactions are created in <strong>pending</strong> state. Later they
      can change state to <strong>committed</strong> or
      <strong>aborted</strong>. Any other state changes are prohibited.
    </td>
  </tr>
</table>

Read Procedure
--------------

After we get record associated with some key from database it can be in one of
four states:

1. **intent** is **None** which means that record is in it's final state and
   value can be found in **value** field.
2. **intent** is not **None** and **XID** pointing to transaction in *pending*
   state which means that some other transaction wrote to the same value we 
   are trying to read from. We try to abort this transaction and then run read
   procedure again.
3. **intent** is not **None** and **XID** pointing to transaction in *aborted*
   state means that we can discard **intent** value, so we perform CAS
   operation setting **intent** to **None** if **XID** value didn't change.
   Then run read procedure again.
4. **intent** is not **None** and **XID** pointing to transaction in
   *committed* state means that we can move **intent** field value to **value**
   field and set **intent** to **None**. We perform CAS operation doing exactly
   that which will check that **XID** didn't change and **intent** is not
   **None** before changing record value. Then we run read procedure again.

Thus read procedure ensures that record will be in state # 1 in the end. It
returns **XID** and **value** fields values.

Write Procedure
---------------

Before any write to database the read attempt for the key will be made.
Read procedure will ensure that **intent** field is **None**. Also, we will use
**XID** field value returned by read procedure to check if object was changed
since the read was performed.

The write itself is a simple CAS operation checking if **XID** field is equal
to **XID** value returned by read procedure; if it didn't change, then new
value is written into **intent** field and current transaction ID is
written to **XID** field; if **XID** field value was changed since last read,
then it means that some other transaction have written to same table record so
we should abort current transaction and start over or fail.


Transaction Commit and Abort Procedures
---------------------------------------

Commit and abort operations are almost the same: both perform CAS operation
that checks if transaction is in *pending* state before changing it to
*committed* or *aborted* state. Then cleanup procedure will be executed which
will make sure that **intent** field value will be moved to **value** field or
discarded depending on final transaction state.

References
----------

[How CockroachDB Does Distributed, Atomic Transactions](
https://www.cockroachlabs.com/blog/how-cockroachdb-distributes-atomic-transactions/)

[Transactions in MongoDB, Cassandra, Zookeeper and others](
http://rystsov.info/2012/09/01/cas.html)

How to run tests
----------------

Running tests against mocked database:

    nosetests -s -v tests

Running tests against real database:

    RDB_DB=test1 RDB_PORT=28015 RDB_HOST=localhost nosetests -s -v tests


Roadmap
-------

* Implement tests that will simulate cluster node failures during execution of
  several concurrent transactions and check database for correctness   
* Table locking for consistent read queries
* Various optimizations that will make certain database interaction patterns
  faster (like reading and updating single document)
