/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.lib.DoubleIntIndex;
import org.hsqldb_voltpatches.lib.HashMap;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.HsqlDeque;
import org.hsqldb_voltpatches.lib.IntKeyHashMapConcurrent;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.LongDeque;
import org.hsqldb_voltpatches.lib.MultiValueHashMap;
import org.hsqldb_voltpatches.persist.CachedObject;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.store.ValuePool;

/**
 * Manages rows involved in transactions
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.0
 * @since 2.0.0
 */
public class TransactionManager {

    Database database;
    boolean  mvcc = false;

    //
    ReentrantReadWriteLock           lock      = new ReentrantReadWriteLock();
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    // functional unit - sessions involved in live transactions

    /** live transactions keeping committed transactions from being merged */
    LongDeque  liveTransactionTimestamps = new LongDeque();
    AtomicLong globalChangeTimestamp     = new AtomicLong();

    // functional unit - merged committed transactions
    HsqlDeque committedTransactions          = new HsqlDeque();
    LongDeque committedTransactionTimestamps = new LongDeque();

    // functional unit - cached table transactions

    /** Map : rowID -> RowAction */
    public IntKeyHashMapConcurrent rowActionMap =
        new IntKeyHashMapConcurrent(10000);

    //
    //
    HashMap           tableWriteLocks = new HashMap();
    MultiValueHashMap tableReadLocks  = new MultiValueHashMap();

    TransactionManager(Database db) {
        database = db;
    }

    public void setMVCC(boolean value) {

        writeLock.lock();

        try {
            synchronized (liveTransactionTimestamps) {
                if (liveTransactionTimestamps.isEmpty()) {
                    mvcc = value;

                    return;
                }
            }
        } finally {
            writeLock.unlock();
        }

        throw Error.error(ErrorCode.X_25001);
    }

    public void completeActions(Session session) {

        Object[] list        = session.rowActionList.getArray();
        int      limit       = session.rowActionList.size();
        boolean  canComplete = true;

        writeLock.lock();

        try {
            for (int i = session.actionIndex; i < limit; i++) {
                RowAction rowact = (RowAction) list[i];

                if (rowact.complete(session, session.tempSet)) {
                    continue;
                }

                canComplete = false;

                if (session.isolationMode == SessionInterface
                        .TX_REPEATABLE_READ || session
                        .isolationMode == SessionInterface.TX_SERIALIZABLE) {
                    session.abortTransaction = true;

                    break;
                }
            }

            for (int i = session.actionIndex; canComplete && i < limit; i++) {
                RowAction action = (RowAction) list[i];

                if (!action.table.isLogged) {
                    continue;
                }

                Row row = action.memoryRow;

                if (row == null) {
                    PersistentStore store =
                        session.sessionData.getRowStore(action.table);

                    row = (Row) store.get(action.getPos(), false);
                }

                Object[] data = row.getData();

                try {
                    int actionType =
                        action.getActionType(session.actionTimestamp);

                    if (actionType == RowActionBase.ACTION_INSERT) {
                        database.logger.writeInsertStatement(session,
                                                             action.table,
                                                             data);
                    } else if (actionType == RowActionBase.ACTION_DELETE) {
                        database.logger.writeDeleteStatement(session,
                                                             action.table,
                                                             data);
                    } else if (actionType == RowActionBase.ACTION_NONE) {

                        // no logging
                    } else {
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "TransactionManager");
                    }
                } catch (HsqlException e) {

                    // can put db in special state
                }
            }

            if (!canComplete && !session.abortTransaction) {
                session.redoAction = true;

                rollbackAction(session);

                if (!session.tempSet.isEmpty()) {
                    session.latch.setCount(session.tempSet.size());

                    for (int i = 0; i < session.tempSet.size(); i++) {
                        Session current = (Session) session.tempSet.get(i);

                        current.waitingSessions.add(session);

                        // waitedSessions.put(current, session);
                        // waitingSessions.put(session, current);
                    }
                }
            }
        } finally {
            writeLock.unlock();
            session.tempSet.clear();
        }
    }

    public boolean prepareCommitActions(Session session) {

        Object[] list  = session.rowActionList.getArray();
        int      limit = session.rowActionList.size();

        if (session.abortTransaction) {

//            System.out.println("cascade fail " + session + " " + session.actionTimestamp);
            return false;
        }

        try {
            writeLock.lock();

            for (int i = 0; i < limit; i++) {
                RowAction rowact = (RowAction) list[i];

                if (!rowact.canCommit(session, session.tempSet)) {

//                System.out.println("commit conflicts " + session + " " + session.actionTimestamp);
                    return false;
                }
            }

            session.actionTimestamp = nextChangeTimestamp();

            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) list[i];

                action.prepareCommit(session);
            }

            for (int i = 0; i < session.tempSet.size(); i++) {
                Session current = (Session) session.tempSet.get(i);

                current.abortTransaction = true;
            }

            return true;
        } finally {
            writeLock.unlock();
            session.tempSet.clear();
        }
    }

    public boolean commitTransaction(Session session) {

        if (session.abortTransaction) {

//            System.out.println("cascade fail " + session + " " + session.actionTimestamp);
            return false;
        }

        int      limit = session.rowActionList.size();
        Object[] list  = limit == 0 ? ValuePool.emptyObjectArray
                                    : session.rowActionList.getArray();

        try {
            writeLock.lock();

            for (int i = 0; i < limit; i++) {
                RowAction rowact = (RowAction) list[i];

                if (!rowact.canCommit(session, session.tempSet)) {

//                System.out.println("commit conflicts " + session + " " + session.actionTimestamp);
                    return false;
                }
            }

            endTransaction(session);

            if (limit == 0) {
                if (!mvcc) {
                    endTransactionTPL(session);
                }

                try {
                    session.logSequences();

                    if (mvcc) {
                        database.logger.writeCommitStatement(session);
                    }
                } catch (HsqlException e) {}

                return true;
            }

            // new actionTimestamp used for commitTimestamp
            session.actionTimestamp = nextChangeTimestamp();

            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) list[i];

                action.commit(session);
            }

            for (int i = 0; i < session.tempSet.size(); i++) {
                Session current = (Session) session.tempSet.get(i);

                current.abortTransaction = true;
            }

            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) list[i];

                if (action.table.hasLobColumn) {
                    int type = action.getCommitType(session.actionTimestamp);

                    switch (type) {

                        case RowActionBase.ACTION_INSERT :
                            Row row = action.memoryRow;

                            if (row == null) {
                                PersistentStore store =
                                    session.sessionData.getRowStore(
                                        action.table);

                                row = (Row) store.get(action.getPos(), false);
                            }

                            action.table.addLobUsageCount(session,
                                                          row.getData());
                            break;

                        default :
                    }
                }

                if (action.table.tableType == TableBase.TEXT_TABLE) {
                    PersistentStore store =
                        session.sessionData.getRowStore(action.table);
                    int type = action.getCommitType(session.actionTimestamp);

                    switch (type) {

                        case RowActionBase.ACTION_DELETE :
                            store.removePersistence(action.getPos());
                            break;

                        case RowActionBase.ACTION_INSERT :
                            Row row = (Row) store.get(action.getPos(), false);

                            store.commitPersistence(row);
                            break;

                        default :
                    }
                }
            }

            // session.actionTimestamp is the committed tx timestamp
            if (getFirstLiveTransactionTimestamp() > session.actionTimestamp) {
                mergeTransaction(session, list, 0, limit,
                                 session.actionTimestamp);
                rowActionMapRemoveTransaction(list, 0, limit, true);
            } else {
                list = session.rowActionList.toArray();

                addToCommittedQueue(session, list);
            }

            try {
                session.logSequences();
                database.logger.writeCommitStatement(session);
            } catch (HsqlException e) {}

            //
            if (mvcc) {
                if (!session.waitingSessions.isEmpty()) {
                    for (int i = 0; i < session.waitingSessions.size(); i++) {
                        Session current =
                            (Session) session.waitingSessions.get(i);

                        current.latch.countDown();
                    }

                    session.waitingSessions.clear();
                }
            } else {
                endTransactionTPL(session);
            }

            return true;
        } finally {
            writeLock.unlock();
            session.tempSet.clear();
        }
    }

    public void rollback(Session session) {

        session.abortTransaction = false;
        session.actionTimestamp  = nextChangeTimestamp();

        rollbackPartial(session, 0, session.transactionTimestamp);
        endTransaction(session);

        if (!mvcc) {
            try {
                writeLock.lock();
                endTransactionTPL(session);
            } finally {
                writeLock.unlock();
            }
        }
    }

    public void rollbackSavepoint(Session session, int index) {

        long timestamp = session.sessionContext.savepointTimestamps.get(index);
        Integer oi = (Integer) session.sessionContext.savepoints.get(index);
        int     start  = oi.intValue();

        while (session.sessionContext.savepoints.size() > index + 1) {
            session.sessionContext.savepoints.remove(
                session.sessionContext.savepoints.size() - 1);
            session.sessionContext.savepointTimestamps.removeLast();
        }

        rollbackPartial(session, start, timestamp);
    }

    public void rollbackAction(Session session) {
        rollbackPartial(session, session.actionIndex, session.actionTimestamp);
    }

    /**
     * rollback the row actions from start index in list and
     * the given timestamp
     */
    void rollbackPartial(Session session, int start, long timestamp) {

        Object[] list  = session.rowActionList.getArray();
        int      limit = session.rowActionList.size();

        if (start == limit) {
            return;
        }

        for (int i = start; i < limit; i++) {
            RowAction action = (RowAction) list[i];

            if (action != null) {
                action.rollback(session, timestamp);
            } else {
                System.out.println("null action in rollback " + start);
            }
        }

        // rolled back transactions can always be merged as they have never been
        // seen by other sessions
        mergeRolledBackTransaction(session.rowActionList.getArray(), start,
                                   limit);
        rowActionMapRemoveTransaction(session.rowActionList.getArray(), start,
                                      limit, false);
        session.rowActionList.setSize(start);
    }

    public RowAction addDeleteAction(Session session, Table table, Row row) {

        RowAction action;

        synchronized (row) {
            action = RowAction.addAction(session, RowActionBase.ACTION_DELETE,
                                         table, row);
        }

        session.rowActionList.add(action);

        if (!row.isMemory()) {
            rowActionMap.put(action.getPos(), action);
        }

        return action;
    }

    public void addInsertAction(Session session, Table table, Row row) {

        RowAction action = row.rowAction;

        if (action == null) {
            System.out.println("null insert action " + session + " "
                               + session.actionTimestamp);
        }

        session.rowActionList.add(action);

        if (!row.isMemory()) {
            rowActionMap.put(action.getPos(), action);
        }
    }

// functional unit - row comparison

    /**
     * Used for inserting rows into unique indexes
     */

    /** @todo test skips some equal rows because it relies on getPos() */
    int compareRowForInsert(Row newRow, Row existingRow) {

        // only allow new inserts over rows deleted by the same session
        if (newRow.rowAction != null
                && !canRead(newRow.rowAction.session, existingRow)) {
            return newRow.getPos() - existingRow.getPos();
        }

        return 0;
    }

    // functional unit - accessibility of rows
    public boolean canRead(Session session, Row row) {

        synchronized (row) {
            RowAction action = row.rowAction;

            if (action == null) {
                return true;
            }

            return action.canRead(session);
        }
    }

    public boolean isDeleted(Session session, Row row) {

        synchronized (row) {
            RowAction action = row.rowAction;

            if (action == null) {
                return false;
            }

            return !action.canRead(session);
        }
    }

    public boolean canRead(Session session, int id) {

        RowAction action = (RowAction) rowActionMap.get(id);

        return action == null ? true
                              : action.canRead(session);
    }

    void rowActionMapRemoveTransaction(Object[] list, int start, int limit,
                                       boolean commit) {

        for (int i = start; i < limit; i++) {
            RowAction rowact = (RowAction) list[i];

            if (!rowact.isMemory) {
                synchronized (rowact) {
                    if (rowact.type == RowActionBase.ACTION_NONE
                            || rowact.type
                               == RowActionBase.ACTION_DELETE_FINAL) {
                        int pos = rowact.getPos();

                        rowActionMap.remove(pos);
                    }
                }
            }
        }

        deleteRows(list, start, limit, commit);
    }

    void deleteRows(Object[] list, int start, int limit, boolean commit) {

        for (int i = start; i < limit; i++) {
            RowAction rowact = (RowAction) list[i];

            if (rowact.type == RowActionBase.ACTION_DELETE_FINAL) {
                try {
                    rowact.type = RowActionBase.ACTION_DELETE_COMMITTED;

                    PersistentStore store =
                        rowact.session.sessionData.getRowStore(rowact.table);
                    Row row = rowact.memoryRow;

                    if (row == null) {
                        row = (Row) store.get(rowact.getPos(), false);
                    }

                    if (commit && rowact.table.hasLobColumn) {
                        Object[] data = row.getData();

                        rowact.table.removeLobUsageCount(rowact.session, data);
                    }

                    store.delete(row);
                } catch (HsqlException e) {

//                    throw unexpectedException(e.getMessage());
                }
            }
        }
    }

    /**
     * add transaction info to a row just loaded from the cache. called only
     * for CACHED tables
     */
    public void setTransactionInfo(CachedObject object) {

        Row row = (Row) object;

        if (row.rowAction != null) {
            return;
        }

        RowAction rowact = (RowAction) rowActionMap.get(row.position);

        row.rowAction = rowact;
    }

    /**
     * merge a given list of transaction rollback action with given timestamp
     */
    void mergeRolledBackTransaction(Object[] list, int start, int limit) {

        for (int i = start; i < limit; i++) {
            RowAction rowact = (RowAction) list[i];

            if (rowact == null || rowact.type == RowActionBase.ACTION_NONE
                    || rowact.type == RowActionBase.ACTION_DELETE_FINAL) {
                continue;
            }

            Row row = rowact.memoryRow;

            if (row == null) {
                PersistentStore store =
                    rowact.session.sessionData.getRowStore(rowact.table);

                row = (Row) store.get(rowact.getPos(), false);
            }

            if (row == null) {
                continue;
            }

            synchronized (row) {
                rowact.mergeRollback(row);
            }
        }

//        } catch (Throwable t) {
//            System.out.println("throw in merge");
//            t.printStackTrace();
//        }
    }

    /**
     * add a list of actions to the end of queue
     */
    void addToCommittedQueue(Session session, Object[] list) {

        synchronized (committedTransactionTimestamps) {

            // add the txList according to commit timestamp
            committedTransactions.addLast(list);

            // get session commit timestamp
            committedTransactionTimestamps.addLast(session.actionTimestamp);
/* debug 190
            if (committedTransactions.size() > 64) {
                System.out.println("******* excessive transaction queue");
            }
// debug 190 */
        }
    }

    /**
     * expire all committed transactions that are no longer in scope
     */
    void mergeExpiredTransactions(Session session) {

        long timestamp = getFirstLiveTransactionTimestamp();

        while (true) {
            long     commitTimestamp = 0;
            Object[] actions         = null;

            synchronized (committedTransactionTimestamps) {
                if (committedTransactionTimestamps.isEmpty()) {
                    break;
                }

                commitTimestamp = committedTransactionTimestamps.getFirst();

                if (commitTimestamp < timestamp) {
                    committedTransactionTimestamps.removeFirst();

                    actions = (Object[]) committedTransactions.removeFirst();
                } else {
                    break;
                }
            }

            mergeTransaction(session, actions, 0, actions.length,
                             commitTimestamp);
            rowActionMapRemoveTransaction(actions, 0, actions.length, true);
        }
    }

    /**
     * merge a transaction committed at a given timestamp.
     */
    void mergeTransaction(Session session, Object[] list, int start,
                          int limit, long timestamp) {

        for (int i = start; i < limit; i++) {
            RowAction rowact = (RowAction) list[i];

            if (rowact == null || rowact.type == RowActionBase.ACTION_NONE
                    || rowact.type == RowActionBase.ACTION_DELETE_FINAL) {
                continue;
            }

            Row row = rowact.memoryRow;

            if (row == null) {
                PersistentStore store =
                    rowact.session.sessionData.getRowStore(rowact.table);

                row = (Row) store.get(rowact.getPos(), false);
            }

            if (row == null) {
                continue;
            }

            synchronized (row) {
                rowact.mergeToTimestamp(row, timestamp);
            }
        }
    }

    /**
     * gets the next timestamp for an action
     */
    long nextChangeTimestamp() {
        return globalChangeTimestamp.incrementAndGet();
    }

    public void beginTransaction(Session session) {

        synchronized (liveTransactionTimestamps) {
            session.actionTimestamp      = nextChangeTimestamp();
            session.transactionTimestamp = session.actionTimestamp;
            session.isTransaction        = true;

            liveTransactionTimestamps.addLast(session.actionTimestamp);

            try {
                if (mvcc) {
                    database.logger.writeToLog(
                        session, session.getStartTransactionSQL());
                }
            } catch (HsqlException e) {}
        }
    }

    /**
     * add session to the end of queue when a transaction starts
     * (depending on isolation mode)
     */
    public void beginAction(Session session, Statement cs) {

        synchronized (liveTransactionTimestamps) {
            session.actionTimestamp = nextChangeTimestamp();

            if (!session.isTransaction) {
                session.transactionTimestamp = session.actionTimestamp;
                session.isTransaction        = true;

                liveTransactionTimestamps.addLast(session.actionTimestamp);

                try {
                    if (this.mvcc) {
                        database.logger.writeToLog(
                            session, session.getStartTransactionSQL());
                    }
                } catch (HsqlException e) {}
            }
        }

        if (session.isReadOnly()) {
            return;
        }

        if (!mvcc) {
            if (session.hasLocks()) {
                return;
            }

            try {
                writeLock.lock();

                boolean canProceed = beginActionTPL(session, cs);

                if (!canProceed) {
                    session.abortTransaction = true;
                }
/* debug 190
                if (!canProceed) {
                    System.out.println("******* cannot start");
                }
// debug 190 */
            } finally {
                writeLock.unlock();
            }
        }
    }

    void endTransactionTPL(Session session) {

        int unlockedCount = 0;

        if (session.isReadOnly()) {
            return;
        }

        unlockTablesTPL(session);

        if (session.waitingSessions.isEmpty()) {
            return;
        }

        for (int i = 0; i < session.waitingSessions.size(); i++) {
            Session current = (Session) session.waitingSessions.get(i);

            current.tempUnlocked = false;

            long count = current.latch.getCount();

            if (count == 1) {
                boolean canProceed = setWaitedSessionsTPL(current,
                    current.currentStatement);

                if (!canProceed) {
                    current.abortTransaction = true;
                }

                if (current.tempSet.isEmpty()) {
                    lockTablesTPL(current, current.currentStatement);

                    current.tempUnlocked = true;

                    unlockedCount++;
                }
            }
        }

        if (unlockedCount > 0) {
/* debug 190

            if (unlockedCount > 1) {
                System.out.println("*********** multiple unlocked");
            }
// debug 190 */
            for (int i = 0; i < session.waitingSessions.size(); i++) {
                Session current = (Session) session.waitingSessions.get(i);

                if (!current.tempUnlocked) {
                    boolean canProceed = setWaitedSessionsTPL(current,
                        current.currentStatement);

                    // this can introduce additional waits for the sessions
                    if (!canProceed) {
                        current.abortTransaction = true;
                    }
/* debug 190
                    if (current.tempSet.isEmpty()) {
                        System.out.println("*********** additional unlocked");

                        // shouldn't get here
                    }
// debug 190 */
                }
            }
        }

        for (int i = 0; i < session.waitingSessions.size(); i++) {
            Session current = (Session) session.waitingSessions.get(i);

            setWaitingSessionTPL(current);
        }

        session.tempSet.clear();
        session.waitingSessions.clear();
    }

    boolean beginActionTPL(Session session, Statement cs) {

        if (session.isReadOnly()) {
            return true;
        }

        boolean canProceed = setWaitedSessionsTPL(session, cs);

        if (canProceed) {
            if (session.tempSet.isEmpty()) {
                lockTablesTPL(session, cs);

                // we dont set other sessions that would now be waiting for this one too
            } else {
                setWaitingSessionTPL(session);
            }

            return true;
        }

        return false;
    }

    boolean setWaitedSessionsTPL(Session session, Statement cs) {

        session.tempSet.clear();

        if (cs == null || session.abortTransaction) {
            return true;
        }

        HsqlName[] nameList = cs.getTableNamesForWrite();

        for (int i = 0; i < nameList.length; i++) {
            HsqlName name = nameList[i];

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            Session holder = (Session) tableWriteLocks.get(name);

            if (holder != null && holder != session) {
                session.tempSet.add(holder);
            }

            Iterator it = tableReadLocks.get(name);

            while (it.hasNext()) {
                holder = (Session) it.next();

                if (holder != session) {
                    session.tempSet.add(holder);
                }
            }
        }

        nameList = cs.getTableNamesForRead();

        for (int i = 0; i < nameList.length; i++) {
            HsqlName name = nameList[i];

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            Session holder = (Session) tableWriteLocks.get(name);

            if (holder != null && holder != session) {
                session.tempSet.add(holder);
            }
        }

        for (int i = 0; i < session.waitingSessions.size(); i++) {
            Session current = (Session) session.waitingSessions.get(i);

            if (session.tempSet.contains(current)) {
                session.tempSet.clear();

                return false;
            }
        }

        return true;
    }

    void setWaitingSessionTPL(Session session) {

        int count = session.tempSet.size();

        for (int i = 0; i < count; i++) {
            Session current = (Session) session.tempSet.get(i);

            current.waitingSessions.add(session);
        }

        session.tempSet.clear();
        session.latch.setCount(count);
    }

    void lockTablesTPL(Session session, Statement cs) {

        if (cs == null || session.abortTransaction) {
            return;
        }

        HsqlName[] nameList = cs.getTableNamesForWrite();

        for (int i = 0; i < nameList.length; i++) {
            HsqlName name = nameList[i];

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            tableWriteLocks.put(name, session);
        }

        nameList = cs.getTableNamesForRead();

        for (int i = 0; i < nameList.length; i++) {
            HsqlName name = nameList[i];

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            tableReadLocks.put(name, session);
        }
    }

    void unlockTablesTPL(Session session) {

        Iterator it = tableWriteLocks.values().iterator();

        while (it.hasNext()) {
            Session s = (Session) it.next();

            if (s == session) {
                it.setValue(null);
            }
        }

        it = tableReadLocks.values().iterator();

        while (it.hasNext()) {
            Session s = (Session) it.next();

            if (s == session) {
                it.remove();
            }
        }
    }

    /**
     * remove session from queue when a transaction ends
     * and expire any committed transactions
     * that are no longer required. remove transactions ended before the first
     * timestamp in liveTransactionsSession queue
     */
    void endTransaction(Session session) {

        try {
            writeLock.lock();

            long timestamp = session.transactionTimestamp;

            synchronized (liveTransactionTimestamps) {
                session.isTransaction = false;

                int index = liveTransactionTimestamps.indexOf(timestamp);

                liveTransactionTimestamps.remove(index);
            }

            mergeExpiredTransactions(session);
        } finally {
            writeLock.unlock();
        }
    }

    long getFirstLiveTransactionTimestamp() {

        synchronized (liveTransactionTimestamps) {
            if (liveTransactionTimestamps.isEmpty()) {
                return Long.MAX_VALUE;
            }

            return liveTransactionTimestamps.get(0);
        }
    }

// functional unit - list actions and translate id's

    /**
     * Return an array of all row actions sorted by System Change No.
     */
    RowAction[] getRowActionList() {

        try {
            writeLock.lock();

            Session[]   sessions = database.sessionManager.getAllSessions();
            int[]       tIndex   = new int[sessions.length];
            RowAction[] rowActions;
            int         rowActionCount = 0;

            {
                int actioncount = 0;

                for (int i = 0; i < sessions.length; i++) {
                    actioncount += sessions[i].getTransactionSize();
                }

                rowActions = new RowAction[actioncount];
            }

            while (true) {
                boolean found        = false;
                long    minChangeNo  = Long.MAX_VALUE;
                int     sessionIndex = 0;

                // find the lowest available SCN across all sessions
                for (int i = 0; i < sessions.length; i++) {
                    int tSize = sessions[i].getTransactionSize();

                    if (tIndex[i] < tSize) {
                        RowAction current =
                            (RowAction) sessions[i].rowActionList.get(
                                tIndex[i]);

                        if (current.actionTimestamp < minChangeNo) {
                            minChangeNo  = current.actionTimestamp;
                            sessionIndex = i;
                        }

                        found = true;
                    }
                }

                if (!found) {
                    break;
                }

                HsqlArrayList currentList =
                    sessions[sessionIndex].rowActionList;

                for (; tIndex[sessionIndex] < currentList.size(); ) {
                    RowAction current =
                        (RowAction) currentList.get(tIndex[sessionIndex]);

                    // if the next change no is in this session, continue adding
                    if (current.actionTimestamp == minChangeNo + 1) {
                        minChangeNo++;
                    }

                    if (current.actionTimestamp == minChangeNo) {
                        rowActions[rowActionCount++] = current;

                        tIndex[sessionIndex]++;
                    } else {
                        break;
                    }
                }
            }

            return rowActions;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Return a lookup of all row ids for cached tables in transactions.
     * For auto-defrag, as currently there will be no RowAction entries
     * at the time of defrag.
     */
    public DoubleIntIndex getTransactionIDList() {

        writeLock.lock();

        try {
            DoubleIntIndex lookup = new DoubleIntIndex(10, false);

            lookup.setKeysSearchTarget();

            Iterator it = this.rowActionMap.keySet().iterator();

            for (; it.hasNext(); ) {
                lookup.addUnique(it.nextInt(), 0);
            }

            return lookup;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Convert row ID's for cached table rows in transactions
     */
    public void convertTransactionIDs(DoubleIntIndex lookup) {

        writeLock.lock();

        try {
            RowAction[] list = new RowAction[rowActionMap.size()];
            Iterator    it   = this.rowActionMap.values().iterator();

            for (int i = 0; it.hasNext(); i++) {
                list[i] = (RowAction) it.next();
            }

            rowActionMap.clear();

            for (int i = 0; i < list.length; i++) {
                int pos = lookup.lookupFirstEqual(list[i].getPos());

                list[i].setPos(pos);
                rowActionMap.put(pos, list[i]);
            }
        } finally {
            writeLock.unlock();
        }
    }

//
    void logTransaction(Object[] list, int size) {}

    void unexpectedException(String message) {}

    public void finalize() {}
}
