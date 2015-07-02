/* Copyright (c) 2001-2011, The HSQL Development Group
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

import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.persist.PersistentStore;

/**
 * Represents the chain of insert / delete / rollback / commit actions on a row.
 *
 * @author Fred Toussi (fredt@users dot sourceforge dot net)
 * @version 2.3.2
 * @since 2.0.0
 */
public class RowAction extends RowActionBase {

    //
    final TableBase       table;
    final PersistentStore store;
    Row                   memoryRow;
    long                  rowId;
    boolean               isMemory;
    RowAction             updatedAction;

    public static RowAction addInsertAction(Session session, TableBase table,
            Row row) {

        RowAction action = new RowAction(session, table, ACTION_INSERT, row,
                                         null);

        row.rowAction = action;

        return action;
    }

    public static RowAction addDeleteAction(Session session, TableBase table,
            Row row, int[] colMap) {

        RowAction action = row.rowAction;

        if (action == null) {
            action = new RowAction(session, table, ACTION_DELETE, row, colMap);
            row.rowAction = action;

            return action;
        }

        return action.addDeleteAction(session, colMap);
    }

    public static boolean addRefAction(Session session, Row row,
                                       int[] colMap) {

        RowAction action = row.rowAction;

        if (action == null) {
            action = new RowAction(session, row.getTable(), ACTION_REF, row,
                                   colMap);
            row.rowAction = action;

            return true;
        }

        return action.addRefAction(session, colMap);
    }

    public RowAction(Session session, TableBase table, byte type, Row row,
                     int[] colMap) {

        this.session         = session;
        this.type            = type;
        this.actionTimestamp = session.actionTimestamp;
        this.table           = table;
        this.store           = table.getRowStore(session);
        this.isMemory        = row.isMemory();
        this.memoryRow       = row;
        this.rowId           = row.getPos();
        this.changeColumnMap = colMap;
    }

    private RowAction(RowAction other) {

        this.session         = other.session;
        this.type            = other.type;
        this.actionTimestamp = other.actionTimestamp;
        this.table           = other.table;
        this.store           = other.store;
        this.isMemory        = other.isMemory;
        this.memoryRow       = other.memoryRow;
        this.rowId           = other.rowId;
        this.changeColumnMap = other.changeColumnMap;
    }

    synchronized public int getType() {
        return type;
    }

    synchronized RowAction addDeleteAction(Session session, int[] colMap) {

        if (type == ACTION_NONE) {
            setAsAction(session, ACTION_DELETE);

            changeColumnMap = colMap;
        } else {
            RowActionBase action = this;

            while (true) {
                if (action.rolledback) {
                    if (action.next == null) {
                        break;
                    }

                    action = action.next;

                    continue;
                }

                switch (action.type) {

                    case ACTION_INSERT : {
                        if (action.commitTimestamp == 0
                                && session != action.session) {
                            throw Error.runtimeError(ErrorCode.U_S0500,
                                                     "RowAction");
                        }

                        break;
                    }
                    case ACTION_DELETE_FINAL :
                    case ACTION_DELETE : {
                        if (session != action.session) {
                            if (action.commitTimestamp == 0) {
                                if (!session.tempSet.isEmpty()) {
                                    session.tempSet.clear();
                                }

                                session.tempSet.add(action);
                            }

                            return null;
                        }

                        break;
                    }
                    case ACTION_REF : {
                        if (session != action.session
                                && action.commitTimestamp == 0) {
                            if (colMap == null
                                    || ArrayUtil.haveCommonElement(
                                        colMap, action.changeColumnMap)) {
                                if (!session.tempSet.isEmpty()) {
                                    session.tempSet.clear();
                                }

                                session.tempSet.add(action);

                                return null;
                            }
                        }

                        break;
                    }
                }

                if (action.next == null) {
                    break;
                }

                action = action.next;
            }

            RowActionBase newAction = new RowActionBase(session,
                ACTION_DELETE);

            newAction.changeColumnMap = colMap;
            action.next               = newAction;
        }

        return this;
    }

    synchronized boolean addRefAction(Session session, int[] colMap) {

        if (type == ACTION_NONE) {
            setAsAction(session, ACTION_REF);

            changeColumnMap = colMap;

            return true;
        }

        RowActionBase action = this;

        do {
            if (session == action.session) {
                if (action.type == ACTION_REF
                        && action.changeColumnMap == colMap
                        && action.commitTimestamp == 0) {
                    return false;
                }

                if (action.type == ACTION_INSERT) {
                    if (action.commitTimestamp == 0) {
                        return false;
                    }
                }
            } else {
                if (action.type == ACTION_DELETE
                        && action.commitTimestamp == 0) {
                    if (action.changeColumnMap == null
                            || ArrayUtil.haveCommonElement(
                                colMap, action.changeColumnMap)) {
                        if (!session.tempSet.isEmpty()) {
                            session.tempSet.clear();
                        }

                        session.tempSet.add(action);

                        return false;
                    }
                }
            }

            if (action.next == null) {
                break;
            }

            action = action.next;
        } while (true);

        RowActionBase newAction = new RowActionBase(session, ACTION_REF);

        newAction.changeColumnMap = colMap;
        action.next               = newAction;

        return true;
    }

    public boolean checkDeleteActions() {
        return false;
    }

    public synchronized RowAction duplicate(Row newRow) {

        RowAction action = new RowAction(session, table, type, newRow,
                                         changeColumnMap);

        return action;
    }

    synchronized void setAsAction(Session session, byte type) {

        this.session    = session;
        this.type       = type;
        actionTimestamp = session.actionTimestamp;
        changeColumnMap = null;
    }

    synchronized void setAsAction(RowActionBase action) {
        super.setAsAction(action);
    }

    public void setAsNoOp() {

//        memoryRow       = null;
        session         = null;
        actionTimestamp = 0;
        commitTimestamp = 0;
        rolledback      = false;
        deleteComplete  = false;
        changeColumnMap = null;
        prepared        = false;
        type            = ACTION_NONE;
        next            = null;
    }

    private void setAsDeleteFinal(long timestamp) {

        actionTimestamp = 0;
        commitTimestamp = timestamp;
        rolledback      = false;
        deleteComplete  = false;
        prepared        = false;
        changeColumnMap = null;
        type            = ACTION_DELETE_FINAL;
        next            = null;
    }

    /** for two-phased pre-commit */
    synchronized void prepareCommit(Session session) {

        RowActionBase action = this;

        do {
            if (action.session == session && action.commitTimestamp == 0) {
                action.prepared = true;
            }

            action = action.next;
        } while (action != null);
    }

    synchronized int commit(Session session) {

        RowActionBase action     = this;
        int           actiontype = ACTION_NONE;

        do {
            if (action.session == session && action.commitTimestamp == 0) {
                action.commitTimestamp = session.actionTimestamp;
                action.prepared        = false;

                if (action.type == ACTION_INSERT) {
                    actiontype = action.type;
                } else if (action.type == ACTION_DELETE) {
                    if (actiontype == ACTION_INSERT) {

                        // ACTION_INSERT + ACTION_DELETE
                        actiontype = ACTION_INSERT_DELETE;
                    } else {
                        actiontype = action.type;
                    }
                }
            }

            action = action.next;
        } while (action != null);

        return actiontype;
    }

    public boolean isDeleted() {

        RowActionBase action = this;

        do {
            if (action.commitTimestamp != 0) {
                if (action.type == ACTION_DELETE
                        || action.type == ACTION_DELETE_FINAL) {
                    return true;
                }
            }

            action = action.next;
        } while (action != null);

        return false;
    }

    /**
     * returns type of commit performed on timestamp. ACTION_NONE if none.
     * assumes rolled-back actions have already been merged
     */
    synchronized int getCommitTypeOn(long timestamp) {

        RowActionBase action     = this;
        int           actionType = ACTION_NONE;

        do {
            if (action.commitTimestamp == timestamp) {
                if (action.type == ACTION_INSERT) {
                    actionType = action.type;
                } else if (action.type == ACTION_DELETE) {
                    if (actionType == ACTION_INSERT) {

                        // ACTION_INSERT + ACTION_DELETE
                        actionType = ACTION_INSERT_DELETE;
                    } else {
                        actionType = action.type;
                    }
                }
            }

            action = action.next;
        } while (action != null);

        return actionType;
    }

    /**
     * returns false if another committed session has altered the same row
     */
    synchronized boolean canCommit(Session session, OrderedHashSet set) {

        RowActionBase action;
        long          timestamp       = session.transactionTimestamp;
        long          commitTimestamp = 0;
        final boolean readCommitted = session.isolationLevel
                                      == SessionInterface.TX_READ_COMMITTED;
        boolean hasDelete = false;

        action = this;

        if (readCommitted) {
            do {
                if (action.session == session
                        && action.type == ACTION_DELETE) {

                    // for READ_COMMITTED, use action timestamp for later conflicts
                    if (action.commitTimestamp == 0) {
                        timestamp = action.actionTimestamp;
                    }
                }

                action = action.next;
            } while (action != null);

            action = this;
        }

        do {
            if (action.session == session) {
                if (action.type == ACTION_DELETE) {
                    hasDelete = true;
                }
            } else {
                if (action.rolledback || action.type != ACTION_DELETE) {
                    action = action.next;

                    continue;
                }

                if (action.prepared) {
                    return false;
                }

                if (action.commitTimestamp == 0) {
                    set.add(action);
                } else if (action.commitTimestamp > commitTimestamp) {
                    commitTimestamp = action.commitTimestamp;
                }
            }

            action = action.next;
        } while (action != null);

        if (!hasDelete) {
            return true;
        }

        return commitTimestamp < timestamp;
    }

    synchronized void complete(Session session) {

        RowActionBase action;

        action = this;

        do {
            if (action.session == session) {
                if (action.actionTimestamp == 0) {
                    action.actionTimestamp = session.actionTimestamp;
                }
            }

            action = action.next;
        } while (action != null);
    }

    /**
     * returns false if cannot complete
     * when READ COMMITTED, false result always means repeat action and adds
     * to set parameter the sessions to wait on (may be no wait)
     */
    synchronized boolean complete(Session session, OrderedHashSet set) {

        RowActionBase action;
        boolean readCommitted = session.isolationLevel
                                == SessionInterface.TX_READ_COMMITTED;
        boolean result = true;

        action = this;

        do {
            if (action.rolledback || action.type == ACTION_NONE) {
                action = action.next;

                continue;
            }

            if (action.session == session) {

                //
            } else {
                if (action.prepared) {
                    set.add(action.session);

                    return false;
                }

                if (readCommitted) {
                    if (action.commitTimestamp > session.actionTimestamp) {

                        // 2.0 -- investigate
                        // can redo - if deletes
                        // can redo - if dup, but will likely fail at retry
                        // can redo - if ref, but will likely fail at retry
                        set.add(session);

                        result = false;
                    } else if (action.commitTimestamp == 0) {
                        set.add(action.session);

                        result = false;
                    }
                } else if (action.commitTimestamp
                           > session.transactionTimestamp) {
                    return false;
                }
            }

            action = action.next;
        } while (action != null);

        return result;
    }

    public synchronized long getPos() {
        return rowId;
    }

    synchronized void setPos(long pos) {
        rowId = pos;
    }

    private int getRollbackType(Session session) {

        int           actionType = ACTION_NONE;
        RowActionBase action     = this;

        do {
            if (action.session == session && action.rolledback) {
                if (action.type == ACTION_DELETE) {
                    if (actionType == ACTION_INSERT) {
                        actionType = ACTION_INSERT_DELETE;
                    } else {
                        actionType = action.type;
                    }
                } else if (action.type == ACTION_INSERT) {
                    actionType = action.type;
                }
            }

            action = action.next;
        } while (action != null);

        return actionType;
    }

    /**
     * Rollback actions for a session including and after the given timestamp
     */
    synchronized void rollback(Session session, long timestamp) {

        RowActionBase action = this;

        do {
            if (action.session == session && action.commitTimestamp == 0) {
                if (action.actionTimestamp >= timestamp) {
                    action.commitTimestamp = session.actionTimestamp;
                    action.rolledback      = true;
                    action.prepared        = false;
                }
            }

            action = action.next;
        } while (action != null);
    }

    /**
     * merge rolled back actions
     */
    synchronized int mergeRollback(Session session, long timestamp, Row row) {

        RowActionBase action         = this;
        RowActionBase head           = null;
        RowActionBase tail           = null;
        int           rollbackAction = getRollbackType(session);

        do {
            if (action.session == session && action.rolledback) {
                if (tail != null) {
                    tail.next = null;
                }
            } else {
                if (head == null) {
                    head = tail = action;
                } else {
                    tail.next = action;
                    tail      = action;
                }
            }

            action = action.next;
        } while (action != null);

        if (head == null) {
            switch (rollbackAction) {

                case ACTION_INSERT :
                case ACTION_INSERT_DELETE :
                    setAsDeleteFinal(timestamp);
                    break;

                case ACTION_DELETE :
                case ACTION_NONE :
                default :
                    setAsNoOp();
                    break;
            }
        } else {
            if (head != this) {
                setAsAction(head);
            }
        }

        return rollbackAction;
    }

    /**
     * merge session actions committed on given timestamp.
     *
     * may be called more than once on same action
     *
     */
    synchronized void mergeToTimestamp(long timestamp) {

        RowActionBase action     = this;
        RowActionBase head       = null;
        RowActionBase tail       = null;
        int           commitType = getCommitTypeOn(timestamp);

        if (type == ACTION_DELETE_FINAL || type == ACTION_NONE) {
            return;
        }

        if (commitType == ACTION_DELETE
                || commitType == ACTION_INSERT_DELETE) {
            setAsDeleteFinal(timestamp);

            return;
        }

        do {
            boolean expired = false;;

            if (action.commitTimestamp != 0) {
                if (action.commitTimestamp <= timestamp) {
                    expired = true;
                } else if (action.type == ACTION_REF) {
                    expired = true;
                }
            }

            if (expired) {
                if (tail != null) {
                    tail.next = null;
                }
            } else {
                if (head == null) {
                    head = tail = action;
                } else {
                    tail.next = action;
                    tail      = action;
                }
            }

            action = action.next;
        } while (action != null);

        if (head == null) {
            switch (commitType) {

                case ACTION_DELETE :
                case ACTION_INSERT_DELETE :
                    setAsDeleteFinal(timestamp);
                    break;

                case ACTION_NONE :
                case ACTION_INSERT :
                default :
                    setAsNoOp();
                    break;
            }
        } else if (head != this) {
            setAsAction(head);
        }

        mergeExpiredRefActions();
    }

    public synchronized boolean canRead(Session session, int mode) {

        long threshold;
        int  actionType = ACTION_NONE;

        if (type == ACTION_DELETE_FINAL) {
            return false;
        }

        if (type == ACTION_NONE) {
            return true;
        }

        RowActionBase action = this;

        if (session == null) {
            threshold = Long.MAX_VALUE;
        } else {
            switch (session.isolationLevel) {

                case SessionInterface.TX_READ_UNCOMMITTED :
                    threshold = Long.MAX_VALUE;
                    break;

                case SessionInterface.TX_READ_COMMITTED :
                    threshold = session.actionTimestamp;
                    break;

                case SessionInterface.TX_REPEATABLE_READ :
                case SessionInterface.TX_SERIALIZABLE :
                default :
                    threshold = session.transactionTimestamp;
                    break;
            }
        }

        do {
            if (action.type == ACTION_REF) {
                action = action.next;

                continue;
            }

            if (action.rolledback) {
                if (action.type == ACTION_INSERT) {
                    actionType = ACTION_DELETE;
                }

                action = action.next;

                continue;
            }

            if (session == action.session) {
                if (action.type == ACTION_DELETE) {
                    actionType = action.type;
                } else if (action.type == ACTION_INSERT) {
                    actionType = action.type;
                }

                action = action.next;

                continue;
            } else if (action.commitTimestamp == 0) {
                if (action.type == ACTION_NONE) {
                    throw Error.runtimeError(ErrorCode.U_S0500, "RowAction");
                } else if (action.type == ACTION_INSERT) {
                    if (mode == TransactionManager.ACTION_READ) {
                        actionType = action.ACTION_DELETE;
                    } else if (mode == TransactionManager.ACTION_DUP) {
                        actionType = ACTION_INSERT;

                        session.tempSet.clear();
                        session.tempSet.add(action);
                    } else if (mode == TransactionManager.ACTION_REF) {
                        actionType = ACTION_DELETE;
                    }

                    break;
                } else if (action.type == ACTION_DELETE) {
                    if (mode == TransactionManager.ACTION_DUP) {

                        //
                    } else if (mode == TransactionManager.ACTION_REF) {
                        actionType = ACTION_DELETE;
                    }
                }

                action = action.next;

                continue;
            } else if (action.commitTimestamp < threshold) {
                if (action.type == ACTION_DELETE) {
                    actionType = ACTION_DELETE;
                } else if (action.type == ACTION_INSERT) {
                    actionType = ACTION_INSERT;
                }
            } else {
                if (action.type == ACTION_INSERT) {
                    if (mode == TransactionManager.ACTION_READ) {
                        actionType = action.ACTION_DELETE;
                    } else if (mode == TransactionManager.ACTION_DUP) {
                        actionType = ACTION_INSERT;

                        session.tempSet.clear();
                        session.tempSet.add(action);
                    } else if (mode == TransactionManager.ACTION_REF) {
                        actionType = ACTION_DELETE;
                    }
                }
            }

            action = action.next;

            continue;
        } while (action != null);

        if (actionType == ACTION_NONE || actionType == ACTION_INSERT) {
            return true;
        }

        return false;
    }

    public boolean hasCurrentRefAction() {

        RowActionBase action = this;

        do {
            if (action.type == ACTION_REF && action.commitTimestamp == 0) {
                return true;
            }

            action = action.next;
        } while (action != null);

        return false;
    }

    /** eliminate all expired updatedAction in chain */
    private RowAction mergeExpiredRefActions() {

        if (updatedAction != null) {
            updatedAction = updatedAction.mergeExpiredRefActions();
        }

        if (hasCurrentRefAction()) {
            return this;
        }

        return updatedAction;
    }

    public synchronized String describe(Session session) {

        StringBuilder sb     = new StringBuilder();
        RowActionBase action = this;

        do {
            if (action == this) {
                sb.append(this.rowId).append(' ');
            }

            sb.append(action.session.getId()).append(' ');
            sb.append(action.type).append(' ').append(action.actionTimestamp);
            sb.append(' ').append(action.commitTimestamp);

            if (action.commitTimestamp != 0) {
                if (action.rolledback) {
                    sb.append('r');
                } else {
                    sb.append('c');
                }
            }

            sb.append(" - ");

            action = action.next;
        } while (action != null);

        return sb.toString();
    }
}
