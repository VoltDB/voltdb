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

import org.hsqldb_voltpatches.lib.OrderedHashSet;

/**
 * Represents the chain of insert / delete / rollback / commit actions on a row.
 *
 * @author Fred Toussi (fredt@users dot sourceforge dot net)
 * @version 2.0.0
 * @since 2.0.0
 */
public class RowAction extends RowActionBase {

    //
    final Table table;
    Row         memoryRow;
    int         rowId;
    boolean     isMemory;

    public static RowAction addAction(Session session, byte type, Table table,
                                      Row row) {

        RowAction action = row.rowAction;

        if (action == null) {
            action = new RowAction(session, table, type);

            if (row.isMemory()) {
                action.isMemory = true;
            }

            action.memoryRow = row;
            action.rowId     = row.getPos();
            row.rowAction    = action;
        } else {
            if (action.type == ACTION_DELETE_FINAL) {
                throw Error.runtimeError(ErrorCode.U_S0500, "RowAction");
            }

            if (action.type == ACTION_NONE) {
                action.setAsAction(session, type);
            } else {
                RowActionBase actionItem = action;

                while (actionItem.next != null) {
                    actionItem = actionItem.next;
                }

                RowActionBase newAction = new RowActionBase(session, type);

                actionItem.next = newAction;
            }
        }

        return action;
    }

    /**
     * Constructor. <p>
     *
     * @param session
     * @param type type of action
     */
    RowAction(Session session, Table table, byte type) {

        super(session, type);

        this.table = table;
    }

    public synchronized RowAction duplicate(int newRowId) {

        RowAction action = duplicate();

        action.rowId = newRowId;

        return action;
    }

    synchronized RowAction duplicate() {

        RowAction action = new RowAction(session, table, type);

        action.setAsAction(this);

        action.memoryRow = memoryRow;
        action.rowId     = rowId;
        action.isMemory  = isMemory;

        return action;
    }

    synchronized void setAsAction(Session session, byte type) {

        this.session    = session;
        this.type       = type;
        changeTimestamp = session.actionTimestamp;
    }

    synchronized void setAsAction(RowActionBase action) {
        super.setAsAction(action);
    }

    private void setAsNoOp(Row row) {

        memoryRow = null;

        synchronized (row) {
            row.hasAction = false;
            row.rowAction = null;
        }

        session = null;

//        actionTimestamp = 0;
        commitTimestamp = 0;

//        rolledback      = false;
//        prepared        = false;
        type = RowActionBase.ACTION_NONE;
        next = null;
    }

    private void setAsDeleteFinal() {

        rolledback = false;
        prepared   = false;
        type       = RowActionBase.ACTION_DELETE_FINAL;
        next       = null;
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

    synchronized void commit(Session session) {

        RowActionBase action = this;

        do {
            if (action.session == session && action.commitTimestamp == 0) {
                action.commitTimestamp = session.actionTimestamp;
                action.prepared        = false;
            }

            action = action.next;
        } while (action != null);
    }

    /**
     * Rollback actions for a session including and after the given timestamp
     */
    synchronized void rollback(Session session, long timestamp) {

        RowActionBase action = this;

        do {
            if (action.session == session && action.commitTimestamp == 0) {
                if (action.actionTimestamp >= timestamp
                        || action.actionTimestamp == 0) {
                    action.commitTimestamp = session.actionTimestamp;
                    action.rolledback      = true;
                    action.prepared        = false;
                }
            }

            action = action.next;
        } while (action != null);
    }

    /**
     * returns type of commit performed on timestamp. ACTION_NONE if none.
     */
    synchronized int getCommitType(long timestamp) {

        RowActionBase action = this;
        int           type   = ACTION_NONE;

        do {
            if (action.commitTimestamp == timestamp) {
                type = action.type;
            }

            action = action.next;
        } while (action != null);

        return type;
    }

    /**
     * returns false if another committed session has altered the same row
     */
    synchronized boolean canCommit(Session session, OrderedHashSet set) {

        RowActionBase action;
        long          timestamp       = session.transactionTimestamp;
        long          commitTimestamp = 0;
        final boolean readCommitted = session.isolationMode
                                      == SessionInterface.TX_READ_COMMITTED;

        action = this;

        if (readCommitted) {
            do {
                if (action.session == session) {

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
            if (action.rolledback || action.type == ACTION_NONE) {
                action = action.next;

                continue;
            }

            if (action.session != session) {
                if (action.prepared) {
                    return false;
                }

                if (action.commitTimestamp == 0
                        && action.actionTimestamp != 0) {
                    set.add(action.session);
                } else if (action.commitTimestamp > commitTimestamp) {
                    commitTimestamp = action.commitTimestamp;
                }
            }

            action = action.next;
        } while (action != null);

        return commitTimestamp < timestamp;
    }

    /**
     * returns false if cannot complete
     * when READ COMMITTED, false result always means repeat action and adds
     * set of sessions to wait on in the parameter (may be no wait)
     */
    synchronized boolean complete(Session session, OrderedHashSet set) {

        RowActionBase action;
        boolean readCommitted = session.isolationMode
                                == SessionInterface.TX_READ_COMMITTED;
        boolean result = true;

        action = this;

        do {
            if (action.rolledback || action.type == ACTION_NONE) {
                action = action.next;

                continue;
            }

            if (action.session == session) {
                if (action.actionTimestamp == 0) {
                    action.actionTimestamp = session.actionTimestamp;
                }
            } else {
                if (action.prepared) {
                    return false;
                }

                if (readCommitted) {
                    if (action.commitTimestamp > session.actionTimestamp) {
                        result = false;
                    } else if (action.commitTimestamp == 0
                               && action.actionTimestamp != 0) {
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

    synchronized int getLastChangeActionType(long timestamp) {

        RowActionBase action     = this;
        int           actionType = ACTION_NONE;

        do {
            if (action.changeTimestamp == timestamp) {
                actionType = action.type;
            }

            action = action.next;
        } while (action != null);

        return actionType;
    }

    synchronized int getActionType(long timestamp) {

        int           actionType = ACTION_NONE;
        RowActionBase action     = this;

        do {
            if (action.actionTimestamp == timestamp) {
                if (action.type == RowActionBase.ACTION_DELETE) {
                    if (actionType == RowActionBase.ACTION_INSERT) {
                        actionType = RowActionBase.ACTION_NONE;
                        action     = action.next;

                        continue;
                    }
                }

                actionType = action.type;
            }

            action = action.next;
        } while (action != null);

        return actionType;
    }

    synchronized int getPos() {
        return rowId;
    }

    synchronized void setPos(int pos) {
        rowId = pos;
    }

    /**
     * merge rolled back actions
     */
    synchronized void mergeRollback(Row row) {

        RowActionBase action = this;
        RowActionBase head   = null;
        RowActionBase tail   = null;

        if (type == RowActionBase.ACTION_DELETE_FINAL
                || type == RowActionBase.ACTION_NONE) {
            return;
        }

        do {
            if (action.rolledback) {
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
            boolean exists = (type == RowActionBase.ACTION_DELETE);

            if (exists) {
                setAsNoOp(row);
            } else {
                setAsDeleteFinal();
            }
        } else {
            if (head != this) {
                setAsAction(head);
            }
        }
    }

    /**
     * merge rolled back actions on a given timestamp
     */
    synchronized boolean mergeRollback(Row row, long timestamp) {

        RowActionBase action = this;
        RowActionBase head   = null;
        RowActionBase tail   = null;

        if (type == RowActionBase.ACTION_DELETE_FINAL
                || type == RowActionBase.ACTION_NONE) {
            return true;
        }

        do {
            if (action.commitTimestamp == timestamp) {
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
            boolean exists = (type == RowActionBase.ACTION_DELETE);

            if (exists) {
                setAsNoOp(row);
            } else {
                setAsDeleteFinal();
            }

            return exists;
        } else {
            if (head != this) {
                setAsAction(head);
            }
        }

        return true;
    }

    /**
     * merge session actions committed on or before given timestamp.
     *
     * return false if row is to be deleted
     *
     */
    synchronized void mergeToTimestamp(Row row, long timestamp) {

        RowActionBase action = this;
        RowActionBase head   = null;
        RowActionBase tail   = null;
        boolean       exists = true;

/* debug 190
        if (row.rowActionB == null) {

//            row.rowActionB = this.duplicate(timestamp);
        } else {
            RowActionBase tailB = row.rowActionB;

            while (tailB.next != null) {
                tailB = tailB.next;
            }

//            tailB.next = this.duplicate(timestamp);
        }

// debug 190
*/
        if (type == RowActionBase.ACTION_DELETE_FINAL
                || type == RowActionBase.ACTION_NONE) {
            return;
        }

        do {
            if (action.commitTimestamp != 0
                    && action.commitTimestamp <= timestamp) {
                if (tail != null) {
                    tail.next = null;
                }

                exists = (action.type == RowActionBase.ACTION_INSERT);
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
            if (exists) {
                setAsNoOp(row);
            } else {
                setAsDeleteFinal();
            }
        } else if (head != this) {
            setAsAction(head);
        }
    }

    synchronized boolean isPriorTo(long threshold) {

        RowActionBase action = this;

        do {
            if (action.type == ACTION_DELETE_FINAL
                    || action.commitTimestamp == 0
                    || action.commitTimestamp > threshold) {
                return false;
            }

            action = action.next;
        } while (action != null);

        return true;
    }

    synchronized boolean canRead(Session session) {

        long    threshold;
        boolean canRead = true;

        if (type == RowActionBase.ACTION_DELETE_FINAL) {
            return false;
        } else if (type == RowActionBase.ACTION_NONE) {
            return true;
        }

        canRead = type == RowActionBase.ACTION_DELETE;

        RowActionBase action = this;

        if (session == null) {
            threshold = Long.MAX_VALUE;
        } else {
            switch (session.isolationMode) {

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
            if (action.rolledback) {
                action = action.next;

                continue;
            }

            if (session == action.session) {
                if (action.type == RowActionBase.ACTION_DELETE) {
                    canRead = false;
                } else if (action.type == RowActionBase.ACTION_INSERT) {
                    canRead = true;
                }

                action = action.next;

                continue;
            } else if (action.commitTimestamp == 0) {
                action = action.next;

                continue;
            }

            if (action.commitTimestamp < threshold) {
                if (action.type == RowActionBase.ACTION_DELETE) {
                    canRead = false;
                } else if (action.type == RowActionBase.ACTION_INSERT) {
                    canRead = true;
                }
            }

            action = action.next;

            continue;
        } while (action != null);

        return canRead;
    }
}
