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

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.HsqlDeque;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.rights.Grantee;

// peterhudson@users 20020130 - patch 478657 by peterhudson - triggers support
// fredt@users 20020130 - patch 1.7.0 by fredt
// added new class as jdk 1.1 does not allow use of LinkedList
// fredt@users 20030727 - signature and other alterations
// fredt@users 20040430 - changes by mattshaw@users to allow termination of the
// trigger thread -
// fredt@users - updated for v. 2.x

/**
 *  Represents an HSQLDB Trigger definition. <p>
 *
 *  Provides services regarding HSQLDB Trigger execution and metadata. <p>
 *
 *  Development of the trigger implementation sponsored by Logicscope
 *  Realisations Ltd
 *
 * @author Peter Hudson (peterhudson@users dot sourceforge.net)
 * @version  2.0.1
 * @since hsqldb_voltpatches 1.61
 */
public class TriggerDef implements Runnable, SchemaObject {

    static final int OLD_ROW     = 0;
    static final int NEW_ROW     = 1;
    static final int RANGE_COUNT = 2;
    static final int OLD_TABLE   = 2;
    static final int NEW_TABLE   = 3;
    static final int BEFORE      = 4;
    static final int AFTER       = 5;
    static final int INSTEAD     = 6;

    //
    static final int NUM_TRIGGER_OPS = 3;                      // {ins,del,upd}
    static final int NUM_TRIGS       = NUM_TRIGGER_OPS * 3;    // {b}{fer}, {a},{fer, fes}

    //
    static final TriggerDef[] emptyArray = new TriggerDef[]{};
    Table[]                   transitions;
    RangeVariable[]           rangeVars;
    Expression                condition;
    boolean                   hasTransitionTables;
    boolean                   hasTransitionRanges;
    String                    conditionSQL;
    Routine                   routine;
    int[]                     updateColumns;

    // other variables
    private HsqlName name;
    long             changeTimestamp;
    int              actionTiming;
    int              operationType;
    boolean          isSystem;
    boolean          forEachRow;
    boolean          nowait;                                   // block or overwrite if queue full
    int              maxRowsQueued;                            // max size of queue of pending triggers
    Table            table;
    Trigger          trigger;
    String           triggerClassName;
    int              triggerType;
    Thread           thread;

    //protected boolean busy;               // firing trigger in progress
    protected HsqlDeque        pendingQueue;                   // row triggers pending
    protected int              rowsQueued;                     // rows in pendingQueue
    protected boolean          valid     = true;               // parsing valid
    protected volatile boolean keepGoing = true;

    TriggerDef() {}

    /**
     *  Constructs a new TriggerDef object to represent an HSQLDB trigger
     *  declared in an SQL CREATE TRIGGER statement.
     *
     *  Changes in 1.7.2 allow the queue size to be specified as 0. A zero
     *  queue size causes the Trigger.fire() code to run in the main thread of
     *  execution (fully inside the enclosing transaction). Otherwise, the code
     *  is run in the Trigger's own thread.
     *  (fredt@users)
     *
     * @param  name The trigger object's HsqlName
     * @param  when whether the trigger fires
     *      before, after or instead of the triggering event
     * @param  operation the triggering operation;
     *      currently insert, update, or delete
     * @param  forEach indicates whether the trigger is fired for each row
     *      (true) or statement (false)
     * @param  table the Table object upon which the indicated operation
     *      fires the trigger
     * @param  triggerClassName the fully qualified named of the class implementing
     *      the org.hsqldb_voltpatches.Trigger (trigger body) interface
     * @param  noWait do not wait for available space on the pending queue; if
     *      the pending queue does not have fewer than nQueueSize queued items,
     *      then overwrite the current tail instead
     * @param  queueSize the length to which the pending queue may grow before
     *      further additions are either blocked or overwrite the tail entry,
     *      as determined by noWait
     */
    public TriggerDef(HsqlNameManager.HsqlName name, int when, int operation,
                      boolean forEach, Table table, Table[] transitions,
                      RangeVariable[] rangeVars, Expression condition,
                      String conditionSQL, int[] updateColumns,
                      String triggerClassName, boolean noWait, int queueSize) {

        this(name, when, operation, forEach, table, transitions, rangeVars,
             condition, conditionSQL, updateColumns);

        this.triggerClassName = triggerClassName;
        this.nowait           = noWait;
        this.maxRowsQueued    = queueSize;
        rowsQueued            = 0;
        pendingQueue          = new HsqlDeque();

        Class cl = null;

        try {
            cl = Class.forName(triggerClassName, true,
                               Thread.currentThread().getContextClassLoader());
        } catch (Throwable t1) {
            try {
                cl = Class.forName(triggerClassName);
            } catch (Throwable t) {}
        }

        if (cl == null) {
            valid   = false;
            trigger = new DefaultTrigger();
        } else {
            try {

                // dynamically instantiate it
                trigger = (Trigger) cl.newInstance();
            } catch (Throwable t1) {
                valid   = false;
                trigger = new DefaultTrigger();
            }
        }
    }

    public TriggerDef(HsqlNameManager.HsqlName name, int when, int operation,
                      boolean forEachRow, Table table, Table[] transitions,
                      RangeVariable[] rangeVars, Expression condition,
                      String conditionSQL, int[] updateColumns) {

        this.name          = name;
        this.actionTiming  = when;
        this.operationType = operation;
        this.forEachRow    = forEachRow;
        this.table         = table;
        this.transitions   = transitions;
        this.rangeVars     = rangeVars;
        this.condition     = condition == null ? Expression.EXPR_TRUE
                                               : condition;
        this.updateColumns = updateColumns;
        this.conditionSQL  = conditionSQL;
        hasTransitionRanges = rangeVars[OLD_ROW] != null
                              || rangeVars[NEW_ROW] != null;
        hasTransitionTables = transitions[OLD_TABLE] != null
                              || transitions[NEW_TABLE] != null;

        setUpIndexesAndTypes();
    }

    public boolean isValid() {
        return valid;
    }

    public int getType() {
        return SchemaObject.TRIGGER;
    }

    public HsqlName getName() {
        return name;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject parentObject) {}

    /**
     *  Retrieves the SQL character sequence required to (re)create the
     *  trigger, as a StringBuffer
     *
     * @return the SQL character sequence required to (re)create the
     *  trigger
     */
    public String getSQL() {

        StringBuffer sb = getSQLMain();

        if (maxRowsQueued != 0) {
            sb.append(Tokens.T_QUEUE).append(' ');
            sb.append(maxRowsQueued).append(' ');

            if (nowait) {
                sb.append(Tokens.T_NOWAIT).append(' ');
            }
        }

        sb.append(Tokens.T_CALL).append(' ');
        sb.append(StringConverter.toQuotedString(triggerClassName, '"',
                false));

        return sb.toString();
    }

    public long getChangeTimestamp() {
        return changeTimestamp;
    }

    public StringBuffer getSQLMain() {

        StringBuffer sb = new StringBuffer(256);

        sb.append(Tokens.T_CREATE).append(' ');
        sb.append(Tokens.T_TRIGGER).append(' ');
        sb.append(name.getSchemaQualifiedStatementName()).append(' ');
        sb.append(getActionTimingString()).append(' ');
        sb.append(getEventTypeString()).append(' ');

        if (updateColumns != null) {
            sb.append(Tokens.T_OF).append(' ');

            for (int i = 0; i < updateColumns.length; i++) {
                if (i != 0) {
                    sb.append(',');
                }

                HsqlName name = table.getColumn(updateColumns[i]).getName();

                sb.append(name.statementName);
            }

            sb.append(' ');
        }

        sb.append(Tokens.T_ON).append(' ');
        sb.append(table.getName().getSchemaQualifiedStatementName());
        sb.append(' ');

        if (hasTransitionRanges || hasTransitionTables) {
            sb.append(Tokens.T_REFERENCING).append(' ');

            if (rangeVars[OLD_ROW] != null) {
                sb.append(Tokens.T_OLD).append(' ').append(Tokens.T_ROW);
                sb.append(' ').append(Tokens.T_AS).append(' ');
                sb.append(
                    rangeVars[OLD_ROW].getTableAlias().getStatementName());
                sb.append(' ');
            }

            if (rangeVars[NEW_ROW] != null) {
                sb.append(Tokens.T_NEW).append(' ').append(Tokens.T_ROW);
                sb.append(' ').append(Tokens.T_AS).append(' ');
                sb.append(
                    rangeVars[NEW_ROW].getTableAlias().getStatementName());
                sb.append(' ');
            }

            if (transitions[OLD_TABLE] != null) {
                sb.append(Tokens.T_OLD).append(' ').append(Tokens.T_TABLE);
                sb.append(' ').append(Tokens.T_AS).append(' ');
                sb.append(transitions[OLD_TABLE].getName().statementName);
                sb.append(' ');
            }

            if (transitions[NEW_TABLE] != null) {
                sb.append(Tokens.T_OLD).append(' ').append(Tokens.T_TABLE);
                sb.append(' ').append(Tokens.T_AS).append(' ');
                sb.append(transitions[NEW_TABLE].getName().statementName);
                sb.append(' ');
            }
        }

        if (forEachRow) {
            sb.append(Tokens.T_FOR).append(' ');
            sb.append(Tokens.T_EACH).append(' ');
            sb.append(Tokens.T_ROW).append(' ');
        }

        if (condition != Expression.EXPR_TRUE) {
            sb.append(Tokens.T_WHEN).append(' ');
            sb.append(Tokens.T_OPENBRACKET).append(conditionSQL);
            sb.append(Tokens.T_CLOSEBRACKET).append(' ');
        }

        return sb;
    }

    public String getClassName() {
        return trigger.getClass().getName();
    }

    public String getActionTimingString() {

        switch (this.actionTiming) {

            case TriggerDef.BEFORE :
                return Tokens.T_BEFORE;

            case TriggerDef.AFTER :
                return Tokens.T_AFTER;

            case TriggerDef.INSTEAD :
                return Tokens.T_INSTEAD + ' ' + Tokens.T_OF;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "TriggerDef");
        }
    }

    public String getEventTypeString() {

        switch (this.operationType) {

            case StatementTypes.INSERT :
                return Tokens.T_INSERT;

            case StatementTypes.DELETE_WHERE :
                return Tokens.T_DELETE;

            case StatementTypes.UPDATE_WHERE :
                return Tokens.T_UPDATE;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "TriggerDef");
        }
    }

    public boolean isSystem() {
        return isSystem;
    }

    public boolean isForEachRow() {
        return forEachRow;
    }

    public String getConditionSQL() {
        return conditionSQL;
    }

    public String getProcedureSQL() {
        return routine == null ? null
                               : routine.getSQLBodyDefinition();
    }

    public int[] getUpdateColumnIndexes() {
        return updateColumns;
    }

    public boolean hasOldTable() {
        return false;
    }

    public boolean hasNewTable() {
        return false;
    }

    public String getOldTransitionRowName() {

        return rangeVars[OLD_ROW] == null ? null
                                          : rangeVars[OLD_ROW].getTableAlias()
                                              .name;
    }

    public String getNewTransitionRowName() {

        return rangeVars[NEW_ROW] == null ? null
                                          : rangeVars[NEW_ROW].getTableAlias()
                                              .name;
    }

    public String getOldTransitionTableName() {

        return transitions[OLD_TABLE] == null ? null
                                              : transitions[OLD_TABLE]
                                              .getName().name;
    }

    public String getNewTransitionTableName() {

        return transitions[NEW_TABLE] == null ? null
                                              : transitions[NEW_TABLE]
                                              .getName().name;
    }

    /**
     *  Given the SQL creating the trigger, set up the index to the
     *  HsqlArrayList[] and the associated GRANT type
     */
    void setUpIndexesAndTypes() {

        triggerType = 0;

        switch (operationType) {

            case StatementTypes.INSERT :
                triggerType = Trigger.INSERT_AFTER;
                break;

            case StatementTypes.DELETE_WHERE :
                triggerType = Trigger.DELETE_AFTER;
                break;

            case StatementTypes.UPDATE_WHERE :
                triggerType = Trigger.UPDATE_AFTER;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "TriggerDef");
        }

        if (forEachRow) {
            triggerType += NUM_TRIGGER_OPS;
        }

        if (actionTiming == TriggerDef.BEFORE
                || actionTiming == TriggerDef.INSTEAD) {
            triggerType += NUM_TRIGGER_OPS;
        }
    }

    /**
     *  Return the type code for operation tokens
     */
    static int getOperationType(int token) {

        switch (token) {

            case Tokens.INSERT :
                return StatementTypes.INSERT;

            case Tokens.DELETE :
                return StatementTypes.DELETE_WHERE;

            case Tokens.UPDATE :
                return StatementTypes.UPDATE_WHERE;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "TriggerDef");
        }
    }

    static int getTiming(int token) {

        switch (token) {

            case Tokens.BEFORE :
                return TriggerDef.BEFORE;

            case Tokens.AFTER :
                return TriggerDef.AFTER;

            case Tokens.INSTEAD :
                return TriggerDef.INSTEAD;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "TriggerDef");
        }
    }

    public int getStatementType() {
        return operationType;
    }

    /**
     *  run method declaration <P>
     *
     *  the trigger JSP is run in its own thread here. Its job is simply to
     *  wait until it is told by the main thread that it should fire the
     *  trigger.
     */
    public void run() {

        while (keepGoing) {
            TriggerData triggerData = popPair();

            if (triggerData != null) {
                if (triggerData.username != null) {
                    trigger.fire(this.triggerType, name.name,
                                 table.getName().name, triggerData.oldRow,
                                 triggerData.newRow);
                }
            }
        }

        try {
            thread.setContextClassLoader(null);
        } catch (Throwable t) {}
    }

    /**
     * start the thread if this is threaded
     */
    public synchronized void start() {

        if (maxRowsQueued != 0) {
            thread = new Thread(this);

            thread.start();
        }
    }

    /**
     * signal the thread to stop
     */
    public synchronized void terminate() {

        keepGoing = false;

        notify();
    }

    /**
     *  pop2 method declaration <P>
     *
     *  The consumer (trigger) thread waits for an event to be queued <P>
     *
     *  <B>Note: </B> This push/pop pairing assumes a single producer thread
     *  and a single consumer thread _only_.
     *
     * @return  Description of the Return Value
     */
    synchronized TriggerData popPair() {

        if (rowsQueued == 0) {
            try {
                wait();    // this releases the lock monitor
            } catch (InterruptedException e) {

                /* ignore and resume */
            }
        }

        rowsQueued--;

        notify();    // notify push's wait

        if (pendingQueue.size() == 0) {
            return null;
        } else {
            return (TriggerData) pendingQueue.removeFirst();
        }
    }

    /**
     *  The main thread tells the trigger thread to fire by this call.
     *  If this Trigger is not threaded then the fire method is caled
     *  immediately and executed by the main thread. Otherwise, the row
     *  data objects are added to the queue to be used by the Trigger thread.
     *
     * @param  row1
     * @param  row2
     */
    synchronized void pushPair(Session session, Object[] row1, Object[] row2) {

        if (maxRowsQueued == 0) {
            session.getInternalConnection();

            try {
                trigger.fire(triggerType, name.name, table.getName().name,
                             row1, row2);
            } finally {
                session.releaseInternalConnection();
            }

            return;
        }

        if (rowsQueued >= maxRowsQueued) {
            if (nowait) {
                pendingQueue.removeLast();    // overwrite last
            } else {
                try {
                    wait();
                } catch (InterruptedException e) {

                    /* ignore and resume */
                }

                rowsQueued++;
            }
        } else {
            rowsQueued++;
        }

        pendingQueue.add(new TriggerData(session, row1, row2));
        notify();    // notify pop's wait
    }

    public boolean isBusy() {
        return rowsQueued != 0;
    }

    public Table getTable() {
        return table;
    }

    public String getActionOrientationString() {
        return forEachRow ? Tokens.T_ROW
                          : Tokens.T_STATEMENT;
    }

    /**
     * Class to store the data used to fire a trigger. The username attribute
     * is not used but it allows developers to change the signature of the
     * fire method of the Trigger class and pass the user name to the Trigger.
     */
    static class TriggerData {

        public Object[] oldRow;
        public Object[] newRow;
        public String   username;

        public TriggerData(Session session, Object[] oldRow, Object[] newRow) {

            this.oldRow   = oldRow;
            this.newRow   = newRow;
            this.username = session.getUsername();
        }
    }

    static class DefaultTrigger implements Trigger {

        public void fire(int i, String name, String table, Object[] row1,
                         Object[] row2) {

            // do nothing
        }
    }
}
