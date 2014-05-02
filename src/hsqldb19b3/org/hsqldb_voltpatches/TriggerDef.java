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

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.lib.HsqlDeque;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.rights.GrantConstants;
import org.hsqldb_voltpatches.rights.Grantee;

// peterhudson@users 20020130 - patch 478657 by peterhudson - triggers support
// fredt@users 20020130 - patch 1.7.0 by fredt
// added new class as jdk 1.1 does not allow use of LinkedList
// fredt@users 20030727 - signature and other alterations
// fredt@users 20040430 - changes by mattshaw@users to allow termination of the
// trigger thread -

/**
 *  Represents an HSQLDB Trigger definition. <p>
 *
 *  Provides services regarding HSLDB Trigger execution and metadata. <p>
 *
 *  Development of the trigger implementation sponsored by Logicscope
 *  Realisations Ltd
 *
 * @author Peter Hudson - Logicscope Realisations Ltd
 * @version  1.7.0 (1.0.0.3)
 *      Revision History: 1.0.0.1 First release in hsqldb 1.61
 *      1.0.0.2 'nowait' support to prevent deadlock 1.0.0.3 multiple row
 *      queue for each trigger
 */
public class TriggerDef implements Runnable, SchemaObject {

    static final int OLD_ROW   = 0;
    static final int NEW_ROW   = 1;
    static final int OLD_TABLE = 2;
    static final int NEW_TABLE = 3;

    //
    static final int NUM_TRIGGER_OPS  = 3;                      // {ins,del,upd}
    static final int NUM_TRIGS        = NUM_TRIGGER_OPS * 2;    // {b, a},{fer, fes}
    static final int defaultQueueSize = 1024;

    //
    static final TriggerDef[] emptyArray = new TriggerDef[]{};
    Table[]                   transitions;
    RangeVariable[]           rangeVars;
    Expression                condition;
    boolean                   hasTransitionTables;
    boolean                   hasTransitionRanges;
    String                    conditionSQL;
    String                    procedureSQL;
    Statement[]               statements = Statement.emptyArray;
    int[]                     updateColumns;

    // other variables
    HsqlName name;
    String   actionTimingString;
    String   eventTimingString;
    int      operationPrivilegeType;
    boolean  forEachRow;
    boolean  nowait;                                            // block or overwrite if queue full
    int      maxRowsQueued;                                     // max size of queue of pending triggers
    Table    table;
    Trigger  trigger;
    String   triggerClassName;
    int      triggerType;
    int      vectorIndex;                                       // index into TriggerDef[][]
    Thread   thread;

    //protected boolean busy;               // firing trigger in progress
    protected HsqlDeque        pendingQueue;                    // row triggers pending
    protected int              rowsQueued;                      // rows in pendingQueue
    protected boolean          valid     = true;                // parsing valid
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
     * @param  when the String representation of whether the trigger fires
     *      before or after the triggering event
     * @param  operation the String representation of the triggering operation;
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
    public TriggerDef(HsqlNameManager.HsqlName name, String when,
                      String operation, boolean forEach, Table table,
                      Table[] transitions, RangeVariable[] rangeVars,
                      Expression condition, String conditionSQL,
                      int[] updateColumns, String triggerClassName,
                      boolean noWait, int queueSize) {

        this.name               = name;
        this.actionTimingString = when;
        this.eventTimingString  = operation;
        this.forEachRow         = forEach;
        this.table              = table;
        this.transitions        = transitions;
        this.rangeVars          = rangeVars;
        this.condition          = condition == null ? Expression.EXPR_TRUE
                                                    : condition;
        this.conditionSQL       = conditionSQL;
        this.updateColumns      = updateColumns;
        this.procedureSQL       = procedureSQL;
        this.triggerClassName   = triggerClassName;
        this.nowait             = noWait;
        this.maxRowsQueued      = queueSize;
        rowsQueued              = 0;
        pendingQueue            = new HsqlDeque();

        setUpIndexesAndTypes();

        Class cl;

        try {
            cl = Class.forName(triggerClassName);
        } catch (ClassNotFoundException e) {
            valid = false;
            cl    = DefaultTrigger.class;
        }

        try {

            // dynamically instantiate it
            trigger = (Trigger) cl.newInstance();
        } catch (Exception e) {
            valid = false;
            cl    = DefaultTrigger.class;
        }
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

    public void compile(Session session) {}

    /**
     *  Retrieves the SQL character sequence required to (re)create the
     *  trigger, as a StringBuffer
     *
     * @return the SQL character sequence required to (re)create the
     *  trigger
     */
    public String getSQL() {

        StringBuffer sb = new StringBuffer(256);

        sb.append(Tokens.T_CREATE).append(' ');
        sb.append(Tokens.T_TRIGGER).append(' ');
        sb.append(name.getSchemaQualifiedStatementName()).append(' ');
        sb.append(actionTimingString).append(' ');
        sb.append(eventTimingString).append(' ');
        sb.append(Tokens.T_ON).append(' ');
        sb.append(table.getName().getSchemaQualifiedStatementName());
        sb.append(' ');

        if (forEachRow) {
            sb.append(Tokens.T_FOR).append(' ');
            sb.append(Tokens.T_EACH).append(' ');
            sb.append(Tokens.T_ROW).append(' ');
        }

        if (nowait) {
            sb.append(Tokens.T_NOWAIT).append(' ');
        }

        if (maxRowsQueued != defaultQueueSize) {
            sb.append(Tokens.T_QUEUE).append(' ');
            sb.append(maxRowsQueued).append(' ');
        }

        sb.append(Tokens.T_CALL).append(' ');
        sb.append(StringConverter.toQuotedString(triggerClassName, '"',
                false));

        return sb.toString();
    }

    public String getClassName() {
        return trigger.getClass().getName();
    }

    public String getActionTimingString() {
        return actionTimingString;
    }

    public String getEventTypeString() {
        return eventTimingString;
    }

    public boolean isForEachRow() {
        return forEachRow;
    }

    public String getConditionSQL() {
        return conditionSQL;
    }

    public String getProcedureSQL() {
        return procedureSQL;
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

        return transitions[OLD_ROW] == null ? null
                                            : transitions[OLD_ROW].getName()
                                            .name;
    }

    public String getNewTransitionRowName() {

        return transitions[NEW_ROW] == null ? null
                                            : transitions[NEW_ROW].getName()
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

        vectorIndex = 0;

        if (eventTimingString.equals(Tokens.T_INSERT)) {
            vectorIndex            = Trigger.INSERT_AFTER;
            operationPrivilegeType = GrantConstants.INSERT;
        } else if (eventTimingString.equals(Tokens.T_DELETE)) {
            operationPrivilegeType = GrantConstants.DELETE;
            vectorIndex            = Trigger.DELETE_AFTER;
        } else if (eventTimingString.equals(Tokens.T_UPDATE)) {
            operationPrivilegeType = GrantConstants.UPDATE;
            vectorIndex            = Trigger.UPDATE_AFTER;
        } else {
            throw Error.runtimeError(ErrorCode.U_S0500, "TriggerDef");
        }

        if (actionTimingString.equals(Tokens.T_BEFORE)
                || actionTimingString.equals(Tokens.T_INSERT)) {
            vectorIndex += NUM_TRIGGER_OPS;    // number of operations
        }

        triggerType = vectorIndex;

        if (forEachRow) {
            triggerType += 2 * NUM_TRIGGER_OPS;
        }
    }

    public int getPrivilegeType() {
        return operationPrivilegeType;
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
                    trigger.fire(this.vectorIndex, name.name,
                                 table.getName().name, triggerData.oldRow,
                                 triggerData.newRow);
                }
            }
        }
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
            trigger.fire(triggerType, name.name, table.getName().name, row1,
                         row2);

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

    static class DefaultTrigger implements org.hsqldb_voltpatches.Trigger {

        public void fire(int i, String name, String table, Object[] row1,
                         Object[] row2) {
            throw new RuntimeException("Missing Trigger class!");
        }
    }
}
