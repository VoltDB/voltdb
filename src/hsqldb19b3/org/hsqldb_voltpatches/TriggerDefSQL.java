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
 * Implementation of SQL TRIGGER objects.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class TriggerDefSQL extends TriggerDef {

    OrderedHashSet references;

    public TriggerDefSQL(HsqlNameManager.HsqlName name, String when,
                         String operation, boolean forEachRow, Table table,
                         Table[] transitions, RangeVariable[] rangeVars,
                         Expression condition, String conditionSQL,
                         int[] updateColumns,
                         StatementDMQL[] compiledStatements,
                         String procedureSQL, OrderedHashSet references) {

        this.name               = name;
        this.actionTimingString = when;
        this.eventTimingString  = operation;
        this.forEachRow         = forEachRow;
        this.table              = table;
        this.transitions        = transitions;
        this.rangeVars          = rangeVars;
        this.condition          = condition == null ? Expression.EXPR_TRUE
                                                    : condition;
        this.updateColumns      = updateColumns;
        this.statements         = compiledStatements;
        this.conditionSQL       = conditionSQL;
        this.procedureSQL       = procedureSQL;
        this.references         = references;
        hasTransitionRanges = transitions[OLD_ROW] != null
                              || transitions[NEW_ROW] != null;
        hasTransitionTables = transitions[OLD_TABLE] != null
                              || transitions[NEW_TABLE] != null;

        setUpIndexesAndTypes();

        //
    }

    public OrderedHashSet getReferences() {
        return references;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session) {}

    public String getClassName() {
        return null;
    }

    public boolean hasOldTable() {
        return transitions[OLD_TABLE] != null;
    }

    public boolean hasNewTable() {
        return transitions[NEW_TABLE] != null;
    }

    synchronized void pushPair(Session session, Object[] oldData,
                               Object[] newData) {

        if (transitions[OLD_ROW] != null) {
            rangeVars[OLD_ROW].getIterator(session).currentData = oldData;
        }

        if (transitions[NEW_ROW] != null) {
            rangeVars[NEW_ROW].getIterator(session).currentData = newData;
        }

        if (!condition.testCondition(session)) {
            return;
        }

        for (int i = 0; i < statements.length; i++) {
            statements[i].execute(session);
        }
    }

    public String getSQL() {

        boolean      isBlock = statements.length > 1;
        StringBuffer sb      = new StringBuffer(256);

        sb.append(Tokens.T_CREATE).append(' ');
        sb.append(Tokens.T_TRIGGER).append(' ');
        sb.append(name.statementName).append(' ');
        sb.append(actionTimingString).append(' ');
        sb.append(eventTimingString).append(' ');
        sb.append(Tokens.T_ON).append(' ');
        sb.append(table.getName().statementName).append(' ');

        if (hasTransitionRanges || hasTransitionTables) {
            sb.append(Tokens.T_REFERENCING).append(' ');

            String separator = "";

            if (transitions[OLD_ROW] != null) {
                sb.append(Tokens.T_OLD).append(' ').append(Tokens.T_ROW);
                sb.append(' ').append(Tokens.T_AS).append(' ');
                sb.append(transitions[OLD_ROW].getName().statementName);

                separator = Tokens.T_COMMA;
            }

            if (transitions[NEW_ROW] != null) {
                sb.append(separator);
                sb.append(Tokens.T_NEW).append(' ').append(Tokens.T_ROW);
                sb.append(' ').append(Tokens.T_AS).append(' ');
                sb.append(transitions[NEW_ROW].getName().statementName);

                separator = Tokens.T_COMMA;
            }

            if (transitions[OLD_TABLE] != null) {
                sb.append(separator);
                sb.append(Tokens.T_OLD).append(' ').append(Tokens.T_TABLE);
                sb.append(' ').append(Tokens.T_AS).append(' ');
                sb.append(transitions[OLD_TABLE].getName().statementName);

                separator = Tokens.T_COMMA;
            }

            if (transitions[NEW_TABLE] != null) {
                sb.append(separator);
                sb.append(Tokens.T_OLD).append(' ').append(Tokens.T_TABLE);
                sb.append(' ').append(Tokens.T_AS).append(' ');
                sb.append(transitions[NEW_TABLE].getName().statementName);
            }

            sb.append(' ');
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

        if (isBlock) {
            sb.append(Tokens.T_BEGIN).append(' ').append(Tokens.T_ATOMIC);
            sb.append(' ');
        }

        sb.append(procedureSQL).append(' ');

        if (isBlock) {
            sb.append(Tokens.T_END);
        }

        return sb.toString();
    }
}
