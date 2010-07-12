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

import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;
import org.hsqldb_voltpatches.result.Result;

/**
 * Implementation of Statement for condition handler objects.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class StatementHandler extends Statement {

    public static final int NONE          = 0;
    public static final int SQL_EXCEPTION = 1;
    public static final int SQL_WARNING   = 2;
    public static final int SQL_NOT_FOUND = 3;
    public static final int SQL_STATE     = 4;

    //
    public static final int CONTINUE = 5;
    public static final int EXIT     = 6;
    public static final int UNDO     = 7;

    //
    public final int handlerType;

    //
    OrderedIntHashSet conditionGroups = new OrderedIntHashSet();
    HashSet           conditionStates = new HashSet();
    Statement         statement;

    //
    public static final StatementHandler[] emptyExceptionHandlerArray =
        new StatementHandler[]{};

    StatementHandler(int handlerType) {

        super(StatementTypes.HANDLER, StatementTypes.X_SQL_CONTROL);

        this.handlerType = handlerType;
    }

    public void addConditionState(String sqlState) {

        boolean result = conditionStates.add(sqlState);

        result &= conditionGroups.isEmpty();

        if (!result) {
            throw Error.error(ErrorCode.X_42604);
        }
    }

    public void addConditionType(int conditionType) {

        boolean result = conditionGroups.add(conditionType);

        result &= conditionStates.isEmpty();

        if (!result) {
            throw Error.error(ErrorCode.X_42604);
        }
    }

    public void addStatement(Statement s) {
        statement = s;
    }

    public boolean handlesConditionType(int type) {
        return conditionGroups.contains(type);
    }

    public boolean handlesCondition(String sqlState) {

        if (conditionStates.contains(sqlState)) {
            return true;
        }

        String conditionClass = sqlState.substring(0, 2);

        if (conditionStates.contains(conditionClass)) {
            return true;
        }

        if (conditionClass.equals("01")) {
            return conditionGroups.contains(SQL_WARNING);
        }

        if (conditionClass.equals("02")) {
            return conditionGroups.contains(SQL_NOT_FOUND);
        }

        return conditionGroups.contains(SQL_EXCEPTION);
    }

    public int[] getConditionTypes() {
        return conditionGroups.toArray();
    }

    public String[] getConditionStates() {
        return (String[]) conditionStates.toArray(
            new String[conditionStates.size()]);
    }

    public Result execute(Session session) {

        if (statement != null) {
            return statement.execute(session);
        } else {
            return Result.updateZeroResult;
        }
    }

    public String describe(Session session) {
        return "";
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer(64);
        String       s;

        s = handlerType == CONTINUE ? Tokens.T_CONTINUE
                                    : handlerType == EXIT ? Tokens.T_EXIT
                                                          : Tokens.T_UNDO;

        sb.append(Tokens.T_DECLARE).append(' ').append(s).append(' ');
        sb.append(Tokens.T_HANDLER).append(' ').append(Tokens.T_FOR);
        sb.append(' ');

        for (int i = 0; i < conditionStates.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }

            sb.append(Tokens.T_SQLSTATE).append(' ');
            sb.append('\'').append(conditionStates.get(i)).append('\'');
        }

        for (int i = 0; i < conditionGroups.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }

            switch (conditionGroups.get(i)) {

                case SQL_EXCEPTION :
                    sb.append(Tokens.T_SQLEXCEPTION);
                    break;

                case SQL_WARNING :
                    sb.append(Tokens.T_SQLWARNING);
                    break;

                case SQL_NOT_FOUND :
                    sb.append(Tokens.T_NOT).append(' ').append(Tokens.FOUND);
                    break;
            }
        }

        if (statement != null) {
            sb.append(' ').append(statement.getSQL());
        }

        return sb.toString();
    }
}
