/* Copyright (c) 2001-2014, The HSQL Development Group
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
import org.hsqldb_voltpatches.lib.ArrayListIdentity;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.types.ArrayType;
import org.hsqldb_voltpatches.types.RowType;

/**
 * Implementation of aggregate operations
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class ExpressionAggregate extends Expression {

    boolean   isDistinctAggregate;
    ArrayType arrayType;

    ExpressionAggregate(int type, boolean distinct, Expression e) {

        super(type);

        nodes               = new Expression[BINARY];
        isDistinctAggregate = distinct;
        nodes[LEFT]         = e;
        nodes[RIGHT]        = Expression.EXPR_TRUE;
    }

    boolean isSelfAggregate() {
        return true;
    }

    public String getSQL() {

        StringBuffer sb   = new StringBuffer(64);
        String       left = getContextSQL(nodes.length > 0 ? nodes[LEFT]
                                                           : null);

        switch (opType) {

            case OpTypes.COUNT :
                sb.append(' ').append(Tokens.T_COUNT).append('(');
                break;

            // A VoltDB extension APPROX_COUNT_DISTINCT
            case OpTypes.APPROX_COUNT_DISTINCT :
                sb.append(' ').append(Tokens.T_APPROX_COUNT_DISTINCT).append('(');
                break;
            // End of VoltDB extension
            case OpTypes.SUM :
                sb.append(' ').append(Tokens.T_SUM).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.MIN :
                sb.append(' ').append(Tokens.T_MIN).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.MAX :
                sb.append(' ').append(Tokens.T_MAX).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.AVG :
                sb.append(' ').append(Tokens.T_AVG).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.EVERY :
                sb.append(' ').append(Tokens.T_EVERY).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.SOME :
                sb.append(' ').append(Tokens.T_SOME).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.STDDEV_POP :
                sb.append(' ').append(Tokens.T_STDDEV_POP).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.STDDEV_SAMP :
                sb.append(' ').append(Tokens.T_STDDEV_SAMP).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.VAR_POP :
                sb.append(' ').append(Tokens.T_VAR_POP).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.VAR_SAMP :
                sb.append(' ').append(Tokens.T_VAR_SAMP).append('(');
                sb.append(left).append(')');
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionAggregate");
        }

        return sb.toString();
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        switch (opType) {

            case OpTypes.COUNT :
                sb.append(Tokens.T_COUNT).append(' ');
                break;

            // A VoltDB extension APPROX_COUNT_DISTINCT
            case OpTypes.APPROX_COUNT_DISTINCT :
                sb.append("APPROX_COUNT_DISTINCT ");
                break;
            // End of VoltDB extension
            case OpTypes.SUM :
                sb.append(Tokens.T_SUM).append(' ');
                break;

            case OpTypes.MIN :
                sb.append(Tokens.T_MIN).append(' ');
                break;

            case OpTypes.MAX :
                sb.append(Tokens.T_MAX).append(' ');
                break;

            case OpTypes.AVG :
                sb.append(Tokens.T_AVG).append(' ');
                break;

            case OpTypes.EVERY :
                sb.append(Tokens.T_EVERY).append(' ');
                break;

            case OpTypes.SOME :
                sb.append(Tokens.T_SOME).append(' ');
                break;

            case OpTypes.STDDEV_POP :
                sb.append(Tokens.T_STDDEV_POP).append(' ');
                break;

            case OpTypes.STDDEV_SAMP :
                sb.append(Tokens.T_STDDEV_SAMP).append(' ');
                break;

            case OpTypes.VAR_POP :
                sb.append(Tokens.T_VAR_POP).append(' ');
                break;

            case OpTypes.VAR_SAMP :
                sb.append(Tokens.T_VAR_SAMP).append(' ');
                break;
        }

        if (getLeftNode() != null) {
            sb.append(" arg=[");
            sb.append(nodes[LEFT].describe(session, blanks + 1));
            sb.append(']');
        }

        return sb.toString();
    }

    public HsqlList resolveColumnReferences(Session session,
            RangeGroup rangeGroup, int rangeCount, RangeGroup[] rangeGroups,
            HsqlList unresolvedSet, boolean acceptsSequences) {

        HsqlList conditionSet = nodes[RIGHT].resolveColumnReferences(session,
            rangeGroup, rangeCount, rangeGroups, null, false);

        if (conditionSet != null) {
            ExpressionColumn.checkColumnsResolved(conditionSet);
        }

        if (unresolvedSet == null) {
            unresolvedSet = new ArrayListIdentity();
        }

        unresolvedSet.add(this);

        return unresolvedSet;
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        if (nodes[LEFT].getDegree() > 1) {
            nodes[LEFT].dataType = new RowType(nodes[LEFT].nodeDataTypes);
        }

        if (nodes[LEFT].isUnresolvedParam()) {
            throw Error.error(ErrorCode.X_42567);
        }

        if (isDistinctAggregate) {
            if (nodes[LEFT].dataType.isLobType()) {
                throw Error.error(ErrorCode.X_42534);
            }

            if (nodes[LEFT].dataType.isCharacterType()) {
                arrayType = new ArrayType(nodes[LEFT].dataType,
                                          Integer.MAX_VALUE);
            }
        }

        dataType = SetFunction.getType(session, opType, nodes[LEFT].dataType);

        nodes[RIGHT].resolveTypes(session, null);
    }

    public boolean equals(Expression other) {

        if (other instanceof ExpressionAggregate) {
            ExpressionAggregate o = (ExpressionAggregate) other;

            if (isDistinctAggregate == o.isDistinctAggregate) {
                return super.equals(other);
            }
        }

        return false;
    }

    public Object updateAggregatingValue(Session session, Object currValue) {

        if (!nodes[RIGHT].testCondition(session)) {
            return currValue;
        }

        if (currValue == null) {
            currValue = new SetFunction(session, opType, nodes[LEFT].dataType,
                                        dataType, isDistinctAggregate,
                                        arrayType);
        }

        Object newValue = nodes[LEFT].opType == OpTypes.ASTERISK
                          ? ValuePool.INTEGER_1
                          : nodes[LEFT].getValue(session);

        ((SetFunction) currValue).add(session, newValue);

        return currValue;
    }

    /**
     * Get the result of a SetFunction or an ordinary value
     *
     * @param currValue instance of set function or value
     * @param session context
     * @return object
     */
    public Object getAggregatedValue(Session session, Object currValue) {

        if (currValue == null) {
            // A VoltDB extension APPROX_COUNT_DISTINCT
            return opType == OpTypes.COUNT || opType == OpTypes.APPROX_COUNT_DISTINCT ?
                    Long.valueOf(0): null;
            /* disable 2 lines...
            return opType == OpTypes.COUNT ? Long.valueOf(0)
                                           : null;
            ...disabled 2 lines */
            // End of VoltDB extension
        }

        return ((SetFunction) currValue).getValue(session);
    }

    public Expression getCondition() {
        return nodes[RIGHT];
    }

    public boolean hasCondition() {
        return !nodes[RIGHT].isTrue();
    }

    public void setCondition(Expression e) {
        nodes[RIGHT] = e;
    }

    // A VoltDB Extension to print HSQLDB ASTs.
    protected String voltDescribe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);
        switch (opType) {

            case OpTypes.COUNT :
                sb.append(Tokens.T_COUNT);
                break;

            case OpTypes.SUM :
                sb.append(Tokens.T_SUM);
                break;

            case OpTypes.MIN :
                sb.append(Tokens.T_MIN);
                break;

            case OpTypes.MAX :
                sb.append(Tokens.T_MAX);
                break;

            case OpTypes.AVG :
                sb.append(Tokens.T_AVG);
                break;

            case OpTypes.EVERY :
                sb.append(Tokens.T_EVERY);
                break;

            case OpTypes.SOME :
                sb.append(Tokens.T_SOME);
                break;

            case OpTypes.STDDEV_POP :
                sb.append(Tokens.T_STDDEV_POP);
                break;

            case OpTypes.STDDEV_SAMP :
                sb.append(Tokens.T_STDDEV_SAMP);
                break;

            case OpTypes.VAR_POP :
                sb.append(Tokens.T_VAR_POP);
                break;

            case OpTypes.VAR_SAMP :
                sb.append(Tokens.T_VAR_SAMP);
                break;

            case OpTypes.SIMPLE_COLUMN:
                sb.append("SIMPLE_COLUMN (Not an aggregate, eh?)");
                break;

            default:
                sb.append(String.format("Unknown operator: %d", opType));
                break;
        }

        voltDescribeArgs(session, blanks + 2, sb);
        return sb.toString();
    }

    // End of VoltDB Extension
}
