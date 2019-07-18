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

import java.util.Objects;

import org.hsqldb_voltpatches.FunctionForVoltDB.FunctionDescriptor;
import org.hsqldb_voltpatches.lib.ArrayListIdentity;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.store.ValuePool;

/**
 * Implementation of aggregate operations
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class ExpressionAggregate extends Expression {

    boolean isDistinctAggregate;
    // if it's a user-defined aggregate function, we will give it an id and a name
    private int m_userAggregateId;
    private String m_name;


    ExpressionAggregate(int type, boolean distinct, Expression e) {

        super(type);

        nodes               = new Expression[UNARY];
        isDistinctAggregate = distinct;
        nodes[LEFT]         = e;
    }

    ExpressionAggregate(ExpressionAggregate e) {

        super(e.opType);

        isDistinctAggregate = e.isDistinctAggregate;
        nodes               = e.nodes;
        m_userAggregateId   = e.m_userAggregateId;
        m_name				= e.m_name;
    }

    ExpressionAggregate(int type, boolean distinct, Expression e, int id, String n) {

        super(type);

        nodes               = new Expression[UNARY];
        isDistinctAggregate = distinct;
        nodes[LEFT]         = e;
        m_userAggregateId   = id;
        m_name                = n;
    }

    @Override
    boolean isSelfAggregate() {
        return true;
    }

    @Override
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
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }

        return sb.toString();
    }

    @Override
    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        switch (opType) {

            case OpTypes.COUNT :
                sb.append("COUNT ");
                break;

            // A VoltDB extension APPROX_COUNT_DISTINCT
            case OpTypes.APPROX_COUNT_DISTINCT :
                sb.append("APPROX_COUNT_DISTINCT ");
                break;
            // End of VoltDB extension
            case OpTypes.SUM :
                sb.append("SUM ");
                break;

            case OpTypes.MIN :
                sb.append("MIN ");
                break;

            case OpTypes.MAX :
                sb.append("MAX ");
                break;

            case OpTypes.AVG :
                sb.append("AVG ");
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

        if (nodes.length > LEFT && nodes[LEFT] != null) {
            sb.append(" arg1=[");
            sb.append(nodes[LEFT].describe(session, blanks + 1));
            sb.append(']');
        }

        return sb.toString();
    }

    @Override
    public HsqlList resolveColumnReferences(RangeVariable[] rangeVarArray,
            int rangeCount, HsqlList unresolvedSet, boolean acceptsSequences) {

        if (unresolvedSet == null) {
            unresolvedSet = new ArrayListIdentity();
        }

        unresolvedSet.add(this);

        return unresolvedSet;
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        if (nodes[LEFT].isParam) {
            throw Error.error(ErrorCode.X_42567);
        }

        if (opType == OpTypes.USER_DEFINED_AGGREGATE) {
            dataType = FunctionDescriptor.fn_by_name(m_name).getDataType();
        } else {
            dataType = SetFunction.getType(opType, nodes[LEFT].dataType);
        }
    }

    @Override
    public int hashCode() {
        return (super.hashCode() * 31 + Objects.hashCode(isDistinctAggregate)) * 1048573 + m_userAggregateId;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!super.equals(other)) {
            return false;
        }
        if (other instanceof ExpressionAggregate) {
            return isDistinctAggregate ==
                    ((ExpressionAggregate) other).isDistinctAggregate;
        }
        return false;
    }

    public Object updateAggregatingValue(Session session, Object currValue) {

        if (currValue == null) {
            currValue = new SetFunction(opType, nodes[LEFT].dataType,
                                        isDistinctAggregate);
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
                    ValuePool.INTEGER_0: null;
            /* disable 2 lines...
            return opType == OpTypes.COUNT ? ValuePool.INTEGER_0
                                           : null;
            ...disabled 2 lines */
            // End of VoltDB extension
        }

        return ((SetFunction) currValue).getValue();
    }

    public int getUserAggregateId() {
        return m_userAggregateId;
    }

    public String getName() {
        return m_name;
    }
}
