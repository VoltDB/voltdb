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

import java.util.List;

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.types.Type;


/**
 * Implementation of RANK operations
 *
 * @author Xin Jia
 */
public class ExpressionRank extends Expression {
    private List<Expression> m_partitionByList;
    private SortAndSlice m_sortAndSlice;

    ExpressionRank(SortAndSlice sortAndSlice, List<Expression> partitionByList, boolean isPercent) {
        super(OpTypes.RANK);
        nodes = Expression.emptyExpressionArray;
        m_partitionByList = partitionByList;
        m_sortAndSlice = sortAndSlice;
    }

    @Override
    public Object getValue(Session session) {
        return 0;
    }

    /**
     * Returns the data type
     */
    @Override
    Type getDataType() {
        return Type.SQL_BIGINT;
    }

    @Override
    public HsqlList resolveColumnReferences(RangeVariable[] rangeVarArray,
            int rangeCount, HsqlList unresolvedSet, boolean acceptsSequences) {
        HsqlList localSet = null;
        for (Expression e : m_partitionByList) {
            localSet = e.resolveColumnReferences(
                    RangeVariable.emptyArray, localSet);
        }

        for (int i = 0; i < m_sortAndSlice.exprList.size(); i++) {
            Expression e = (Expression) m_sortAndSlice.exprList.get(i);
            assert(e instanceof ExpressionOrderBy);
            ExpressionOrderBy expr = (ExpressionOrderBy)e;
            localSet = expr.resolveColumnReferences(
                    RangeVariable.emptyArray, localSet);
        }

        if (localSet != null) {
            isCorrelated = true;
            for (int i = 0; i < localSet.size(); i++) {
                Expression e = (Expression) localSet.get(i);
                unresolvedSet = e.resolveColumnReferences(rangeVarArray,
                        unresolvedSet);
            }
            unresolvedSet = Expression.resolveColumnSet(rangeVarArray,
                    localSet, unresolvedSet);
        }

        return unresolvedSet;
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {
        for (Expression expr : m_partitionByList) {
            expr.resolveTypes(session, parent);
        }
        for (int i = 0; i < m_sortAndSlice.exprList.size(); i++) {
            Expression e = (Expression) m_sortAndSlice.exprList.get(i);
            e.resolveTypes(session, parent);
        }
        dataType = Type.SQL_BIGINT;
    }

    @Override
    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_RANK).append("()").append(Tokens.T_ORDER + ' ' + Tokens.T_BY).append(' ');

        // Todo
        return sb.toString();
    }

    @Override
    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer();

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        sb.append(Tokens.T_ORDER).append(' ').append(Tokens.T_BY);
        sb.append(' ');

        return sb.toString();
    }

    public VoltXMLElement voltAnnotateRankXML(VoltXMLElement exp, SimpleColumnContext context)
            throws HSQLParseException {
    	if (m_partitionByList.size() > 0) {
            VoltXMLElement pxe = new VoltXMLElement("partitionbyList");
            exp.children.add(pxe);
            for (Expression e : m_partitionByList) {
                pxe.children.add(e.voltGetXML(context, null));
            }
        }

        VoltXMLElement rxe = new VoltXMLElement("orderbyList");
        exp.children.add(rxe);

        for (int i = 0; i < m_sortAndSlice.exprList.size(); i++) {
            Expression e = (Expression) m_sortAndSlice.exprList.get(i);
            assert(e instanceof ExpressionOrderBy);
            ExpressionOrderBy expr = (ExpressionOrderBy)e;
            VoltXMLElement orderby = expr.voltGetXML(context, null);
            boolean isDecending = expr.isDescending();
            orderby.attributes.put("decending", isDecending ? "true": "false");
            rxe.children.add(orderby);
        }

        return exp;
    }

}
