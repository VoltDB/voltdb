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
public class ExpressionWindowed extends Expression {
    private List<Expression> m_partitionByList;
    private SortAndSlice     m_sortAndSlice;
    private boolean          m_isDistinctAggregate;

    ExpressionWindowed(int tokenT,
                       Expression aggExprs[],
                       boolean isDistinct,
                       SortAndSlice sortAndSlice,
                       List<Expression> partitionByList) {
        super(ParserBase.getWindowedExpressionType(tokenT));

        nodes = aggExprs;
        m_isDistinctAggregate = isDistinct;
        m_partitionByList = partitionByList;
        m_sortAndSlice = sortAndSlice;
        validateWindowedSyntax();
    }

    /**
     * Validate that this is a collection of values.
     */
    private void validateWindowedSyntax() {
        // Check that the aggregate is one of the supported ones, and
        // that the number of aggregate parameters is right.
        switch (opType) {
        case OpTypes.WINDOWED_RANK:
        case OpTypes.WINDOWED_DENSE_RANK:
        case OpTypes.WINDOWED_ROW_NUMBER:
            if (nodes.length != 0) {
                throw Error.error("Windowed Aggregate " + OpTypes.aggregateName(opType) + " expects no arguments.", "", 0);
            }
            break;
        case OpTypes.WINDOWED_COUNT:
        case OpTypes.WINDOWED_MIN:
        case OpTypes.WINDOWED_MAX:
        case OpTypes.WINDOWED_SUM:
        	break;
        default:
            throw Error.error("Unsupported window function " + OpTypes.aggregateName(opType), "", 0);
        }
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
        switch (opType) {
        case OpTypes.WINDOWED_RANK:
        case OpTypes.WINDOWED_DENSE_RANK:
        case OpTypes.WINDOWED_ROW_NUMBER:
        case OpTypes.WINDOWED_COUNT:
            return Type.SQL_BIGINT;
        case OpTypes.WINDOWED_MAX:
        case OpTypes.WINDOWED_MIN:
        case OpTypes.WINDOWED_SUM:
        	return dataType;
        default:
            throw Error.error("Unsupported windowed function " + OpTypes.aggregateName(opType), "", 0);
        }
    }

    @Override
    public HsqlList resolveColumnReferences(RangeVariable[] rangeVarArray,
            int rangeCount, HsqlList unresolvedSet, boolean acceptsSequences) {
        HsqlList localSet = null;
        // Resolve the aggregate expression.  For the RANK-like aggregates
        // this is a no-op, because nodes is empty.
        for (Expression e : nodes) {
            localSet = e.resolveColumnReferences(RangeVariable.emptyArray, localSet);
        }
        for (Expression e : m_partitionByList) {
            localSet = e.resolveColumnReferences(
                    RangeVariable.emptyArray, localSet);
        }

        if (m_sortAndSlice != null) {
            for (int i = 0; i < m_sortAndSlice.exprList.size(); i++) {
                Expression e = (Expression) m_sortAndSlice.exprList.get(i);
                assert(e instanceof ExpressionOrderBy);
                ExpressionOrderBy expr = (ExpressionOrderBy)e;
                localSet = expr.resolveColumnReferences(
                                RangeVariable.emptyArray, localSet);
            }
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
        for (Expression expr : nodes) {
            expr.resolveTypes(session, parent);
        }
        for (Expression expr : m_partitionByList) {
            expr.resolveTypes(session, parent);
        }
        if (m_sortAndSlice != null) {
            for (int i = 0; i < m_sortAndSlice.exprList.size(); i++) {
                Expression e = (Expression) m_sortAndSlice.exprList.get(i);
                e.resolveTypes(session, parent);
            }
        }
        dataType = Type.SQL_BIGINT;
    }

    @Override
    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(OpTypes.aggregateName(opType)).append("(");
        for (Expression e : nodes) {
            sb.append(e.getSQL());
        }
        sb.append(") ")
          .append(Tokens.T_OVER + " (");
        if (m_partitionByList.size() > 0) {
            sb.append(Tokens.T_PARTITION + ' ' + Tokens.T_BY + ' ');
            String sep = "";
            for (int idx = 0; idx < m_partitionByList.size(); idx += 1) {
                Expression expr = m_partitionByList.get(idx);
                sb.append(sep)
                  .append(expr.getSQL());
                sep = ", ";
            }
        }
        if (m_sortAndSlice != null && m_sortAndSlice.getOrderLength() > 0) {
            sb.append(Tokens.T_ORDER + ' ' + Tokens.T_BY + ' ');
            for (int idx = 0; idx < m_sortAndSlice.getOrderLength(); idx += 1) {
                Expression obExpr = (Expression) m_sortAndSlice.exprList.get(idx);
                assert(obExpr instanceof ExpressionOrderBy);
                ExpressionOrderBy obOrderByExpression = (ExpressionOrderBy)obExpr;
                sb.append(obExpr.getSQL())
                  .append(' ')
                  .append(obOrderByExpression.isDescending() ? Tokens.T_DESC : Tokens.T_ASC);
            }
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    protected String describe(Session session, int blanks) {
        return getSQL();
    }

    /**
     * Create a VoltXMLElement for a windowed aggregate expression.  The
     * children are parts of the expression.  For example, consider the
     * expression <code>MAX(A+B) OVER (PARTITION BY E1, E2 ORDER BY E3 ASC)</code>.
     * There will be these children.
     * <ul>
     *   <li>A child named "winspec" with the windowed specification.  This
     *       will have two children.
     *       <ul>
     *         <li>One will be named "partitionbyList", and will contain the
     *             expressions E1 and E2.</li>
     *         <li>The other will contain a list of expressions and sort orders
     *             for the order by list, &lt;E3, ASC&gt;.</li>
     *       </ul>
     *    </li>
     *   <li>All other children are the arguments to the aggregate.  This
     *       would be <code>A+B</code> in the expression above.  Note that there are no
     *       arguments to the rank functions, so this will be empty for the rank functions.
     * </ul>
     *
     * @param exp
     * @param context
     * @return
     * @throws HSQLParseException
     */
    public VoltXMLElement voltAnnotateWindowedAggregateXML(VoltXMLElement exp, SimpleColumnContext context)
            throws HSQLParseException {
        VoltXMLElement winspec = new VoltXMLElement("winspec");
        exp.children.add(winspec);
        if (m_partitionByList.size() > 0) {
            VoltXMLElement pxe = new VoltXMLElement("partitionbyList");
            winspec.children.add(pxe);
            for (Expression e : m_partitionByList) {
                pxe.children.add(e.voltGetXML(context, null));
            }
        }

        VoltXMLElement rxe = new VoltXMLElement("orderbyList");
        winspec.children.add(rxe);

        if (m_sortAndSlice != null) {
            for (int i = 0; i < m_sortAndSlice.exprList.size(); i++) {
                Expression e = (Expression) m_sortAndSlice.exprList.get(i);
                assert(e instanceof ExpressionOrderBy);
                ExpressionOrderBy expr = (ExpressionOrderBy)e;
                VoltXMLElement orderby = expr.voltGetXML(context, null);
                boolean isDescending = expr.isDescending();
                orderby.attributes.put("descending", isDescending ? "true": "false");
                rxe.children.add(orderby);
            }
        }
        return exp;
    }

	public boolean isDistinct() {
		return m_isDistinctAggregate;
	}
}
