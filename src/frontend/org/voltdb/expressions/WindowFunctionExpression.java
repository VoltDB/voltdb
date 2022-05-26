/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.expressions;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.types.ExpressionType;
import org.voltdb.types.SortDirectionType;

/**
 * Objects of this class represent windowed expressions in the Volt
 * AST.  We currently implement only the RANK function, and only the
 * slow path.  This class for the slow path is just a container with
 * three pieces.
 * <ol>
 *   <li>A sequence of partition by expressions.</li>
 *   <li>A sequence of pairs of order by expressions and sort directions.</li>
 *   <li> An aggregate operation.
 * </ol>
 * The aggregate operation is sorted in the AbstractExpression base class.
 *
 * There will be a fast path which will need an index and a single
 * table, as well as perhaps some other metadata.  But we currently just
 * implement the slow path.
 */
public class WindowFunctionExpression extends AbstractExpression {
    public enum Members {
        PARTITION_BY_EXPRESSIONS
    }

    private List<AbstractExpression> m_partitionByExpressions = new ArrayList<>();
    private List<AbstractExpression> m_orderByExpressions     = new ArrayList<>();
    private List<SortDirectionType>  m_orderByDirections      = new ArrayList<>();

    // This object is not in the display list.  It's squirreled away in the ParsedSelectStatment.  But
    // the display list has a TVE which references the column which holds the values this aggregate
    // expression will compute.  This field holds this TVE.
    private TupleValueExpression m_displayListExpression;

    private int m_xmlID = -1;

    private boolean m_isDistinct = false;

    public WindowFunctionExpression() {
        //
        // This is needed for serialization
        //
        super();
    }

    public WindowFunctionExpression(
            ExpressionType operationType,  // RANK, MAX, etc.
            List<AbstractExpression> partitionbyExprs,
            List<AbstractExpression> orderbyExprs,
            List<SortDirectionType>  orderByDirections,
            List<AbstractExpression> aggArguments,
            int                      id) {
        super(operationType);
        m_partitionByExpressions.addAll(partitionbyExprs);
        m_orderByExpressions.addAll(orderbyExprs);
        m_orderByDirections.addAll(orderByDirections);
        if (m_args == null) {
            m_args = new ArrayList<>();
        }
        m_args.addAll(aggArguments);
        finalizeValueTypes();
        m_xmlID = id;
    }


    public int getOrderbySize() {
        return m_orderByExpressions.size();
    }

    public int getPartitionbySize() {
        return m_partitionByExpressions.size();
    }

    public List<AbstractExpression> getPartitionByExpressions() {
        return m_partitionByExpressions;
    }

    public List<AbstractExpression> getOrderByExpressions() {
        return m_orderByExpressions;
    }

    public List<SortDirectionType> getOrderByDirections() {
        return m_orderByDirections;
    }

    public List<AbstractExpression> getAggregateArguments() {
        return m_args;
    }

    public boolean hasSubqueryArgs() {
        return m_args != null && m_args.stream().anyMatch(AbstractExpression::hasSubquerySubexpression);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) && obj instanceof WindowFunctionExpression) {
            WindowFunctionExpression oWindow = (WindowFunctionExpression)obj;
            return m_orderByExpressions.equals(oWindow.getOrderByExpressions())
                    && m_orderByDirections.equals(oWindow.getOrderByDirections())
                    && m_partitionByExpressions.equals(oWindow.getPartitionByExpressions());
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        hash += m_orderByDirections.hashCode();
        hash += m_orderByExpressions.hashCode();
        hash += m_partitionByExpressions.hashCode();
        hash += m_args.hashCode();
        return hash;
    }

    @Override
    public void finalizeValueTypes() {
        // This is a kind of logical aggregate
        // function, even if it is not a subclass
        // of AggregateExpression.
        AggregateExpression.finalizeAggregateValueTypes(this);
    }

    @Override
    public String explain(String impliedTableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("WINDOW expression").append(" expression");
        return sb.toString();
    }

    /*
     * Functions to find subexpressions by class.  We need to search the
     * partition by and order by lists.
     */
    @Override
    public <aeClass> List<aeClass> findAllSubexpressionsOfClass(Class< ? extends AbstractExpression> aeClass) {
        List<aeClass> list = super.findAllSubexpressionsOfClass(aeClass);
        for (AbstractExpression pbexpr : m_partitionByExpressions) {
            list.addAll(pbexpr.findAllSubexpressionsOfClass(aeClass));
        }
        for (AbstractExpression sortExpr : m_orderByExpressions) {
            list.addAll(sortExpr.findAllSubexpressionsOfClass(aeClass));
        }
        for (AbstractExpression aggExpr : m_args) {
            list.addAll(aggExpr.findAllSubexpressionsOfClass(aeClass));
        }
        return list;
    }

    @Override
    public boolean hasAnySubexpressionOfClass(Class< ? extends AbstractExpression> aeClass) {
        if (super.hasAnySubexpressionOfClass(aeClass)) {
            return true;
        }
        return m_partitionByExpressions.stream().anyMatch(e -> e.hasAnySubexpressionOfClass(aeClass)) ||
                m_orderByExpressions.stream().anyMatch(e -> e.hasAnySubexpressionOfClass(aeClass)) ||
                m_args.stream().anyMatch(e -> e.hasAnySubexpressionOfClass(aeClass));
    }

    /**
     * Return the index of the given partition by expression in the
     * order by list.  This is used when trying to rationalize partition by
     * and order by expressions.
     *
     * @param partitionByExpression
     * @return
     */
    public int getSortIndexOfOrderByExpression(AbstractExpression partitionByExpression) {
        for (int idx = 0; idx < m_orderByExpressions.size(); ++idx) {
            if (m_orderByExpressions.get(idx).equals(partitionByExpression)) {
                return idx;
            }
        }
        return -1;
    }

    public final TupleValueExpression getDisplayListExpression() {
        return m_displayListExpression;
    }

    public final void setDisplayListExpression(TupleValueExpression displayListExpression) {
        m_displayListExpression = displayListExpression;
    }

    /**
     * When a VoltXMLElement is translated to an expression, we remember the
     * ID.  We may see it again in the order by expression.  This gets the ID number.
     * @return
     */
    public final int getXMLID() {
        return m_xmlID;
    }

    public boolean getIsDistinct() {
        // TODO Auto-generated method stub
        return m_isDistinct;
    }
}

