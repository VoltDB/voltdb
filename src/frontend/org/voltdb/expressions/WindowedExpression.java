/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.util.Collection;
import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
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
 * The aggregate operation is sotred in the AbstractExpression base class.
 *
 * There will be a fast path which will need an index and a single
 * table, as well as perhaps some other metadata.  But we currently just
 * implement the slow path.
 */
public class WindowedExpression extends AbstractExpression {
    public enum Members {
        PARTITION_BY_EXPRESSIONS
    }

    public static SortDirectionType DEFAULT_ORDER_BY_DIRECTION = SortDirectionType.ASC;

    private List<AbstractExpression> m_partitionByExpressions = new ArrayList<>();
    private List<AbstractExpression> m_orderByExpressions = new ArrayList<>();
    private List<SortDirectionType>  m_orderByDirections = new ArrayList<>();

    public WindowedExpression() {
        //
        // This is needed for serialization
        //
        super();
    }

    public WindowedExpression(
            ExpressionType operationType,  // RANK, MAX, etc.
            List<AbstractExpression> partitionbyExprs,
            List<AbstractExpression> orderbyExprs,
            List<SortDirectionType>  orderByDirections,
            Database db, boolean isDecending, boolean isPercentRank)
    {
        super(operationType);
        m_partitionByExpressions.addAll(partitionbyExprs);
        m_orderByExpressions.addAll(orderbyExprs);
        m_orderByDirections.addAll(orderByDirections);

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

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) && obj instanceof WindowedExpression) {
            WindowedExpression oWindow = (WindowedExpression)obj;
            if (m_orderByExpressions.equals(oWindow.getOrderByExpressions())
                    && m_orderByDirections.equals(oWindow.getOrderByDirections())
                    && m_partitionByExpressions.equals(oWindow.getPartitionByExpressions())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        hash += m_orderByDirections.hashCode();
        hash += m_orderByExpressions.hashCode();
        hash += m_partitionByExpressions.hashCode();
        return hash;
    }

    @Override
    public Object clone() {
        WindowedExpression clone = (WindowedExpression) super.clone();
        clone.getOrderByDirections().addAll(m_orderByDirections);
        clone.getOrderByExpressions().addAll(copyOrderByExpressions());
        clone.getPartitionByExpressions().addAll(copyPartitionByExpressions());
        return clone;
    }

    private Collection<? extends AbstractExpression> copyPartitionByExpressions() {
        List<AbstractExpression> copy = new ArrayList<AbstractExpression>();
        for (AbstractExpression ae : m_partitionByExpressions) {
            copy.add((AbstractExpression)ae.clone());
        }
        return copy;
    }

    private Collection<? extends AbstractExpression> copyOrderByExpressions() {
        List<AbstractExpression> copy = new ArrayList<AbstractExpression>();
        for (AbstractExpression ae : m_orderByExpressions) {
            copy.add((AbstractExpression)ae.clone());
        }
        return copy;
    }

    @Override
    protected void loadFromJSONObject(JSONObject jobj) throws JSONException {
        super.loadFromJSONObject(jobj);
        m_partitionByExpressions.clear();

        /*
         * Load the array of partition expressions.  The AbstractExpression class
         * has a useful routine to do just this for us.
         */
        AbstractExpression.loadFromJSONArrayChild(m_partitionByExpressions, jobj, Members.PARTITION_BY_EXPRESSIONS.name(), null);

        /*
         * Unfortunately we cannot use AbstractExpression.loadFromJSONArrayChild here,
         * as we need to get a sort expression and a sort order for each column.
         */
        AbstractExpression.loadSortListFromJSONArray(m_orderByExpressions, m_orderByDirections, jobj);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        assert (m_orderByExpressions.size() == m_orderByDirections.size());
        /*
         * Serialize the sort columns.
         */
        AbstractExpression.toJSONArrayFromSortList(stringer,
                                                   m_orderByExpressions,
                                                   m_orderByDirections);

        /*
         * Serialize the partition expressions.
         */
        stringer.key(Members.PARTITION_BY_EXPRESSIONS.name()).array();
        for (int idx = 0; idx < m_partitionByExpressions.size(); idx += 1) {
            stringer.object();
            m_partitionByExpressions.get(idx).toJSONString(stringer);
            stringer.endObject();
        }
        stringer.endArray();
    }

    @Override
    public void finalizeValueTypes() {
        m_valueType = VoltType.BIGINT;
        m_valueSize = VoltType.BIGINT.getLengthInBytesForFixedTypes();
    }

    @Override
    public String explain(String impliedTableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("WINDOW expression").append(" expression");
        return sb.toString();
    }

    /*
     * Functions to find subexpressions by type.
     */
    @Override
    public ArrayList<AbstractExpression> findAllSubexpressionsOfType(ExpressionType type) {
        ArrayList<AbstractExpression> list = super.findAllSubexpressionsOfType(type);
        for (AbstractExpression pbexpr : m_partitionByExpressions) {
            list.addAll(pbexpr.findAllSubexpressionsOfType(type));
        }
        for (AbstractExpression sortExpr : m_orderByExpressions) {
            list.addAll(sortExpr.findAllSubexpressionsOfType(type));
        }
        return list;
    }

    @Override
    public boolean hasAnySubexpressionOfType(ExpressionType type) {
        if (super.hasAnySubexpressionOfType(type)) {
            return true;
        }
        for (AbstractExpression pbExpr : m_partitionByExpressions) {
            if (pbExpr.hasAnySubexpressionOfType(type)) {
                return true;
            }
        }
        for (AbstractExpression sortExpr : m_orderByExpressions) {
            if (sortExpr.hasAnySubexpressionOfType(type)) {
                return true;
            }
        }
        return false;
    }

    /*
     * Functions to find subexpressions by class.  We need to search the
     * partition by and order by lists.
     */
    @Override
    public ArrayList<AbstractExpression> findAllSubexpressionsOfClass(Class< ? extends AbstractExpression> aeClass) {
        ArrayList<AbstractExpression> list = super.findAllSubexpressionsOfClass(aeClass);
        for (AbstractExpression pbexpr : m_partitionByExpressions) {
            list.addAll(pbexpr.findAllSubexpressionsOfClass(aeClass));
        }
        for (AbstractExpression sortExpr : m_orderByExpressions) {
            list.addAll(sortExpr.findAllSubexpressionsOfClass(aeClass));
        }
        return list;
    }

    @Override
    public boolean hasAnySubexpressionOfClass(Class< ? extends AbstractExpression> aeClass) {
        if (super.hasAnySubexpressionOfClass(aeClass)) {
            return true;
        }
        for (AbstractExpression pbexpr : m_partitionByExpressions) {
            if (pbexpr.hasAnySubexpressionOfClass(aeClass)) {
                return true;
            }
        }
        for (AbstractExpression sortExpr : m_orderByExpressions) {
            if (sortExpr.hasAnySubexpressionOfClass(aeClass)) {
                return true;
            }
        }
        return false;
    }

    /**
     *
     * Given an expression, E, in the partition by list, if E is in the
     * order by list, then return the order direction of E in that list.
     * Otherwise, return the default value.
     *
     * There's got to be a better way to do this.
     *
     * @param partitionByExpr
     * @return
     */
    public SortDirectionType getOrderByDirectionOfExpression(AbstractExpression partitionByExpr) {
        for (int idx = 0; idx < m_orderByExpressions.size(); idx += 1) {
            if (m_orderByExpressions.get(idx).equals(partitionByExpr)) {
                return m_orderByDirections.get(idx);
            }
        }
        return DEFAULT_ORDER_BY_DIRECTION;
    }
}

