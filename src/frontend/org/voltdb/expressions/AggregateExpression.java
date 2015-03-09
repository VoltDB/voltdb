/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

public class AggregateExpression extends AbstractExpression {

    /** True if this aggregate requires distinct: e.g. count(distinct A) */
    private boolean m_distinct = false;

    public AggregateExpression(ExpressionType type) {
        super(type);
    }

    public AggregateExpression() {
        //
        // This is needed for serialization
        //
        super();
    }

    public void setDistinct() { m_distinct = true; }
    public boolean isDistinct() { return m_distinct;  }

    @Override
    public boolean equals(Object obj) {
        if(super.equals(obj) == false) return false;

        if (obj instanceof AggregateExpression == false) return false;
        AggregateExpression expr = (AggregateExpression) obj;
        if (m_distinct != expr.isDistinct()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        // based on implementation of equals
        int result = super.hashCode();
        result += new HashCodeBuilder(17, 31).append(m_distinct).toHashCode();
        return result;
    }


    @Override
    public void finalizeValueTypes()
    {
        finalizeChildValueTypes();
        ExpressionType type = getExpressionType();
        switch (type) {
        case AGGREGATE_COUNT:
        case AGGREGATE_COUNT_STAR:
            //
            // Always an integer
            //
            m_valueType = VoltType.BIGINT;
            m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
            break;
        case AGGREGATE_AVG:
        case AGGREGATE_MAX:
        case AGGREGATE_MIN:
            //
            // It's always whatever the base type is
            //
            m_valueType = m_left.getValueType();
            m_valueSize = m_left.getValueSize();
            break;
        case AGGREGATE_SUM:
            if (m_left.getValueType() == VoltType.TINYINT ||
                m_left.getValueType() == VoltType.SMALLINT ||
                m_left.getValueType() == VoltType.INTEGER) {
                m_valueType = VoltType.BIGINT;
                m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
            } else {
                m_valueType = m_left.getValueType();
                m_valueSize = m_left.getValueSize();
            }
            break;
        default:
            throw new RuntimeException("ERROR: Invalid Expression type '" + type + "' for Expression '" + this + "'");
        }
    }

    @Override
    public String explain(String impliedTableName) {
        ExpressionType type = getExpressionType();
        if (type == ExpressionType.AGGREGATE_COUNT_STAR) {
            return "COUNT(*)";
        }
        return type.symbol() + ( m_distinct ? " DISTINCT(" : "(" ) +
            m_left.explain(impliedTableName) + ")";
    }

}
