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

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

public class AggregateExpression extends AbstractExpression {

    /** True if this aggregate requires distinct: e.g. count(distinct A) */
    private boolean m_distinct = false;
    protected final int m_userAggregateId;
    protected final String m_name;

    public AggregateExpression(ExpressionType type) {
        super(type);
        m_userAggregateId = -1;
        m_name = "";
    }

    public AggregateExpression(ExpressionType type, int id, String n) {
        super(type);
        m_userAggregateId = id;
        m_name = n;
    }

    public void setDistinct() { m_distinct = true; }
    public boolean isDistinct() { return m_distinct;  }

    /**
     * @return user aggregate id
     */
    public int getUserAggregateId() {
        return m_userAggregateId;
    }

    @Override
    public boolean equals(Object obj) {
        if(!super.equals(obj)) return false;

        if (!(obj instanceof AggregateExpression)) return false;
        AggregateExpression expr = (AggregateExpression) obj;
        return m_distinct == expr.isDistinct();
    }

    @Override
    public int hashCode() {
        // based on implementation of equals
        int result = super.hashCode();
        result += new HashCodeBuilder(17, 31).append(m_distinct).toHashCode();
        return result;
    }

    private static final String FLOAT_AGG_ERR_MSG =
            "Aggregate functions of floating point columns may not be deterministic.  We suggest converting to DECIMAL.";

    @Override
    public void finalizeValueTypes()
    {
        finalizeAggregateValueTypes(this);
    }

    public static void finalizeAggregateValueTypes(AbstractExpression expr) {
        expr.finalizeChildValueTypes();
        ExpressionType type = expr.getExpressionType();
        AbstractExpression aggArg;

        switch (type) {
            case AGGREGATE_COUNT:
            case AGGREGATE_WINDOWED_RANK:
            case AGGREGATE_WINDOWED_DENSE_RANK:
            case AGGREGATE_WINDOWED_ROW_NUMBER:
            case AGGREGATE_WINDOWED_COUNT:
            case AGGREGATE_COUNT_STAR:
            case AGGREGATE_APPROX_COUNT_DISTINCT:
            case AGGREGATE_HYPERLOGLOGS_TO_CARD:
                //
                // Always an integer
                //
                expr.m_valueType = VoltType.BIGINT;
                expr.m_valueSize = expr.m_valueType.getLengthInBytesForFixedTypes();
                break;
            case AGGREGATE_VALS_TO_HYPERLOGLOG:
                expr.m_valueType = VoltType.VARBINARY;
                expr.m_valueSize = 65537;
                break;
            case AGGREGATE_AVG:
            case AGGREGATE_MAX:
            case AGGREGATE_MIN:
            case AGGREGATE_WINDOWED_MIN:
            case AGGREGATE_WINDOWED_MAX:
            case USER_DEFINED_AGGREGATE:
                //
                // It's always whatever the base type is
                //
                aggArg = expr.getFirstArgument();
                assert(aggArg != null);
                expr.m_valueType = aggArg.getValueType();
                expr.m_valueSize = aggArg.getValueSize();
                expr.m_inBytes = aggArg.getInBytes();
                // Of these aggregate functions, only AVG is
                // non-deterministic on floating point types.
                if (expr.m_valueType == VoltType.FLOAT && type == ExpressionType.AGGREGATE_AVG) {
                    expr.updateContentDeterminismMessage(FLOAT_AGG_ERR_MSG);
                }
                break;
            case AGGREGATE_WINDOWED_SUM:
            case AGGREGATE_SUM:
                aggArg = expr.getFirstArgument();
                assert(aggArg != null);
                if (aggArg.getValueType() == VoltType.TINYINT ||
                        aggArg.getValueType() == VoltType.SMALLINT ||
                        aggArg.getValueType() == VoltType.INTEGER) {
                    expr.m_valueType = VoltType.BIGINT;
                    expr.m_valueSize = expr.m_valueType.getLengthInBytesForFixedTypes();
                } else {
                    expr.m_valueType = aggArg.getValueType();
                    expr.m_valueSize = aggArg.getValueSize();
                }
                if (expr.m_valueType == VoltType.FLOAT) {
                    expr.updateContentDeterminismMessage(FLOAT_AGG_ERR_MSG);
                }
                break;
            default:
                throw new RuntimeException("ERROR: Invalid Expression type '" + type + "' for Expression '" + expr + "'");
        }
    }

    @Override
    public String explain(String impliedTableName) {
        ExpressionType type = getExpressionType();
        if (type == ExpressionType.AGGREGATE_COUNT_STAR) {
            return "COUNT(*)";
        } else {
            return type.symbol() + (m_distinct ? " DISTINCT(" : "(") +
                    m_left.explain(impliedTableName) + ")";
        }
    }

    public boolean isUserDefined() {
        return FunctionForVoltDB.isUserDefinedFunctionId(m_userAggregateId);
    }

    public String getFunctionName() {
        return m_name;
    }
}
