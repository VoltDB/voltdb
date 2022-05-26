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

package org.voltdb.plannodes;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.exceptions.ValidationError;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;

public class LimitPlanNode extends AbstractPlanNode {

    public enum Members {
        OFFSET,
        LIMIT,
        OFFSET_PARAM_IDX,
        LIMIT_PARAM_IDX,
        LIMIT_EXPRESSION;
    }

    protected int m_offset = 0;
    protected int m_limit = -1;

    // -1 also interpreted by EE as uninitialized
    private long m_limitParameterId = -1;
    private long m_offsetParameterId = -1;

    private AbstractExpression m_limitExpression = null;

    public LimitPlanNode() {
        super();
    }

    public LimitPlanNode(LimitPlanNode limit) {
        super();
        m_offset = limit.getOffset();
        m_limit = limit.getLimit();
        m_limitParameterId = limit.m_limitParameterId;
        m_offsetParameterId = limit.m_offsetParameterId;
        if (limit.getLimitExpression() != null) {
            m_limitExpression = limit.getLimitExpression().clone();
        }
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.LIMIT;
    }

    @Override
    public void validate() {
        super.validate();
        if (m_offset < 0) {
            throw new ValidationError("The offset amount  is negative [%d]", m_offset);
        }
        if (m_limitExpression != null) {
            m_limitExpression.validate();
        }
    }

    /**
     * @return the limit
     */
    public int getLimit() {
        return m_limit;
    }
    /**
     * @param limit the limit to set
     */
    public void setLimit(int limit) {
        m_limit = limit;
    }
    /**
     * @return the offset
     */
    public int getOffset() {
        return m_offset;
    }
    /**
     * @param offset the offset to set
     */
    public void setOffset(int offset) {
        m_offset = offset;
    }

    public boolean hasOffset() {
        return m_offsetParameterId != -1 || m_offset != 0;
    }

    public AbstractExpression getLimitExpression() {
        return m_limitExpression;
    }

    public void setLimitExpression(AbstractExpression expr) {
        m_limitExpression = expr;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.OFFSET.name(), m_offset);
        stringer.keySymbolValuePair(Members.LIMIT.name(), m_limit);
        stringer.keySymbolValuePair(Members.OFFSET_PARAM_IDX.name(), m_offsetParameterId);
        stringer.keySymbolValuePair(Members.LIMIT_PARAM_IDX.name(), m_limitParameterId);
        stringer.key(Members.LIMIT_EXPRESSION.name()).value(m_limitExpression);
    }

    public void setLimitParameterIndex(long limitParameterId) {
        m_limitParameterId = limitParameterId;
    }

    public long getLimitParameterIndex() {
        return m_limitParameterId;
    }

    public void setOffsetParameterIndex(long offsetParameterId) {
        m_offsetParameterId = offsetParameterId;
    }

    public long getOffsetParameterIndex() {
        return m_offsetParameterId;
    }

    @Override
    public void resolveColumnIndexes() {
        // Need to order and resolve indexes of output columns
        assert(m_children.size() == 1);
        AbstractPlanNode childNode = m_children.get(0);
        childNode.resolveColumnIndexes();
        NodeSchema inputSchema = childNode.getOutputSchema();
        for (SchemaColumn col : m_outputSchema) {
            AbstractExpression colExpr = col.getExpression();
            // At this point, they'd better all be TVEs.
            assert(colExpr instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) colExpr;
            tve.setColumnIndexUsingSchema(inputSchema);
        }
        m_outputSchema.sortByTveIndex();
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        m_offset = jobj.getInt( Members.OFFSET.name() );
        m_limit = jobj.getInt( Members.LIMIT.name() );
        m_limitParameterId = jobj.getLong( Members.LIMIT_PARAM_IDX.name() );
        m_offsetParameterId = jobj.getLong( Members.OFFSET_PARAM_IDX.name() );
        m_limitExpression = AbstractExpression.fromJSONChild(jobj, Members.LIMIT_EXPRESSION.name());
    }

    @Override
    protected String explainPlanForNode(String indent) {
        String retval = "";
        if (m_limit >= 0)
            retval += "LIMIT " + String.valueOf(m_limit) + " ";
        if (m_offset > 0)
            retval += "OFFSET " + String.valueOf(m_offset) + " ";
        if (retval.length() > 0) {
            // remove the last space
            return retval.substring(0, retval.length() - 1);
        }
        else {
            return "LIMIT with parameter";
        }
    }
}
