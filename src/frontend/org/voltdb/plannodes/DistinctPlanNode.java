/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.plannodes;

import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;

public class DistinctPlanNode extends AbstractPlanNode {

    public enum Members {
        DISTINCT_EXPRESSION;
    }

    //
    // TODO: How will this work for multi-column Distincts?
    //
    protected AbstractExpression m_distinctExpression;

    public DistinctPlanNode() {
        super();
    }

    /**
     * Create a DistinctPlanNode that clones the configuration information but
     * is not inserted in the plan graph and has a unique plan node id.
     * @return copy
     */
    public DistinctPlanNode produceCopyForTransformation() {
        DistinctPlanNode copy = new DistinctPlanNode();
        super.produceCopyForTransformation(copy);
        copy.m_distinctExpression = m_distinctExpression;
        return copy;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.DISTINCT;
    }

    /**
     * Set the expression to be distinct'd (verbing nouns weirds language)
     * @param expr
     */
    public void setDistinctExpression(AbstractExpression expr)
    {
        if (expr != null)
        {
            // PlanNodes all need private deep copies of expressions
            // so that the resolveColumnIndexes results
            // don't get bashed by other nodes or subsequent planner runs
            try
            {
                m_distinctExpression = (AbstractExpression) expr.clone();
            }
            catch (CloneNotSupportedException e)
            {
                // This shouldn't ever happen
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    @Override
    public void resolveColumnIndexes()
    {
        // Need to order and resolve indexes of output columns AND
        // the distinct column
        assert(m_children.size() == 1);
        m_children.get(0).resolveColumnIndexes();
        NodeSchema input_schema = m_children.get(0).getOutputSchema();
        for (SchemaColumn col : m_outputSchema.getColumns())
        {
            // At this point, they'd better all be TVEs.
            assert(col.getExpression() instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)col.getExpression();
            int index = input_schema.getIndexOfTve(tve);
            tve.setColumnIndex(index);
        }
        m_outputSchema.sortByTveIndex();

        // Now resolve the indexes in the distinct expression
        List<TupleValueExpression> distinct_tves =
            ExpressionUtil.getTupleValueExpressions(m_distinctExpression);
        for (TupleValueExpression tve : distinct_tves)
        {
            int index = input_schema.getIndexOfTve(tve);
            tve.setColumnIndex(index);
        }
    }

    @Override
    public void validate() throws Exception {
        super.validate();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.DISTINCT_EXPRESSION.name());
        stringer.object();
        m_distinctExpression.toJSONString(stringer);
        stringer.endObject();
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "DISTINCT";
    }
}
