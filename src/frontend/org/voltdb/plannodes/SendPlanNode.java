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
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;

public class SendPlanNode extends AbstractPlanNode {

    public SendPlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.SEND;
    }

    @Override
    public void resolveColumnIndexes() {
        // Need to order and resolve indexes of output columns
        assert(m_children.size() == 1);
        AbstractPlanNode childNode = m_children.get(0);
        childNode.resolveColumnIndexes();
        NodeSchema inputSchema = childNode.getOutputSchema();
        assert(inputSchema.equalsOnlyNames(m_outputSchema));
        for (SchemaColumn col : m_outputSchema) {
            AbstractExpression colExpr = col.getExpression();
            // At this point, they'd better all be TVEs.
            assert(colExpr instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) colExpr;
            tve.setColumnIndexUsingSchema(inputSchema);
        }
        // output schema for SendPlanNode should not ever be changed
    }


    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate,
            DatabaseEstimates estimates,
            ScalarValueHints[] paramHints) {
        assert(estimates != null);
        // Recursively compute and collect stats from the child node,
        // but don't add any costs for this Send node.
        // Let the parent "RecievePlanNode" account for any (theoretical)
        // cost differences due to how much data
        // different plans must distribute as intermediate results.
        // Don't bother accounting for how much data is sent as the FINAL
        // result -- when there is no ReceivePlanNode --
        // that cost is constant for a given query, so it would be a wash
        // when comparing any two plans for that query.
        m_estimatedOutputTupleCount = childOutputTupleCountEstimate;
        m_estimatedProcessedTupleCount = 0;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
    }

    @Override
    protected String explainPlanForNode(String indent) {
        if (m_parents.size() == 0)
            return "RETURN RESULTS TO STORED PROCEDURE";
        else
            return "SEND PARTITION RESULTS TO COORDINATOR";
    }

    @Override
    public void loadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
    }

    @Override
    public String getUpdatedTable() {
        assert(m_children.size() == 1);
        AbstractPlanNode child = m_children.get(0);
        return child.getUpdatedTable();
    }

}
