/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;

public class ReceivePlanNode extends AbstractPlanNode {

    boolean m_isOrderDeterministic = false;
    boolean m_isContentDeterministic = true;
    String m_nondeterminismDetail = "no ordering was asserted for Receive Plan Node";

    public ReceivePlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.RECEIVE;
    }

    @Override
    public void resolveColumnIndexes()
    {
        // Need to order and resolve indexes of output columns
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
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "RECEIVE FROM ALL PARTITIONS";
    }

    /**
     * Accessor for flag marking the plan as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return previously cached value.
     */
    @Override
    public boolean isOrderDeterministic() {
        return m_isOrderDeterministic;
    }

    /**
     * Accessor for flag marking the plan as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return previously cached value.
     */
    @Override
    public boolean isContentDeterministic() {
        return m_isContentDeterministic;
    }

    /**
     * Accessor
     */
    @Override
    public String nondeterminismDetail() { return m_nondeterminismDetail; }

    /**
     * Write accessor for determinism flags and optional description.
     * This must be cached before fragmentation makes the child info harder to reach.
     */
    public void cacheDeterminism() {
        AbstractPlanNode childNode = getChild(0);
        m_isOrderDeterministic = childNode.isOrderDeterministic();
        if (m_isOrderDeterministic) {
            m_nondeterminismDetail = null;
            m_isContentDeterministic = true;
        } else {
            m_nondeterminismDetail = childNode.nondeterminismDetail();
            m_isContentDeterministic = childNode.isContentDeterministic();
        }
    }

    @Override
    public boolean reattachFragment( SendPlanNode child  ) {
        this.addAndLinkChild(child);
        return true;
    }
}
