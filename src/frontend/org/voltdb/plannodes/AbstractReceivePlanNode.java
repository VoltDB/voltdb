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

import java.util.Collection;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;

public abstract class AbstractReceivePlanNode extends AbstractPlanNode {

    final static String m_nondeterminismDetail = "multi-fragment plan results can arrive out of order";

    public AbstractReceivePlanNode() {
        super();
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
    public void getTablesAndIndexes(Map<String, StmtTargetTableScan> tablesRead, Collection<String> indexes) {
        // ReceiveNode is a dead end. This method is not intended to cross fragments
        // even within a pre-fragmented plan tree.
    }

    /**
     * Accessor for flag marking the plan as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return previously cached value.
     */
    @Override
    public boolean isOrderDeterministic() {
        return false;
    }

    /**
     * Accessor
     */
    @Override
    public String nondeterminismDetail() { return m_nondeterminismDetail; }

    @Override
    public boolean reattachFragment(AbstractPlanNode child) {
        addAndLinkChild(child);
        return true;
    }

    protected void resolveColumnIndexes(NodeSchema outputSchema) {
        // Need to order and resolve indexes of output columns
        assert(m_children.size() == 1);
        AbstractPlanNode childNode = m_children.get(0);
        childNode.resolveColumnIndexes();
        NodeSchema inputSchema = childNode.getOutputSchema();
        assert (inputSchema.equals(outputSchema));
        for (SchemaColumn col : outputSchema) {
            AbstractExpression colExpr = col.getExpression();
            // At this point, they'd better all be TVEs.
            assert(colExpr instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) colExpr;
            tve.setColumnIndexUsingSchema(inputSchema);
        }
        // output schema for ReceivePlanNode should never be re-sorted
    }

}
