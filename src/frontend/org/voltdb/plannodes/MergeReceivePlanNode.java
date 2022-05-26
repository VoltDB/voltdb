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

import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class MergeReceivePlanNode extends AbstractReceivePlanNode {

    public enum Members {
        OUTPUT_SCHEMA_PRE_AGG;
    }

    // Output schema before the inline aggregate node
    private NodeSchema m_outputSchemaPreInlineAgg = null;

    public MergeReceivePlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.MERGERECEIVE;
    }

    @Override
    public void generateOutputSchema(Database db) {
        assert(m_children.size() == 1);
        m_children.get(0).generateOutputSchema(db);
        m_outputSchemaPreInlineAgg =
            m_children.get(0).getOutputSchema().copyAndReplaceWithTVE();

        AbstractPlanNode aggrNode = AggregatePlanNode.getInlineAggregationNode(this);
        if (aggrNode != null) {
            aggrNode.generateOutputSchema(db);
            m_outputSchema = aggrNode.getOutputSchema().copyAndReplaceWithTVE();
        } else {
            m_outputSchema = m_outputSchemaPreInlineAgg;
        }

        // except, while technically the resulting output schema is just a pass-through,
        // when the plan gets fragmented, this receive node will be at the bottom of the
        // fragment and will need its own serialized copy of its (former) child's output schema.
        m_hasSignificantOutputSchema = true;
    }

    @Override
    public void resolveColumnIndexes() {
        resolveColumnIndexes(m_outputSchemaPreInlineAgg);

        AbstractPlanNode orderNode = getInlinePlanNode(PlanNodeType.ORDERBY);
        assert(orderNode instanceof OrderByPlanNode);
        OrderByPlanNode opn = (OrderByPlanNode) orderNode;
        opn.resolveSortIndexesUsingSchema(m_outputSchemaPreInlineAgg);

        AggregatePlanNode aggrNode = AggregatePlanNode.getInlineAggregationNode(this);
        if (aggrNode != null) {
            aggrNode.resolveColumnIndexesUsingSchema(m_outputSchemaPreInlineAgg);
            m_outputSchema = aggrNode.getOutputSchema().clone();
            m_outputSchema.sortByTveIndex();
        }
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        if (m_outputSchemaPreInlineAgg != m_outputSchema) {
            stringer.key(Members.OUTPUT_SCHEMA_PRE_AGG.name());
            stringer.array();
            for (int colNo = 0; colNo < m_outputSchemaPreInlineAgg.size(); colNo += 1) {
                SchemaColumn column = m_outputSchemaPreInlineAgg.getColumn(colNo);
                column.toJSONString(stringer, true, colNo);
            }
            stringer.endArray();
        }
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        if (jobj.has(Members.OUTPUT_SCHEMA_PRE_AGG.name())) {
            m_hasSignificantOutputSchema = true;
            String jsonKey = Members.OUTPUT_SCHEMA_PRE_AGG.name();
            m_outputSchemaPreInlineAgg = loadSchemaFromJSONObject(jobj, jsonKey);
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "MERGE RECEIVE FROM ALL PARTITIONS";
    }

    @Override
    public boolean isOutputOrdered (List<AbstractExpression> sortExpressions, List<SortDirectionType> sortDirections) {
        return true;
    }

}
