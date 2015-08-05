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

package org.voltdb.plannodes;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class ReceivePlanNode extends AbstractPlanNode {

    final static String m_nondeterminismDetail = "multi-fragment plan results can arrive out of order";

    public enum Members {
        MERGE_RECEIVE,
        OUTPUT_SCHEMA_PRE_AGG;
    }

    // Indicator to tell whether the Receive node needs to mergesort results from individual partitions
    private boolean m_mergeReceive = false;
    // Output schema before the inline aggregate node
    private NodeSchema m_outputSchemaPreInlineAgg = null;

    public ReceivePlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.RECEIVE;
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        // default behavior: just copy the input schema
        // to the output schema
        assert(m_children.size() == 1);
        m_children.get(0).generateOutputSchema(db);
        m_outputSchemaPreInlineAgg =
            m_children.get(0).getOutputSchema().copyAndReplaceWithTVE();

        if (m_mergeReceive) {
            AbstractPlanNode aggrNode = AggregatePlanNode.getInlineAggregationNode(this);
            if (aggrNode != null) {
                aggrNode.generateOutputSchema(db);
                m_outputSchema = aggrNode.getOutputSchema().copyAndReplaceWithTVE();
            } else {
                m_outputSchema = m_outputSchemaPreInlineAgg;
            }
        } else {
            m_outputSchema = m_outputSchemaPreInlineAgg;
        }

        // except, while technically the resulting output schema is just a pass-through,
        // when the plan gets fragmented, this receive node will be at the bottom of the
        // fragment and will need its own serialized copy of its (former) child's output schema.
        m_hasSignificantOutputSchema = true;
    }

    @Override
    public void resolveColumnIndexes()
    {
        // Need to order and resolve indexes of output columns
        assert(m_children.size() == 1);
        m_children.get(0).resolveColumnIndexes();
        NodeSchema input_schema = m_children.get(0).getOutputSchema();
        assert (input_schema.equals(m_outputSchemaPreInlineAgg));
        for (SchemaColumn col : m_outputSchemaPreInlineAgg.getColumns())
        {
            // At this point, they'd better all be TVEs.
            assert(col.getExpression() instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)col.getExpression();
            int index = tve.resolveColumnIndexesUsingSchema(input_schema);
            tve.setColumnIndex(index);
        }
        m_outputSchemaPreInlineAgg.sortByTveIndex();

        if (m_mergeReceive) {
            AbstractPlanNode orderNode = getInlinePlanNode(PlanNodeType.ORDERBY);
            assert(orderNode != null && orderNode instanceof OrderByPlanNode);
            OrderByPlanNode opn = (OrderByPlanNode) orderNode;
            opn.resolveSortIndexesUsingSchema(m_outputSchemaPreInlineAgg);

            AggregatePlanNode aggrNode = AggregatePlanNode.getInlineAggregationNode(this);
            if (aggrNode != null) {
                aggrNode.resolveColumnIndexesUsingSchema(m_outputSchemaPreInlineAgg);
                m_outputSchema = aggrNode.getOutputSchema().clone();
                m_outputSchema.sortByTveIndex();
            } else {
                m_outputSchema = m_outputSchemaPreInlineAgg;
            }
        } else {
            m_outputSchema = m_outputSchemaPreInlineAgg;
        }
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        if (m_mergeReceive == true) {
            stringer.key(Members.MERGE_RECEIVE.name()).value(m_mergeReceive);
            if (m_outputSchemaPreInlineAgg != m_outputSchema) {
                stringer.key(Members.OUTPUT_SCHEMA_PRE_AGG.name());
                stringer.array();
                for (SchemaColumn column : m_outputSchemaPreInlineAgg.getColumns()) {
                    column.toJSONString(stringer, true);
                }
                stringer.endArray();
            }
        }
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        if (jobj.has(Members.MERGE_RECEIVE.name())) {
            m_mergeReceive = jobj.getBoolean(Members.MERGE_RECEIVE.name());
            if (m_mergeReceive == true) {
                if (jobj.has(Members.OUTPUT_SCHEMA_PRE_AGG.name())) {
                    m_outputSchemaPreInlineAgg = new NodeSchema();
                    m_hasSignificantOutputSchema = true;
                    JSONArray jarray = jobj.getJSONArray( Members.OUTPUT_SCHEMA_PRE_AGG.name() );
                    int size = jarray.length();
                    for( int i = 0; i < size; i++ ) {
                        m_outputSchemaPreInlineAgg.addColumn( SchemaColumn.fromJSONObject(jarray.getJSONObject(i)) );
                    }
                } else {
                    m_outputSchemaPreInlineAgg = m_outputSchema;
                }
            }
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return (m_mergeReceive == true) ? "MERGE RECEIVE FROM ALL PARTITIONS": "RECEIVE FROM ALL PARTITIONS";
    }

    @Override
    public void getTablesAndIndexes(Map<String, StmtTargetTableScan> tablesRead,
            Collection<String> indexes)
    {
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
    public boolean reattachFragment( SendPlanNode child  ) {
        this.addAndLinkChild(child);
        return true;
    }

    public void setMergeReceive(boolean needMerge) {
        m_mergeReceive = needMerge;
    }

    public boolean isMergeReceive() {
        return m_mergeReceive;
    }

    @Override
    public boolean isOutputOrdered (List<AbstractExpression> sortExpressions, List<SortDirectionType> sortDirections) {
        if (isMergeReceive()) {
            AbstractPlanNode orderBy = getInlinePlanNode(PlanNodeType.ORDERBY);
            assert(orderBy != null);
            return orderBy.isOutputOrdered(sortExpressions, sortDirections);
        } else {
            // MP output is unordered
            return false;
        }
    }

}
