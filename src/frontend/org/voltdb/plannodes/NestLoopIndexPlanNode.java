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

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;

public class NestLoopIndexPlanNode extends AbstractJoinPlanNode {

    public NestLoopIndexPlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.NESTLOOPINDEX;
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        // Important safety tip regarding this inlined
        // index scan and ITS inlined projection:
        // That projection is currently only used/usable as
        // a means to narrow the set of columns from the
        // indexscan's target table that make it into the
        // rest of the plan.  the expressions that are
        // given to the projection are currently not ever used
        IndexScanPlanNode inlineScan =
            (IndexScanPlanNode) m_inlineNodes.get(PlanNodeType.INDEXSCAN);
        assert(inlineScan != null);
        inlineScan.generateOutputSchema(db);
        assert(m_children.size() == 1);
        m_children.get(0).generateOutputSchema(db);
        // Join the schema together to form the output schema
        // The child subplan's output is the outer table
        // The inlined node's output is the inner table
        m_outputSchema =
            m_children.get(0).getOutputSchema().
            join(inlineScan.getOutputSchema()).copyAndReplaceWithTVE();
    }

    @Override
    public void resolveColumnIndexes()
    {
        IndexScanPlanNode inline_scan =
            (IndexScanPlanNode) m_inlineNodes.get(PlanNodeType.INDEXSCAN);
        assert (m_children.size() == 1 && inline_scan != null);
        for (AbstractPlanNode child : m_children)
        {
            child.resolveColumnIndexes();
        }
        for (AbstractPlanNode inline : m_inlineNodes.values())
        {
            // Some of this will get undone for the inlined index scan later
            // but this will resolve any column tracking with an inlined projection
            inline.resolveColumnIndexes();
        }
        // We need the schema from the target table from the inlined index
        NodeSchema index_schema = inline_scan.getTableSchema();
        // We need the output schema from the child node
        NodeSchema outer_schema = m_children.get(0).getOutputSchema();

        // pull every expression out of the inlined index scan
        // and resolve all of the TVEs against our two input schema from above.
        // The EE still uses assignTupleValueIndexes to figure out which input
        // table to use for each of these TVEs
        //  get the predicate (which is the inlined node's predicate,
        //  the planner currently blithely ignores that abstractjoinnode also
        //  has a predicate field, and so do we.
        List<TupleValueExpression> predicate_tves =
            ExpressionUtil.getTupleValueExpressions(inline_scan.getPredicate());
        for (TupleValueExpression tve : predicate_tves)
        {
            // this double-schema search is somewhat common, maybe it
            // can find a static home in NodeSchema or something --izzy
            int index = outer_schema.getIndexOfTve(tve);
            if (index == -1)
            {
                index = index_schema.getIndexOfTve(tve);
                if (index == -1)
                {
                    throw new RuntimeException("Unable to find index for nestloopindexscan TVE: " +
                                               tve.toString());
                }
            }
            tve.setColumnIndex(index);
        }

        //  get the end expression and search key expressions
        List<TupleValueExpression> index_tves =
            new ArrayList<TupleValueExpression>();
        index_tves.addAll(ExpressionUtil.getTupleValueExpressions(inline_scan.getEndExpression()));
        for (AbstractExpression search_exp : inline_scan.getSearchKeyExpressions())
        {
            index_tves.addAll(ExpressionUtil.getTupleValueExpressions(search_exp));
        }
        for (TupleValueExpression tve : index_tves)
        {
            int index = outer_schema.getIndexOfTve(tve);
            if (index == -1)
            {
                index = index_schema.getIndexOfTve(tve);
                if (index == -1)
                {
                    throw new RuntimeException("Unable to find index for nestloopindexscan TVE: " +
                                               tve.toString());
                }
            }
            tve.setColumnIndex(index);
        }

        // need to resolve the indexes of the output schema and
        // order the combined output schema coherently
        TreeMap<Integer, SchemaColumn> sort_cols =
            new TreeMap<Integer, SchemaColumn>();
        for (SchemaColumn col : m_outputSchema.getColumns())
        {
            // Right now these all need to be TVEs
            assert(col.getExpression() instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)col.getExpression();
            int index = outer_schema.getIndexOfTve(tve);
            if (index == -1)
            {
                index = index_schema.getIndexOfTve(tve);
                if (index == -1)
                {
                    throw new RuntimeException("Unable to find index for column: " +
                                               col.toString());
                }
                sort_cols.put(index + outer_schema.size(), col);
            }
            else
            {
                sort_cols.put(index, col);
            }
            tve.setColumnIndex(index);
        }
        // rebuild the output schema from the tree-sorted columns
        NodeSchema new_output_schema = new NodeSchema();
        for (SchemaColumn col : sort_cols.values())
        {
            new_output_schema.addColumn(col);
        }
        m_outputSchema = new_output_schema;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // Check that we have an inline IndexScanPlanNode
        if (m_inlineNodes.isEmpty()) {
            throw new Exception("ERROR: No inline PlanNodes are set for " + this);
        } else if (!m_inlineNodes.containsKey(PlanNodeType.INDEXSCAN)) {
            throw new Exception("ERROR: No inline PlanNode with type '" + PlanNodeType.INDEXSCAN + "' was set for " + this);
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "NESTLOOP INDEX JOIN";
    }
}
