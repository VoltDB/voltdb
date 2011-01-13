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
import java.util.TreeMap;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.*;
import org.voltdb.types.*;

public abstract class AbstractJoinPlanNode extends AbstractPlanNode {

    public enum Members {
        JOIN_TYPE,
        PREDICATE;
    }

    protected JoinType m_joinType = JoinType.INNER;
    protected AbstractExpression m_predicate;

    protected AbstractJoinPlanNode() {
        super();
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        if (m_predicate != null) {
            m_predicate.validate();
        }
    }

    /**
     * @return the join_type
     */
    public JoinType getJoinType() {
        return m_joinType;
    }

    /**
     * @param join_type the join_type to set
     */
    public void setJoinType(JoinType join_type) {
        m_joinType = join_type;
    }

    /**
     * @return the predicate
     */
    public AbstractExpression getPredicate() {
        return m_predicate;
    }

    /**
     * @param predicate the predicate to set
     */
    public void setPredicate(AbstractExpression predicate)
    {
        if (predicate != null)
        {
            // PlanNodes all need private deep copies of expressions
            // so that the resolveColumnIndexes results
            // don't get bashed by other nodes or subsequent planner runs
            try
            {
                m_predicate = (AbstractExpression) predicate.clone();
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
    public void generateOutputSchema(Database db)
    {
        // FUTURE: At some point it would be awesome to further
        // cull the columns out of the join to remove columns that were only
        // used by scans/joins.  I think we can coerce HSQL into provide this
        // info relatively easily. --izzy

        // Index join will have to override this method.
        // Assert and provide functionality for generic join
        assert(m_children.size() == 2);
        for (AbstractPlanNode child : m_children)
        {
            child.generateOutputSchema(db);
        }
        // Join the schema together to form the output schema
        m_outputSchema =
            m_children.get(0).getOutputSchema().
            join(m_children.get(1).getOutputSchema()).copyAndReplaceWithTVE();
    }

    // Given any non-inlined type of join, this method will resolve the column
    // order and TVE indexes for the output SchemaColumns.
    @Override
    public void resolveColumnIndexes()
    {
        // First, assert that our topology is sane and then
        // recursively resolve all child/inline column indexes
        IndexScanPlanNode index_scan =
            (IndexScanPlanNode) getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assert(m_children.size() == 2 && index_scan == null);
        for (AbstractPlanNode child : m_children)
        {
            child.resolveColumnIndexes();
        }
        // for seq scan, child Table order in the EE is in child order in the plan.
        // order our output columns by outer table then inner table
        // I dislike this magically implied ordering, should be fixable --izzy
        NodeSchema outer_schema = m_children.get(0).getOutputSchema();
        NodeSchema inner_schema = m_children.get(1).getOutputSchema();

        // need to order the combined input schema coherently.  We make the
        // output schema ordered: [outer table columns][inner table columns]
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
                index = inner_schema.getIndexOfTve(tve);
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

        // Finally, resolve m_predicate
        List<TupleValueExpression> predicate_tves =
            ExpressionUtil.getTupleValueExpressions(m_predicate);
        for (TupleValueExpression tve : predicate_tves)
        {
            int index = outer_schema.getIndexOfTve(tve);
            if (index == -1)
            {
                index = inner_schema.getIndexOfTve(tve);
                if (index == -1)
                {
                    throw new RuntimeException("Unable to find index for join TVE: " +
                                               tve.toString());
                }
            }
            tve.setColumnIndex(index);
        }
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.JOIN_TYPE.name()).value(m_joinType.toString());
        stringer.key(Members.PREDICATE.name()).value(m_predicate);
    }
}
