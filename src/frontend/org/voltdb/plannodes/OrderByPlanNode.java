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

import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class OrderByPlanNode extends AbstractPlanNode {

    public enum Members {
        SORT_COLUMNS,
        SORT_EXPRESSION,
        SORT_DIRECTION;
    }

    protected List<AbstractExpression> m_sortExpressions = new ArrayList<AbstractExpression>();
    /**
     * Sort Directions
     */
    protected List<SortDirectionType> m_sortDirections = new ArrayList<SortDirectionType>();

    public OrderByPlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.ORDERBY;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // Make sure that they have the same # of columns and directions
        if (m_sortExpressions.size() != m_sortDirections.size()) {
            throw new Exception("ERROR: PlanNode '" + toString() + "' has " +
                                "'" + m_sortExpressions.size() + "' sort expressions but " +
                                "'" + m_sortDirections.size() + "' sort directions");
        }

        // Make sure that none of the items are null
        for (int ctr = 0, cnt = m_sortExpressions.size(); ctr < cnt; ctr++) {
            if (m_sortExpressions.get(ctr) == null) {
                throw new Exception("ERROR: PlanNode '" + toString() + "' has a null " +
                                    "sort expression at position " + ctr);
            } else if (m_sortDirections.get(ctr) == null) {
                throw new Exception("ERROR: PlanNode '" + toString() + "' has a null " +
                                    "sort direction at position " + ctr);
            }
        }
    }

    /**
     * Add a sort to the order-by
     * @param sortExpr  The input expression on which to order the rows
     * @param sortDir
     */
    public void addSort(AbstractExpression sortExpr, SortDirectionType sortDir)
    {
        assert(sortExpr != null);
        // PlanNodes all need private deep copies of expressions
        // so that the resolveColumnIndexes results
        // don't get bashed by other nodes or subsequent planner runs
        m_sortExpressions.add((AbstractExpression) sortExpr.clone());
        m_sortDirections.add(sortDir);
    }

    public int countOfSortExpressions() {
        return m_sortExpressions.size();
    }

    public List<AbstractExpression> getSortExpressions() {
        return m_sortExpressions;
    }

    @Override
    public void resolveColumnIndexes()
    {
        // Need to order and resolve indexes of output columns AND
        // the sort columns
        assert(m_children.size() == 1);
        m_children.get(0).resolveColumnIndexes();
        NodeSchema input_schema = m_children.get(0).getOutputSchema();
        for (SchemaColumn col : m_outputSchema.getColumns())
        {
            // At this point, they'd better all be TVEs.
            assert(col.getExpression() instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)col.getExpression();
            int index = tve.resolveColumnIndexesUsingSchema(input_schema);
            tve.setColumnIndex(index);
        }
        m_outputSchema.sortByTveIndex();

        // Find the proper index for the sort columns.  Not quite
        // sure these should be TVEs in the long term.
        List<TupleValueExpression> sort_tves =
            new ArrayList<TupleValueExpression>();
        for (AbstractExpression sort_exps : m_sortExpressions)
        {
            sort_tves.addAll(ExpressionUtil.getTupleValueExpressions(sort_exps));
        }
        for (TupleValueExpression tve : sort_tves)
        {
            int index = tve.resolveColumnIndexesUsingSchema(input_schema);
            tve.setColumnIndex(index);
        }
    }

    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate,
                                     Cluster cluster,
                                     Database db,
                                     DatabaseEstimates estimates,
                                     ScalarValueHints[] paramHints)
    {
        // This method doesn't do anything besides what the parent method does,
        // but it is a nice place to put a comment. Really, sorts should be pretty
        // expensive and they're not costed as such yet because sometimes that
        // backfires in our simplistic model.
        //
        // What's really interesting here is costing an index scan who has sorted
        // output vs. an index scan that filters out a bunch of tuples, but still
        // requires a sort. Which is best depends on how well the filter reduces the
        // number of tuples, and that's not possible for us to know right now.
        // The index scanner cost has been pretty carefully set up to do what we think
        // we want, given the lack of table stats.
        m_estimatedOutputTupleCount = childOutputTupleCountEstimate;
        m_estimatedProcessedTupleCount = childOutputTupleCountEstimate;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        assert (m_sortExpressions.size() == m_sortDirections.size());
        stringer.key(Members.SORT_COLUMNS.name()).array();
        for (int ii = 0; ii < m_sortExpressions.size(); ii++) {
            stringer.object();
            stringer.key(Members.SORT_EXPRESSION.name());
            stringer.object();
            m_sortExpressions.get(ii).toJSONString(stringer);
            stringer.endObject();
            stringer.key(Members.SORT_DIRECTION.name()).value(m_sortDirections.get(ii).toString());
            stringer.endObject();
        }
        stringer.endArray();
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        JSONArray jarray = null;
        if( !jobj.isNull(Members.SORT_COLUMNS.name()) ){
            jarray = jobj.getJSONArray( Members.SORT_COLUMNS.name() );
        }
        int size = jarray.length();
        for( int i = 0; i < size; i++ ) {
            JSONObject tempObj = jarray.getJSONObject(i);
            m_sortDirections.add( SortDirectionType.get(tempObj.getString( Members.SORT_DIRECTION.name())) );
            m_sortExpressions.add( AbstractExpression.fromJSONChild(tempObj, Members.SORT_EXPRESSION.name()) );
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "ORDER BY (SORT)";
    }
}
