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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.exceptions.ValidationError;
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

    protected List<AbstractExpression> m_sortExpressions = new ArrayList<>();
    /**
     * Sort Directions
     */
    protected List<SortDirectionType> m_sortDirections = new ArrayList<>();

    public OrderByPlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.ORDERBY;
    }

    @Override
    public void validate() {
        super.validate();

        // Make sure that they have the same # of columns and directions
        if (m_sortExpressions.size() != m_sortDirections.size()) {
            throw new ValidationError("PlanNode '%s' has %d sort expressions but %d sort directions" +
                    toString(), m_sortExpressions.size(), m_sortDirections.size());
        }

        // Make sure that none of the items are null
        for (int ctr = 0, cnt = m_sortExpressions.size(); ctr < cnt; ctr++) {
            if (m_sortExpressions.get(ctr) == null) {
                throw new ValidationError("PlanNode '%s' has a null sort expression at position %d",
                        toString(), ctr);
            } else if (m_sortDirections.get(ctr) == null) {
                throw new ValidationError("PlanNode '%s' has a null sort direction at position %d",
                        toString(), ctr);
            }
        }
    }

    /**
     * Add multiple sort expressions to the order-by
     * @param sortExprs  List of the input expression on which to order the rows
     * @param sortDirs List of the corresponding sort order for each input expression
     */
    public void addSortExpressions(List<AbstractExpression> sortExprs, List<SortDirectionType> sortDirs) {
        assert(sortExprs.size() == sortDirs.size());
        for (int i = 0; i < sortExprs.size(); ++i) {
            addSortExpression(sortExprs.get(i), sortDirs.get(i));
        }
    }

    /**
     * Add a sort expression to the order-by
     * @param sortExpr  The input expression on which to order the rows
     * @param sortDir
     */
    public void addSortExpression(AbstractExpression sortExpr, SortDirectionType sortDir)
    {
        assert(sortExpr != null);
        // PlanNodes all need private deep copies of expressions
        // so that the resolveColumnIndexes results
        // don't get bashed by other nodes or subsequent planner runs
        m_sortExpressions.add(sortExpr.clone());
        m_sortDirections.add(sortDir);
    }

    public int countOfSortExpressions() {
        return m_sortExpressions.size();
    }

    public List<AbstractExpression> getSortExpressions() {
        return m_sortExpressions;
    }

    public List<SortDirectionType> getSortDirections() {
        return m_sortDirections;
    }

    @Override
    public void resolveColumnIndexes() {
        // Need to order and resolve indexes of output columns AND
        // the sort columns
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

        resolveSortIndexesUsingSchema(inputSchema);
    }

    public void resolveSortIndexesUsingSchema(NodeSchema inputSchema) {
        // Find the proper index for the sort columns.  Not quite
        // sure these should be TVEs in the long term.
        for (AbstractExpression sort_exps : m_sortExpressions) {
            List<TupleValueExpression> sort_tves =
                    ExpressionUtil.getTupleValueExpressions(sort_exps);
            for (TupleValueExpression tve : sort_tves) {
                tve.setColumnIndexUsingSchema(inputSchema);
            }
        }
    }

    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate,
                                     DatabaseEstimates estimates,
                                     ScalarValueHints[] paramHints) {
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
        AbstractExpression.toJSONArrayFromSortList(stringer, m_sortExpressions, m_sortDirections);
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        AbstractExpression.loadSortListFromJSONArray(m_sortExpressions, m_sortDirections, jobj);
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "ORDER BY (SORT)";
    }

    @Override
    public void findAllExpressionsOfClass(Class< ? extends AbstractExpression> aeClass, Set<AbstractExpression> collected) {
        super.findAllExpressionsOfClass(aeClass, collected);
        for (AbstractExpression ae : m_sortExpressions) {
            collected.addAll(ae.findAllSubexpressionsOfClass(aeClass));
        }
    }

    @Override
    public boolean isOutputOrdered (List<AbstractExpression> sortExpressions, List<SortDirectionType> sortDirections) {
        assert(sortExpressions.equals(m_sortExpressions));
        assert(sortDirections.equals(m_sortDirections));
        return true;
    }
}
