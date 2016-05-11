/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.WindowedExpression;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

/**
 * This plan node represents windowed aggregate computations.
 * The only one we implement now is windowed RANK.  But more
 * could be possible.
 *
 * In particular, this node represents the ability to aggregate
 * state over a partitioned, ordered table.  The partitioning and
 * ordering in the table are both created by ordering the table,
 * first on the partition by expressions, and then on the
 * windowed order by expressions.  These partition by expressions are the ones
 * found in a windowed aggregate function call, not the columns
 * partitioning tables into sites.
 *
 * Note that we don't care about the sort order
 * or the expression order of the partition expressions, but we
 * do care about the sort order and expression order of the
 * order by expressions.
 *
 * For the purposes of this note, call this order, first on window partition expressions
 * then on window order by, as the <em>windowed order</em>.  There is
 * some ambiguity here, as there may be several such windowed
 * orders.  We assume we can calculate the best one somehow, but leave that
 * for later.
 *
 * A select statement may have two different order criteria.
 * <ol>
 *   <li>A windowed expression with some partition by expressions and some
 *       order by expression.</li>
 *   <li>An order by expression in the statement outside of a windowed
 *       function call.</li>
 * </ol>
 * This plan node, and the planner's actions in creating it, helps
 * us combine and constrast these two criteria.
 *
 * <h2>Plans for windowed aggregate functions.</h2>
 * There are several possible plans for a windowed aggregate function.
 * They depend on quite a large number of questions.
 * <ol>
 *   <li>Are the windowed order by and the statement order by compatible?</li>
 *   <li>Are the windowed order by and the site partitioning columns compatible?</li>
 *   <li>Is there an index which can be used to sort the windowed order by expressions,
 *       the statement order by expressions or both?</li>
 * </ol>
 *
 * <h3>The slowest path.</h3>
 * The slowest plan node is one which answers to all these questions is no.  The
 * plan for this case would be this diagram:
 * <pre>
 *   Send --> Projection --> OrderBy(Stmt) -> PartitionBy -> MergeReceive --> Send(DISTR) --> OrderBy(1) -> SeqScan
 * </pre>
 * The %%%
 *
 * The plan must be a 2 fragment plan, as we can see with the MergeReceive node.
 * </ul>
 *   <li>The distributed fragments need to generate columns sorted by the windowed order on
 *       each site.</li>
 *   <li>These are sent to a merge receive node which knows the windowed order, and
 *       merges the distributed rows in a single temporary table in the windowed
 *       order.  Since we don't have any group by clauses, these are rows directly from
 *       joins of persistent tables or subqueries.</li>
 *   <li>The PartitionByPlanNode scans the result of the MergeReceivePlanNode,
 *       aggregating results and calculating rank.</li>
 *   <li>The OrderByPlanNode for the statement calculates the query level order on the
 *       output of the PartitionByPlanNode.</li>
 *   <li>The final ProjectionPlanNode calculates the display list's expressions.  The rank function
 *       in the display list will be one of the input columns to the ProjectPlanNode, so it can be
 *       used in the output appropriately.</li>
 * </ul>
 * We can do the windowed sort in parallel in the distributed fragments, but we must do the
 * windowed partitioning in the collector fragment.
 *
 * <h3>Compatible Partitions.</h3>
 * In this case the windowed partition by clause is compatible with the tables' site partitioning.
 * This will happen if the tables' site partition clauses occur as an expression in the windowed
 * partition by clause.  The tables from the distributed fragments will be unions of the window
 * partitions.
 *
 * The plan will look like this:
 * <pre>
 *   Send --> Projection --> OrderBy(Stmt) -> MergeReceive -> Send(DISTR) --> PartitionBy --> OrderBy(1) -> SeqScan
 * </pre>
 * Here we distribute the windowed expression sort and the partitioning.
 *
 * <h3>Compatible Partitions and Order Bys</h3>
 * <h3>Using indices for order by nodes.</h3>
 */
public class PartitionByPlanNode extends AggregatePlanNode {
    public enum Members {
        WINDOWED_COLUMN
    };

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.PARTITIONBY;
    }

    @Override
    public void resolveColumnIndexes() {
        super.resolveColumnIndexes();
    }

    @Override
    protected String explainPlanForNode(String indent) {
        String optionalTableName = "*NO MATCH -- USE ALL TABLE NAMES*";
        StringBuilder sb = new StringBuilder(" PARTITION BY PLAN: " + super.explainPlanForNode(indent) + "\n");
        sb.append("  PARTITION BY:\n");
        WindowedExpression we = getWindowedExpression();
        for (int idx = 0; idx < numberPartitionByExpressions(); idx += 1) {
            sb.append("  ")
              .append(idx).append(": ")
              .append(we.getPartitionByExpressions().get(idx).toString())
              .append("\n");
        }
        sb.append(indent).append(" SORT BY: \n");
        for (int idx = 0; idx < numberSortExpressions(); idx += 1) {
            AbstractExpression ae = getSortExpression(idx);
            SortDirectionType dir = getSortDirection(idx);
            sb.append(indent)
               .append(ae.explain(optionalTableName))
               .append(": ")
               .append(dir.name())
               .append("\n");
        }
        return sb.toString();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        if (m_windowedSchemaColumn != null) {
            stringer.key(Members.WINDOWED_COLUMN.name());
            m_windowedSchemaColumn.toJSONString(stringer, true);
        }
    }

    @Override
    public void loadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        JSONObject windowedColumn = (JSONObject) jobj.get(Members.WINDOWED_COLUMN.name());
        if (windowedColumn != null) {
            m_windowedSchemaColumn = SchemaColumn.fromJSONObject(windowedColumn);
        }
    }

    private WindowedExpression getWindowedExpression() {
        AbstractExpression abstractSE = m_windowedSchemaColumn.getExpression();
        assert(abstractSE instanceof WindowedExpression);
        return (WindowedExpression)abstractSE;
    }

    public AbstractExpression getPartitionByExpression(int idx) {
        WindowedExpression we = getWindowedExpression();
        return we.getPartitionByExpressions().get(idx);
    }

    public int numberPartitionByExpressions() {
        return getWindowedExpression().getPartitionbySize();
    }

    public AbstractExpression getSortExpression(int idx) {
        WindowedExpression we = getWindowedExpression();
        return we.getOrderByExpressions().get(idx);
    }

    public SortDirectionType getSortDirection(int idx) {
        WindowedExpression we = getWindowedExpression();
        return (we.getOrderByDirections().get(idx));
    }

    public int numberSortExpressions() {
        WindowedExpression we = getWindowedExpression();
        return we.getOrderbySize();
    }

    public void addWindowedColumn(SchemaColumn col) {
        m_windowedSchemaColumn = col;
    }

    public SchemaColumn getWindowedColumns() {
        return m_windowedSchemaColumn;
    }

    public void setWindowedColumn(SchemaColumn col) {
        m_windowedSchemaColumn = col;
    }
    SchemaColumn       m_windowedSchemaColumn;
}
