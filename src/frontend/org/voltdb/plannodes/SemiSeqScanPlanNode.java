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

import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.VectorValueExpression;
import org.voltdb.types.PlanNodeType;

public class SemiSeqScanPlanNode extends AbstractScanPlanNode {

    public SemiSeqScanPlanNode() {
        super();
    }

    /*
     * @param subqueryName The subquery temp table name
     * @param inColumns - expression for each column from the IN list combined by the AND expression
     */
    public SemiSeqScanPlanNode(String subqueryName, AbstractExpression inColumns) {
        super(subqueryName, subqueryName);
        // we can build the predicate only after the child's output schema is generated
        // the IN expression:
        // outer_expr IN (SELECT inner_expr FROM ... WHERE subq_where)
        // The predicate: outer_expr=inner_expr
        if (inColumns instanceof VectorValueExpression) {
            m_inColumnList = ((AbstractExpression)inColumns.clone()).getArgs();
        } else {
            m_inColumnList = new ArrayList<AbstractExpression>();
            m_inColumnList.add((AbstractExpression)inColumns.clone());
        }
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.SEMISEQSCAN;
    }

    @Override
    public void generateOutputSchema(Database db) {
        assert(!m_children.isEmpty());
        AbstractPlanNode subqueryNode = m_children.get(0);
        subqueryNode.generateOutputSchema(db);
        List<SchemaColumn> subqueryColumns = subqueryNode.getOutputSchema().getColumns();
        setScanColumns(subqueryColumns);
        m_tableSchema = m_tableScanSchema.clone();
        m_outputSchema = m_tableScanSchema.clone();
        // Now, that the subquery's output schema is generated and the inner_expr is
        // available we can build the predicate
        assert(subqueryColumns.size() == m_inColumnList.size());
        List<AbstractExpression> columnExprs = new ArrayList<AbstractExpression>();
        for (SchemaColumn subqueryColumn : subqueryColumns) {
            columnExprs.add(subqueryColumn.getExpression());
        }
        AbstractExpression predicate = ExpressionUtil.buildEquavalenceExpression(m_inColumnList, columnExprs);
        setPredicate(predicate);
    }

    @Override
    public void resolveColumnIndexes() {
        // resolve children indexes
        assert(m_children.size() == 1);
        m_children.get(0).resolveColumnIndexes();
        // resolve own indexes
        super.resolveColumnIndexes();
    }

    @Override
    protected String explainPlanForNode(String indent) {
        String tableName = m_targetTableName == null? m_targetTableAlias: m_targetTableName;
        return "SEQUENTIAL SCAN of \"" + tableName + "\"" + explainPredicate("\n" + indent + " filter by ");
    }

    private List<AbstractExpression> m_inColumnList ;
}
