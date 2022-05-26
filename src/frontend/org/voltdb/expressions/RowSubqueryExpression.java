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

package org.voltdb.expressions;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.plannodes.TupleScanPlanNode;
import org.voltdb.types.ExpressionType;

/**
 * Expression to represent row subqueries (C1, C2, ...)
  */
public class RowSubqueryExpression extends AbstractSubqueryExpression {

    public RowSubqueryExpression() {
        super();
        m_type = ExpressionType.ROW_SUBQUERY;
        m_subqueryId = AbstractParsedStmt.NEXT_STMT_ID++;
    }

    /**
     * Constructor to build the expression from the list of column expressions
     * @param args
     */
    public RowSubqueryExpression(List<AbstractExpression> args) {
        this();
        assert(args != null);
        m_args = args;
        // replace the TVE and aggregated expressions from the IN List with the correlated parameters
        List<AbstractExpression> pves = new ArrayList<>();
        for (AbstractExpression expr : args) {
            collectParameterValueExpressions(expr, pves);
        }
        String tableName = AbstractParsedStmt.TEMP_TABLE_NAME + "_" + m_subqueryId;
        setSubqueryNode(new TupleScanPlanNode(tableName, pves));
    }

    @Override
    public void finalizeValueTypes() {
        super.finalizeValueTypes();
        if (m_args != null) {
            m_args.forEach(AbstractExpression::finalizeValueTypes);
        }
    }

    @Override
    public String explain(String impliedTableName) {
        StringBuilder result = new StringBuilder("(");
        String connector = "";
        assert (m_args != null);
        for (AbstractExpression arg : m_args) {
            result.append(connector).append(arg.explain(impliedTableName));
            connector = ", ";
        }
        result.append(")");
        return result.toString();
    }

    // Recursively collect all TVE and aggregate expressions to be replaced with the corresponding
    // PVE inside the Row subquery
    private void collectParameterValueExpressions(AbstractExpression expr, List<AbstractExpression> pves) {
        if (expr == null) {
            return;
        }

        if (expr instanceof TupleValueExpression || expr instanceof AggregateExpression) {
            // Create a matching PVE for this expression to be used on the EE side
            // to get the original expression value
            addCorrelationParameterValueExpression(expr, pves);
            return;
        }
        collectParameterValueExpressions(expr.getLeft(), pves);
        collectParameterValueExpressions(expr.getRight(), pves);
        if (expr.getArgs() != null) {
            for (AbstractExpression arg : expr.getArgs()) {
                collectParameterValueExpressions(arg, pves);
            }
        }
    }

}
