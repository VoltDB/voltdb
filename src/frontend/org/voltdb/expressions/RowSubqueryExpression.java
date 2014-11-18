/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
 * Expression to represent scalar/row subqueries with the exception of IN (LIST)
 * where a VectorValueExpression is used.
 */
public class RowSubqueryExpression extends AbstractSubqueryExpression {

    public RowSubqueryExpression() {
        super();
        m_type = ExpressionType.ROW_SUBQUERY;
        m_subqueryId = AbstractParsedStmt.NEXT_STMT_ID++;
    }

    public RowSubqueryExpression(List<AbstractExpression> args) {
        this();
        assert(args != null);
        m_args = args;
        // List of correlated parameters that match the original expressions from
        // a row subquery and need to be set by this RowExpression on the EE side prior
        // to the evaluation
        List<AbstractExpression> pves = new ArrayList<AbstractExpression>();
        for (AbstractExpression expr : args) {
            collectParameterValueExpressions(expr, pves);
        }
        String tableName = "VOLT_TEMP_TABLE_" + m_subqueryId;
        m_subqueryNode = new TupleScanPlanNode(tableName, pves);
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        if ((m_args.size() != m_parameterIdxList.size()))
            throw new Exception("ERROR: A row expression is invalid");

    }

    @Override
    public String explain(String impliedTableName) {
        String result = "(";
        String connector = "";
        assert (m_args != null);
        for (AbstractExpression arg : m_args) {
            result += connector + arg.explain(impliedTableName);
            connector = ", ";
        }
        result += ")";
        return result;
    }

    // Recursively collect all TVE and aggregate expressions to be replaced with the corresponding
    // PVE inside the Row subquery
    private void collectParameterValueExpressions(AbstractExpression expr, List<AbstractExpression> pves) {
        if (expr == null) {
            return;
        }

        // Create a matching PVE for this expression to be used on the EE side
        // to get the original expression value
        if (expr instanceof TupleValueExpression || expr instanceof AggregateExpression) {
            int paramIdx = AbstractParsedStmt.NEXT_PARAMETER_ID++;
            m_parameterIdxList.add(paramIdx);
            ParameterValueExpression pve = new ParameterValueExpression();
            pve.setParameterIndex(paramIdx);
            pve.setValueSize(expr.getValueSize());
            pve.setValueType(expr.getValueType());
            pve.setCorrelatedExpression(expr);
            pves.add(pve);
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
