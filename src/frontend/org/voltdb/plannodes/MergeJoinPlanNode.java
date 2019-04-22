/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class MergeJoinPlanNode extends AbstractJoinPlanNode {

    public enum Members {
        LESS_JOIN_PREDICATE;
    }

    public MergeJoinPlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.MERGEJOIN;
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "MERGE " + this.m_joinType.toString() + " JOIN" +
                (m_sortDirection == SortDirectionType.INVALID ? "" : " (" + m_sortDirection + ")") +
                explainFilters(indent);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        // Create a LESS expression from the JOIN predicate to be able to order tuples from each side
        AbstractExpression lessExpr = createLessJoinPredicate(getJoinPredicate());
        stringer.key(Members.LESS_JOIN_PREDICATE.name()).value(lessExpr);
    }

    // Build a Less predicate out of an equality predicate to compare tuples from outer and inner scans
    // This predictae will be used by the MergeJoin executor
    //
    // while(has outer tuple T1 and has inner tuple T2 {
    //      if (T1 EQ T2) {
    //              match - output joined tuple
    //      } else if (T1 LESS T2) {
    //              advance Outer
    //      } else {
    //              advance Inner
    //      }
    // }
    //
    // T1.C1 = T2.C1                   ->  T1.C1 < T2.C1
    // T1.C1 = T2.C1 AND T1.C2 = T2.C2 -> (T1.C1 < T2.C1)  OR (T1.C1 = T2.C1 AND T1.C2 < T2.C2)
    private AbstractExpression createLessJoinPredicate(AbstractExpression joinExpr) {
        Collection<AbstractExpression> predicates = ExpressionUtil.uncombineAny(joinExpr);
        List<AbstractExpression> lessExprs = new ArrayList<>();
        int idx = 0;
        for (AbstractExpression equalExpr : predicates) {
            AbstractExpression nextExpr = compareEqualToCompareLess(equalExpr);
            if (idx > 0) {
                List<AbstractExpression> exprs = predicates.stream().limit(idx).collect(Collectors.toList());
                exprs.add(nextExpr);
                nextExpr = ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_AND, exprs);
            }
            lessExprs.add(nextExpr);
            ++idx;
        }

        return ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_OR, lessExprs);
    }

    private AbstractExpression compareEqualToCompareLess(AbstractExpression expr) {
        assert(expr instanceof ComparisonExpression);
        assert(ExpressionType.COMPARE_EQUAL == expr.getExpressionType());
        AbstractExpression lessExpr = expr.clone();
        lessExpr.setExpressionType(ExpressionType.COMPARE_LESSTHAN);
        return lessExpr;
    }
}
