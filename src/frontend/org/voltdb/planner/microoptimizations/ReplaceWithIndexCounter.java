/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.planner.microoptimizations;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexCountPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.utils.CatalogUtil;

public class ReplaceWithIndexCounter implements MicroOptimization {

    @Override
    public List<CompiledPlan> apply(CompiledPlan plan) {
        ArrayList<CompiledPlan> retval = new ArrayList<CompiledPlan>();

        AbstractPlanNode planGraph = plan.rootPlanGraph;
        planGraph = recursivelyApply(planGraph);
        plan.rootPlanGraph = planGraph;

        retval.add(plan);
        return retval;
    }

    AbstractPlanNode recursivelyApply(AbstractPlanNode plan) {
        assert(plan != null);

        // depth first:
        //     find AggregatePlanNode with exactly one child
        //     where that child is an AbstractScanPlanNode.
        //     Replace the AggregatePlanNode and AbstractScanPlanNode
        //     with IndexCountPlanNode

        ArrayList<AbstractPlanNode> children = new ArrayList<AbstractPlanNode>();

        for (int i = 0; i < plan.getChildCount(); i++)
            children.add(plan.getChild(i));
        plan.clearChildren();

        for (AbstractPlanNode child : children) {
            // TODO this will break when children feed multiple parents
            child = recursivelyApply(child);
            child.clearParents();
            plan.addAndLinkChild(child);
        }

        if ((plan instanceof AggregatePlanNode) == false)
            return plan;
        if (plan.getChildCount() != 1)
            return plan;
        // check aggregation type
        List <ExpressionType> et = ((AggregatePlanNode) plan).getAggregateTypes();
        if ((et.size() == 1 &&
             et.get(0).equals(ExpressionType.AGGREGATE_COUNT_STAR)) == false)
            return plan;

        AbstractPlanNode child = plan.getChild(0);
        if ((child instanceof IndexScanPlanNode) == false)
            return plan;

        IndexScanPlanNode isp = (IndexScanPlanNode)child;
        // rule out query without where clause
        if (isp.getPredicate() == null && isp.getEndExpression() == null &&
                isp.getSearchKeyExpressions().size() == 0)
            return plan;
        // check index type
        Index idx = ((IndexScanPlanNode)child).getCatalogIndex();
        if (idx.getCountable() == false)
            return plan;

        // we are on the right track, but right now only deal with cases:
        // (1) counter index on 1 column:
        // Col >= ?, END EXPRE: null
        // or Col == ?, END EXPRE: null
        // or Col >= ? AND Col <= ?. END EXPRE: Col <= ?
        // Col < ?, END EXPRE: Col <=?, Search key Expre: null
        // (2) counter index on 2 or more columns.
        // Col_A = ? AND Col_B =? AND Col_C <= ? END EXPRE: Col <= ?

        // The core idea is that counting index should know the start key and end key to
        // jump to instead of doing index scan
        // End expression should indicate the END key or the whole size of the index

        IndexCountPlanNode icpn = null;
        if (isReplaceable((IndexScanPlanNode)child)) {
            icpn = new IndexCountPlanNode((IndexScanPlanNode)child);
            if (icpn.isEndExpreValid() == false)
                return plan;

            icpn.setOutputSchema(plan.getOutputSchema());

            // TODO(xin): I am not sure if there is a null case or not
            if (plan.getParent(0) != null) {
                plan.addIntermediary(plan.getParent(0));
            }

            plan.removeFromGraph();
            child.removeFromGraph();

            return icpn;
        }
        return plan;
    }

    // Rule it out of index count case if there is post expression for index scan
    boolean isReplaceable(IndexScanPlanNode child) {
        AbstractExpression predicateExpr = child.getPredicate();
        if (predicateExpr == null) return true;

        AbstractExpression endExpr = child.getEndExpression();
        ArrayList<AbstractExpression> subEndExpr = null;
        if (endExpr != null) {
            subEndExpr = endExpr.findAllSubexpressionsOfClass(TupleValueExpression.class);
        }

        assert(predicateExpr != null);
        ArrayList<AbstractExpression> hasLeft = predicateExpr.findAllSubexpressionsOfClass(TupleValueExpression.class);

        Set<String> columnsLeft = new HashSet<String>();
        for (AbstractExpression ae: hasLeft) {
            TupleValueExpression tve = (TupleValueExpression) ae;
            columnsLeft.add(tve.getColumnName());
        }

        if (endExpr != null && subEndExpr != null) {
            for (AbstractExpression ae: subEndExpr) {
                TupleValueExpression tve = (TupleValueExpression) ae;
                String columnName = tve.getColumnName();
                if (columnsLeft.contains(columnName)) {
                    columnsLeft.remove(columnName);
                }
            }
        }

        Index idx = child.getCatalogIndex();
        List<ColumnRef> sortedColumns = CatalogUtil.getSortedCatalogItems(idx.getColumns(), "index");
        int searchKeySize = child.getSearchKeyExpressions().size();

        if (columnsLeft.size() > searchKeySize)
            return false;

        // assume it has searchKey
        for (int i = 0; i < searchKeySize; i++) {
            ColumnRef cr = sortedColumns.get(i);
            String colName = cr.getColumn().getName();
            if (columnsLeft.contains(colName)) {
                columnsLeft.remove(colName);
            }
        }

        if (columnsLeft.size() != 0)
            return false;
        return true;
    }

}
