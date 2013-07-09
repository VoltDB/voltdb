/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.planner.microoptimizations;

import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONException;
import org.voltdb.catalog.*;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

public class ReplaceWithIndexLimit extends MicroOptimization {

    Database db = null;
    int indent = 0;
    @Override
    public List<CompiledPlan> apply(CompiledPlan plan, Database db) {
        ArrayList<CompiledPlan> retval = new ArrayList<CompiledPlan>();
        this.db = db;
        AbstractPlanNode planGraph = plan.rootPlanGraph;
        planGraph = recursivelyApply(planGraph);
        plan.rootPlanGraph = planGraph;
        retval.add(plan);
        return retval;
    }

    void recursivelyPrint(AbstractPlanNode node)
    {
        for (int i = 0; i < indent; i++)
        System.out.printf("\t");
        System.out.println(node.toJSONString());
        indent++;
        if(node.getChildCount() > 0)
            recursivelyPrint(node.getChild(0));
    }

    AbstractPlanNode recursivelyApply(AbstractPlanNode plan)
    {
        assert(plan != null);

        // depth first:
        //     find AggregatePlanNode with exactly one child
        //     where that child is an AbstractScanPlanNode.
        //     Replace any qualifying AggregatePlanNode / AbstractScanPlanNode pair
        //     with an IndexCountPlanNode or TableCountPlanNode

        ArrayList<AbstractPlanNode> children = new ArrayList<AbstractPlanNode>();

        for (int i = 0; i < plan.getChildCount(); i++)
            children.add(plan.getChild(i));

        for (AbstractPlanNode child : children) {
            // TODO this will break when children feed multiple parents
            AbstractPlanNode newChild = recursivelyApply(child);
            // Do a graft into the (parent) plan only if a replacement for a child was found.
            if (newChild == child) {
                continue;
            }
            child.removeFromGraph();
            plan.addAndLinkChild(newChild);
        }

        // check for an aggregation of the right form
        if ((plan instanceof AggregatePlanNode) == false)
            return plan;
        assert(plan.getChildCount() == 1);
        AggregatePlanNode aggplan = (AggregatePlanNode)plan;

        // handle one single min() / max() now
        // TODO: combination of [min(), max(), count()]
        if (aggplan.isTableMin() == false && aggplan.isTableMax() == false) {
            return plan;
        }

        AbstractPlanNode child = plan.getChild(0);

        /**
         * For generated SeqScan plan, look through all available indexes, if the first key
         * of any one index matches the min()/max(), use that index with an inlined LIMIT
         * node.
         * For generated IndexScan plan, verify current index can be used for min()/max(), if
         * so, appending an inlined LIMIT node to it.
         * To avoid further handling to locate aggExpr for partitioned table in upper AGGREGATOR
         * (coordinator), keep this old trivial AGGREGATOR node.
         */

        // for a SEQSCAN, replace it with a INDEXSCAN node with an inline LIMIT plan node
        if (child instanceof SeqScanPlanNode) {
            // only replace SeqScan when no predicate
            // should have other index access plan if any qualified index found for the predicate
            if (((SeqScanPlanNode)child).getPredicate() != null) {
                return plan;
            }

            // get all indexes for the table
            CatalogMap<Index> allIndexes = db.getTables().get(((SeqScanPlanNode)child).getTargetTableName()).getIndexes();

            if(allIndexes.isEmpty()){
                return plan;
            }

            Index ret = findQualifiedIndex(allIndexes, aggplan.getAggregateExpressions().get(0), null);

            if(ret == null) {
                return plan;
            } else {
                // create one INDEXSCAN plan node with inlined LIMIT
                // and replace the SEQSCAN node with it
                IndexScanPlanNode ispn = new IndexScanPlanNode((SeqScanPlanNode) child, aggplan);
                ispn.setCatalogIndex(ret);
                ispn.setTargetIndexName(ret.getTypeName());
                ispn.setLookupType(IndexLookupType.GTE);    // a safe way

                // we know which end row we want to fetch, so it's safe to
                // specify sorting direction here
                if(aggplan.isTableMin()) {
                    ispn.setSortDirection(SortDirectionType.ASC);
                }
                if(aggplan.isTableMax()) {
                    ispn.setSortDirection(SortDirectionType.DESC);
                }

                LimitPlanNode lpn = new LimitPlanNode();
                lpn.setLimit(1);
                lpn.setOffset(0);

                // add other inline nodes
                for(AbstractPlanNode inlineChild : child.getInlinePlanNodes().values()) {
                    ispn.addInlinePlanNode(inlineChild);
                }
                ispn.addInlinePlanNode(lpn);
                ispn.generateOutputSchema(db);

                // remove old SeqScan and link the new generated IndexScan
                plan.clearChildren();
                plan.addAndLinkChild(ispn);
                return plan;
            }
        }

        if ((child instanceof IndexScanPlanNode) == false) {
            return plan;
        }

        // no non-indexable (post-)predicates allowed
        if (((IndexScanPlanNode)child).getPredicate() != null) {
            return plan;
        }

        // already have the IndexScanPlanNode
        IndexScanPlanNode ispn = (IndexScanPlanNode)child;

        // only handle ALL equality filters case
        // TODO: edge cases, eg. SELECT MIN(C) / MAX(C) FROM T WHERE C > ?
        if(ispn.getLookupType() != IndexLookupType.EQ &&
                ispn.getSearchKeyExpressions().size() != ExpressionUtil.uncombine(ispn.getEndExpression()).size()) {
            return plan;
        }

        AbstractExpression aggExpr = aggplan.getAggregateExpressions().get(0);

        // do not aggressively evaluate all indexes, just examine the index currently in use;
        // because for all qualified indexes, one access plan must have been generated already,
        // and we can take advantage of that
        Index origIndex = ispn.getCatalogIndex();

        // get indexable filters' expressions
        List<AbstractExpression> exprs = ExpressionUtil.uncombine(ispn.getEndExpression());

        if(!checkIndex(origIndex, aggExpr, exprs)) {
            return plan;
        } else {
            if (aggplan.isTableMin()) {
                ispn.setSortDirection(SortDirectionType.ASC);
            }
            if (aggplan.isTableMax()) {
                ispn.setSortDirection(SortDirectionType.DESC);
            }
            // add an inline LIMIT plan node to this index scan plan node
            LimitPlanNode lpn = new LimitPlanNode();
            lpn.setLimit(1);
            lpn.setOffset(0);
            ispn.addInlinePlanNode(lpn);
            plan.generateOutputSchema(db);

            return plan;
        }
    }

    private Index findQualifiedIndex(CatalogMap<Index> candidates, AbstractExpression aggExpr, List<AbstractExpression> filterExprs) {
        Index retval = null;

        for(Index index : candidates) {
            boolean found = checkIndex(index, aggExpr, filterExprs);

            if(found) {
                retval = index;
                break;
            }
        }

        return retval;
    }

    private boolean checkIndex(Index index, AbstractExpression aggExpr, List<AbstractExpression> filterExprs) {
        String exprsjson = index.getExpressionsjson();

        if(exprsjson.isEmpty()) {
            // if the index is on simple columns, aggregate expression must be a simple column too
            if(aggExpr.getExpressionType() != ExpressionType.VALUE_TUPLE) {
                return false;
            }

            return checkPureColumnIndex(index, ((TupleValueExpression)aggExpr).getColumnIndex(), filterExprs);

        } else {
            // either pure expression index or mix of expressions and simple columns
            List<AbstractExpression> indexedExprs = null;
            try {
                indexedExprs = AbstractExpression.fromJSONArrayString(exprsjson, null);
            } catch (JSONException e) {
                e.printStackTrace();
                assert(false);
                return false;
            }

            return checkExpressionIndex(indexedExprs, aggExpr, filterExprs);
        }
    }

    private boolean checkPureColumnIndex(Index index, Integer aggCol, List<AbstractExpression> filterExprs) {

        int filterSize = ((filterExprs == null) ? 0 : filterExprs.size());

        if(filterSize > 0) {
            // all left child of filterExprs must be of type TupleValueExpression in equality comparison
            for (AbstractExpression expr : filterExprs) {
                if(expr.getExpressionType() != ExpressionType.COMPARE_EQUAL) {
                    return false;
                }
                if(!(expr.getLeft() instanceof TupleValueExpression)) {
                    return false;
                }
            }
        }

        List<ColumnRef> indexedColRefs = CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");

        List<Integer> indexedColIdx = new ArrayList<Integer>();
        for(ColumnRef cr : indexedColRefs) {
            indexedColIdx.add(cr.getColumn().getIndex());
        }

        // lookup aggCol up to min((filterSize + 1), indexedColIdx.size())
        // aggCol can be one of equality comparison key (then a constant value),
        // or all filters compose the complete set of prefix key components
        for(int i = 0; i < Math.min(indexedColIdx.size(), filterSize + 1); i++) {
            if(indexedColIdx.get(i) == aggCol) {
                return true;
            }
        }

        return false;
    }

    private boolean checkExpressionIndex(List<AbstractExpression> indexedExprs, AbstractExpression aggExpr, List<AbstractExpression> filterExprs) {

        int filterSize = ((filterExprs == null) ? 0 : filterExprs.size());

        if(filterSize > 0) {
            for(AbstractExpression expr : filterExprs) {
                if(expr.getExpressionType() != ExpressionType.COMPARE_EQUAL) {
                    return false;
                }
            }
        }

        for(int i = 0; i < Math.min(indexedExprs.size(), filterSize + 1); i++) {
            AbstractExpression expr = indexedExprs.get(i);
            if(aggExpr.equals(expr)) {
                return true;
            }
        }

        return false;
    }

}
