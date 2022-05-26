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

package org.voltdb.planner.microoptimizations;

import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONException;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Index;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.IndexType;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

public class ReplaceWithIndexLimit extends MicroOptimization {

    // for debug purpose only, this might not be called
    protected void recursivelyPrint(AbstractPlanNode node, StringBuilder sb) {
        recursivelyPrint(node, sb, 0);
    }

    private void recursivelyPrint(AbstractPlanNode node, StringBuilder sb, int indent) {
        for (int i = 0; i < indent; i++) {
            sb.append("\t");
        }
        sb.append(node.toJSONString()).append("\n");
        if (node.getChildCount() > 0) {
            recursivelyPrint(node.getChild(0), sb, indent);
        }
    }

    @Override
    protected AbstractPlanNode recursivelyApply(AbstractPlanNode plan, AbstractParsedStmt parsedStmt) {
        assert plan != null;

        // depth first:
        //     Find AggregatePlanNode with exactly one child
        //     where that child is an AbstractScanPlanNode.
        //     Replace qualifying SeqScanPlanNode with an
        //     IndexScanPlanNode with an inlined LimitPlanNode;
        //     or appending the LimitPlanNode to the existing
        //     qualified IndexScanPlanNode.
        for (int i = 0; i < plan.getChildCount(); i++) {
            final AbstractPlanNode child = plan.getChild(i);
            // TODO this will break when children feed multiple parents
            AbstractPlanNode newChild = recursivelyApply(child, parsedStmt);
            // Do a graft into the (parent) plan only if a replacement for a child was found.
            if (newChild == child) {
                continue;
            }
            child.removeFromGraph();
            plan.addAndLinkChild(newChild);
        }

        // check for an aggregation of the right form
        if (! (plan instanceof AggregatePlanNode)) {
            return plan;
        }
        assert plan.getChildCount() == 1;
        AggregatePlanNode aggplan = (AggregatePlanNode)plan;

        // handle one single min() / max() now
        // TODO: combination of [min(), max(), count()]
        final SortDirectionType sortDirection;
        if (aggplan.isTableMin()) {
            sortDirection = SortDirectionType.ASC;
        } else if (aggplan.isTableMax()) {
            sortDirection = SortDirectionType.DESC;
        } else {
            return plan;
        }

        AbstractPlanNode child = plan.getChild(0);
        AbstractExpression aggExpr = aggplan.getFirstAggregateExpression();

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
            } else if (! ((AbstractScanPlanNode)child).isPersistentTableScan()) {
                return plan;
            }

            // create an empty bindingExprs list, used for store (possible) bindings for adHoc query
            List<AbstractExpression> bindings = new ArrayList<>();
            Index ret = findQualifiedIndex(((SeqScanPlanNode)child), aggExpr, bindings, parsedStmt);

            if (ret == null) {
                return plan;
            } else {
                // 1. create one INDEXSCAN plan node with inlined LIMIT
                // and replace the SEQSCAN node with it
                // 2. we know which end row we want to fetch, so it's safe to
                // specify sorting direction here
                IndexScanPlanNode ispn = new IndexScanPlanNode((SeqScanPlanNode) child, aggplan, ret, sortDirection);
                ispn.setBindings(bindings);
                assert(ispn.getSearchKeyExpressions().size() == 0);
                if (sortDirection == SortDirectionType.ASC) {
                    assert aggplan.isTableMin();
                    ispn.setSkipNullPredicate(0, 0);
                }

                LimitPlanNode lpn = new LimitPlanNode();
                lpn.setLimit(1);
                lpn.setOffset(0);

                ispn.addInlinePlanNode(lpn);

                // remove old SeqScan node and link the new generated IndexScan node
                plan.clearChildren();
                plan.addAndLinkChild(ispn);
                return plan;
            }
        }

        if (! (child instanceof IndexScanPlanNode)) {
            return plan;
        }

        // already have the IndexScanPlanNode
        IndexScanPlanNode ispn = (IndexScanPlanNode)child;

        // can do optimization only if it has no (post-)predicates
        // except those (post-)predicates are artifact predicates
        // we added for reverse scan purpose only
        if (((IndexScanPlanNode)child).getPredicate() != null &&
                !((IndexScanPlanNode)child).isPredicatesOptimizableForAggregate() ||
                child.isSubQuery()) { // Guard against (possible future?) cases of indexable subquery.
            return plan;
        }
        // 1. Handle ALL equality filters case.
        // In the IndexScanPlanNode:
        //      -- EQFilterExprs were put in searchkeyExpressions and endExpressions
        //      -- startCondition is only in searchKeyExpressions
        //      -- endCondition is only in endExpressions
        // So, if the lookup type is EQ, then all filters must be equality; or if
        // there are extra startCondition / endCondition, some filters are not equality
        // 2. Handle equality filters and one other comparison operator (<, <=, >, >=), see comments below
        else if (ispn.getLookupType() != IndexLookupType.EQ &&
                Math.abs(ispn.getSearchKeyExpressions().size() -
                        ExpressionUtil.uncombinePredicate(ispn.getEndExpression()).size()) > 1) {
            return plan;
        }

        // exprs will be used as filterExprs to check the index
        // For forward scan, the initial value is endExprs and might be changed in different values in variant cases
        // For reverse scan, the initial value is initialExprs which is the "old" endExprs
        List<AbstractExpression> exprs;
        int numOfSearchKeys = ispn.getSearchKeyExpressions().size();
        if (ispn.getLookupType() == IndexLookupType.LT || ispn.getLookupType() == IndexLookupType.LTE) {
            exprs = ExpressionUtil.uncombinePredicate(ispn.getInitialExpression());
            numOfSearchKeys -= 1;
        } else {
            exprs = ExpressionUtil.uncombinePredicate(ispn.getEndExpression());
        }
        int numberOfExprs = exprs.size();

        /* Retrieve the index expressions from the target index. (ENG-8819, Ethan)
         * This is because we found that for the following two queries:
         *     #1: explain select max(c2/2) from t where c1=1 and c2/2<=3;
         *     #2: explain select max(c2/2) from t where c1=1 and c2/2<=?;
         * We can get an inline limit 1 for #2 but not for #1. This is because all constants in #1 got parameterized.
         * The result is that the query cannot pass the bindingToIndexedExpression() tests below
         * because we lost all the constant value expressions (cannot attempt to bind a pve to a pve!).
         * Those constant values expressions can only be accessed from the idnex.
         * We will not add those bindings to the ispn.getBindings() here because they will be added anyway in checkIndex().
         * PS: For this case (i.e. index on expressions), checkIndex() will call checkExpressionIndex(),
         * where bindings will be added.
         */
        final Index indexToUse = ispn.getCatalogIndex();
        final String tableAlias = ispn.getTargetTableAlias();
        List<AbstractExpression> indexedExprs = null;
        if (! indexToUse.getExpressionsjson().isEmpty()) {
            try {
                indexedExprs = AbstractExpression.fromJSONArrayString(
                        indexToUse.getExpressionsjson(), parsedStmt.getStmtTableScanByAlias(tableAlias));
            } catch (JSONException e) {
                throw new PlanningErrorException(e.getCause());
            }
        }

        /* If there is only 1 difference between searchkeyExprs and endExprs,
         * 1. trivial filters can be discarded, 2 possibilities:
         *      a. SELECT MIN(X) FROM T WHERE [other prefix filters] X < / <= ?
         *         <=> SELECT MIN(X) FROM T WHERE [other prefix filters] && the X < / <= ? filter
         *      b. SELECT MAX(X) FROM T WHERE X > / >= ?
         *         <=> SELECT MAX(X) FROM T with post-filter
         * 2. filter should act as equality filter, 2 possibilities
         *      SELECT MIN(X) FROM T WHERE [other prefix filters] X > / >= ?
         *      SELECT MAX(X) FROM T WHERE [other prefix filters] X < / <= ?

         * check if there is other filters for SELECT MAX(X) FROM T WHERE [other prefix filter AND ] X > / >= ?
         * but we should allow SELECT MAX(X) FROM T WHERE X = ?

         * This is for queries having MAX() but no ORDER BY. (ENG-8819, Ethan)
         * sortDirection == DESC if max, ASC if min. ispn.getSortDirection() == INVALID if no ORDER BY. */
        if (sortDirection == SortDirectionType.DESC && ispn.getSortDirection() == SortDirectionType.INVALID) {
            /* numberOfExprs = exprs.size(), exprs are initial expressions for reversed index scans (lookupType LT, LTE),
             * are end expressions for forward index scans (lookupType GT, GTE, EQ).
             * Note, lookupType doesn't decide the scan direction for sure. MIN(X) where X < ? is still a forward scan.
             * X < ? will be a post filter for the scan rather than an initial expression. */
            if (numberOfExprs == 1) {
                // e.g.: explain select max(c2/2) from t where c2/2<=3;
                // In this case, as long as the where condition (exprs.get(0)) matches the aggregation argument, continue.
                AbstractExpression exprToBind = indexedExprs == null ? exprs.get(0).getLeft() : indexedExprs.get(0);
                if (aggExpr.bindingToIndexedExpression(exprToBind) == null) {
                    return plan;
                }
            } else if (numberOfExprs > 1) {
                // ENG-4016: Optimization for query SELECT MAX(X) FROM T WHERE [other prefix filters] X < / <= ?
                // Just keep trying, don't return early.
                boolean earlyReturn = true;
                for (int i=0; i<numberOfExprs; ++i) {
                    AbstractExpression expr = exprs.get(i);
                    AbstractExpression indexedExpr = indexedExprs == null ? expr.getLeft() : indexedExprs.get(i);
                    if (aggExpr.bindingToIndexedExpression(indexedExpr) != null &&
                            (expr.getExpressionType() == ExpressionType.COMPARE_LESSTHANOREQUALTO ||
                             expr.getExpressionType() == ExpressionType.COMPARE_LESSTHAN ||
                             expr.getExpressionType() == ExpressionType.COMPARE_EQUAL) ) {
                        earlyReturn = false;
                        break;
                    }
                }
                if (earlyReturn) {
                    return plan;
                }
            }
        }

        // have an upper bound: # of endingExpr is more than # of searchExpr
        if (numberOfExprs > numOfSearchKeys) {
            AbstractExpression lastEndExpr = exprs.get(numberOfExprs - 1);
            // check last ending condition, see whether it is
            //      SELECT MIN(X) FROM T WHERE [other prefix filters] X < / <= ? or
            // other filters will be checked later
            AbstractExpression exprToBind = indexedExprs == null ? lastEndExpr.getLeft() : indexedExprs.get(numberOfExprs - 1);
            if ((lastEndExpr.getExpressionType() == ExpressionType.COMPARE_LESSTHAN ||
                 lastEndExpr.getExpressionType() == ExpressionType.COMPARE_LESSTHANOREQUALTO)
                    && aggExpr.bindingToIndexedExpression(exprToBind) != null) {
                exprs.remove(lastEndExpr);
            }
        }

        // do not aggressively evaluate all indexes, just examine the index currently in use;
        // because for all qualified indexes, one access plan must have been generated already,
        // and we can take advantage of that
        if (checkIndex(ispn.getCatalogIndex(), aggExpr, exprs, ispn.getBindings(), tableAlias, parsedStmt)) {
            // we know which end we want to fetch, set the sort direction
            ispn.setSortDirection(sortDirection);

            // for SELECT MIN(X) FROM T WHERE [prefix filters] = ?
            if (numberOfExprs == numOfSearchKeys && sortDirection == SortDirectionType.ASC) {
                if (ispn.getLookupType() == IndexLookupType.GTE) {
                    assert aggplan.isTableMin();
                    ispn.setSkipNullPredicate(numOfSearchKeys, 0);
                }
            }

            // for SELECT MIN(X) FROM T WHERE [...] X < / <= ?
            // reset the IndexLookupType, remove "added" searchKey, add back to endExpression, and clear "added" predicate
            if (sortDirection == SortDirectionType.ASC &&
                    (ispn.getLookupType() == IndexLookupType.LT || ispn.getLookupType() == IndexLookupType.LTE)){
                ispn.setLookupType(IndexLookupType.GTE);
                ispn.removeLastSearchKey();
                ispn.addEndExpression(ExpressionUtil.uncombinePredicate(ispn.getInitialExpression()).get(numberOfExprs - 1));
                ispn.setSkipNullPredicate(numOfSearchKeys, 0);
                ispn.resetPredicate();
            }
            // add an inline LIMIT plan node to this index scan plan node
            final LimitPlanNode lpn = new LimitPlanNode();
            lpn.setLimit(1);
            lpn.setOffset(0);
            ispn.addInlinePlanNode(lpn);

            // ENG-1565: For SELECT MAX(X) FROM T WHERE X > / >= ?, turn the pre-filter to post filter.
            // The current approach is:
            // AggregatePlanNode                AggregatePlanNode with filter
            //  |__ IndexScanPlanNode       =>      |__IndexScanPlanNode with no filter
            //                                              |__LimitPlanNode
            if (sortDirection == SortDirectionType.DESC && !ispn.getSearchKeyExpressions().isEmpty() && exprs.isEmpty() &&
                    ExpressionUtil.uncombinePredicate(ispn.getInitialExpression()).isEmpty()) {
                AbstractExpression newPredicate = new ComparisonExpression();
                if (ispn.getLookupType() == IndexLookupType.GT)
                    newPredicate.setExpressionType(ExpressionType.COMPARE_GREATERTHAN);
                if (ispn.getLookupType() == IndexLookupType.GTE)
                    newPredicate.setExpressionType(ExpressionType.COMPARE_GREATERTHANOREQUALTO);
                newPredicate.setRight(ispn.getSearchKeyExpressions().get(0));
                newPredicate.setLeft(aggExpr);
                newPredicate.setValueType(aggExpr.getValueType());
                ispn.clearSearchKeyExpression();
                aggplan.setPrePredicate(newPredicate);
            }
        }
        return plan;
    }

    private Index findQualifiedIndex(SeqScanPlanNode seqScan, AbstractExpression aggExpr,
                                     List<AbstractExpression> bindingExprs, AbstractParsedStmt parsedStmt) {
        String tableName = seqScan.getTargetTableName();
        CatalogMap<Index> allIndexes = parsedStmt.m_db.getTables().get(tableName).getIndexes();

        String fromTableAlias = seqScan.getTargetTableAlias();

        for (Index index : allIndexes) {
            if (checkIndex(index, aggExpr, new ArrayList<>(), bindingExprs, fromTableAlias, parsedStmt)) {
                return index;
            }
        }
        return null;
    }

    private boolean checkIndex(Index index, AbstractExpression aggExpr, List<AbstractExpression> filterExprs,
                               List<AbstractExpression> bindingExprs, String fromTableAlias, AbstractParsedStmt parsedStmt) {

        if (!IndexType.isScannable(index.getType()) || !index.getPredicatejson().isEmpty()) { // Skip partial indexes
            return false;
        }

        final String exprsjson = index.getExpressionsjson();

        if (exprsjson.isEmpty()) {
            // if the index is on simple columns, aggregate expression must be a simple column too
            if (aggExpr.getExpressionType() != ExpressionType.VALUE_TUPLE) {
                return false;
            } else {
                return checkPureColumnIndex(index, ((TupleValueExpression) aggExpr).getColumnIndex(), filterExprs);
            }
        } else {
            // either pure expression index or mix of expressions and simple columns
            try {
                return checkExpressionIndex(
                        AbstractExpression.fromJSONArrayString(exprsjson,
                                parsedStmt.getStmtTableScanByAlias(fromTableAlias)),
                        aggExpr, filterExprs, bindingExprs);
            } catch (JSONException e) {
                throw new PlanningErrorException(e.getCause());
            }
        }
    }

    // lookup aggCol up to min((filterSize + 1), indexedColIdx.size())
    // aggCol can be one of equality comparison key (then a constant value),
    // or all filters compose the complete set of prefix key components
    private static boolean checkPureColumnIndex(Index index, int aggCol, List<AbstractExpression> filterExprs) {

        boolean found = false;

        // all left child of filterExprs must be of type TupleValueExpression in equality comparison
        for (AbstractExpression expr : filterExprs) {
            if (expr.getExpressionType() != ExpressionType.COMPARE_EQUAL) {
                return false;
            } else if (!(expr.getLeft() instanceof TupleValueExpression)) {
                return false;
            } else if (((TupleValueExpression)expr.getLeft()).getColumnIndex() == aggCol) {
                found = true;
            }
        }

        if (found) {
            return true;
        } else if (index.getColumns().size() > filterExprs.size()) {
            return aggCol == CatalogUtil.getSortedCatalogItems(index.getColumns(), "index")
                    .get(filterExprs.size()).getColumn().getIndex();
        } else {
            return false;
        }
    }

    private static boolean checkExpressionIndex(List<AbstractExpression> indexedExprs,
            AbstractExpression aggExpr, List<AbstractExpression> filterExprs, List<AbstractExpression> bindingExprs) {

        List<AbstractExpression> newBindings;

        // check type of every filters
        if (! filterExprs.stream().allMatch(expr -> expr.getExpressionType() == ExpressionType.COMPARE_EQUAL)) {
            return false;
        }

        // first check the indexExpr which is the immediate next one after filterExprs
        if (indexedExprs.size() > filterExprs.size()) {
            newBindings = aggExpr.bindingToIndexedExpression(indexedExprs.get(filterExprs.size()));
            if (newBindings != null) {
                bindingExprs.addAll(newBindings);
                return true;
            }
        }

        // indexedExprs.size() == filterExprs.size()
        // bind aggExpr with indexedExprs
        // add the binding and return when found one (must be in filter as well)
        for (AbstractExpression expr : indexedExprs) {
            newBindings = aggExpr.bindingToIndexedExpression(expr);
            if (newBindings != null) {
                bindingExprs.addAll(newBindings);
                return true;
            }
        }

        return false;
    }

}
