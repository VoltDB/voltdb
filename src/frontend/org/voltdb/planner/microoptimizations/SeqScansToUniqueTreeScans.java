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

package org.voltdb.planner.microoptimizations;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.IndexType;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

/// An end-stage plan rewriter that replaces a plan that uses sequential scans
/// with a slightly less efficient one that uses deterministic (unique)
/// index scans in their place.
/// This optimization is intended for use where a non-deterministic result
/// can lead to unreliability -- where a query might be feeding information
/// to a database write within the same transaction.
/// Unlike the other MicroOptimization classes, this is not actually a
/// performance optimization. It is instead "optimizing for reliability".
public class SeqScansToUniqueTreeScans extends MicroOptimization {

    /**
     * Only applies when stronger determinism is needed.
     */
    @Override
    boolean shouldRun(DeterminismMode detMode, boolean hasDeterministicStatement)
    {
        return ( ! hasDeterministicStatement) && detMode != DeterminismMode.FASTER;
    }

    @Override
    public List<CompiledPlan> apply(CompiledPlan plan, AbstractParsedStmt parsedStmt) {
        this.m_parsedStmt = parsedStmt;
        ArrayList<CompiledPlan> retval = new ArrayList<CompiledPlan>();

        // The statement is already known NOT to be inherently order deterministic.
        // Some PLANs for a non-ordered query may turn out to be deterministic anyway.
        // So, check first.
        AbstractPlanNode planGraph = plan.rootPlanGraph;

        if ( ! planGraph.isOrderDeterministic()) {
            plan.rootPlanGraph = recursivelyApply(planGraph);
        }

        retval.add(plan);
        return retval;
    }

    AbstractPlanNode recursivelyApply(AbstractPlanNode plan)
    {
        assert(plan != null);

        // depth first:
        //     Find Sequential Scan node.
        //     Replace with any unique tree index scan if possible.

        // Skip the collector fragment because its result will get aggregated
        // in a non-deterministic order regardless of the original scan ordering.
        if (plan instanceof ReceivePlanNode) {
            return plan;
        }

        ArrayList<AbstractPlanNode> children = new ArrayList<AbstractPlanNode>();

        for (int i = 0; i < plan.getChildCount(); i++) {
            children.add(plan.getChild(i));
        }

        for (AbstractPlanNode child : children) {
            // TODO this will break when children feed multiple parents
            AbstractPlanNode newChild = recursivelyApply(child);
            // Do a graft into the (parent) plan only if a replacement for a child was found.
            if (newChild == child) {
                continue;
            }
            boolean replaced = plan.replaceChild(child, newChild);
            assert(replaced);
        }

        // skip the meat if this isn't a scan node
        if ( ! (plan instanceof SeqScanPlanNode)) {
            return plan;
        }
        assert(plan.getChildCount() == 0);

        // got here? we're got ourselves a sequential scan
        SeqScanPlanNode scanNode = (SeqScanPlanNode) plan;

        String tableName = scanNode.getTargetTableName();
        Table table = m_parsedStmt.m_db.getTables().get(tableName);
        assert(table != null);

        Index indexToScan = null;

        // Pick the narrowest index from all of the unique tree indexes.
        // note: This is not the same as picking the narrowest key in c++,
        // which is probably what you want if it turns out this optimization
        // does anything for performance at all.
        for (Index index : table.getIndexes()) {
            // skip non-unique indexes
            if (index.getUnique() == false) {
                continue;
            }
            // skip hash indexes
            else if (index.getType() != IndexType.BALANCED_TREE.getValue()) {
                continue;
            }
            else {
                if (indexToScan == null) {
                    indexToScan = index;
                }
                else {
                    if (CatalogUtil.getCatalogIndexSize(indexToScan) > CatalogUtil.getCatalogIndexSize(index)) {
                        indexToScan = index;
                    }
                }
            }
        }

        if (indexToScan == null) {
            return plan;
        }

        // make an index node from the scan node
        IndexScanPlanNode indexScanNode = new IndexScanPlanNode(scanNode, null, indexToScan, SortDirectionType.ASC);
        indexScanNode.setForDeterminismOnly();

        return indexScanNode;
    }

}
