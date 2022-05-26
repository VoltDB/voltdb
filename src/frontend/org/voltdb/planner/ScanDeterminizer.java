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

package org.voltdb.planner;

import java.util.ArrayList;

import org.voltdb.catalog.Index;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
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
public class ScanDeterminizer {

    /**
     * Only applies when stronger determinism is needed.
     */
    public static void apply(CompiledPlan plan, DeterminismMode detMode)
    {
        if (detMode == DeterminismMode.FASTER) {
            return;
        }
        if (plan.hasDeterministicStatement()) {
            return;
        }
        AbstractPlanNode planGraph = plan.rootPlanGraph;
        if (planGraph.isOrderDeterministic()) {
            return;
        }

        AbstractPlanNode root = plan.rootPlanGraph;
        root = recursivelyApply(root);
        plan.rootPlanGraph = root;
    }

    static private AbstractPlanNode recursivelyApply(AbstractPlanNode plan)
    {
        assert(plan != null);
        // depth first:
        //     Find Sequential Scan node.
        //     Replace with any unique tree index scan if possible.

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
        SeqScanPlanNode scanNode = (SeqScanPlanNode) plan;

        if (! scanNode.isPersistentTableScan()) {
            // This is a subquery or common table and can't have indexes
            return plan;
        }

        // got here? we're got ourselves a sequential scan over a real table
        assert (scanNode.getChildCount() == 0);
        StmtTableScan tableScan = scanNode.getTableScan();
        assert(tableScan != null);

        Index indexToScan = null;

        // Pick the narrowest index from all of the unique tree indexes.
        // note: This is not the same as picking the narrowest key in c++,
        // which is probably what you want if it turns out this optimization
        // does anything for performance at all.
        for (Index index : tableScan.getIndexes()) {
            // skip non-unique indexes
            if (index.getUnique() == false) {
                continue;
            }
            // skip hash indexes
            else if (index.getType() != IndexType.BALANCED_TREE.getValue()) {
                continue;
            }
            // skip partial indexes
            else if ( ! index.getPredicatejson().isEmpty()) {
                continue;
            }
            else if (indexToScan == null ||
                    CatalogUtil.getCatalogIndexSize(indexToScan) > CatalogUtil.getCatalogIndexSize(index)) {
                indexToScan = index;
            }
        }

        if (indexToScan == null) {
            return plan;
        }

        // make an index node from the scan node
        IndexScanPlanNode indexScanNode =
                new IndexScanPlanNode(scanNode, null, indexToScan, SortDirectionType.ASC);
        indexScanNode.setForDeterminismOnly();
        return indexScanNode;
    }

}
