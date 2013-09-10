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

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.planner.CompiledPlan;
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
public class SeqScansToUniqueTreeScans extends MicroOptimization {

    /**
     * Only applies when stronger determinism is needed.
     */
    @Override
    boolean shouldRun(DeterminismMode detMode) {
        return detMode != DeterminismMode.FASTER;
    }

    @Override
    public List<CompiledPlan> apply(CompiledPlan plan, Database db) {
        ArrayList<CompiledPlan> retval = new ArrayList<CompiledPlan>();

        //TODO: This should not further penalize seqscan plans that have
        // already been post-sorted into strict order determinism,
        // so, check first for plan.isOrderDeterministic()?
        AbstractPlanNode planGraph = plan.rootPlanGraph;
        planGraph = recursivelyApply(planGraph, db);
        plan.rootPlanGraph = planGraph;

        retval.add(plan);
        return retval;
    }

    AbstractPlanNode recursivelyApply(AbstractPlanNode plan, Database db)
    {
        assert(plan != null);

        // depth first:
        //     Find Sequential Scan node.
        //     Replace any unique tree index scan if possible.

        ArrayList<AbstractPlanNode> children = new ArrayList<AbstractPlanNode>();

        for (int i = 0; i < plan.getChildCount(); i++) {
            children.add(plan.getChild(i));
        }

        for (AbstractPlanNode child : children) {
            // TODO this will break when children feed multiple parents
            AbstractPlanNode newChild = recursivelyApply(child, db);
            // Do a graft into the (parent) plan only if a replacement for a child was found.
            if (newChild == child) {
                continue;
            }
            boolean replaced = plan.replaceChild(child, newChild);
            assert(true == replaced);
        }

        // skip the meat if this isn't a scan node
        if ((plan instanceof SeqScanPlanNode) == false) {
            return plan;
        }
        assert(plan.getChildCount() == 0);

        // got here? we're got ourselves a sequential scan
        SeqScanPlanNode scanNode = (SeqScanPlanNode) plan;

        String tableName = scanNode.getTargetTableName();
        Table table = db.getTables().get(tableName);
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
        indexScanNode.setKeyIterate(true);
        indexScanNode.setForDeterminismOnly();

        return indexScanNode;
    }

}
