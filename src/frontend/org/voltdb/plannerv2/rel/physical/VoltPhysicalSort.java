/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.plannerv2.rel.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.plannerv2.rel.logical.VoltLogicalTableScan;
import org.voltdb.plannerv2.rel.util.PlanCostUtil;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class VoltPhysicalSort extends Sort implements VoltPhysicalRel {

    private final int m_splitCount;

    public VoltPhysicalSort(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, int splitCount) {
        this(cluster, traitSet, input, collation, null, null, splitCount);
    }

    private VoltPhysicalSort(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset,
            RexNode limit, int splitCount) {
        super(cluster, traitSet, input, collation, offset, limit);
        Preconditions.checkArgument(getConvention() == VoltPhysicalRel.CONVENTION);
        m_splitCount = splitCount;
    }

    @Override
    public VoltPhysicalSort copy(
            RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode limit) {
        return copy(traitSet, input, collation, offset, limit, m_splitCount);
    }

    public VoltPhysicalSort copy(
            RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode limit, int splitCount) {
        return new VoltPhysicalSort(getCluster(), traitSet, input, collation, offset, limit, splitCount);
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }

    @Override
    protected String computeDigest() {
        String digest = super.computeDigest();
        digest += "_split_" + m_splitCount;
        return digest;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("split", m_splitCount);
        return pw;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return getInput(0).estimateRowCount(mq);
     }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final double rowCount = estimateRowCount(mq);
        final List<Integer> collations =
                PlanCostUtil.collationIndices(getTraitSet().getTrait(RelCollationTraitDef.INSTANCE));
        final double cpu;
        // NOTE: it is not necessary to discount based on index, since VoltPhysicalSort is only used on single table scan;
        // and the VoltPhysicalTable{Sequential,Index}TableScan node beneath are sufficient to pick the best candidate.
        /*
        if (getIndexes((VolcanoPlanner) planner).stream()
                .filter(index -> index.getPredicatejson().isEmpty())   // partial index cannot be used
                .map(PlanCostUtil::indexColumns)
                .anyMatch(cols -> PlanCostUtil.commonPrefixLength(cols, collations) == collations.size())) {
            // there is an index on the table with identical (or includes) the collation order
            cpu = 1;
        } else { // no matching index: the worst-case time complexity is mandated to be O(nlogn)
            cpu = rowCount * Math.log(rowCount);
        }*/
        cpu = rowCount * Math.log(rowCount);
        return planner.getCostFactory().makeCost(rowCount, cpu, 0);
    }

    private static List<Index> getIndexes(VolcanoPlanner planner) {
        final List<Table> tables = planner.getRelNodes().stream()
                .flatMap(n -> n instanceof VoltLogicalTableScan ?
                        Stream.of(((VoltLogicalTableScan) n).getVoltTable().getCatalogTable()) : Stream.empty())
                .collect(Collectors.toList());
        if (tables.size() == 1) {
            return StreamSupport.stream((tables.get(0).getIndexes()).spliterator(), false)
                    .collect(Collectors.toList());
        } else {    // either sort from multiple table joins, or from a calc
            return Collections.emptyList();
        }
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        final AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
        final LimitPlanNode lpn;
        if (fetch != null || offset != null) {
            lpn = VoltPhysicalLimit.toPlanNode(fetch, offset);
        } else {
            lpn = null;
        }
        final RelCollation collation = getCollation();
        if (collation != null) {
            final OrderByPlanNode opn = VoltRexUtil.collationToOrderByNode(collation, fieldExps);
            opn.addAndLinkChild(child);
            if (lpn != null) {
                opn.addInlinePlanNode(lpn);
            }
            return opn;
        } else {
            assert lpn != null;
            lpn.addAndLinkChild(child);
            return lpn;
        }
    }
}
