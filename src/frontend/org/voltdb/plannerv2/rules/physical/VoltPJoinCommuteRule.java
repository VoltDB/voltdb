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

package org.voltdb.plannerv2.rules.physical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Index;
import org.voltdb.plannerv2.rel.AbstractVoltTableScan;
import org.voltdb.plannerv2.rel.VoltPRelBuilder;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalNestLoopJoin;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableSequentialScan;

import com.google.common.base.Preconditions;

/**
 * Volt extension of the JoinCommuteRule {@link org.apache.calcite.rel.rules.JoinCommuteRule}
 * that operates on VoltPhysicalNestLoopJoin relation
 */
public class VoltPJoinCommuteRule extends JoinCommuteRule {

    /** Instance of the rule that only swaps inner NL joins that have
     * Sequential scan on the left. This join has a potential to be converted to a NLIJ
     */
    public static final VoltPJoinCommuteRule INSTANCE_OUTER_CALC_SSCAN = new VoltPJoinCommuteRule(
            operand(VoltPhysicalCalc.class, operand(VoltPhysicalTableSequentialScan.class, none())),
            operand(RelNode.class, any()),
            "VoltPJoinCommuteRule:cacl_sscan");

    public static final VoltPJoinCommuteRule INSTANCE_OUTER_SSCAN = new VoltPJoinCommuteRule(
            operand(VoltPhysicalTableSequentialScan.class, none()),
            operand(RelNode.class, any()),
            "VoltPJoinCommuteRule:sscan");

    private VoltPJoinCommuteRule(RelOptRuleOperand outerChild, RelOptRuleOperand innerChild, String desc) {
        super(VoltPhysicalNestLoopJoin.class,
                outerChild, innerChild, VoltPRelBuilder.PHYSICAL_BUILDER, false, desc);
    }

    @Override
    public boolean matches(final RelOptRuleCall call) {
        // Matches only if an outer scan has an index. There is a possibility of NLIJ then
        Preconditions.checkArgument(call.rels.length > 2);
        final AbstractVoltTableScan scan = call.rel(call.rels.length - 2);
        Preconditions.checkArgument(scan.getVoltTable() != null &&
                scan.getVoltTable().getCatalogTable() != null);
        final CatalogMap<Index> indexMap = scan.getVoltTable().getCatalogTable().getIndexes();
        return indexMap != null && !indexMap.isEmpty();
    }
}
