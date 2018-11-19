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

package org.voltdb.newplanner.rules.physical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.voltdb.calciteadapter.rel.logical.VoltDBLRel;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPExchange;
import org.voltdb.calciteadapter.rel.physical.VoltDBPRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSort;

public class VoltDBPSortConvertRule extends ConverterRule {

    public static final VoltDBPSortConvertRule INSTANCE_NONE =
            new VoltDBPSortConvertRule(Convention.NONE);
    public static final VoltDBPSortConvertRule INSTANCE_VOLTDB =
            new VoltDBPSortConvertRule(VoltDBLRel.VOLTDB_LOGICAL);

    VoltDBPSortConvertRule(RelTrait inTrait) {
        super(
                Sort.class,
                inTrait,
                VoltDBPRel.VOLTDB_PHYSICAL,
                "VoltDBSortConvertRule" + inTrait.toString());
    }

    @Override
    public RelNode convert(RelNode rel) {
        Sort sort = (Sort) rel;
        RelTraitSet traits = sort.getInput().getTraitSet()
                .replace(VoltDBPRel.VOLTDB_PHYSICAL)
                // This is important. Since the new sort is recreated by Calcite from a collation trait
                // it must have the RelDistributions.ANY distribution trait to be properly
                // exchanged / transposed with an Exchange relation
                .replace(RelDistributions.ANY);
        RelNode input = sort.getInput();
        RelNode convertedInput = convert(input,
                input.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL).simplify());
        RelTraitSet childTraits = convertedInput.getTraitSet();
        RelDistribution childDistribution = childTraits.getTrait(RelDistributionTraitDef.INSTANCE);
        // If a RelDistribution trait is ANY then this Sort relation still sits above an Exchange node
        // and its distribution is unknown yet. Set it simply to 1
        int splitCount = (childDistribution.getType().equals(RelDistributions.ANY.getType())) ?
                1 : AbstractVoltDBPExchange.DISTRIBUTED_SPLIT_COUNT;

        RelNode newRel = new VoltDBPSort(
                sort.getCluster(),
                traits.plus(sort.getCollation()),
                convert(sort.getInput(), traits.simplify()),
                sort.getCollation(),
                splitCount);

        return newRel;
    }
}
