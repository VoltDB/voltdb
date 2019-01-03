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

package org.voltdb.plannerv2.rules.physical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.physical.AbstractVoltDBPExchange;
import org.voltdb.plannerv2.rel.physical.VoltDBPRel;
import org.voltdb.plannerv2.rel.physical.VoltDBPSort;
import org.voltdb.plannerv2.utils.VoltRelUtil;

/**
 * VoltDB physical rule that transform {@link Sort} to {@link VoltDBPSort}.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPSortConvertRule extends ConverterRule {

    // TODO: when this rule will be applied?
    public static final VoltPSortConvertRule INSTANCE_NONE =
            new VoltPSortConvertRule(Convention.NONE);
    public static final VoltPSortConvertRule INSTANCE_VOLTDB =
            new VoltPSortConvertRule(VoltLogicalRel.CONVENTION);

    VoltPSortConvertRule(RelTrait inTrait) {
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
                .replace(VoltDBPRel.VOLTDB_PHYSICAL);
        RelNode input = sort.getInput();
        RelNode convertedInput = convert(input,
                input.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL).simplify());
        int splitCount = VoltRelUtil.decideSplitCount(convertedInput);

        return new VoltDBPSort(
                sort.getCluster(),
                traits.plus(sort.getCollation()),
                convertedInput,
                sort.getCollation(),
                splitCount);
    }
}
