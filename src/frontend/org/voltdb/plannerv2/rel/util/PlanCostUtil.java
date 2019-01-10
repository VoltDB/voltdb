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

package org.voltdb.plannerv2.rel.util;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rex.RexProgram;

/**
 * Helper utilities to determine Plan Cost.
 */
public final class PlanCostUtil {
    private static final double MAX_PER_POST_FILTER_DISCOUNT = 0.1;
    private static final double MAX_PER_COLLATION_DISCOUNT = 0.1;

    private PlanCostUtil() {
    }

    public static double discountRowCountTableScan(double rowCount, RexProgram program) {
        if (program != null && program.getCondition() != null) {
            double discountFactor = 1.0;
            // Eliminated filters discount the cost of processing tuples with a rapidly
            // diminishing effect that ranges from a discount of 0.9 for one skipped filter
            // to a discount approaching 0.888... (=8/9) for many skipped filters.

            // Avoid applying the discount to an initial tie-breaker value of 2 or 3
            int condSize = RelOptUtil.conjunctions(program.getCondition()).size();
            for (int i = 0; i < condSize; ++i) {
                discountFactor -= Math.pow(MAX_PER_POST_FILTER_DISCOUNT, i + 1);
            }
            if (discountFactor < 1.0) {
                rowCount *= discountFactor;
                if (rowCount < 4) {
                    rowCount = 4;
                }
            }
        }
        return rowCount;
    }

    public static double discountRowCountSerialAggregate(double rowCount, int groupCount) {
        // Give a discount to the Aggregate based on the number of the collation fields.
        //  - Serial Aggregate - the collation size is equal to the number of the GROUP BY columns
        //          and max discount 1 - 0.1 -  0.01 - 0.001 - ...
        //  - Partial Aggregate - anything in between
        // The required order will be enforced by some index which collation would match / satisfy
        // the aggregate's collation. If a table has more than one index multiple Aggregate / IndexScan
        // combinations are possible and we want to pick the one that has the maximum GROUP BY columns
        // covered resulting in a more efficient aggregation (less hashing)
        double discountFactor = 1.0;

        for (int i = 0; i < groupCount; ++i) {
            discountFactor -= Math.pow(MAX_PER_COLLATION_DISCOUNT, i + 1);
        }
        rowCount *= discountFactor;
        return rowCount;
    }

    /**
     * Discourage Calcite from picking a plan with a Calc that have a RelDistributions.ANY
     * distribution trait.
     *
     * @param rowCount
     * @param traitSet
     * @return new rowCount
     */
    public static double adjustRowCountOnRelDistribution(double rowCount, RelTraitSet traitSet) {
        if (traitSet.getTrait(RelDistributionTraitDef.INSTANCE) != null &&
                RelDistributions.ANY.getType().equals(traitSet.getTrait(RelDistributionTraitDef.INSTANCE).getType())) {
            rowCount *= 10000;
        }
        return rowCount;
    }
}
