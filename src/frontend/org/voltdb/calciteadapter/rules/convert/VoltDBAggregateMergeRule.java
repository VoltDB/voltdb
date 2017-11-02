/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter.rules.convert;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.voltdb.calciteadapter.VoltDBConvention;
import org.voltdb.calciteadapter.rel.LogicalAggregateMerge;
import org.voltdb.calciteadapter.rel.VoltDBAggregate;

public class VoltDBAggregateMergeRule extends ConverterRule {

        public static final VoltDBAggregateMergeRule INSTANCE = new VoltDBAggregateMergeRule();

        VoltDBAggregateMergeRule() {
            super(
                    LogicalAggregateMerge.class,
                    Convention.NONE,
                    VoltDBConvention.INSTANCE,
                    "VoltDBAggregateMergeRule");
        }

        @Override public RelNode convert(RelNode rel) {
            LogicalAggregateMerge aggr = (LogicalAggregateMerge) rel;
            RelNode input = aggr.getInput();
            if (!(input.getConvention() instanceof VoltDBConvention)) {
                input =
                        convert(
                                input,
                                input.getTraitSet()
                                .replace(VoltDBConvention.INSTANCE));
            }

            final RelOptCluster cluster = aggr.getCluster();
            final RelTraitSet traitSet = aggr.getTraitSet().replace(VoltDBConvention.INSTANCE);
            RelNode newRel = new VoltDBAggregate(
                  cluster,
                  traitSet,
                  input,
                  aggr.indicator,
                  aggr.getGroupSet(),
                  aggr.getGroupSets(),
                  aggr.getAggCallList(),
                  aggr.getPostPredicate());

            return newRel;
          }
      }