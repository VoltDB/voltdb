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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalSort;
import org.voltdb.calciteadapter.VoltDBConvention;
import org.voltdb.calciteadapter.rel.VoltDBSort;

public class VoltDBSortRule extends ConverterRule {

        public static final VoltDBSortRule INSTANCE = new VoltDBSortRule();

        VoltDBSortRule() {
            super(
                    LogicalSort.class,
                    Convention.NONE,
                    VoltDBConvention.INSTANCE,
                    "VoltDBSortRule");
        }

        @Override public RelNode convert(RelNode rel) {
            LogicalSort sort = (LogicalSort) rel;
            List<RelNode> newInputs = new ArrayList<>();
            RelNode input = sort.getInput();
            if (!(input.getConvention() instanceof VoltDBConvention)) {
                input =
                        convert(
                                input,
                                input.getTraitSet()
                                .replace(VoltDBConvention.INSTANCE));
            }
            newInputs.add(input);

            final RelOptCluster cluster = sort.getCluster();
            final RelTraitSet traitSet =
                sort.getTraitSet().replace(VoltDBConvention.INSTANCE);
            RelNode newRel = new VoltDBSort(
                  cluster,
                  traitSet,
                  input,
                  sort.getCollation(),
                  sort.offset,
                  sort.fetch);

            return newRel;
          }
      }