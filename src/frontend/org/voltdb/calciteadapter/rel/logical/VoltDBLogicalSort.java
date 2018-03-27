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

package org.voltdb.calciteadapter.rel.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

public class VoltDBLogicalSort extends Sort implements VoltDBLogicalRel {

    public VoltDBLogicalSort(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelCollation collation) {
            super(cluster, traitSet, input, collation, null, null);
            assert VoltDBLogicalRel.VOLTDB_LOGICAL.equals(getConvention());
        }

        @Override
        public VoltDBLogicalSort copy(RelTraitSet traitSet, RelNode input,
                RelCollation collation,
                RexNode offset,
                RexNode fetch) {
          return new VoltDBLogicalSort(getCluster(), traitSet, input, collation);
        }
}
