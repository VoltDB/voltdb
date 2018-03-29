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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

public class VoltDBLogicalCalc extends Calc implements VoltDBLogicalRel {

    public VoltDBLogicalCalc(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RexProgram program) {
          super(cluster,
                  traitSet,
                  input,
                  program);
          assert traitSet.contains(VoltDBLogicalRel.VOLTDB_LOGICAL);
        }

        @Override
        public Calc copy(RelTraitSet traitSet, RelNode child,
                RexProgram program) {
            return new VoltDBLogicalCalc(this.getCluster(), traitSet, child, program);
        }
}
