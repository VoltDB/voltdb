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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

public class VoltDBLogicalProject extends Project implements VoltDBLogicalRel {

    public VoltDBLogicalProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
          super(cluster,
                  traitSet,
                  input,
                  projects,
                  rowType);
          assert traitSet.contains(VoltDBLogicalRel.VOLTDB_LOGICAL);
        }

        @Override
        public VoltDBLogicalProject copy(RelTraitSet traitSet, RelNode input,
            List<RexNode> projects, RelDataType rowType) {
          return new VoltDBLogicalProject(getCluster(),
                  traitSet.plus(VoltDBLogicalRel.VOLTDB_LOGICAL),
                  input,
                  projects,
                  rowType);
        }

        public VoltDBLogicalProject copy() {
              return new VoltDBLogicalProject(
                      getCluster(),
                      getTraitSet(),
                      getInput(),
                      getProjects(),
                      deriveRowType());
            }
}
