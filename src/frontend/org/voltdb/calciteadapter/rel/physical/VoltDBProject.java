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

package org.voltdb.calciteadapter.rel.physical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.voltdb.calciteadapter.converter.RexConverter;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.ProjectionPlanNode;

import com.google.common.base.Supplier;

public class VoltDBProject extends Project implements VoltDBPhysicalRel {

    public VoltDBProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traitSet, input, projects, rowType);
        assert traitSet.contains(VoltDBPhysicalRel.VOLTDB_PHYSICAL);
    }

        /** Creates an VoltDBProject, specifying row type rather than field
         * names. */
        public static VoltDBProject create(final RelNode input,
            final List<? extends RexNode> projects, RelDataType rowType) {
          final RelOptCluster cluster = input.getCluster();
          final RelMetadataQuery mq = RelMetadataQuery.instance();
          final RelTraitSet traitSet =
              cluster.traitSet().replace(VoltDBPhysicalRel.VOLTDB_PHYSICAL)
                  .replaceIfs(RelCollationTraitDef.INSTANCE,
                      new Supplier<List<RelCollation>>() {
                        @Override
                        public List<RelCollation> get() {
                          return RelMdCollation.project(mq, input, projects);
                        }
                      });
          return new VoltDBProject(cluster, traitSet, input, projects, rowType);
        }

        @Override
        public VoltDBProject copy(RelTraitSet traitSet, RelNode input,
            List<RexNode> projects, RelDataType rowType) {
          return new VoltDBProject(getCluster(), traitSet, input,
              projects, rowType);
        }

        public VoltDBProject copy() {
            return new VoltDBProject(
                    getCluster(),
                    getTraitSet(),
                    getInput(),
                    getProjects(),
                    deriveRowType());
          }

        @Override
        public AbstractPlanNode toPlanNode() {
            AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
            NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(getNamedProjects());
            ProjectionPlanNode ppn = new ProjectionPlanNode(schema);
            ppn.addAndLinkChild(child);
            return ppn;
        }
}
