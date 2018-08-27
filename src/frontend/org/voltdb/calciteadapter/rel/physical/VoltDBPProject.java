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
import java.util.function.Supplier;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.voltdb.calciteadapter.converter.RexConverter;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.ProjectionPlanNode;

public class VoltDBPProject extends Project implements VoltDBPRel {

    private final int m_splitCount;

    public VoltDBPProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType,
            int splitCount) {
        super(cluster, traitSet, input, projects, rowType);
        assert traitSet.contains(VoltDBPRel.VOLTDB_PHYSICAL);
        m_splitCount = splitCount;
    }

        /** Creates an VoltDBProject, specifying row type rather than field
         * names. */
        public static VoltDBPProject create(
                final RelTraitSet traits,
                final RelNode input,
                final List<? extends RexNode> projects,
                final RelDataType rowType) {
          final RelOptCluster cluster = input.getCluster();
          final RelMetadataQuery mq = RelMetadataQuery.instance();
          final RelTraitSet traitSet =
                  traits.replaceIfs(RelCollationTraitDef.INSTANCE,
                      new Supplier<List<RelCollation>>() {
                        @Override
                        public List<RelCollation> get() {
                          return RelMdCollation.project(mq, input, projects);
                        }
                      });
          return new VoltDBPProject(cluster, traitSet, input, projects, rowType, 1);
        }

        @Override
        public VoltDBPProject copy(RelTraitSet traitSet, RelNode input,
            List<RexNode> projects, RelDataType rowType) {
          return new VoltDBPProject(
                  getCluster(),
                  traitSet,
                  input,
                  projects,
                  rowType,
                  m_splitCount);
        }

        public VoltDBPProject copy() {
            return new VoltDBPProject(
                    getCluster(),
                    getTraitSet(),
                    getInput(),
                    getProjects(),
                    deriveRowType(),
                    m_splitCount);
          }

        @Override
        public AbstractPlanNode toPlanNode() {
            AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
            NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(getNamedProjects());
            ProjectionPlanNode ppn = new ProjectionPlanNode(schema);
            ppn.addAndLinkChild(child);
            return ppn;
        }

        @Override
        public int getSplitCount() {
            return m_splitCount;
        }

        @Override
        protected String computeDigest() {
            String digest = super.computeDigest();
            digest += "_split_" + m_splitCount;
            return digest;
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            super.explainTerms(pw);
            pw.item("split", m_splitCount);
            return pw;
        }

}
