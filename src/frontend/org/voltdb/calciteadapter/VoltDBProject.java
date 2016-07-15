package org.voltdb.calciteadapter;

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
import org.apache.calcite.util.Util;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;

import com.google.common.base.Supplier;

class VoltDBProject extends Project implements VoltDBRel {

    public VoltDBProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
          super(cluster, traitSet, input, projects, rowType);
          assert getConvention() instanceof VoltDBConvention;
        }

        @Deprecated // to be removed before 2.0
        public VoltDBProject(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode input, List<? extends RexNode> projects, RelDataType rowType,
            int flags) {
          this(cluster, traitSet, input, projects, rowType);
          Util.discard(flags);
        }

        /** Creates an VoltDBProject, specifying row type rather than field
         * names. */
        public static VoltDBProject create(final RelNode input,
            final List<? extends RexNode> projects, RelDataType rowType) {
          final RelOptCluster cluster = input.getCluster();
          final RelMetadataQuery mq = RelMetadataQuery.instance();
          final RelTraitSet traitSet =
              cluster.traitSet().replace(VoltDBConvention.INSTANCE)
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

        @Override
        public AbstractPlanNode toPlanNode() {
            AbstractPlanNode child = ((VoltDBRel)getInput(0)).toPlanNode();
            ProjectionPlanNode ppn = new ProjectionPlanNode();
            ppn.setOutputSchema(RexConverter.convertToVoltDBNodeSchema(getNamedProjects()));
            ppn.addAndLinkChild(child);
            return ppn;
        }
}
