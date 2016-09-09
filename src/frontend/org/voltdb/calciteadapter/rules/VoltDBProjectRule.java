package org.voltdb.calciteadapter.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.voltdb.calciteadapter.VoltDBConvention;
import org.voltdb.calciteadapter.rel.VoltDBProject;

// unneeded for now???
class VoltDBProjectRule extends ConverterRule {

    static final VoltDBProjectRule INSTANCE = new VoltDBProjectRule();

    private VoltDBProjectRule() {
      super(LogicalProject.class, RelOptUtil.PROJECT_PREDICATE, Convention.NONE,
          VoltDBConvention.INSTANCE, "VoltDBProjectRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      return VoltDBProject.create(
          convert(project.getInput(),
              project.getInput().getTraitSet()
                  .replace(VoltDBConvention.INSTANCE)),
          project.getProjects(),
          project.getRowType());
    }
  }