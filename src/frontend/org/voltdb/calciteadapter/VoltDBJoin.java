package org.voltdb.calciteadapter;

import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.plannodes.AbstractPlanNode;

public class VoltDBJoin extends Join implements VoltDBRel {

    final RexProgram m_program;

    protected VoltDBJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        m_program = null;
    }

    protected VoltDBJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType,
            RexProgram program) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        m_program = program;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        if (m_program != null) {
            m_program.explainCalc(pw);
        }
        return pw;
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        assert false;
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
            RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        assert false;
        return null;
    }

    public RelNode copy(RexProgram program) {
        return new VoltDBJoin(
                getCluster(),
                getTraitSet(),
                left.copy(left.getTraitSet(), left.getInputs()),
                right.copy(left.getTraitSet(), left.getInputs()),
                getCondition(),
                getVariablesSet(),
                getJoinType(),
                program
                );
    }



}
