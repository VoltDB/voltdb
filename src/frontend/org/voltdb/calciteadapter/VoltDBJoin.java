package org.voltdb.calciteadapter;

import java.util.Set;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.types.JoinType;

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
        assert program == null;
        m_program = program;
        //rowType = m_program.getOutputRowType();
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
        NestLoopPlanNode nlpn = new NestLoopPlanNode();

        AbstractPlanNode lch = ((VoltDBRel)getInput(0)).toPlanNode();
        AbstractPlanNode rch = ((VoltDBRel)getInput(1)).toPlanNode();
        nlpn.addAndLinkChild(lch);
        nlpn.addAndLinkChild(rch);
        int numLhsFields = getInput(0).getRowType().getFieldCount();
        nlpn.setJoinPredicate(RexConverter.convertJoinPred(numLhsFields, getCondition()));

        assert m_program == null;
        NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(getInput(0).getRowType());
        schema = schema.join(RexConverter.convertToVoltDBNodeSchema(getInput(1).getRowType()));
        nlpn.setOutputSchemaPreInlineAgg(schema);
        nlpn.setOutputSchema(schema);
        nlpn.setJoinType(JoinType.INNER);

        return nlpn;
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
            RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new VoltDBJoin(getCluster(),
                getTraitSet(), left, right, conditionExpr,
                variablesSet, joinType, m_program);
       }

    public RelNode copy(RexProgram program) {
        return new VoltDBJoin(
                getCluster(),
                getTraitSet(),
                left,
                right,
                getCondition(),
                getVariablesSet(),
                getJoinType(),
                program
                );
    }



}
