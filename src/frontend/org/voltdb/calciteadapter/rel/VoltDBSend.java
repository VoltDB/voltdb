package org.voltdb.calciteadapter.rel;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.VoltDBPartitioning;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SendPlanNode;

public class VoltDBSend extends Project implements VoltDBRel {

	private RelDataType m_rowType;
	private List<? extends RexNode> m_identities;
	private VoltDBPartitioning m_partitioning;
	private double m_costFactor = 100000.;

    public VoltDBSend(
	            RelOptCluster cluster,
	            RelTraitSet traitSet,
	            RelNode childNode,
	            List<? extends RexNode> identities,
	            RelDataType rowType,
	            VoltDBPartitioning partitioning) {
        this(cluster, traitSet, childNode, identities, rowType, partitioning, 1.0);
    }

   public VoltDBSend(
    		RelOptCluster cluster,
    	    RelTraitSet traitSet,
    	    RelNode childNode,
    	    List<? extends RexNode> identities,
    	    RelDataType rowType,
    	    VoltDBPartitioning partitioning,
    	    double costFactor) {
        super(cluster, traitSet, childNode, identities, rowType);
        m_rowType = rowType;
        m_identities = identities;
        m_partitioning = partitioning;
        m_costFactor *= costFactor;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        return pw;
    }

    @Override 
    public RelOptCost computeSelfCost(RelOptPlanner planner,
            RelMetadataQuery mq) {
//        double rowCount = getInput().estimateRowCount(mq) * m_costFactor;
        return planner.getCostFactory().makeCost(m_costFactor, 0, 0);
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        SendPlanNode nlpn = new SendPlanNode();

        AbstractPlanNode child = ((VoltDBRel)getInput(0)).toPlanNode();
        nlpn.addAndLinkChild(child);

        // Should set output schema here ???

        return nlpn;
    }

    
    public static VoltDBSend convert(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, 
    		List<? extends RexNode> identities,
    	    RelDataType rowType,
    	    VoltDBPartitioning partitioning,
    	    double costFactor) {
        return new VoltDBSend(cluster, traitSet, child, identities, rowType, partitioning, costFactor);
       }

    public RelNode copy(List<RexNode> exprs, RelDataType rowType) {
        // Create a new LogicalProject that will be below the send node
        LogicalProject proj = LogicalProject.create(input, exprs, rowType);
        return convert(getCluster(), traitSet, proj, exprs, rowType, m_partitioning, m_costFactor);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input,
            List<RexNode> projects, RelDataType rowType) {
        return convert(getCluster(), traitSet, input, m_identities, rowType, m_partitioning, m_costFactor);
    }

    public RelNode copy(RexProgram program) {

        //@TODO Calcite - properly push down program including conditions.
        RelDataType rowType = program.getOutputRowType();
        List<RexNode> exprs = new ArrayList<>();
        for (RelDataTypeField fieldExpr : rowType.getFieldList()) {
            int index = fieldExpr.getIndex();
            RelDataType dataType = fieldExpr.getType();
            RexInputRef expr = new RexInputRef(index, dataType);
            exprs.add(expr);
        }
        VoltDBSend newSend = new VoltDBSend(getCluster(), getTraitSet(), getInput(), exprs, rowType, m_partitioning);
        return newSend;
    }

    public RelNode copy(SingleRel input) {
        VoltDBSend newSend = new VoltDBSend(getCluster(), getTraitSet(), input, m_identities, m_rowType, m_partitioning);

        return newSend;
    }
 
    public List<? extends RexNode> getIdentities() {
        return m_identities;
    }

    public VoltDBPartitioning getPartitioning() {
        return m_partitioning;
    }

 }
