package org.voltdb.calciteadapter;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;

class VoltDBTableScan extends TableScan implements VoltDBRel {

    private final VoltDBTable m_voltDBTable;
    private final RexProgram m_program;

    protected VoltDBTableScan(RelOptCluster cluster, RelOptTable table,
            VoltDBTable voltDBTable) {
          super(cluster, cluster.traitSetOf(VoltDBConvention.INSTANCE), table);
          this.m_voltDBTable = voltDBTable;
          m_program = null;
    }
    protected VoltDBTableScan(RelOptCluster cluster, RelOptTable table,
            VoltDBTable voltDBTable, RexProgram program) {
          super(cluster, cluster.traitSetOf(VoltDBConvention.INSTANCE), table);
          this.m_voltDBTable = voltDBTable;
          m_program = program;
    }

    public VoltDBTable getVoltDBTable() {
        return m_voltDBTable;
    }

    @Override public RelDataType deriveRowType() {
        if (m_program == null) {
            return table.getRowType();
        }
        else {
            return m_program.getOutputRowType();
        }
      }

    @Override
    public AbstractPlanNode toPlanNode() {
        // Start with the inlined project.
        assert m_program != null;

        ProjectionPlanNode ppn = new ProjectionPlanNode();
        ppn.setOutputSchema(RexConverter.convertToVoltDBNodeSchema(m_program));

        SeqScanPlanNode sspn = new SeqScanPlanNode();
        List<String> qualName = table.getQualifiedName();
        sspn.setTargetTableAlias(qualName.get(0));
        sspn.setTargetTableName(m_voltDBTable.m_catTable.getTypeName());
        List<AbstractExpression> predList = new ArrayList<>();
        RexLocalRef condition = m_program.getCondition();
        if (condition != null) {
            predList.add(RexConverter.convert(m_program.expandLocalRef(condition)));
        }
        sspn.setPredicate(predList);

        sspn.addInlinePlanNode(ppn);
        return sspn;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        if (m_program != null) {
            m_program.explainCalc(pw);
        }
        return pw;
    }

    public RelNode copy(RexProgram program) {
        VoltDBTableScan newScan = new VoltDBTableScan(getCluster(), getTable(), m_voltDBTable, program);

        return newScan;
    }


}
