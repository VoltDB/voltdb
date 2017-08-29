/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter.rel;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.voltdb.calciteadapter.RexConverter;
import org.voltdb.calciteadapter.VoltDBConvention;
import org.voltdb.calciteadapter.VoltDBTable;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;

public abstract class AbstractVoltDBTableScan extends TableScan implements VoltDBRel {

    public static final int MAX_TABLE_ROW_COUNT = 1000000;

    protected final VoltDBTable m_voltDBTable;
    protected final RexProgram m_program;

    protected RexNode m_offset = null;
    protected RexNode m_limit = null;

    protected AbstractVoltDBTableScan(RelOptCluster cluster, RelOptTable table,
            VoltDBTable voltDBTable) {
          this(cluster, table, voltDBTable,
                  RexProgram.createIdentity(voltDBTable.getRowType(cluster.getTypeFactory())));
    }

    protected AbstractVoltDBTableScan(RelOptCluster cluster, RelOptTable table,
            VoltDBTable voltDBTable, RexProgram program) {
          super(cluster, cluster.traitSetOf(VoltDBConvention.INSTANCE), table);
          this.m_voltDBTable = voltDBTable;
          m_program = program;
    }

    protected AbstractVoltDBTableScan(RelOptCluster cluster, RelOptTable table,
            VoltDBTable voltDBTable, RexProgram program, RexNode limit, RexNode offset) {
          super(cluster, cluster.traitSetOf(VoltDBConvention.INSTANCE), table);
          this.m_voltDBTable = voltDBTable;
          m_program = program;
          m_limit = limit;
          m_offset = offset;
    }

    public RexProgram getProgram() {
        return m_program;
    }

    public void setOffset(RexNode offset) {
        m_offset = offset;
    }

    public void setLimit(RexNode limit) {
        m_limit = limit;
    }

    /**
     * The digest needs to be updated because Calcite considers any two nodes with the same digest
     * to be identical.
     */
    @Override
    protected String computeDigest() {
        // Make an instance of the scan unique for Calcite to be able to distinguish them
        // specially when we merge scans with other redundant nodes like sort for example.
        // Are there better ways of doing this?
        String dg = id + "_" + super.computeDigest();
        if (m_program != null) {
            dg += "_program_" + m_program.toString();
        }
        if (m_limit != null) {
            dg += "_limit_" + Integer.toString(getLimit());
        }
        if (m_offset != null) {
            dg += "_offset_" + Integer.toString(getOffset());
        }
        return dg;
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

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
            RelMetadataQuery mq) {
        double dRows = estimateRowCount(mq);
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        RexLocalRef cond = m_program.getCondition();
        if (cond != null) {
            dRows *=  0.9;
            }
        RelOptCost cost = planner.getCostFactory().makeCost(dRows, dCpu, dIo);
        return cost;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double dRows = super.estimateRowCount(mq);
        RexLocalRef cond = m_program.getCondition();
        if (cond != null) {
            dRows *=  0.2;
            }
        if (m_limit != null) {
            double limit = getLimit();
            dRows = Math.min(limit, dRows);
        }
        return dRows;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        if (m_program != null) {
            m_program.explainCalc(pw);
        }
        if (m_limit != null) {
            pw.item("limit", m_limit);
        }
        if (m_offset != null) {
            pw.item("offset", m_offset);
        }
        return pw;
    }

    public RexNode getLimitRexNode() {
        return m_limit;
    }

    protected int getLimit() {
        if (m_limit != null) {
            return RexLiteral.intValue(m_limit);
        } else {
            return Integer.MAX_VALUE;
        }
    }

    public RexNode getOffsetRexNode() {
        return m_offset;
    }

    protected int getOffset() {
        if (m_offset != null) {
            return RexLiteral.intValue(m_offset);
        } else {
            return 0;
        }
    }

    protected void addPredicate(AbstractScanPlanNode scan) {
        RexLocalRef condition = m_program.getCondition();
        if (condition != null) {
            List<AbstractExpression> predList = new ArrayList<>();
            predList.add(RexConverter.convert(m_program.expandLocalRef(condition)));
            scan.setPredicate(predList);
        }
    }

    protected void addLimitOffset(AbstractScanPlanNode scan) {
        if (m_limit != null || m_offset != null) {
            LimitPlanNode limitPlanNode = new LimitPlanNode();
            if (m_limit != null) {
                int limit = getLimit();
                limitPlanNode.setLimit(limit);
            }
            if (m_offset != null) {
                int offset = RexLiteral.intValue(m_offset);
                limitPlanNode.setOffset(offset);
            }
            scan.addInlinePlanNode(limitPlanNode);
        }
    }

    protected void addProjection(AbstractScanPlanNode scan) {
        assert m_program != null;

        ProjectionPlanNode ppn = new ProjectionPlanNode();
        ppn.setOutputSchemaWithoutClone(RexConverter.convertToVoltDBNodeSchema(m_program));
        scan.addInlinePlanNode(ppn);
    }

    public static RelNode copy(AbstractVoltDBTableScan relScan, RexProgram program, RexBuilder programRexBuilder) {
        RexProgram newProgram;
        if (relScan.m_program == null) {
            newProgram = program;
        } else {
            // Merge two programs program / m_program into a new merged program
            newProgram = RexProgramBuilder.mergePrograms(
                            program,
                            relScan.m_program,
                            programRexBuilder);
            assert(newProgram.getOutputRowType().equals(program.getOutputRowType()));
        }

        if (relScan instanceof VoltDBTableSeqScan) {
            // Sequential Scan
            return new VoltDBTableSeqScan(
                    relScan.getCluster(),
                    relScan.getTable(),
                    relScan.getVoltDBTable(),
                    newProgram,
                    relScan.m_limit,
                    relScan.m_offset);
        } else {
            // Index Scan
            assert(relScan instanceof VoltDBTableIndexScan);
            VoltDBTableIndexScan indexScan = (VoltDBTableIndexScan) relScan;
            return new VoltDBTableIndexScan(
                    indexScan.getCluster(),
                    indexScan.getTable(),
                    indexScan.getVoltDBTable(),
                    newProgram,
                    indexScan.getIndex(),
                    indexScan.getAccessPath(),
                    indexScan.m_limit,
                    indexScan.m_offset);
        }
    }
}
