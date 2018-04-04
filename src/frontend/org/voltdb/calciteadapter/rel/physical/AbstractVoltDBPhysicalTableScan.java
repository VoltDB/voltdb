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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.converter.RexConverter;
import org.voltdb.calciteadapter.rel.AbstractVoltDBTableScan;
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;

public abstract class AbstractVoltDBPhysicalTableScan extends AbstractVoltDBTableScan implements VoltDBPhysicalRel {

    public static final int MAX_TABLE_ROW_COUNT = 1000000;

    protected final RexProgram m_program;

    protected RexNode m_offset = null;
    protected RexNode m_limit = null;

    protected AbstractVoltDBPhysicalTableScan(RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            VoltDBTable voltDBTable,
            RexProgram program,
            RexNode offset,
            RexNode limit) {
          super(cluster, traitSet.plus(VoltDBPhysicalRel.VOLTDB_PHYSICAL), table, voltDBTable);
          assert(program != null);
          m_program = program;
          m_offset = offset;
          m_limit = limit;
    }

    public RexProgram getProgram() {
        return m_program;
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
        String dg = super.computeDigest();
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
            RelDataType rowDataType = m_program.getOutputRowType();
            if (rowDataType.getFieldCount() > 0) {
                return rowDataType;
            } else {
                return table.getRowType();
            }
        }
      }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
            RelMetadataQuery mq) {
        double dRows = estimateRowCount(mq);
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        RelOptCost cost = planner.getCostFactory().makeCost(dRows, dCpu, dIo);
        return cost;
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

    public abstract RelNode copy(RelTraitSet traitSet, RexNode offset, RexNode limit);

    public abstract RelNode copy(RelTraitSet traitSet, RexProgram program, RexBuilder rexBuilder);

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

    protected double estimateRowCountWithLimit(double rowCount) {
        if (m_limit != null) {
            int limitInt = getLimit();
            if (limitInt == -1) {
                // If Limit ?, it's likely to be a small number. So pick up 50 here.
                limitInt = 50;
            }

            rowCount = Math.min(rowCount, limitInt);

            if ((m_program == null || m_program.getCondition() == null) && m_offset == null) {
                rowCount = limitInt;
            }
        }
        return rowCount;
    }

    protected double estimateRowCountWithPredicate(double rowCount) {
        if (m_program != null && m_program.getCondition() != null) {
            double discountFactor = 1.0;
            // Eliminated filters discount the cost of processing tuples with a rapidly
            // diminishing effect that ranges from a discount of 0.9 for one skipped filter
            // to a discount approaching 0.888... (=8/9) for many skipped filters.
            final double MAX_PER_POST_FILTER_DISCOUNT = 0.1;
            // Avoid applying the discount to an initial tie-breaker value of 2 or 3
            int condSize = RelOptUtil.conjunctions(m_program.getCondition()).size();
            for (int i = 0; i < condSize; ++i) {
                discountFactor -= Math.pow(MAX_PER_POST_FILTER_DISCOUNT, i + 1);
            }
            if (discountFactor < 1.0) {
                rowCount *= discountFactor;
                if (rowCount < 4) {
                    rowCount = 4;
                }
            }
        }
        return rowCount;
    }
}
