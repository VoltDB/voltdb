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

import com.google_voltpatches.common.base.Preconditions;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.rel.AbstractVoltDBTableScan;
import org.voltdb.calciteadapter.rel.VoltTable;

/**
 * Abstract sub-class of {@link AbstractVoltDBTableScan}
 * target at {@link #VOLTDB_PHYSICAL} convention
 *
 * @author Michael Alexeev
 * @since 8.4
 */
public abstract class AbstractVoltDBPTableScan extends AbstractVoltDBTableScan implements VoltDBPRel {

    // TODO: verify this
    public static final int MAX_TABLE_ROW_COUNT = 1000000;

    // If Limit ?, it's likely to be a small number. So pick up 50 here.
    private static final int DEFAULT_LIMIT_VALUE_PARAMETERIZED = 50;

    private static final double MAX_PER_POST_FILTER_DISCOUNT = 0.1;

    protected final RexProgram m_program;
    protected final int m_splitCount;

    // Inline Rels
    protected RexNode m_offset;
    protected RexNode m_limit;
    protected RelNode m_aggregate;
    protected RelDataType m_preAggregateRowType;
    protected RexProgram m_preAggregateProgram;

    /**
     * Constructor.
     *
     * @param cluster Cluster
     * @param traitSet Traits
     * @param table The table definition
     * @param voltDBTable The target {@link VoltTable}
     * @param program Program
     * @param offset Offset
     * @param limit Limmit
     * @param aggregate Aggregate
     * @param preAggregateRowType The type of the rows returned by this relational expression before aggregation
     * @param preAggregateProgram The program before aggregation
     * @param splitCount Number of concurrent processes that this relational expression will be executed in
     */
    protected AbstractVoltDBPTableScan(RelOptCluster cluster,
                                       RelTraitSet traitSet,
                                       RelOptTable table,
                                       VoltTable voltDBTable,
                                       RexProgram program,
                                       RexNode offset,
                                       RexNode limit,
                                       RelNode aggregate,
                                       RelDataType preAggregateRowType,
                                       RexProgram preAggregateProgram,
                                       int splitCount) {
        super(cluster, traitSet.plus(VoltDBPRel.VOLTDB_PHYSICAL), table, voltDBTable);
        Preconditions.checkNotNull(program);
        Preconditions.checkArgument(aggregate == null || aggregate instanceof AbstractVoltDBPAggregate);
        Preconditions.checkArgument(program.getOutputRowType().getFieldCount() > 0);
        m_program = program;
        m_offset = offset;
        m_limit = limit;
        m_aggregate = aggregate;
        m_preAggregateRowType = preAggregateRowType;
        m_preAggregateProgram = preAggregateProgram;
        m_splitCount = splitCount;
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
        dg += "_split_" + m_splitCount;
        if (m_program != null) {
            dg += "_program_" + m_program.toString();
        }
        if (m_limit != null) {
            dg += "_limit_" + Integer.toString(getLimit());
        }
        if (m_offset != null) {
            dg += "_offset_" + Integer.toString(getOffset());
        }
        if (m_aggregate != null) {
            dg += "_aggr_" + m_aggregate.getDigest();
        }
        return dg;
    }

    public VoltTable getVoltTable() {
        return m_voltTable;
    }

    @Override
    public RelDataType deriveRowType() {
        if (m_program == null) {
            return table.getRowType();
        } else {
            RelDataType rowDataType = m_program.getOutputRowType();
            if (rowDataType.getFieldCount() > 0) {
                return rowDataType;
            } else {
                throw new IllegalStateException("Row count can not be 0.");
            }
        }
    }

    /**
     * Returns the cost of this plan (not including children).
     * We will consider the row count as estimateRowCount,
     * and the CPU cost as estimateRowCount+1.
     * The IO cost is always 0 cause we are in-memory.
     * The actual plan cost is depend on the planner implementation.
     *
     * @param planner Planner for cost calculation
     * @param mq      Metadata query
     * @return Cost of this plan (not including children)
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        double dRows = estimateRowCount(mq);
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("split", m_splitCount);
        if (m_program != null) {
            m_program.explainCalc(pw);
        }
        pw.itemIf("limit", m_limit, m_limit != null);
        pw.itemIf("offset", m_offset, m_offset != null);
        if (m_aggregate != null) {
            pw.item("aggregate", m_aggregate.getDigest());
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

    public RelNode getAggregateRelNode() {
        return m_aggregate;
    }

    public RelDataType getPreAggregateRowType() {
        return m_preAggregateRowType;
    }

    public RexProgram getPreAggregateProgram() {
        return m_preAggregateProgram;
    }

    /**
     * Returns a copy of this {@link AbstractVoltDBTableScan} but with a new
     * traitSet, limit and offset.
     */
    public abstract AbstractVoltDBTableScan copyWithLimitOffset(RelTraitSet traitSet, RexNode offset, RexNode limit);

    /**
     * Returns a copy of this {@link AbstractVoltDBTableScan} but with a new traitSet and program.
     */
    public abstract AbstractVoltDBTableScan copyWithProgram(RelTraitSet traitSet, RexProgram program, RexBuilder rexBuilder);

    /**
     * Returns a copy of this {@link AbstractVoltDBTableScan} but with a new traitSet and aggregate.
     */
    public abstract AbstractVoltDBTableScan copyWithAggregate(RelTraitSet traitSet, RelNode aggregate);

    protected double estimateRowCountWithLimit(double rowCount) {
        if (m_limit != null) {
            int limitInt = getLimit();
            // TODO: when could it be -1?
            if (limitInt == -1) {
                limitInt = DEFAULT_LIMIT_VALUE_PARAMETERIZED;
            }

            rowCount = Math.min(rowCount, limitInt);

            if ((m_program == null || m_program.getCondition() == null) && m_offset == null) {
                rowCount = limitInt;
            }
        }
        return rowCount;
    }

    private boolean hasLimitOffset() {
        return (m_limit != null || m_offset != null);
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }

}
