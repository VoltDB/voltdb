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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.voltdb.calciteadapter.converter.RexConverter;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SendPlanNode;

public abstract class AbstractVoltDBPExchange extends Exchange implements VoltDBPRel {

    // Exchange's split count is always one
    protected final int m_splitCount = 1;
    // This is a split count of the exchange's input
    protected final int m_childSplitCount;

    private int m_level = 1;

    protected AbstractVoltDBPExchange(RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelDistribution distribution,
            int childSplitCount) {
        super(cluster, traitSet, input, distribution);
        m_childSplitCount = childSplitCount;
    }

    protected AbstractPlanNode toPlanNode(AbstractPlanNode epn) {
        SendPlanNode spn = new SendPlanNode();
        epn.addAndLinkChild(spn);

        AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
        spn.addAndLinkChild(child);

        // Generate output schema
        NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(getInput().getRowType());
        epn.setOutputSchema(schema);
        epn.setHaveSignificantOutputSchema(true);
        return epn;
    }

    @Override
    protected String computeDigest() {
        String digest = super.computeDigest();
        digest += "_level_" + m_level;
        return digest;
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }

    public int getChildSplitCount() {
        return m_childSplitCount;
    }

    public void setLevel(int level) {
        m_level = level;
    }

    public int getLevel() {
        return m_level;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rowCount = super.estimateRowCount(mq);
        // The total count is multiplied by the child's split count
        return rowCount * m_childSplitCount;
    }

    @Override
    public AbstractVoltDBPExchange copy(RelTraitSet traitSet, RelNode newInput,
            RelDistribution newDistribution) {
        return copy(
                traitSet,
                newInput,
                newDistribution,
                getLevel());
    }

    public abstract AbstractVoltDBPExchange copy(
            RelTraitSet traitSet,
            RelNode newInput,
            RelDistribution newDistribution,
            int level);

}
