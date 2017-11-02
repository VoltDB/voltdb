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

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * The purpose of this RelNode is to keep together adjacent LogicalFilter and LogicalAggregate nodes
 * representing an aggregate and its HAVING expression to be converted to a single VoltDBAggregateNode
 *
 */
public class LogicalAggregateMerge extends Aggregate {

    private RexNode m_postPredicate = null;

    /** Constructor */
    public LogicalAggregateMerge(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode postPredicate) {
      super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
      m_postPredicate = postPredicate;
    }

    public LogicalAggregateMerge(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            ImmutableBitSet groupSet,
            List<AggregateCall> aggCalls) {
      this(cluster, traitSet, child, false, groupSet, null, aggCalls, null);
    }

    public LogicalAggregateMerge(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            ImmutableBitSet groupSet,
            List<AggregateCall> aggCalls,
            RexNode havingExpressions) {
      this(cluster, traitSet, child, false, groupSet, null, aggCalls, havingExpressions);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        if (m_postPredicate != null) {
            pw.item("having", m_postPredicate);
        }
        return pw;
    }

    @Override
    protected String computeDigest() {
        String d = super.computeDigest();
        if (m_postPredicate != null) {
            d += m_postPredicate.toString();
        }
        return d;
    }

    @Override
    public LogicalAggregateMerge copy(RelTraitSet traitSet, RelNode input,
            boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return LogicalAggregateMerge.create(getCluster(), traitSet, input, indicator, groupSet, aggCalls, m_postPredicate);
    }

    public void setPostPredicate(RexNode postPredicate) {
        m_postPredicate = postPredicate;
    }

    public RexNode getPostPredicate() {
        return m_postPredicate;
    }

    public static LogicalAggregateMerge create(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<AggregateCall> aggCalls,
            RexNode postPredicate) {
        return new LogicalAggregateMerge(cluster, traitSet, child, groupSet, aggCalls, postPredicate);
    }

}
