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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * The purpose of this RelNode is to keep together adjacent LogicalFilter and LogicalAggregate nodes
 * representing an aggregate and its HAVING expression to be converted to a single VoltDBAggregateNode
 *
 */
public class LogicalAggregateMerge extends Aggregate {

    final private RexNode m_postPredicate;
    // Is this aggregate relation is at a coordinator of a fragment node for a distributed query?
    // Initially, this indicator is set to FALSE when Calcite creates the LogicalAggregate ant is it
    // flipped to TRUE when a Send node is pulled up through the aggregate
    final private boolean m_coordinatorAggregate;

    /** Constructor */
    private LogicalAggregateMerge(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode postPredicate,
            boolean coordinatorAggregate) {
      super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
      m_postPredicate = postPredicate;
      m_coordinatorAggregate = coordinatorAggregate;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("coordinator", m_coordinatorAggregate);
        if (m_postPredicate != null) {
            pw.item("having", m_postPredicate);
        }
        return pw;
    }

    @Override
    protected String computeDigest() {
        String d = super.computeDigest();
        d += Boolean.toString(m_coordinatorAggregate);
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
        return LogicalAggregateMerge.create(
                getCluster(),
                traitSet,
                input,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                m_postPredicate,
                m_coordinatorAggregate);
    }

    public RexNode getPostPredicate() {
        return m_postPredicate;
    }

    public boolean isCoordinatorPredicate() {
        return m_coordinatorAggregate;
    }

    public static LogicalAggregateMerge create(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode postPredicate,
            boolean coordinatorAggregate) {
        return new LogicalAggregateMerge(
                cluster,
                traitSet,
                child,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                postPredicate,
                coordinatorAggregate);
    }

    public static LogicalAggregateMerge createFrom(
            Aggregate aggregate,
            RelNode child,
            RexNode postPredicate) {
        boolean coordinatorAggregate = false;
        RexNode combinedPostPredicate = postPredicate;
        if (aggregate instanceof LogicalAggregateMerge) {
            coordinatorAggregate = ((LogicalAggregateMerge) aggregate).isCoordinatorPredicate();
            if (postPredicate != null && ((LogicalAggregateMerge) aggregate).getPostPredicate() != null) {
                List<RexNode> combinedConditions = new ArrayList<>();
                combinedConditions.add(((LogicalAggregateMerge) aggregate).getPostPredicate());
                combinedConditions.add(postPredicate);
                combinedPostPredicate = RexUtil.composeConjunction(aggregate.getCluster().getRexBuilder(), combinedConditions, true);
            } else {
                combinedPostPredicate = (postPredicate == null) ?
                        ((LogicalAggregateMerge) aggregate).getPostPredicate() : postPredicate;
            }
        }
        return LogicalAggregateMerge.create(
                aggregate.getCluster(),
                aggregate.getTraitSet(),
                child,
                aggregate.indicator,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList(),
                combinedPostPredicate,
                coordinatorAggregate);
    }

    public static LogicalAggregateMerge createFrom(
            Aggregate aggregate,
            RelNode child,
            boolean coordinatorAggregate) {
        RexNode postPredicate = (aggregate instanceof LogicalAggregateMerge) ?
                ((LogicalAggregateMerge) aggregate).getPostPredicate() : null;
        return LogicalAggregateMerge.create(
                        aggregate.getCluster(),
                        aggregate.getTraitSet(),
                        child,
                        aggregate.indicator,
                        aggregate.getGroupSet(),
                        aggregate.getGroupSets(),
                        aggregate.getAggCallList(),
                        postPredicate,
                        coordinatorAggregate);
    }

    public static LogicalAggregateMerge createFrom(
            Aggregate aggregate,
            RelNode child,
            RelCollation collation) {
        RexNode postPredicate = (aggregate instanceof LogicalAggregateMerge) ?
                ((LogicalAggregateMerge) aggregate).getPostPredicate() : null;
        LogicalAggregateMerge newAggregate = LogicalAggregateMerge.create(
                        aggregate.getCluster(),
                        aggregate.getTraitSet(),
                        child,
                        aggregate.indicator,
                        aggregate.getGroupSet(),
                        aggregate.getGroupSets(),
                        aggregate.getAggCallList(),
                        postPredicate,
                        false);
        newAggregate.traitSet = newAggregate.getTraitSet().replace(collation);
        return newAggregate;
    }

}
