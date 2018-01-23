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

package org.voltdb.calciteadapter.rules.rel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.CompositeList;
import org.voltdb.calciteadapter.CalcitePlanningException;
import org.voltdb.calciteadapter.VoltDBConvention;
import org.voltdb.calciteadapter.rel.VoltDBAggregate;
import org.voltdb.calciteadapter.rel.VoltDBProject;
import org.voltdb.calciteadapter.rel.VoltDBSend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Transform Aggregate(fragment)/VoltDBSend expression to an equivalent one
 * Aggregate(coordinator)/VoltDBSend/Aggregate(fragment)
 *
 *
 */
public class VoltDBAggregateSendTransposeRule extends RelOptRule {

    public static final VoltDBAggregateSendTransposeRule INSTANCE = new VoltDBAggregateSendTransposeRule();

    // Aggregate functions that require transformation for a distributed query
    private static final Set<SqlKind> SEND_AGGR_FUNCTIONS =
            EnumSet.of(SqlKind.AVG, SqlKind.COUNT);

    private VoltDBAggregateSendTransposeRule() {
        // Match LogicalAggregate or VoltDBAggregate
        super(operand(Aggregate.class, operand(VoltDBSend.class, none())));
    }

    /**
     * A visitor to convert transformed LogicalAggregate and LogicalProject RelNodes
     * to the VoltDB convention
     *
     */
    private class RewriteRelVisitor extends RelVisitor {
        // The root of the converted expression
        private RelNode m_voltRootNode = null;
        private RexNode m_postPredicate = null;

        RewriteRelVisitor(Aggregate aggrRelNode) {
            if (aggrRelNode instanceof VoltDBAggregate) {
                m_postPredicate = ((VoltDBAggregate) aggrRelNode).getPostPredicate();
            }
        }

        public RelNode getRootNode() {
            return m_voltRootNode;
        }

        @Override
        public void visit(RelNode relNode, int ordinal, RelNode parentNode) {
            // rewrite children first
            super.visit(relNode, ordinal, parentNode);

            if (relNode.getConvention() instanceof VoltDBConvention) {
                return;
            }
            final RelTraitSet traitSet = relNode.getTraitSet().replace(VoltDBConvention.INSTANCE);
            RelNode voltRelNode = null;
            if (relNode instanceof LogicalAggregate) {
                LogicalAggregate aggregate = (LogicalAggregate) relNode;
                voltRelNode = VoltDBAggregate.create(
                        aggregate.getCluster(),
                        traitSet,
                        aggregate.getInput(),
                        aggregate.indicator,
                        aggregate.getGroupSet(),
                        aggregate.getGroupSets(),
                        aggregate.getAggCallList(),
                        m_postPredicate,
                        true);
            } else if (relNode instanceof LogicalProject) {
                LogicalProject project = (LogicalProject) relNode;
                voltRelNode = VoltDBProject.create(
                        project.getInput(),
                        project.getProjects(),
                        project.getRowType());
            } else {
                // Should not be any other types. Just in case
                voltRelNode = relNode.copy(traitSet, relNode.getInputs());
            }
            if (parentNode != null) {
                // Replace the old Logical expression with the new VoltDB one
                parentNode.replaceInput(ordinal, voltRelNode);
            } else {
                // It's the root
                m_voltRootNode = voltRelNode;
            }
        }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Aggregate aggregate = call.rel(0);
        // Match Logical Aggregate
        if (aggregate instanceof LogicalAggregate) {
            return true;
        }
        // Only match lower fragment VoltDBAggregate
        if (aggregate instanceof VoltDBAggregate &&
                ((VoltDBAggregate)aggregate).isCoordinatorAggregate()) {
            // This is already a coordinator aggregate
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Aggregate aggregate = call.rel(0);
        VoltDBSend send = call.rel(1);

        RelNode sendInput = send.getInput();
        // Copy fragment Aggregate
        RelNode fragmentAggregate = VoltDBAggregate.createFrom(
                aggregate,
                sendInput,
                false);
        // Copy Send node and increase its level because it will sit above
        // the fragment Aggregate expression
        RelNode newSend = VoltDBSend.create(
                send.getCluster(),
                send.getTraitSet(),
                fragmentAggregate,
                send.getPartitioning(),
                send.getLevel() + 1);
        RelNode newRoot = null;
        if (needCoordinatorAggregate(aggregate)) {
            if (needTransformation(aggregate)) {
                newRoot = transformAggregates(call, aggregate, newSend);
                // Convert Logical to VoltDB convention
                RewriteRelVisitor visitor = new RewriteRelVisitor(aggregate);
                visitor.visit(newRoot, 0, null);
                newRoot = visitor.getRootNode();
            } else {
                newRoot = VoltDBAggregate.createFrom(
                    aggregate,
                    newSend,
                    true);
            }
        } else {
            // Coordinator Aggregate is not required
            newRoot = newSend;
        }
        call.transformTo(newRoot);

        // Set importance of the original aggregate expression to ZERO
        // to eliminate it from further considereation since it's an invalid expression
        call.getPlanner().setImportance(aggregate, 0.);
    }

    private boolean needCoordinatorAggregate(Aggregate aggregate) {
        return true;
    }

    private boolean needTransformation(Aggregate aggregate) {
        List<AggregateCall> aggrCalls = aggregate.getAggCallList();
        for(AggregateCall aggrCall : aggrCalls) {
            if (SEND_AGGR_FUNCTIONS.contains(aggrCall.getAggregation().getKind())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Borrowed from Calcite's AggregateReduceFunctionsRule.
     *
     * @param ruleCall
     * @param oldAggRel
     * @param newInput
     * @return
     */
    private RelNode transformAggregates(RelOptRuleCall ruleCall, Aggregate oldAggRel, RelNode newInput) {
        RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

        List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
        final int groupCount = oldAggRel.getGroupCount();
        final int indicatorCount = oldAggRel.getIndicatorCount();

        final List<AggregateCall> newCalls = Lists.newArrayList();
        final Map<AggregateCall, RexNode> aggCallMapping = Maps.newHashMap();

        final List<RexNode> projList = Lists.newArrayList();

        // pass through group key (+ indicators if present)
        for (int i = 0; i < groupCount + indicatorCount; ++i) {
          projList.add(
              rexBuilder.makeInputRef(
                  getFieldType(oldAggRel, i),
                  i));
        }

        // List of input expressions. If a particular aggregate needs more, it
        // will add an expression to the end, and we will create an extra
        // project.
        final RelBuilder relBuilder = ruleCall.builder();
        relBuilder.push(newInput);
        final List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());

        // create new agg function calls and rest of project list together
        int oldCallIdx = 0;
        for (AggregateCall oldCall : oldCalls) {
          projList.add(
                  transformAggregate(
                          oldAggRel, oldCall, oldCallIdx++, newCalls, aggCallMapping, inputExprs));
        }

        final int extraArgCount =
            inputExprs.size() - relBuilder.peek().getRowType().getFieldCount();
        if (extraArgCount > 0) {
          relBuilder.project(inputExprs,
              CompositeList.of(
                  relBuilder.peek().getRowType().getFieldNames(),
                  Collections.<String>nCopies(extraArgCount, null)));
        }
        newAggregateRel(relBuilder, oldAggRel, newCalls);
        relBuilder.project(projList, oldAggRel.getRowType().getFieldNames());
        return relBuilder.build();
    }

    private RexNode transformAggregate(Aggregate oldAggRel,
            AggregateCall oldCall,
            int oldCallIdx,
            List<AggregateCall> newCalls,
            Map<AggregateCall, RexNode> aggCallMapping,
            List<RexNode> inputExprs) {
        final SqlKind kind = oldCall.getAggregation().getKind();
        if (SEND_AGGR_FUNCTIONS.contains(kind)) {
            switch (kind) {
                case COUNT:
                    // replace original COUNT(x) with SUM(x)
                    return transformCount(oldAggRel, oldCall,oldCallIdx,  newCalls, aggCallMapping,
                            inputExprs);
                case AVG:
                    // replace original AVG(x) with SUM(x) / COUNT(x)
                    return transformAvg(oldAggRel, oldCall, newCalls, aggCallMapping,
                            inputExprs);
            default:
                throw new CalcitePlanningException(
                            "Unexpected Aggregate function " + kind.toString());
            }
        } else {
            // anything else: preserve original call
            RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
            final int nGroups = oldAggRel.getGroupCount();
            List<RelDataType> oldArgTypes = SqlTypeUtil.projectTypes(
                    oldAggRel.getInput().getRowType(), oldCall.getArgList());
            return rexBuilder.addAggCall(oldCall, nGroups, oldAggRel.indicator,
                    newCalls, aggCallMapping, oldArgTypes);
        }
    }

    /**
     * Do a shallow clone of oldAggRel and update aggCalls. Could be refactored
     * into Aggregate and subclasses - but it's only needed for some
     * subclasses.
     *
     * @param relBuilder Builder of relational expressions; at the top of its
     *                   stack is its input
     * @param oldAggregate LogicalAggregate to clone.
     * @param newCalls  New list of AggregateCalls
     */
    protected void newAggregateRel(RelBuilder relBuilder,
        Aggregate oldAggregate,
        List<AggregateCall> newCalls) {
      relBuilder.aggregate(
          relBuilder.groupKey(oldAggregate.getGroupSet(),
              oldAggregate.getGroupSets()),
          newCalls);
    }

    private RelDataType getFieldType(RelNode relNode, int i) {
        final RelDataTypeField inputField =
            relNode.getRowType().getFieldList().get(i);
        return inputField.getType();
      }

    private RexNode transformCount(Aggregate oldAggRel,
            AggregateCall oldCall,
            int oldCallIdx,
            List<AggregateCall> newCalls,
            Map<AggregateCall, RexNode> aggCallMapping,
            List<RexNode> inputExprs) {
        final int nGroups = oldAggRel.getGroupCount();
        final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
        RelDataType avgInputType = null;
        List<Integer> newArgList = null;
        if (oldCall.getArgList().isEmpty())  {
            // count(*)
            avgInputType = oldCall.getType();
            newArgList = new ArrayList<>();
            int sumArgInputIdx = oldAggRel.getGroupCount() + oldCallIdx;
            newArgList.add(sumArgInputIdx);
        } else {
            final int iAvgInput = oldCall.getArgList().get(0);
            avgInputType = getFieldType(oldAggRel.getInput(),
                iAvgInput);
            newArgList = oldCall.getArgList();
        }

        // The difference between SUM0 and SUM aggregate functions is that the former's
        // return type is NOT NULL matching the COUNT's return type
        final AggregateCall sumCall = AggregateCall.create(
                SqlStdOperatorTable.SUM0, oldCall.isDistinct(),
                newArgList, oldCall.filterArg,
                oldAggRel.getGroupCount(), oldAggRel.getInput(), oldCall.getType(), null);

        RexNode sumRef = rexBuilder.addAggCall(sumCall, nGroups,
                oldAggRel.indicator, newCalls, aggCallMapping,
                ImmutableList.of(avgInputType));
        RexNode retRef = rexBuilder.makeCast(oldCall.getType(), sumRef);
        return retRef;
    }

    private RexNode transformAvg(Aggregate oldAggRel,
            AggregateCall oldCall,
            List<AggregateCall> newCalls,
            Map<AggregateCall, RexNode> aggCallMapping,
            List<RexNode> inputExprs) {
        final int nGroups = oldAggRel.getGroupCount();
        final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
        final int iAvgInput = oldCall.getArgList().get(0);
        final RelDataType avgInputType = getFieldType(oldAggRel.getInput(),
                iAvgInput);
        final AggregateCall sumCall = AggregateCall.create(
                SqlStdOperatorTable.SUM, oldCall.isDistinct(),
                oldCall.getArgList(), oldCall.filterArg,
                oldAggRel.getGroupCount(), oldAggRel.getInput(), null, null);
        final AggregateCall countCall = AggregateCall.create(
                SqlStdOperatorTable.COUNT, oldCall.isDistinct(),
                oldCall.getArgList(), oldCall.filterArg,
                oldAggRel.getGroupCount(), oldAggRel.getInput(), null, null);

        // NOTE: these references are with respect to the output
        // of newAggRel
        RexNode numeratorRef = rexBuilder.addAggCall(sumCall, nGroups,
                oldAggRel.indicator, newCalls, aggCallMapping,
                ImmutableList.of(avgInputType));
        final RexNode denominatorRef = rexBuilder.addAggCall(countCall, nGroups,
                oldAggRel.indicator, newCalls, aggCallMapping,
                ImmutableList.of(avgInputType));

        final RelDataTypeFactory typeFactory = oldAggRel.getCluster()
                .getTypeFactory();
        final RelDataType avgType = typeFactory.createTypeWithNullability(
                oldCall.getType(), numeratorRef.getType().isNullable());
        numeratorRef = rexBuilder.ensureType(avgType, numeratorRef, true);
        final RexNode divideRef = rexBuilder.makeCall(
                SqlStdOperatorTable.DIVIDE, numeratorRef, denominatorRef);
        return rexBuilder.makeCast(oldCall.getType(), divideRef);
    }

}