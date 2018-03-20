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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.voltdb.calciteadapter.rules.calcite;

import java.util.List;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
//import org.voltdb.calciteadapter.rel.physicalOld.VoltDBAggregate;

import com.google.common.collect.Lists;

/**
 * Adopted from {@link org.apache.calcite.rel.core.Aggregate}
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Filter}
 * past a {@link org.apache.calcite.rel.core.Aggregate}.
 * The remaining Filter representing HAVING expressions is merged with
 * the Aggregate to a single {@link org.voltdb.calciteadapter.rel.physical.VoltDBAggregate}.
 *
 */
public class FilterAggregateTransposeRule extends RelOptRule {

    /** The default instance of
     * {@link FilterAggregateTransposeRule}.
     *
     * <p>It matches any kind of agg. or filter */
    public static final FilterAggregateTransposeRule INSTANCE =
        new FilterAggregateTransposeRule(Filter.class,
            RelFactories.LOGICAL_BUILDER, Aggregate.class);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FilterAggregateTransposeRule.
     *
     * <p>If {@code filterFactory} is null, creates the same kind of filter as
     * matched in the rule. Similarly {@code aggregateFactory}.</p>
     */
    public FilterAggregateTransposeRule(
        Class<? extends Filter> filterClass,
        RelBuilderFactory builderFactory,
        Class<? extends Aggregate> aggregateClass) {
      this(
          operand(filterClass,
              operand(aggregateClass, any())),
          builderFactory);
    }

    protected FilterAggregateTransposeRule(RelOptRuleOperand operand,
        RelBuilderFactory builderFactory) {
      super(operand, builderFactory, null);
    }

    @Deprecated // to be removed before 2.0
    public FilterAggregateTransposeRule(
        Class<? extends Filter> filterClass,
        RelFactories.FilterFactory filterFactory,
        Class<? extends Aggregate> aggregateClass) {
      this(filterClass, RelBuilder.proto(Contexts.of(filterFactory)),
          aggregateClass);
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call) {
      final Filter filterRel = call.rel(0);
      final Aggregate aggRel = call.rel(1);

      final List<RexNode> conditions =
          RelOptUtil.conjunctions(filterRel.getCondition());
      final RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
      final List<RelDataTypeField> origFields =
          aggRel.getRowType().getFieldList();
      final int[] adjustments = new int[origFields.size()];
      int j = 0;
      for (int i : aggRel.getGroupSet()) {
        adjustments[j] = i - j;
        j++;
      }
      final List<RexNode> pushedConditions = Lists.newArrayList();
      final List<RexNode> remainingConditions = Lists.newArrayList();

      for (RexNode condition : conditions) {
        ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(condition);
        if (canPush(aggRel, rCols)) {
          pushedConditions.add(
              condition.accept(
                  new RelOptUtil.RexInputConverter(rexBuilder, origFields,
                      aggRel.getInput(0).getRowType().getFieldList(),
                      adjustments)));
        } else {
          remainingConditions.add(condition);
        }
      }

      final RelBuilder builder = call.builder();
      RelNode rel =
          builder.push(aggRel.getInput()).filter(pushedConditions).build();
      RexNode aggrPostPredicate = null;
      if (rel == aggRel.getInput(0)) {
          aggrPostPredicate = filterRel.getCondition();
      } else {
          aggrPostPredicate = RexUtil.composeConjunction(rexBuilder, remainingConditions, true);
      }
//      VoltDBAggregate newAggr = VoltDBAggregate.createFrom(
//              aggRel,
//              rel,
//              aggrPostPredicate);
//
//      call.transformTo(newAggr);
    }

    private boolean canPush(Aggregate aggregate, ImmutableBitSet rCols) {
      // If the filter references columns not in the group key, we cannot push
      final ImmutableBitSet groupKeys =
          ImmutableBitSet.range(0, aggregate.getGroupSet().cardinality());
      if (!groupKeys.contains(rCols)) {
        return false;
      }

      if (aggregate.getGroupSets().size() > 1) {
        // If grouping sets are used, the filter can be pushed if
        // the columns referenced in the predicate are present in
        // all the grouping sets.
        for (ImmutableBitSet groupingSet : aggregate.getGroupSets()) {
          if (!groupingSet.contains(rCols)) {
            return false;
          }
        }
      }
      return true;
    }

}
