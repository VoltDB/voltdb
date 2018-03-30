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

package org.voltdb.calciteadapter.rules.physical;

import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.mapping.Mappings;
import org.voltdb.calciteadapter.converter.RexConverter;
import org.voltdb.calciteadapter.rel.physical.VoltDBCalc;
import org.voltdb.calciteadapter.rel.physical.VoltDBSort;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 *  Adopted from the Calcite's SortProjectTransposeRule
 *
 */
public class VoltDBSortCalcTransposeRule extends RelOptRule {

    public static final VoltDBSortCalcTransposeRule INSTANCE= new VoltDBSortCalcTransposeRule();

    private VoltDBSortCalcTransposeRule() {
        super(operand(VoltDBSort.class,
                operand(VoltDBCalc.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltDBSort sort = call.rel(0);
        final VoltDBCalc calc = call.rel(1);
        final RexProgram program = calc.getProgram();
        final RelOptCluster cluster = calc.getCluster();

        // Determine mapping between project input and output fields. If sort
        // relies on non-trivial expressions, we can't push.
        List<RexLocalRef> projectRefList = program.getProjectList();
        List<RexNode> projectList = RexConverter.expandLocalRef(projectRefList, program);
        final Mappings.TargetMapping map =
            RelOptUtil.permutationIgnoreCast(
                    projectList, calc.getInput().getRowType());
        for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
          if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
            return;
          }
          final RexNode node = projectList.get(fc.getFieldIndex());
          if (node.isA(SqlKind.CAST)) {
            // Check whether it is a monotonic preserving cast, otherwise we cannot push
            final RexCall cast = (RexCall) node;
            final RexCallBinding binding =
                RexCallBinding.create(cluster.getTypeFactory(), cast,
                    ImmutableList.of(RelCollations.of(RexUtil.apply(map, fc))));
            if (cast.getOperator().getMonotonicity(binding) == SqlMonotonicity.NOT_MONOTONIC) {
              return;
            }
          }
        }

        final RelCollation newCollation =
                cluster.traitSet().canonize(
                    RexUtil.apply(map, sort.getCollation()));
        final VoltDBSort newSort =
                sort.copy(
                        sort.getTraitSet().replace(newCollation),
                        calc.getInput(),
                        newCollation,
                        sort.offset,
                        sort.fetch);
        RelNode newCalc =
                calc.copy(
                        sort.getTraitSet(),
                        ImmutableList.<RelNode>of(newSort));

        // Not only is newProject equivalent to sort;
        // newSort is equivalent to project's input
        // (but only if the sort is not also applying an offset/limit).
        Map<RelNode, RelNode> equiv;
        if (sort.offset == null
            && sort.fetch == null
            && cluster.getPlanner().getRelTraitDefs()
                .contains(RelCollationTraitDef.INSTANCE)) {
          equiv = ImmutableMap.of((RelNode) newSort, calc.getInput());
        } else {
          equiv = ImmutableMap.of();
        }
        call.transformTo(newCalc, equiv);
      }

}
