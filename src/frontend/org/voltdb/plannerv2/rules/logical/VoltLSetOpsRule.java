/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.plannerv2.rules.logical;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.voltdb.plannerv2.rel.logical.VoltLogicalIntersect;
import org.voltdb.plannerv2.rel.logical.VoltLogicalMinus;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.logical.VoltLogicalUnion;

public class VoltLSetOpsRule extends RelOptRule {

    public static final VoltLSetOpsRule INSTANCE_UNION = new VoltLSetOpsRule(
            operand(LogicalUnion.class, Convention.NONE, any()), MatchType.UNION_TYPE);

    public static final VoltLSetOpsRule INSTANCE_INTERSECT = new VoltLSetOpsRule(
            operand(LogicalIntersect.class, Convention.NONE, any()), MatchType.INTERSECT_TYPE);

    public static final VoltLSetOpsRule INSTANCE_EXCEPT = new VoltLSetOpsRule(
            operand(LogicalMinus.class, Convention.NONE, any()), MatchType.EXCEPT_TYPE);

    private enum MatchType {
        UNION_TYPE {
            @Override
            public RelNode convertToVoltLogical(SetOp setOp, RelTraitSet convertedTraits, List<RelNode> convertedInputs) {
                return new VoltLogicalUnion(setOp.getCluster(), convertedTraits, convertedInputs, setOp.all);
            }
        },

        INTERSECT_TYPE {
            @Override
            public RelNode convertToVoltLogical(SetOp setOp, RelTraitSet convertedTraits, List<RelNode> convertedInputs) {
                return new VoltLogicalIntersect(setOp.getCluster(), convertedTraits, convertedInputs, setOp.all);
            }
        },

        EXCEPT_TYPE {
            @Override
            public RelNode convertToVoltLogical(SetOp setOp, RelTraitSet convertedTraits, List<RelNode> convertedInputs) {
                return new VoltLogicalMinus(setOp.getCluster(), convertedTraits, convertedInputs, setOp.all);
            }
        };

        public abstract RelNode convertToVoltLogical(SetOp setOp, RelTraitSet convertedTraits, List<RelNode> convertedInputs);
    }

    final private MatchType m_matchType;

    private VoltLSetOpsRule(RelOptRuleOperand operand, MatchType matchType) {
        super(operand, "VoltLSetOpsRule_" + matchType.name());
        m_matchType = matchType;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SetOp setOp = call.rel(0);
        List<RelNode> inputs = setOp.getInputs();
        RelTraitSet convertedTraits = setOp.getTraitSet().replace(VoltLogicalRel.CONVENTION);
        List<RelNode> convertedInputs = convertList(inputs, VoltLogicalRel.CONVENTION);
        RelNode convertedSetOP = m_matchType.convertToVoltLogical(setOp, convertedTraits, convertedInputs);
        call.transformTo(convertedSetOP);
    }
}
