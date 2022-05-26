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

package org.voltdb.plannerv2.rules.physical;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.voltdb.plannerv2.rel.logical.VoltLogicalIntersect;
import org.voltdb.plannerv2.rel.logical.VoltLogicalMinus;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.logical.VoltLogicalUnion;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalIntersect;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalMinus;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalUnion;

public class VoltPSetOpsRule extends RelOptRule {

    public static final VoltPSetOpsRule INSTANCE_UNION = new VoltPSetOpsRule(
            operand(VoltLogicalUnion.class, VoltLogicalRel.CONVENTION, any()), MatchType.UNION_TYPE);

    public static final VoltPSetOpsRule INSTANCE_INTERSECT = new VoltPSetOpsRule(
            operand(VoltLogicalIntersect.class, VoltLogicalRel.CONVENTION, any()), MatchType.INTERSECT_TYPE);

    public static final VoltPSetOpsRule INSTANCE_EXCEPT = new VoltPSetOpsRule(
            operand(VoltLogicalMinus.class, VoltLogicalRel.CONVENTION, any()), MatchType.EXCEPT_TYPE);

    private enum MatchType {
        UNION_TYPE {
            @Override
            public RelNode convertToVoltPhysical(SetOp setOp, RelTraitSet convertedTraits, List<RelNode> convertedInputs) {
                return new VoltPhysicalUnion(setOp.getCluster(), convertedTraits, convertedInputs, setOp.all);
            }
        },

        INTERSECT_TYPE {
            @Override
            public RelNode convertToVoltPhysical(SetOp setOp, RelTraitSet convertedTraits, List<RelNode> convertedInputs) {
                return new VoltPhysicalIntersect(setOp.getCluster(), convertedTraits, convertedInputs, setOp.all);
            }
        },

        EXCEPT_TYPE {
            @Override
            public RelNode convertToVoltPhysical(SetOp setOp, RelTraitSet convertedTraits, List<RelNode> convertedInputs) {
                return new VoltPhysicalMinus(setOp.getCluster(), convertedTraits, convertedInputs, setOp.all);
            }
        };

        public abstract RelNode convertToVoltPhysical(SetOp setOp, RelTraitSet convertedTraits, List<RelNode> convertedInputs);
    }

    private final MatchType m_matchType;

    private VoltPSetOpsRule(RelOptRuleOperand operand, MatchType matchType) {
        super(operand, "VoltPSetOpsRule_" + matchType.name());
        m_matchType = matchType;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SetOp setOp = call.rel(0);
        List<RelNode> inputs = setOp.getInputs();
        RelTraitSet convertedTraits = setOp.getTraitSet().replace(VoltPhysicalRel.CONVENTION);
        List<RelNode> convertedInputs = convertList(inputs, VoltPhysicalRel.CONVENTION);
        RelNode convertedSetOP = m_matchType.convertToVoltPhysical(setOp, convertedTraits, convertedInputs);
        call.transformTo(convertedSetOP);
    }
}
