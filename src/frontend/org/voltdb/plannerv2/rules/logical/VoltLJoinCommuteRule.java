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

import java.util.function.Predicate;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.voltdb.plannerv2.rel.VoltLRelBuilder;
import org.voltdb.plannerv2.rel.logical.VoltLogicalJoin;

/**
 * Volt extension of the JoinCommuteRule {@link org.apache.calcite.rel.rules.JoinCommuteRule}
 * that operates on VoltLogicallJoin relation
 *
 */
public class VoltLJoinCommuteRule extends JoinCommuteRule {

    /**
     * Predicate to match RIGHT joins
    */
    private static final Predicate<Join> RIGHT_JOIN_PREDICATE = join -> join.getJoinType() == JoinRelType.RIGHT;

    /** Instance of the rule that only swaps RIGHT NL joins to become LEFT joins
     */
    public static final VoltLJoinCommuteRule INSTANCE_RIGHT_TO_LEFT = new VoltLJoinCommuteRule();

    private VoltLJoinCommuteRule() {
        super(VoltLogicalJoin.class, RIGHT_JOIN_PREDICATE,
                VoltLRelBuilder.LOGICAL_BUILDER, true, "VoltLJoinCommuteRule:right_to_left");
    }
}
