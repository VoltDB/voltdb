/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.voltdb.plannerv2.rel.VoltLRelBuilder;
import org.voltdb.plannerv2.rel.logical.VoltLogicalAggregate;

/**
 * Volt extension of the AggregateReduceFunctionsRule {@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule}
 * that operates on VoltLogicallAggregate relation.
 * Rewrites AVG(x) as SUM(X) / COUNT(X)
 *
 */
public class VoltLAggregateReduceFunctionsRule extends AggregateReduceFunctionsRule {

    public static final VoltLAggregateReduceFunctionsRule INSTANCE = new VoltLAggregateReduceFunctionsRule();

    private VoltLAggregateReduceFunctionsRule() {
        super(operand(VoltLogicalAggregate.class, any()),
                VoltLRelBuilder.LOGICAL_BUILDER);
    }
}
