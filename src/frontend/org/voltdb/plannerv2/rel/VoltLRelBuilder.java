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

package org.voltdb.plannerv2.rel;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

public class VoltLRelBuilder extends RelBuilder{

    protected VoltLRelBuilder(Context context, RelOptCluster cluster,
            RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    /** Creates a {@link RelBuilderFactory}, a partially-created VoltPRelBuilder.
     * Just add a {@link RelOptCluster} and a {@link RelOptSchema} */
    public static RelBuilderFactory proto(final Context context) {
      return (cluster, schema) -> new VoltLRelBuilder(context, cluster, schema);
    }

    /** Creates a {@link RelBuilderFactory} that uses a given set of factories. */
    public static RelBuilderFactory proto(Object... factories) {
      return proto(Contexts.of(factories));
    }

    /** A {@link RelBuilderFactory} that creates a {@link RelBuilder} that will
     * create Volt Logical relational expressions for everything. */
    public static final RelBuilderFactory LOGICAL_BUILDER =
            VoltLRelBuilder.proto(
            Contexts.of(VoltLRelFactories.VOLT_LOGICAL_PROJECT_FACTORY,
                    VoltLRelFactories.VOLT_LOGICAL_FILTER_FACTORY,
                    VoltLRelFactories.VOLT_LOGICAL_AGGREGATE_FACTORY,
                    VoltLRelFactories.VOLT_LOGICAL_JOIN_FACTORY,
                    VoltLRelFactories.VOLT_LOGICAL_SET_OP_FACTORY,
                    VoltLRelFactories.VOLT_LOGICAL_SORT_FACTORY,
                    VoltLRelFactories.VOLT_LOGICAL_VALUES_FACTORY));

}
