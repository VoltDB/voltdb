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

package org.voltdb.plannerv2.rel.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

import com.google.common.base.Preconditions;

/**
 * Logical Sort with <code>VOLTDB_LOGICAL</code> convention trait.
 *
 * @author Michael Alexeev
 * @since 8.4
 */
public class VoltDBLSort extends Sort implements VoltRel {

    public VoltDBLSort(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelCollation collation) {
        super(cluster, traitSet, input, collation, null, null);
        Preconditions.checkArgument(getConvention() == VoltRel.CONVENTION);
    }

    @Override
    public VoltDBLSort copy(RelTraitSet traitSet, RelNode input,
                            RelCollation collation,
                            RexNode offset,
                            RexNode fetch) {
        return new VoltDBLSort(getCluster(), traitSet, input, collation);
    }
}
