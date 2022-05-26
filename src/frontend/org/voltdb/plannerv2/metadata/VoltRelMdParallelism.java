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

package org.voltdb.plannerv2.metadata;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdParallelism;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

/**
 * VoltDB implementations of the
 * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism}
 * metadata provider for the standard logical algebra.
 *
 * @see RelMetadataQuery#splitCount(RelNode)
 */
public class VoltRelMdParallelism extends RelMdParallelism {

    public static final int DISTRIBUTED_SPLIT_COUNT = 30;

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    new VoltRelMdParallelism(), BuiltInMethod.SPLIT_COUNT.method);

    /**
     * Return number of concurrent processes that this VoltPhysicalRel will be executed in.
     * If this rel/plan node belongs to a coordinator then its split count is 1
     * For a fragment rel/node the split count = a number of hosts * number of sites per host
     *
     * @return Split count
     */
    @Override public Integer splitCount(RelNode rel, RelMetadataQuery mq) {
        RelDistribution distribution = rel.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
        return (distribution == null ||
                RelDistributions.ANY.getType() == distribution.getType() ||
                RelDistributions.SINGLETON.getType() == distribution.getType()) ?
                        1 : DISTRIBUTED_SPLIT_COUNT;
    }
}
