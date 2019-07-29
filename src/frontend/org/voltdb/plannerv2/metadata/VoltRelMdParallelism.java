/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdParallelism;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;

/**
 * VoltDB implementations of the
 * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism}
 * metadata provider for the standard logical algebra.
 *
 * @see RelMetadataQuery#splitCount(RelNode)
 */
public class VoltRelMdParallelism extends RelMdParallelism {

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    new VoltRelMdParallelism(), BuiltInMethod.SPLIT_COUNT.method);

    @Override public Integer splitCount(RelNode rel, RelMetadataQuery mq) {
        if (rel instanceof VoltPhysicalRel) {
            return ((VoltPhysicalRel) rel).getSplitCount();
        } else {
            return 1;
        }
    }
}
