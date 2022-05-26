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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalExchange;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalLimit;

/**
 * VoltDB extension of the
 * RelMdRowCount supplies a default implementation of
 * {@link RelMetadataQuery#getRowCount} for the standard logical algebra.
 */
public class VoltRelMdRowCount extends RelMdRowCount {

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.ROW_COUNT.method, new VoltRelMdRowCount());

    /** VoltPhysicalLimit implementation for
     * {@link RelMdRowCount#getRowCount()},
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getRowCount(RelNode)
     */
    public Double getRowCount(VoltPhysicalLimit rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
      }

    /** VoltPhysicalExchange implementation for
     * {@link RelMdRowCount#getRowCount()},
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getRowCount(RelNode)
     */
    public Double getRowCount(VoltPhysicalExchange rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
      }

}
