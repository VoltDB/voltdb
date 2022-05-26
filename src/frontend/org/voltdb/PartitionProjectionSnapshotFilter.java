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
package org.voltdb;

import java.util.concurrent.Callable;

import org.voltcore.utils.DBBPool.BBContainer;

/*
 * Filter that projects out data from all partitions except for the ones specified
 */
public class PartitionProjectionSnapshotFilter implements SnapshotDataFilter {

    private final int m_partitions[];
    private final int m_partitionIdOffset;

    public PartitionProjectionSnapshotFilter(int partitions[], int partitionIdOffset) {
        m_partitions = partitions;
        m_partitionIdOffset = partitionIdOffset;
    }

    @Override
    public Callable<BBContainer> filter(final Callable<BBContainer> input) {
        return new Callable<BBContainer>() {
            @Override
            public BBContainer call() throws Exception {
                final BBContainer cont = input.call();
                final int partitionId = cont.b().getInt(m_partitionIdOffset);
                boolean hasPartition = false;

                for (int acceptedPartitionId : m_partitions) {
                    if (partitionId == acceptedPartitionId) {
                        hasPartition = true;
                    }
                }

                if (hasPartition) {
                    return cont;
                } else {
                    cont.discard();
                    return null;
                }
            }
        };

    }

}
