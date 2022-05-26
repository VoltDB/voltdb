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

import java.nio.ByteBuffer;

public interface DRConflictManager {
    public int resolveConflict(int partitionId,
                               int remoteClusterId,
                               long remoteTimestamp,
                               String tableName,
                               boolean isReplicatedTable,
                               PartitionDRGateway.DRRecordType action,
                               PartitionDRGateway.DRConflictType deleteConflictType,
                               ByteBuffer existingMetaTableForDelete,
                               ByteBuffer existingTupleTableForDelete,
                               ByteBuffer expectedMetaTableForDelete,
                               ByteBuffer expectedTupleTableForDelete,
                               PartitionDRGateway.DRConflictType insertConflictType,
                               ByteBuffer existingMetaTableForInsert,
                               ByteBuffer existingTupleTableForInsert,
                               ByteBuffer newMetaTableForInsert,
                               ByteBuffer newTupleTableForInsert);
}
