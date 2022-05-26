/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import org.voltdb.dr2.conflicts.DRConflictsTracker;
import org.voltdb.utils.MiscUtils;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

public class DRConflictReporter {

    private static DRConflictManager s_conflictManager;

    public static void init(DRConflictsTracker drConflictsTracker) {
        if (MiscUtils.isPro()) {
            try {
                Class<?> klass = Class.forName("org.voltdb.dr2.DRConflictManagerImpl");
                Constructor<?> constructor = klass.getConstructor(DRConflictsTracker.class);
                s_conflictManager = (DRConflictManager) constructor.newInstance(drConflictsTracker);
            } catch (Exception e) {}
        }
    }

    public static int reportDRConflict(int partitionId, int remoteClusterId, long remoteTimestamp, String tableName, boolean isReplicatedTable, int action,
                                       int deleteConflict, ByteBuffer existingMetaTableForDelete, ByteBuffer existingTupleTableForDelete,
                                       ByteBuffer expectedMetaTableForDelete, ByteBuffer expectedTupleTableForDelete,
                                       int insertConflict, ByteBuffer existingMetaTableForInsert, ByteBuffer existingTupleTableForInsert,
                                       ByteBuffer newMetaTableForInsert, ByteBuffer newTupleTableForInsert) {
        assert s_conflictManager != null : "Community edition should not have any conflicts";
        return s_conflictManager.resolveConflict(partitionId,
                remoteClusterId,
                remoteTimestamp,
                tableName,
                isReplicatedTable,
                PartitionDRGateway.DRRecordType.values()[action],
                PartitionDRGateway.DRConflictType.values()[deleteConflict],
                existingMetaTableForDelete, existingTupleTableForDelete,
                expectedMetaTableForDelete, expectedTupleTableForDelete,
                PartitionDRGateway.DRConflictType.values()[insertConflict],
                existingMetaTableForInsert, existingTupleTableForInsert,
                newMetaTableForInsert, newTupleTableForInsert);
    }
}
