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

import org.voltcore.utils.Pair;
import org.voltdb.types.TimestampType;

public interface TopicsSystemTableConnection {
    /**
     * Store a topics group and its members in the system tables
     *
     * @param serializedGroup topics group serialized into a byte[]
     */
    void storeGroup(byte[] groupMetadata);

    /**
     * Delete a topics group, all members and all offsets
     *
     * @param groupId ID of group to be deleted
     */
    void deleteGroup(String groupId);

    /**
     * Start or continue a fetch of all groups from this site.
     *
     * If the Boolean in the return is {@code true} then there are more groups to fetch and this method should be called
     * repeatedly with the last groupId returned until {@code false} is returned.
     *
     * @param maxResultSize for any result returned by this fetch
     * @param groupId       non-inclusive start point for iterating over groups. {@code null} means start at begining
     * @return A {@link Pair} indicating if there are more groups and the serialized groups
     */
    Pair<Boolean, byte[]> fetchGroups(int maxResultSize, String startGroupId);

    /**
     * Commit topic partition offsets for a topics group in the system tables
     *
     * @param spHandle       for this transaction
     * @param requestVersion Version of the topics message making this request
     * @param groupId        ID of the group
     * @param offsets        serialized offsets to be stored
     * @return response to requester
     */
    byte[] commitGroupOffsets(long spHandle, short requestVersion, String groupId, byte[] offsets);

    /**
     * Fetch topic partition offsets for a topics group from the system tables
     *
     * @param requestVersion version of the topics message making this request
     * @param groupId        IF of group
     * @param offsets        serialized offsets being requested
     * @return response to requester
     */
    byte[] fetchGroupOffsets(short requestVersion, String groupId, byte[] offsets);

    /**
     * Delete the expired offsets of standalone groups. An offset is expired if its commit timestamp is <
     * deleteOlderThan
     *
     * @param deleteOlderThan timestamp to use to select what offsets should be deleted
     */
    void deleteExpiredOffsets(TimestampType deleteOlderThan);
}
