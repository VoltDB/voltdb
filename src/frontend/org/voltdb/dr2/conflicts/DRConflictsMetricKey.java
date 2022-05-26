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

package org.voltdb.dr2.conflicts;

import java.util.Objects;

public class DRConflictsMetricKey {
    private final int m_remoteClusterId;
    private final int m_partitionId;
    private final String m_tableName;

    DRConflictsMetricKey(int remoteClusterId, int partitionId, String tableName) {
        this.m_remoteClusterId = remoteClusterId;
        this.m_partitionId = partitionId;
        this.m_tableName = tableName;
    }

    public int getRemoteClusterId() {
        return m_remoteClusterId;
    }

    public int getPartitionId() {
        return m_partitionId;
    }

    public String getTableName() {
        return m_tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DRConflictsMetricKey) {
            DRConflictsMetricKey that = (DRConflictsMetricKey) o;
            return m_remoteClusterId == that.m_remoteClusterId
                    && m_partitionId == that.m_partitionId
                    && Objects.equals(m_tableName, that.m_tableName);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = m_remoteClusterId;
        result = 31 * result + m_partitionId;
        result = 31 * result + (m_tableName != null ? m_tableName.hashCode() : 0);
        return result;
    }
}
