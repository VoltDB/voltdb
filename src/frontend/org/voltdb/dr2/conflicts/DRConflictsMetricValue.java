/* This file is part of VoltDB.
 * Copyright (C) 2022 VoltDB Inc.
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

public class DRConflictsMetricValue {
    private final long m_missingRowCount;
    private final long m_rowTimestampMismatchCount;
    private final long m_constraintViolationCount;
    private final long m_conflictsCount;
    private final long m_lastConflictTimestamp;

    public DRConflictsMetricValue(long missingRowCount,
                                  long rowTimestampMismatchCount,
                                  long constraintViolationCount,
                                  long conflictsCount,
                                  long lastConflictTimestamp) {
        this.m_missingRowCount = missingRowCount;
        this.m_rowTimestampMismatchCount = rowTimestampMismatchCount;
        this.m_constraintViolationCount = constraintViolationCount;
        this.m_conflictsCount = conflictsCount;
        this.m_lastConflictTimestamp = lastConflictTimestamp;
    }

    public long getMissingRowCount() {
        return m_missingRowCount;
    }

    public long getRowTimestampMismatchCount() {
        return m_rowTimestampMismatchCount;
    }

    public long getConstraintViolationCount() {
        return m_constraintViolationCount;
    }

    public long getConflictsCount() {
        return m_conflictsCount;
    }

    public long getLastConflictTimestamp() {
        return m_lastConflictTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DRConflictsMetricValue) {
            DRConflictsMetricValue that = (DRConflictsMetricValue) o;
            return m_missingRowCount == that.m_missingRowCount
                    && m_rowTimestampMismatchCount == that.m_rowTimestampMismatchCount
                    && m_constraintViolationCount == that.m_constraintViolationCount
                    && m_conflictsCount == that.m_conflictsCount
                    && m_lastConflictTimestamp == that.m_lastConflictTimestamp;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = (int) (m_missingRowCount ^ (m_missingRowCount >>> 32));
        result = 31 * result + (int) (m_rowTimestampMismatchCount ^ (m_rowTimestampMismatchCount >>> 32));
        result = 31 * result + (int) (m_constraintViolationCount ^ (m_constraintViolationCount >>> 32));
        result = 31 * result + (int) (m_conflictsCount ^ (m_conflictsCount >>> 32));
        result = 31 * result + (int) (m_lastConflictTimestamp ^ (m_lastConflictTimestamp >>> 32));
        return result;
    }
}
