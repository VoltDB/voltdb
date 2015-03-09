/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

/**
 * Supported snapshot formats
 */
public enum SnapshotFormat {
    NATIVE (true,  true,  TableStreamType.SNAPSHOT),
    CSV    (true,  true,  TableStreamType.SNAPSHOT),
    STREAM (false, false, TableStreamType.SNAPSHOT),
    INDEX  (false, false, TableStreamType.ELASTIC_INDEX);

    private final boolean m_isFileBased;
    private final boolean m_canCloseEarly;
    private final TableStreamType m_streamType;
    private SnapshotFormat(boolean isFileBased, boolean canCloseEarly, TableStreamType streamType) {
        m_isFileBased = isFileBased;
        m_canCloseEarly = canCloseEarly;
        m_streamType = streamType;
    }

    /**
     * Whether or not this snapshot format writes to file. The snapshot
     * subsystem will do file creation if the format is file based.
     *
     * @return true if file based, otherwise false.
     */
    public boolean isFileBased() {
        return m_isFileBased;
    }

    /**
     * Whether or not the snapshot target can be closed early. If true,
     * and the data target is associated with a replicated table, the SnapshotSiteProcessor may
     * close it immediately after the replicated table finishes serialization.
     */
    public boolean canCloseEarly() {
        return m_canCloseEarly;
    }

    public TableStreamType getStreamType() {
        return m_streamType;
    }

    /**
     * Get the snapshot format enum from the string. Letter case of the string
     * doesn't matter.
     *
     * @param s
     * @return The snapshot format
     * @throws IllegalArgumentException If the string does not match any format
     */
    public static SnapshotFormat getEnumIgnoreCase(String s) {
        for (SnapshotFormat value : values()) {
            if (value.toString().equalsIgnoreCase(s)) {
                return value;
            }
        }

        throw new IllegalArgumentException("Unknown snapshot format " + s);
    }
}
