/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

/**
 * Supported snapshot formats
 */
public enum SnapshotFormat {
    NATIVE (true,  true),
    CSV    (true,  true),
    STREAM (false, false);

    private final boolean m_isFileBased;
    private final boolean m_isTableBased;
    private SnapshotFormat(boolean isFileBased, boolean isTableBased) {
        m_isFileBased = isFileBased;
        m_isTableBased = isTableBased;
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
     * Whether or not a per-table snapshot target should be created. If false, a
     * single snapshot target instance will be used for all tables on a single
     * partition replica.
     *
     * @return
     */
    public boolean isTableBased() {
        return m_isTableBased;
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
