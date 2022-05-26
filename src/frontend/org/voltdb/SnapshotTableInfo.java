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

import java.util.Arrays;

import org.voltdb.catalog.Table;

/**
 * Class used to describe a table that can be involved in a snapshot. This is basically to have a single class that can
 * be used to describe a catalog or system table.
 */
public class SnapshotTableInfo {
    private final String m_name;
    private final int m_tableId;
    private final boolean m_replicated;
    private final boolean m_systemTable;
    private byte[] m_schema;
    private int m_partitionColumn;

    /**
     * Construct an instance from a catalog table
     *
     * @param table Catalog table
     */
    public SnapshotTableInfo(Table table) {
        this(table.getTypeName(), table.getRelativeIndex(), table.getIsreplicated(), false);
    }

    /**
     * Construct an instance for a system table
     *
     * @param name    of table
     * @param tableId ID of the table
     */
    public SnapshotTableInfo(String name, int tableId) {
        this(name, tableId, false, true);
    }

    private SnapshotTableInfo(String name, int tableId, boolean replicated, boolean systemTable) {
        m_name = name;
        m_tableId = tableId;
        m_replicated = replicated;
        m_systemTable = systemTable;
    }

    /**
     * Set the schema and partition column for this table. As retrieved from the EE
     *
     * @param schema          Schema of table including any hidden columns included in snapshot
     * @param partitionColumn Index of partition column if this is not a replicated table
     */
    public synchronized void setSchema(byte[] schema, int partitionColumn) {
        if (m_schema == null) {
            m_schema = schema;
            m_partitionColumn = partitionColumn;
        } else {
            assert m_partitionColumn == partitionColumn && Arrays.equals(m_schema, schema) : "Schema for " + m_name
                    + " does not match " + Arrays.toString(m_schema) + " vs " + Arrays.toString(schema) + " and "
                    + m_partitionColumn + " vs " + partitionColumn;
        }
    }

    /**
     * @return The name of the table
     */
    public String getName() {
        return m_name;
    }

    /**
     * @return The ID/relative index of the table
     */
    public int getTableId() {
        return m_tableId;
    }

    /**
     * @return {@code true} if this table is replicated
     */
    public boolean isReplicated() {
        return m_replicated;
    }

    /**
     * @return {@code true} if this is a system table
     */
    public boolean isSystemTable() {
        return m_systemTable;
    }

    /**
     * @return The schema of this table as a byte array
     * @throws IllegalStateException If {@link #setSchema(byte[], int)} has not been called yet
     */
    public synchronized byte[] getSchema() throws IllegalStateException {
        if (m_schema == null) {
            throw new IllegalStateException("schema not set");
        }
        return m_schema;
    }

    /**
     * Return value is only valid if {@link #isReplicated()} returns {@code false}
     *
     * @return The index of the partition column
     * @throws IllegalStateException If {@link #setSchema(byte[], int)} has not been called yet
     */
    public synchronized int getPartitionColumn() throws IllegalStateException {
        if (m_schema == null) {
            throw new IllegalStateException("schema not set");
        }
        return m_partitionColumn;
    }

    @Override
    public String toString() {
        return "SnapshotTableInfo [name=" + m_name + ", tableId=" + m_tableId + ", replicated=" + m_replicated
                + ", systemTable=" + m_systemTable + ", partitionColumn=" + m_partitionColumn + "]";
    }
}
