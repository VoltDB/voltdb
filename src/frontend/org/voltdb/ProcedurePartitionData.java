/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import org.voltdb.catalog.Procedure;

/**
 * Partition data for a procedure
 */
public class ProcedurePartitionData {
    public String m_tableName;
    public String m_columnName;
    public String m_paramIndex;

    // the next extra fields are for 2P transactions
    public String m_tableName2;
    public String m_columnName2;
    public String m_paramIndex2;

    // constructor for NULL partition data
    public ProcedurePartitionData () {
        init(null, null, null, null, null, null);
    }

    public ProcedurePartitionData(String tableName, String columnName) {
        init(tableName, columnName, "0", null, null, null);
    }

    public ProcedurePartitionData(String tableName, String columnName, String paramIndex) {
        init(tableName, columnName, paramIndex, null, null, null);
    }

    public ProcedurePartitionData(String tableName, String columnName, String paramIndex,
            String tableName2, String columnName2, String paramIndex2) {
        init(tableName, columnName, paramIndex, tableName2, columnName2, paramIndex2);
    }

    private void init(String tableName, String columnName, String paramIndex,
            String tableName2, String columnName2, String paramIndex2) {
        m_tableName = tableName;
        m_columnName = columnName;
        m_paramIndex = paramIndex;

        m_tableName2 = tableName2;
        m_columnName2 = columnName2;
        m_paramIndex2 = paramIndex2;
    }

    public boolean isSinglePartition() {
        return m_tableName != null && m_tableName2 == null;
    }

    public boolean isTwoPartitionProcedure() {
        return  m_tableName != null && m_tableName2 != null;
    }

    public boolean isMultiPartitionProcedure() {
        return m_tableName == null && m_tableName2 == null;
    }

    public static ProcedurePartitionData extractPartitionData(Procedure proc) {
        // if this is MP procedure
        if (proc.getPartitiontable() == null) {
            return new ProcedurePartitionData();
        }

        // extract partition data for single partition procedure
        String partitionTableName = proc.getPartitiontable().getTypeName();
        String columnName = proc.getPartitioncolumn().getTypeName();
        String partitionIndex = Integer.toString(proc.getPartitionparameter());

        // handle two partition transaction
        String partitionTableName2 = null, columnName2 = null, partitionIndex2 = null;
        if (proc.getPartitiontable2() != null) {
            partitionTableName2 = proc.getPartitiontable2().getTypeName();
            columnName2 = proc.getPartitioncolumn2().getTypeName();
            partitionIndex2 = Integer.toString(proc.getPartitionparameter2());
        }

        return new ProcedurePartitionData(partitionTableName, columnName, partitionIndex,
                partitionTableName2, columnName2, partitionIndex2);
    }

    /**
     * For Testing usage ONLY.
     * From a partition information string to @ProcedurePartitionData
     * string format:
     *     1) String.format("%s.%s: %s", tableName, columnName, parameterNo)
     *     1) String.format("%s.%s: %s, %s.%s: %s", tableName, columnName, parameterNo, tableName2, columnName2, parameterNo2)
     * @return
     */
    public static ProcedurePartitionData fromPartitionInfoString(String partitionInfoString) {
        if (partitionInfoString == null || partitionInfoString.trim().isEmpty()) {
            return new ProcedurePartitionData();
        }

        String[] partitionInfoParts = new String[0];
        partitionInfoParts = partitionInfoString.split(",");

        assert(partitionInfoParts.length <= 2);
        if (partitionInfoParts.length == 2) {
            ProcedurePartitionData partitionInfo = fromPartitionInfoString(partitionInfoParts[0]);
            ProcedurePartitionData partitionInfo2 = fromPartitionInfoString(partitionInfoParts[1]);
            partitionInfo.addSecondPartitionInfo(partitionInfo2);
            return partitionInfo;
        }

        String subClause = partitionInfoParts[0];
        // split on the colon
        String[] parts = subClause.split(":");
        assert(parts.length == 2);

        // relabel the parts for code readability
        String columnInfo = parts[0].trim();
        String paramIndex = parts[1].trim();

        // split the columninfo
        parts = columnInfo.split("\\.");
        assert(parts.length == 2);

        // relabel the parts for code readability
        String tableName = parts[0].trim();
        String columnName = parts[1].trim();

        return new ProcedurePartitionData(tableName, columnName, paramIndex);
    }

    public void addSecondPartitionInfo (ProcedurePartitionData infoData) {
        m_tableName2 = infoData.m_tableName;
        m_columnName2 = infoData.m_columnName;
        m_paramIndex2 = infoData.m_paramIndex;
    }


}
