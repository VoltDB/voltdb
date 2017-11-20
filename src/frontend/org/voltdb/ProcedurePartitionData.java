/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
 * Partition data for a procedure
 */
public class ProcedurePartitionData {
    public String m_tableName = null;
    public String m_columnName = null;
    public String m_paramIndex = null;

    public String m_tableName2 = null;
    public String m_columnName2 = null;
    public String m_paramIndex2 = null;

    public ProcedurePartitionData() {
    }

    public void addSecondPartitionInfo (ProcedurePartitionData infoData) {
        m_tableName2 = infoData.m_tableName;
        m_columnName2 = infoData.m_columnName;
        m_paramIndex2 = infoData.m_paramIndex;
    }

    /*
     * Multi-partition:
     *   1. All-partition -> AP
     *   2. N-partition (currently supporting 2P only) -> NP
     *
     * Single-partition -> NP
     */

    // Useful helper functions
    public boolean isSinglePartition() {
        return m_tableName == null && m_tableName2 == null;
    }

    public boolean isAllPartition() {
        return !isSinglePartition() && !partitionInfo.length() == 0;
    }

    public boolean isTwoPartitionProcedure() {
        return  m_tableName != null && m_tableName2 != null;
    }


    /**
     * From a partition information string to @ProcInfoData
     * string format:
     *     1) String.format("%s.%s: %s", tableName, columnName, parameterNo)
     *     1) String.format("%s.%s: %s, %s.%s: %s", tableName, columnName, parameterNo, tableName2, columnName2, parameterNo2)
     * @return
     */
    public static ProcedurePartitionData fromPartitionInfoString(String ddlPartitionString) {
        String[] partitionInfoParts = new String[0];
        partitionInfoParts = ddlPartitionString.split(",");

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
        int paramIndex = Integer.parseInt(parts[1].trim());

        // split the columninfo
        parts = columnInfo.split("\\.");
        assert(parts.length == 2);

        // relabel the parts for code readability
        String tableName = parts[0].trim();
        String columnName = parts[1].trim();

        return new ProcedurePartitionData(tableName, columnName, paramIndex);
    }

}
