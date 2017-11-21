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

import org.voltdb.catalog.Procedure;

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

    public ProcedurePartitionData(String tableName, String columnName, String paramIndex) {
        m_tableName = tableName;
        m_columnName = columnName;
        m_paramIndex = paramIndex;
    }

    public ProcedurePartitionData(String tableName, String columnName, String paramIndex,
            String tableName2, String columnName2, String paramIndex2) {
        m_tableName = tableName;
        m_columnName = columnName;
        m_paramIndex = paramIndex;

        m_tableName2 = tableName2;
        m_columnName2 = columnName2;
        m_paramIndex2 = paramIndex2;
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

//    public boolean isAllPartition() {
//        return !isSinglePartition() && !partitionInfo.length() == 0;
//    }

    public boolean isTwoPartitionProcedure() {
        return  m_tableName != null && m_tableName2 != null;
    }

    public static ProcedurePartitionData constructProcInfoData(Procedure proc) {
        String partitionTableName = proc.getPartitiontable().getTypeName();
        String columnName = proc.getPartitioncolumn().getTypeName();
        String partitionIndex = Integer.toString(proc.getPartitionparameter());
        return new ProcedurePartitionData(partitionTableName, columnName, partitionIndex);
    }

}
