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
    public String m_tableName;
    public String m_columnName;
    public String m_paramIndex;

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
        return m_tableName != null;
    }

    public boolean isTwoPartitionProcedure() {
        return  m_tableName != null && m_tableName2 != null;
    }

    public boolean isMultiPartitionProcedure() {
        return !isSinglePartition() && !isTwoPartitionProcedure();
    }

    public static ProcedurePartitionData extractPartitionData(Procedure proc) {
        String partitionTableName = proc.getPartitiontable().getTypeName();
        String columnName = proc.getPartitioncolumn().getTypeName();
        String partitionIndex = Integer.toString(proc.getPartitionparameter());
        return new ProcedurePartitionData(partitionTableName, columnName, partitionIndex);
    }

}
