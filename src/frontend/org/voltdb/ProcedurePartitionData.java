/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import org.voltdb.utils.CatalogUtil;

/**
 * Partition data for a procedure
 */
public class ProcedurePartitionData {

    public enum Type { MULTI, SINGLE, TWOPAR, DIRECTED, COMPOUND };

    public final Type m_type;
    public final String m_tableName;
    public final String m_columnName;
    public final String m_paramIndex;

    // the next extra fields are for 2P transactions
    public final String m_tableName2;
    public final String m_columnName2;
    public final String m_paramIndex2;

    // Common internal construction
    private ProcedurePartitionData(Type typ, String tbl, String col, String idx,
                                   String tbl2, String col2, String idx2) {
        m_type = typ;
        if (tbl == null) {
            m_tableName = m_columnName = m_paramIndex = null;
            m_tableName2 = m_columnName2 = m_paramIndex2 = null;
        }
        else {
            m_tableName = tbl;
            m_columnName = col;
            m_paramIndex = (idx == null ? "0" : idx);
            if (tbl2 == null) {
                m_tableName2 = m_columnName2 = m_paramIndex2 = null;
            }
            else {
                m_tableName2 = tbl2;
                m_columnName2 = col2;
                m_paramIndex2 = (idx2 == null && m_paramIndex.equals("0") ? "1" : idx2);
            }
        }
    }

    public ProcedurePartitionData(Type typ) {
        this(typ, null, null, null, null, null, null);
        assert typ != Type.SINGLE && typ != Type.TWOPAR;
    }

    public ProcedurePartitionData(String tableName, String columnName) {
        this(Type.SINGLE, tableName, columnName, "0", null, null, null);
        assert tableName != null;
    }

    public ProcedurePartitionData(String tableName, String columnName, String paramIndex) {
        this(Type.SINGLE, tableName, columnName, paramIndex, null, null, null);
        assert tableName != null;
    }

    public ProcedurePartitionData(String tableName, String columnName, String paramIndex,
                                  String tableName2, String columnName2, String paramIndex2) {
        this(tableName == null ? Type.MULTI : tableName2 == null ? Type.SINGLE : Type.TWOPAR,
             tableName, columnName, paramIndex, tableName2, columnName2, paramIndex2);
    }

    public boolean isSinglePartition() {
        // For historical reasons, non-partitioned types are reported as single-partition.
        // TODO: can we fix this?
        return m_type == Type.SINGLE || m_type == Type.DIRECTED || m_type == Type.COMPOUND;
    }

    public boolean isTwoPartitionProcedure() {
        return m_type == Type.TWOPAR;
    }

    public boolean isMultiPartitionProcedure() {
        return m_type == Type.MULTI;
    }

    public boolean isDirectedProcedure() {
        return m_type == Type.DIRECTED;
    }

    public boolean isCompoundProcedure() {
        return m_type == Type.COMPOUND;
    }

    public static ProcedurePartitionData extractPartitionData(Procedure proc) {

        // Quickly dispose of types that are not partitioned
        if (proc.getPartitiontable() == null) {
            Type type = Type.MULTI;
            if (proc.getSinglepartition()) {
                if (CatalogUtil.isCompoundProcedure(proc)) {
                    type = Type.COMPOUND;
                }
                else {
                    type = Type.DIRECTED;
                }
            }
            return new ProcedurePartitionData(type);
        }

        // Single-partition cases
        String tableName =  proc.getPartitiontable().getTypeName(),
               columnName = proc.getPartitioncolumn().getTypeName(),
               paramIndex = Integer.toString(proc.getPartitionparameter());
        if (proc.getPartitiontable2() == null) {
            return new ProcedurePartitionData(tableName, columnName, paramIndex);
        }

        // Two-partition transaction
        String tableName2 =  proc.getPartitiontable2().getTypeName(),
               columnName2 = proc.getPartitioncolumn2().getTypeName(),
               paramIndex2 = Integer.toString(proc.getPartitionparameter2());
        return new ProcedurePartitionData(tableName, columnName, paramIndex,
                                          tableName2, columnName2, paramIndex2);
    }

    /**
     * For Testing usage ONLY.
     * From a partition information string to @ProcedurePartitionData
     *
     * string format:
     *     1) String.format("%s.%s: %s", tableName, columnName, parameterNo)
     *     1) String.format("%s.%s: %s, %s.%s: %s", tableName, columnName, parameterNo, tableName2, columnName2, parameterNo2)
     *
     * Result is limited to SINGLE, MULTI, TWOPAR types
     *
     * @return
     */
    public static ProcedurePartitionData fromPartitionInfoString(String partitionInfoString) {
        if (partitionInfoString == null || partitionInfoString.trim().isEmpty()) {
            return new ProcedurePartitionData(Type.MULTI);
        }

        String[] infoParts = partitionInfoString.split(",");
        assert infoParts.length <= 2;

        if (infoParts.length == 2) {
            ProcedurePartitionData info1 = fromPartitionInfoString(infoParts[0]);
            ProcedurePartitionData info2 = fromPartitionInfoString(infoParts[1]);
            return new ProcedurePartitionData(info1.m_tableName, info1.m_columnName, info1.m_paramIndex,
                                              info2.m_tableName, info2.m_columnName, info2.m_paramIndex);
        }

        // Split on the colon: "columnInfo : paramIndex"
        String[] partsA = infoParts[0].split(":");
        assert partsA.length == 2;

        // Split columnInfo on the dot: "tableName . columnName"
        String[] partsB = partsA[0].split("\\.");
        assert partsB.length == 2;

        return new ProcedurePartitionData(partsB[0].trim(), partsB[1].trim(), partsA[1].trim());
    }

    /**
     * Debug use
     */
    @Override
    public String toString() {
        return String.format("%s [%s/%s/%s%s]", m_type, m_tableName, m_columnName, m_paramIndex,
                             m_tableName2 == null ? ""
                             : String.format(", %s/%s/%s", m_tableName2, m_columnName2, m_paramIndex2));
    }
}
