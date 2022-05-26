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

package org.voltdb.volttableutil;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.voltdb.VoltTable;
import org.voltdb.plannerv2.ColumnTypes;

/**
 * An adaptor between a {@link VoltTable} and a {@link ScannableTable}.
 */
public class VoltScannableTable extends AbstractTable implements ScannableTable {
    private final VoltTable m_table;

    private RelDataType m_dataType;

    VoltScannableTable(VoltTable table) {
        m_table = table;
        m_dataType = null;
    }

    /**
     * Returns an array of integers {0, ..., n - 1}.
     */
    private static int[] identityList(int n) {
        int[] integers = new int[n];
        for (int i = 0; i < n; i++) {
            integers[i] = i;
        }
        return integers;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (m_dataType == null) {
            RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
            for (int i = 0; i < m_table.getColumnCount(); i++) {
                RelDataType sqlType = typeFactory.createSqlType(
                        ColumnTypes.getCalciteType(m_table.getColumnType(i)));
                sqlType = SqlTypeUtil.addCharsetAndCollation(sqlType, typeFactory);
                fieldInfo.add(m_table.getColumnName(i).toUpperCase(), sqlType);
            }
            m_dataType = typeFactory.createStructType(fieldInfo);
        }
        return m_dataType;
    }

    public Enumerable<Object[]> scan(DataContext root) {
        final int[] fields = identityList(this.m_dataType.getFieldCount());
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new VoltTableEnumerator<>(fields, m_table);
            }
        };
    }
}
