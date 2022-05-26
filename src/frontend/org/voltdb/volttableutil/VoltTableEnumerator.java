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

import org.apache.calcite.linq4j.Enumerator;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

import java.sql.Timestamp;

/**
 * Enumerator that reads from a VoltTable object.
 *
 * @param <E> Row type
 */
public class VoltTableEnumerator<E> implements Enumerator<E> {
    private VoltTable m_table;
    private RowConverter<E> rowConvert;

    public VoltTableEnumerator(int[] fields, VoltTable table) {
        m_table = table;
        reset();
        rowConvert = (RowConverter<E>) new VoltTableRowConverter(fields);
    }

    /**
     * Row converter.
     *
     * @param <E> element type
     */
    abstract static class RowConverter<E> {
        abstract E convertRow(VoltTableRow row);
    }

    /**
     * Row converter for VoltTable.
     */
    static class VoltTableRowConverter extends RowConverter<Object[]> {
        private int[] fields;

        public VoltTableRowConverter(int[] fields) {
            this.fields = fields;
        }

        @Override
        Object[] convertRow(VoltTableRow row) {
            Object[] objects = new Object[fields.length];
            int i = 0;
            for (int field : this.fields) {
                VoltType colType = row.getColumnType(field);
                Object value;
                if (colType == VoltType.TIMESTAMP) {
                    // TODO: this is not quite correct, need double check
                    value = row.getTimestampAsSqlTimestamp(field);
                    if (value != null) {
                        value = ((Timestamp) value).getTime();
                    }
                } else {
                    value = row.get(field, colType);
                }
                if (row.wasNull()) {
                    value = row.getColumnType(field).getNullValue();
                }
                objects[i++] = value;
            }
            return objects;
        }
    }

    public void close() {
    }

    public E current() {
        return rowConvert.convertRow(m_table);
    }

    public boolean moveNext() {
        return m_table.advanceRow();
    }

    public void reset() {
        m_table.resetRowPosition();
    }
}
