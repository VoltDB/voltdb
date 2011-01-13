/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.catalog.gui;

import org.voltdb.catalog.CatalogType;
import javax.swing.table.AbstractTableModel;

public class FieldViewerModel extends AbstractTableModel {
    private static final long serialVersionUID = 1L;

    String[] m_columnNames = { "field", "value" };
    CatalogType m_type = null;

    void setCatalogType(CatalogType type) {
        m_type = type;
    }

    @Override
    public int getColumnCount() {
        return 2;
    }

    @Override
    public int getRowCount() {
        if (m_type == null)
            return 0;
        return m_type.getFields().size();
    }

    @Override
    public String getColumnName(int col) {
        return m_columnNames[col];
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        if (rowIndex >= getRowCount())
            return null;
        if (columnIndex >= 2)
            return null;

        int i = 0;
        for (String fieldName : m_type.getFields()) {
            if (i == rowIndex) {
                if (columnIndex == 0)
                    return fieldName;
                if (columnIndex == 1)
                    return m_type.getField(fieldName);
            }
            i++;
        }

        return null;
    }

}
