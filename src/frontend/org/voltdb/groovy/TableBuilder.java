/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.groovy;

import groovy.lang.Closure;
import groovy.lang.GString;

import java.util.Map;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;

/**
 * It allows a more fluent but yet terse way to build a VoltTable
 * Example usage:
 * <code><pre>
 *   buildTable(id:INTEGER,name:STRING) {
 *     row 1, John
 *     row 2, Claire
 *   }
 *   </pre></code>
 *
 */
public class TableBuilder {
    private final VoltTable table;

    public static TableBuilder newInstance(Map<String,VoltType> cols) {
        return new TableBuilder(cols);
    }

    public TableBuilder(Map<String,VoltType> cols) {
        VoltTable.ColumnInfo [] infos = new  VoltTable.ColumnInfo [cols.size()];
        int i = 0;
        for (Map.Entry<String, VoltType> e: cols.entrySet()) {
            infos[i++] = new VoltTable.ColumnInfo(e.getKey(), e.getValue());
        }
        table = new VoltTable(infos);
    }

    @SuppressWarnings("unchecked")
    public VoltTable make(Closure<Void> rows) {
        Closure<Void> clone = (Closure<Void>)rows.clone();

        clone.setDelegate(this);
        clone.setResolveStrategy(Closure.DELEGATE_FIRST);
        clone.call();

        return table;
    }

    public VoltTable getTable() {
        return table;
    }

    public void row(Object...values) {
        for (int i = 0; i < values.length; ++i) {
            values[i] = (values[i] instanceof GString) ? values[i].toString() : values[i];
        }
        table.addRow(values);
    }
}
