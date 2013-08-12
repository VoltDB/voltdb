/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import static java.lang.Character.toLowerCase;
import static java.lang.Character.toUpperCase;
import groovy.lang.Closure;
import groovy.lang.GString;
import groovy.lang.GroovyObjectSupport;

import java.util.NavigableMap;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import com.google.common.collect.ImmutableSortedMap;

public class Tuplerator extends GroovyObjectSupport {

    private final VoltTable table;
    private final VoltType [] byIndex;
    private final NavigableMap<String, Integer> byName;

    public static Tuplerator newInstance(VoltTable table) {
        return new Tuplerator(table);
    }

    public Tuplerator(final VoltTable table) {
        this.table = table;
        this.byIndex = new VoltType[table.getColumnCount()];

        ImmutableSortedMap.Builder<String, Integer> byNameBuilder =
                ImmutableSortedMap.naturalOrder();

        for (int c = 0; c < byIndex.length; ++c) {
            VoltType cType = table.getColumnType(c);
            StringBuilder cName = new StringBuilder(table.getColumnName(c));

            byIndex[c] = cType;
            // byNameBuilder.put(cName.toString(), c);

            boolean upperCaseIt = false;
            for (int i = 0; i < cName.length();) {
                char chr = cName.charAt(i);
                if (chr == '_' || chr == '.' || chr == '$') {
                    cName.deleteCharAt(i);
                    upperCaseIt = true;
                } else {
                    chr = upperCaseIt ? toUpperCase(chr) : toLowerCase(chr);
                    cName.setCharAt(i, chr);
                    upperCaseIt = false;
                    ++i;
                }
            }
            byNameBuilder.put(cName.toString(),c);
        }
        byName = byNameBuilder.build();
    }

    public void eachRow(Closure<Void> c) {
        while (table.advanceRow()) {
            c.call(this);
        }
        table.resetRowPosition();
    }

    public void eachRow(int maxRows, Closure<Void> c) {
        while (--maxRows >= 0 && table.advanceRow()) {
            c.call(this);
        }
    }

    public Object getAt(int cidx) {
        Object cval = table.get(cidx, byIndex[cidx]);
        if (table.wasNull()) cval = null;
        return cval;
    }

    public Object getAt(String cname) {
        Integer cidx = byName.get(cname);
        if (cidx == null) {
            throw new IllegalArgumentException("No Column named '" + cname + "'");
        }
        return getAt(cidx);
    }

    public Object getAt(GString cname) {
        return getAt(cname.toString());
    }

    @Override
    public Object getProperty(String name) {
        return getAt(name);
    }

    public Tuplerator atRow(int num) {
        table.advanceToRow(num);
        return this;
    }

    public Tuplerator reset() {
        table.resetRowPosition();
        return this;
    }

    public VoltTable getTable() {
        return table;
    }
}
