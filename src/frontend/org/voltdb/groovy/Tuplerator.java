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

import static java.lang.Character.toLowerCase;
import static java.lang.Character.toUpperCase;
import groovy.lang.Closure;
import groovy.lang.GString;
import groovy.lang.GroovyObjectSupport;

import java.util.NavigableMap;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import com.google_voltpatches.common.collect.ImmutableSortedMap;

/**
 * Groovy table access expediter. It allows you to easily navigate a VoltTable,
 * and access its column values.
 * <p>
 * Example usage on a query that returns results for the :
 * <code><pre>
 * cr = client.callProcedure('@AdHoc','select INTEGER_COL, STRING_COL from FOO')
 * tuplerator(cr.results[0]).eachRow {
 *
 *   integerColValueByIndex = it[0]
 *   stringColValueByIndex = it[1]
 *
 *   integerColValueByName = it['integerCol']
 *   stringColValyeByName = it['stringCol']
 *
 *   integerColValueByField = it.integerCol
 *   stringColValyeByField = it.stringCol
 * }
 *
 * </code></pre>
 *
 */
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

    /**
     * It calls the given closure on each row of the underlying table by passing itself
     * as the only closure parameter
     *
     * @param c the self instance of Tuplerator
     */
    public void eachRow(Closure<Void> c) {
        while (table.advanceRow()) {
            c.call(this);
        }
        table.resetRowPosition();
    }

    /**
     * It calls the given closure on each row of the underlying table for up to the specified limit,
     * by passing itself as the only closure parameter
     *
     * @param maxRows maximum rows to call the closure on
     * @param c closure
     */
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

    /**
     * Sets the table row cursor to the given position
     * @param num row number to set the row cursor to
     * @return an instance of self
     */
    public Tuplerator atRow(int num) {
        table.advanceToRow(num);
        return this;
    }

    /**
     * Resets the table row cursor
     * @return an instance of self
     */
    public Tuplerator reset() {
        table.resetRowPosition();
        return this;
    }

    /**
     * Returns the underlying table
     * @return the underlying table
     */
    public VoltTable getTable() {
        return table;
    }
}
