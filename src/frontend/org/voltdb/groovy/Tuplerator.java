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

import groovy.lang.Closure;

import org.voltdb.VoltTable;

public class Tuplerator {

    private final VoltTable table;

    public static Tuplerator newInstance(VoltTable table) {
        return new Tuplerator(table);
    }

    public Tuplerator(final VoltTable table) {
        this.table = table;
    }

    public void eachRow(Closure<Void> c) {
        while (table.advanceRow()) {
            c.call(table);
        }
    }

    public void eachRow(int maxRows, Closure<Void> c) {
        while (--maxRows >= 0 && table.advanceRow()) {
            c.call(table);
        }
    }

}
