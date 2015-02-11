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

package org.hsqldb_voltpatches;

import java.util.Iterator;

import junit.framework.TestCase;

public class TestVoltTokenizer extends TestCase {

    public void testVoltTokenIterator() {
        VoltTokenStream str = new VoltTokenStream("SELECT * FROM foo; -- what? \n SELECT * FROM bar;");
        Iterator<VoltToken> it = str.iterator();

        assertTrue(it != null);
        assertTrue(it.hasNext());

        StringBuilder sb = new StringBuilder();
        for (VoltToken t : str) {
            sb.append(t + "__");
        }

        // Whitespace stripped, comments removed, identifiers upper-cased.
        String expected = "SELECT__*__FROM__FOO__;__SELECT__*__FROM__BAR__;__";
        assertEquals(expected, sb.toString());
    }

}
