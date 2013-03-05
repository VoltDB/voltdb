/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

public class TestVoltSystemProcedure {
    @Test
    public void testUnionTables()
    {
        VoltTable.ColumnInfo[] columns =
                new VoltTable.ColumnInfo[] {new VoltTable.ColumnInfo("ID", VoltType.BIGINT)};
        VoltTable table1 = new VoltTable(columns);
        table1.addRow(1);
        VoltTable table2 = new VoltTable(columns);
        table2.addRow(2);

        VoltTable result = VoltSystemProcedure.unionTables(Arrays.asList(null, table1,
                null, table2));
        assertNotNull(result);

        Set<Long> numbers = new HashSet<Long>();
        result.resetRowPosition();
        while (result.advanceRow()) {
            long i = result.getLong(0);
            numbers.add(i);
        }

        assertEquals(2, numbers.size());
        assertTrue(numbers.contains(1l));
        assertTrue(numbers.contains(2l));
    }
}
