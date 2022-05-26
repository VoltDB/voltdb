/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

public class VoltTableTestHelpers {

    static public boolean moveToMatchingRow(VoltTable table, String columnName,
                              String columnValue)
    {
        boolean found = false;
        table.resetRowPosition();
        while (table.advanceRow())
        {
            if (((String)table.get(columnName, VoltType.STRING)).
                    equalsIgnoreCase(columnValue.toUpperCase()))
            {
                found = true;
                break;
            }
        }
        return found;
    }

    static public boolean moveToMatchingTupleRow(VoltTable table, String column1Name,
                              String column1Value, String column2Name,
                              String column2Value)
    {
        boolean found = false;
        table.resetRowPosition();
        while (table.advanceRow())
        {
            if (((String)table.get(column1Name, VoltType.STRING)).
                    equalsIgnoreCase(column1Value.toUpperCase()) &&
                    ((String)table.get(column2Name, VoltType.STRING)).
                    equalsIgnoreCase(column2Value.toUpperCase()))
            {
                found = true;
                break;
            }
        }
        return found;
    }
}
