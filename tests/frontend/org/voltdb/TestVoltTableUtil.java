/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.utils.VoltTableUtil;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

public class TestVoltTableUtil {
    @Test
    public void testCSVNullConversion() throws IOException {
        CSVWriter writer = mock(CSVWriter.class);
        ColumnInfo[] columns = new ColumnInfo[] {new ColumnInfo("", VoltType.BIGINT),
                                                 new ColumnInfo("", VoltType.FLOAT),
                                                 new ColumnInfo("", VoltType.DECIMAL),
                                                 new ColumnInfo("", VoltType.STRING),
                                                 new ColumnInfo("", VoltType.TIMESTAMP),
                                                 new ColumnInfo("", VoltType.VARBINARY)};
        ArrayList<VoltType> columnTypes = new ArrayList<VoltType>();
        VoltTable vt = new VoltTable(columns);
        vt.addRow(VoltType.NULL_BIGINT,
                  VoltType.NULL_FLOAT,
                  VoltType.NULL_DECIMAL,
                  VoltType.NULL_STRING_OR_VARBINARY,
                  VoltType.NULL_TIMESTAMP,
                  VoltType.NULL_STRING_OR_VARBINARY);

        for (ColumnInfo ci : columns) {
            columnTypes.add(ci.type);
        }

        VoltTableUtil.toCSVWriter(writer, vt, columnTypes);

        ArgumentCaptor<String[]> captor = ArgumentCaptor.forClass(String[].class);
        verify(writer).writeNext(captor.capture());
        String[] values = captor.getValue();
        assertEquals(6, values.length);
        for (String v : values) {
            assertEquals("\\N", v);
        }
    }
}
