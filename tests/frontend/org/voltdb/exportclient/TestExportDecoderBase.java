/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.exportclient;

import java.io.IOException;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.voltdb.VoltType;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.messaging.FastDeserializer;

public class TestExportDecoderBase extends TestCase
{
    class StubExportDecoder extends ExportDecoderBase
    {
        public StubExportDecoder(AdvertisedDataSource source)
        {
            super(source);
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData)
        {
            return false;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            // TODO Auto-generated method stub

        }
    }

    static final String[] COLUMN_NAMES =
    {"tinyint", "smallint", "integer", "bigint", "float", "timestamp",
     "string", "decimal"};

    static final VoltType[] COLUMN_TYPES =
    {VoltType.TINYINT, VoltType.SMALLINT, VoltType.INTEGER,
     VoltType.BIGINT, VoltType.FLOAT, VoltType.TIMESTAMP,
     VoltType.STRING, VoltType.DECIMAL};

    static AdvertisedDataSource constructTestSource() {
        return constructTestSource(0);
    }

    static AdvertisedDataSource constructTestSource(int partition)
    {
        ArrayList<String> col_names = new ArrayList<String>();
        ArrayList<VoltType> col_types = new ArrayList<VoltType>();
        for (int i = 0; i < COLUMN_TYPES.length; i++)
        {
            col_names.add(COLUMN_NAMES[i]);
            col_types.add(COLUMN_TYPES[i]);
        }
        AdvertisedDataSource source =
            new AdvertisedDataSource(partition, "foo", "yankeelover", 0, 32,
                                     col_names, col_types);
        return source;
    }

    public void testNullFlags() throws IOException
    {
        StubExportDecoder dut =
            new StubExportDecoder(constructTestSource());
        for (int i = 0; i < COLUMN_TYPES.length; i++)
        {
            byte[] flag = new byte[1];
            flag[0] = (byte)(1 << 8 - i - 1);
            FastDeserializer fds = new FastDeserializer(flag);
            boolean[] nulls = dut.extractNullFlags(fds);
            for (int j = 0; j < COLUMN_TYPES.length; j++)
            {
                if (j == i)
                {
                    assertTrue(nulls[j]);
                }
                else
                {
                    assertFalse(nulls[j]);
                }
            }
        }
    }
}
