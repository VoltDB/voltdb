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

package org.voltdb_testprocs.regressionsuites.malicious;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class GoSleep extends VoltProcedure {

    private static final String returnString;

    static {
        byte stringBytes[] = null;
        try {
            stringBytes =
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        ByteBuffer b = ByteBuffer.allocate(Short.MAX_VALUE - 200);
        while(b.remaining() > stringBytes.length) {
            b.put(stringBytes);
        }
        String tempString = null;
        try {
            tempString = new String(b.array(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        returnString = tempString;
    }
    public VoltTable[] run(short sleepTime, int returnData, byte junkData[])
    {
        try {
            Thread.sleep(sleepTime);
            if (sleepTime != 0) {
                System.out.println("Go sleep slept for " + sleepTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (returnData != 0) {
            /**
             * Return large data set so that IO buffers fill faster
             */
            final VoltTable table = new VoltTable(new VoltTable.ColumnInfo[] {
                    new VoltTable.ColumnInfo("foo", VoltType.STRING),
                    new VoltTable.ColumnInfo("foo", VoltType.STRING),
                    new VoltTable.ColumnInfo("foo", VoltType.STRING)
            });
            table.addRow(returnString, returnString, returnString);

            return new VoltTable[] {table};
        } else {
            return new VoltTable[0];
        }
    }
}
