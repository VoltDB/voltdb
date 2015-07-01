/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb_testprocs.regressionsuites;

import org.voltdb.VoltProcedure;
import org.voltdb.common.Constants;
import org.voltdb.utils.Encoder;

/**
 * For testing arrays of varbinaries and arrays of strings that are serialized as bytes.
 * This proc expects certain values from TestProcedureAPISuite.
 *
 */
public class BufferArrayProc extends VoltProcedure {

    public long run(byte[][] data1, String[] data2, byte[][] data3) {
        for (byte[] b : data1) {
            if (new String(b, Constants.UTF8ENCODING).equals("Hello") == false) {
                throw new VoltAbortException("bad match a");
            }
        }
        for (String s : data2) {
            if (s.equals("Hello") == false) {
                throw new VoltAbortException("bad match b");
            }
        }
        if (Encoder.hexEncode(data3[0]).equalsIgnoreCase("AAbbff00") == false) {
            throw new VoltAbortException("bad match d");
        }
        if (Encoder.hexEncode(data3[1]).equalsIgnoreCase("AAbbff0011") == false) {
            throw new VoltAbortException("bad match e");
        }
        if (Encoder.hexEncode(data3[2]).equalsIgnoreCase("1234567890abcdef") == false) {
            throw new VoltAbortException("bad match f");
        }

        return 0;
    }
}
