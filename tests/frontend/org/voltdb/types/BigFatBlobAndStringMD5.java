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

package org.voltdb.types;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

/**
 * Accepts varbinary and string blobs and returns an MD5 digest for each so that the
 * caller can compare against its own MD5 calculations. Can't return the data directly
 * due to VoltTable size limitations (1 MB).
 */
public class BigFatBlobAndStringMD5 extends VoltProcedure {
    public VoltTable run(byte[] b, String s) {
        VoltTable t = new VoltTable(new ColumnInfo("b_md5", VoltType.VARBINARY),
                                    new ColumnInfo("s_md5", VoltType.VARBINARY));
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            t.addRow(md5.digest(b), md5.digest(s.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            // If the row wasn't added the test caller will consider it a failure.
        }
        return t;
    }
}
