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
package eng1969.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;

/**
 *  Load the levelDB backing store with raw data.
 *  This creates a 100% cold cache.
 */
public class CreateKey extends VoltProcedure {

    public final SQLStmt unused = new SQLStmt("SELECT TOP 1 rowid FROM backed WHERE rowid = ?");

    public long run(long rowid, long rowid_group, byte[] payload)
    {
        try {
            DB db = m_site.getLevelDBInstance();
            Long id = Long.valueOf(rowid);
            Long group = Long.valueOf(rowid_group);
            String key = group.toString() + "_" + id.toString();
            db.put(key.getBytes(), payload);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to put a DB key.", true, e);
        }

        return 1L;
    }
}
