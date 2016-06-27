/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package xdcrSelfCheck.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.SQLException;

import java.util.Arrays;

public class DeletePartitionedSP extends VoltProcedure {

    public final SQLStmt p_getCIDData = new SQLStmt(
            "SELECT * FROM xdcr_partitioned p WHERE p.cid=? AND p.rid =? ORDER BY p.cid, p.rid desc;");

    public final SQLStmt p_delete = new SQLStmt(
            "DELETE FROM xdcr_partitioned WHERE cid=? AND rid=?;");

    public VoltTable[] run(byte cid, long rid, byte[] key, byte[] value, byte rollback, String scenario) {
        voltQueueSQL(p_getCIDData, cid, rid);
        VoltTable[] results = voltExecuteSQL();
        VoltTable data = results[0];
        if (data.getRowCount() == 0) {
            throw new SQLException(getClass().getName() +
                    " No record in table xdcr_partitioned for cid " + cid + ", rid " + rid + ", scenario " + scenario);
        }

        data.advanceRow();
        byte[] extKey = data.getVarbinary("key");
        if (! Arrays.equals(extKey, key)) {
            throw new SQLException(getClass().getName() +
                    " existing key " + extKey + " does not match expected key " + key +
                    " for cid " + cid + ", rid " + rid + ", scenario " + scenario);
        }

        byte[] extValue = data.getVarbinary("value");
        if (! Arrays.equals(extValue, value)) {
            throw new SQLException(getClass().getName() +
                    " existing value " + extValue + " does not match expected key " + value +
                    " for cid " + cid + ", rid " + rid + ", scenario " + scenario);
        }

        voltQueueSQL(p_delete, cid, rid);
        voltQueueSQL(p_getCIDData, cid, rid);
        return voltExecuteSQL(true);
    }
}
