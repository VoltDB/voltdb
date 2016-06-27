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

public class InsertXdcrReplicatedActualSP extends VoltProcedure {

    private final SQLStmt r_insert = new SQLStmt(
            "INSERT INTO xdcr_replicated_conflict_actual (cid, rid, clusterid, current_clusterid, current_ts," +
                    " row_type, action_type, conflict_type, conflict_on_primary_key, decision, ts, divergence, tuple)" +
                    "   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public VoltTable[] run(byte cid, long rid, long clusterid, long current_clusterid, String current_ts,
                           String row_type, String action_type, String conflict_type, byte conflict_on_primary_key,
                           String decision, String ts, String divergence, byte[] tuple) {
        voltQueueSQL(r_insert, cid, rid, clusterid, current_clusterid, current_ts,
                row_type, action_type, conflict_type, conflict_on_primary_key, decision, ts, divergence, tuple);
        return voltExecuteSQL(true);
    }
}
