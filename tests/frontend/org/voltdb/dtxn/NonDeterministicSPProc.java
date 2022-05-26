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

package org.voltdb.dtxn;

import java.util.Random;

import org.voltdb.SQLStmt;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.iv2.DeterminismHash;

public class NonDeterministicSPProc extends VoltProcedure {

    static final int NO_PROBLEM = 0;
    static final int MISMATCH_VALUES = 1;
    static final int MISMATCH_INSERTION = 2;
    static final int MISMATCH_WHITESPACE_IN_SQL = 3;
    static final int MULTI_STATEMENT_MISMATCH = 4;
    static final int TXN_ABORT = 5;
    static final int PARTIAL_STATEMENT_MISMATCH = 6;

    public static final SQLStmt sql = new SQLStmt("insert into kv \nvalues ?, ?");
    public static final SQLStmt sql2 = new SQLStmt("insert into  kv values ?, ?");
    public static final SQLStmt sql3 = new SQLStmt("select key from kv where nondetval = ? order by key limit 100");
    public static final SQLStmt sql4 = new SQLStmt("update kv set nondetval = ? where key = ?;");

    public VoltTable run(long key, long value, int failType) {
        long id = VoltDB.instance().getHostMessenger().getHostId();
        String hostId = System.getProperty("__VOLTDB_CLUSTER_HOSTID__");
        String targetHostId = System.getProperty("__VOLTDB_TARGET_CLUSTER_HOSTID__");
        if (hostId != null && targetHostId != null && (hostId.equals(targetHostId) || targetHostId.equals("*"))) {
            System.loadLibrary("VoltDBMissingLibraryTrap");
        }

        String disabled = System.getProperty("DISABLE_HASH_MISMATCH_TEST");
        if ("true".equalsIgnoreCase(disabled)) {
            failType = NO_PROBLEM;
        }
        if (failType == MISMATCH_INSERTION) {
            voltQueueSQL(sql, key, id);
        } else if (failType == MULTI_STATEMENT_MISMATCH) {
            // get 100 random rows
            voltQueueSQL(sql3, value);
            VoltTable[] result = voltExecuteSQL();
            // non-deterministic insertion
            VoltTable rows = result[0];
            Random r = new Random();
            int i = 0;
            while (rows.advanceRow()) {
                if (i > DeterminismHash.MAX_HASHES_COUNT && i % 13 == 0) {
                    voltQueueSQL(sql4, r.nextLong(), rows.getLong(0));
                } else {
                    voltQueueSQL(sql4, value, rows.getLong(0));
                }
                i++;
            }
        } else if (failType == TXN_ABORT) {
            voltQueueSQL(sql, key, value);
        }
        else if ((failType == MISMATCH_WHITESPACE_IN_SQL) && (id % 2 == 0)) {
            voltQueueSQL(sql2, key, value);
        }
        else if(failType == PARTIAL_STATEMENT_MISMATCH) {
            int pos = (int) (Math.random()*(30));
            // get 100 random rows
            voltQueueSQL(sql3, value);
            VoltTable[] result = voltExecuteSQL();
            // non-deterministic insertion
            VoltTable rows = result[0];
            Random r = new Random();
            int i = 0;
            while (rows.advanceRow()) {
                if (i > pos) {
                    voltQueueSQL(sql4, r.nextLong(), rows.getLong(0));
                } else {
                    voltQueueSQL(sql4, value, rows.getLong(0));
                }
                i++;
            }
        }
        else {
            voltQueueSQL(sql, key, value);
        }
        voltExecuteSQL();

        if ((failType == TXN_ABORT) && (id % 2 == 1)) {
            throw new VoltAbortException("Crash deliberately.");
        }

        VoltTable retval = new VoltTable(new ColumnInfo("", VoltType.BIGINT));

        // non deterministic by value
        if (failType == MISMATCH_VALUES) {
            retval.addRow(id);
        }

        return retval;
    }

}
