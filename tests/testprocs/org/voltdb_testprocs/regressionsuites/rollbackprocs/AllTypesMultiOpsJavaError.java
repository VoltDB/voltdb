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

package org.voltdb_testprocs.regressionsuites.rollbackprocs;

import java.math.BigDecimal;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.types.TimestampType;

public class AllTypesMultiOpsJavaError extends VoltProcedure {
    public final static int INSERT   = 0;
    public final static int UPDATE   = 1;
    public final static int DELETE   = 2;
    public final static int TRUNCATE = 3;

    public static final SQLStmt INSERT_STMT = new SQLStmt(
            "INSERT INTO ALL_TYPES VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");
    public static final SQLStmt UPDATE_STMT = new SQLStmt(
            "UPDATE ALL_TYPES SET TINY = ?, SMALL = ?, BIG = ?,"
                     + " T = ?, RATIO = ?, MONEY = ?, INLINED = ?, UNINLINED = ?"
                     + " WHERE ID = ?;");
    public static final SQLStmt DELETE_STMT = new SQLStmt("DELETE FROM ALL_TYPES WHERE ID = ?;");
    public static final SQLStmt TRUNCATE_STMT = new SQLStmt("TRUNCATE TABLE ALL_TYPES;");
    public final SQLStmt[] statements =
        {INSERT_STMT,
         UPDATE_STMT,
         DELETE_STMT,
         TRUNCATE_STMT};

    public long run(int[] order, int id) {
        byte base = 1;

        for (int i : order) {
            char[] uninlineable = new char[1000];
            for (int j = 0; j < uninlineable.length; j++)
                uninlineable[j] = (char) (base + 32);

            switch (i) {
            case INSERT:
                voltQueueSQL(statements[i], id, base + 1, base + 2, base + 3,
                             new TimestampType().getTime(),
                             base / 100.0, new BigDecimal(base),
                             String.valueOf(base + 32),
                             String.valueOf(uninlineable));
            case UPDATE:
                voltQueueSQL(statements[i], base + 1, base + 2, base + 3,
                             new TimestampType().getTime(),
                             base / 100.0, new BigDecimal(base),
                             String.valueOf(base + 32),
                             String.valueOf(uninlineable), id);
            case DELETE:
                voltQueueSQL(statements[i], id);
            case TRUNCATE:
                voltQueueSQL(statements[i]);
            }

            base++;
        }

        long tupleChanged = voltExecuteSQL()[0].asScalarLong();
        assert(tupleChanged > 0);

        // divide by 0 to cause error so that everything will be rolled back.
        return (5 / 0);
    }
}
