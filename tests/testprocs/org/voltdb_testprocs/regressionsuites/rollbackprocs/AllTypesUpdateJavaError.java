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

public class AllTypesUpdateJavaError extends VoltProcedure {
    public final SQLStmt update =
        new SQLStmt("UPDATE ALL_TYPES SET TINY = ?, SMALL = ?, BIG = ?,"
                    + " T = ?, RATIO = ?, MONEY = ?, INLINED = ?, UNINLINED = ?"
                    + " WHERE ID = ?;");

    public long run(int id) {
        // update 100 times using different values
        byte base = 1;

        for (int i = 0; i < 100; i++, base++) {
            char[] uninlineable = new char[1000];
            for (int j = 0; j < uninlineable.length; j++)
                uninlineable[j] = (char) (base + 32);

            voltQueueSQL(update, base + 1, base + 2, base + 3,
                         new TimestampType().getTime(),
                         base / 100.0, new BigDecimal(base),
                         String.valueOf(base + 32),
                         String.valueOf(uninlineable), id);
        }

        long tupleChanged = voltExecuteSQL()[0].asScalarLong();
        assert(tupleChanged > 0);

        // divide by 0 to ensure the transaction gets rolled back
        return (5 / 0);
    }
}
