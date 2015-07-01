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

//
// Initializes the database, pushing the list of contestants and documenting domain data (Area codes and States).
//

package LiveRejoinConsistency.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
        singlePartition = false
        )
public class Initialize extends VoltProcedure
{

    // Inserts a counter
    public final SQLStmt insert1 = new SQLStmt("INSERT INTO joiner (id) VALUES (?);");
    public final SQLStmt insert2 = new SQLStmt("INSERT INTO counters_ptn (id, counter) VALUES (?, ?);");
    public final SQLStmt insert3 = new SQLStmt("INSERT INTO counters_rep (id, counter) VALUES (?, ?);");


    // Delete a counter
    public final SQLStmt delete1 = new SQLStmt("DELETE FROM joiner;");
    public final SQLStmt delete2 = new SQLStmt("DELETE FROM counters_rep;");
    public final SQLStmt delete3 = new SQLStmt("DELETE FROM counters_ptn;");
    public final SQLStmt delete4 = new SQLStmt("DELETE FROM like_counters_rep;");
    public final SQLStmt delete5 = new SQLStmt("DELETE FROM like_counters_ptn;");


    public long run(int inc) {

        voltQueueSQL(delete1);
        voltQueueSQL(delete2);
        voltQueueSQL(delete3);
        voltQueueSQL(delete4);
        voltQueueSQL(delete5);

        // initialize the data
        for (int i=0; i < inc; i++) {
            voltQueueSQL(insert1, i);
            voltQueueSQL(insert2, i, i);
            voltQueueSQL(insert3, i, i);
        }
        voltExecuteSQL();

        return 0;
    }
}
