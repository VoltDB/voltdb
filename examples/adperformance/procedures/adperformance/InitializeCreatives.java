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

package adperformance;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class InitializeCreatives extends VoltProcedure {
    public final SQLStmt upsert = new SQLStmt(
        "UPSERT INTO creatives VALUES (?,?,?,?,?,?);"
        );

    public long run(long id, int advertiser, int campaigns, int creatives)
        throws VoltAbortException {
        for (int campaign=1; campaign<=campaigns; campaign++) {
            for (int i=1; i<=creatives; i++) {
                id++;
                voltQueueSQL(upsert, id, campaign, advertiser, "https://example.com",
                             "ExampleName", "ExampleDescription");
            }
        }

        voltExecuteSQL(true);

        return 0;
    }
}
