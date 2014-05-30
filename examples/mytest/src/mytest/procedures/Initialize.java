/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package mytest.procedures;

import java.lang.Integer;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class Initialize extends VoltProcedure
{
    // Check if the database has already been initialized
    public final SQLStmt checkStmt = new SQLStmt("SELECT COUNT(*) FROM movies;");

    public final SQLStmt insertStmt = new SQLStmt("INSERT INTO movies VALUES (?, ?);");

    public long run() {
        voltQueueSQL(checkStmt, EXPECT_SCALAR_LONG);
        long existingMoviesCount = voltExecuteSQL()[0].asScalarLong();

        if (existingMoviesCount != 0)
            return existingMoviesCount;

        for (int i=0; i<1000;i++)
            voltQueueSQL(insertStmt, i, "" + i);
        voltExecuteSQL();

        voltQueueSQL(checkStmt, EXPECT_SCALAR_LONG);
        existingMoviesCount = voltExecuteSQL()[0].asScalarLong();

        if (existingMoviesCount != 1000)
        {
            System.out.println("error in initilization\n");
            return existingMoviesCount;
        }

        return 5;
    }
}
