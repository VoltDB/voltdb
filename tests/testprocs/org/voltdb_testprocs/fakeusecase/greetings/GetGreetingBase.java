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

package org.voltdb_testprocs.fakeusecase.greetings;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/** A base class with no external dependencies. */
public class GetGreetingBase extends VoltProcedure {

    protected static final SQLStmt SELECT_BY_LANGUAGE_STATEMENT = new SQLStmt("SELECT * FROM greetings WHERE language = ?");
    protected static final SQLStmt INCREMENT_STATS_STATEMENT = new SQLStmt("UPDATE greetings SET querycount = querycount + 1 WHERE language = ?");

    protected void incrementCounterIfNeeded(VoltTable[] resultsFromSelect, boolean thisIsLast) {
        if (resultsFromSelect.length > 0 && resultsFromSelect[0].getRowCount() > 0) {
            resultsFromSelect[0].advanceRow();
            // Fetch language again because, in derived classes, it may be different than the argument passed in.
            String language = resultsFromSelect[0].getString("language");
            voltQueueSQL(INCREMENT_STATS_STATEMENT, EXPECT_EMPTY, language);
            voltExecuteSQL(thisIsLast);
        }
    }
}
