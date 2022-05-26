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
import org.voltdb.VoltTable;

/** A class which depends on a base class. */
public class GetGreetingCaseInsensitive extends GetGreetingBase {

    protected static final SQLStmt SELECT_BY_LANGUAGE_STATEMENT = new SQLStmt("SELECT * FROM greetings WHERE LOWER(language) = ?");

    /** Gets the greeting which matches the specified language exactly */
    public VoltTable[] run(String language) {
        voltQueueSQL(SELECT_BY_LANGUAGE_STATEMENT, EXPECT_ZERO_OR_ONE_ROW, language.toLowerCase());
        VoltTable[] results = voltExecuteSQL();
        incrementCounterIfNeeded(results, true);
        return results;
    }
}
