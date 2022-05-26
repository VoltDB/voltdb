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
package voltkvqa.procedures;

import org.voltdb.*;

public class Initialize extends VoltProcedure
{
    // Delete everything
    public final SQLStmt cleanStmt = new SQLStmt("DELETE FROM store;");

    // Inserts a key/value pair
    public final SQLStmt insertStmt = new SQLStmt("INSERT INTO store (key, value) VALUES (?, ?);");

    public long run(int startIndex, int stopIndex, String keyFormat, byte[] defaultValue)
    {
        // Wipe out the data store to re-initialize
        if (startIndex == 0)
        {
            voltQueueSQL(cleanStmt);
            voltExecuteSQL();
        }

        // Initialize the data store with given parameters
        int batchSize = 0;
        for(int i=startIndex;i<stopIndex;i++)
        {
            voltQueueSQL(insertStmt, String.format(keyFormat, i), defaultValue);
            batchSize++;
            if (batchSize > 499) // We can batch up to 500 statements to push in one single execution call
            {
                voltExecuteSQL();
                batchSize = 0;
            }
        }
        // Make sure we post the last batch!
        if (batchSize > 0)
            voltExecuteSQL(true);

        return stopIndex;
    }
}
