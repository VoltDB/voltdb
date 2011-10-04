/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package voltcache.procedures;

import org.voltdb.*;

@ProcInfo(partitionInfo = "cache.Key: 0", singlePartition = true)

public class Get extends VoltProcedure
{
    private final SQLStmt clean  = Shared.CleanSQLStmt;
    private final SQLStmt select = new SQLStmt("SELECT Key, Flags, Value, CASVersion, Expires FROM cache WHERE Key = ? AND Expires > ? AND CASVersion > -1;");

    public VoltTable[] run(String key)
    {
        final int now = Shared.init(this, key);

        // Select item (only if not expired/queued for deletion)
        voltQueueSQL(select, key, now);
        return new VoltTable[] { voltExecuteSQL(true)[1] };
    }
}
