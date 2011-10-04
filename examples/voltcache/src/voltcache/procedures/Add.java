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

import voltcache.api.VoltCacheResult;
import org.voltdb.*;

@ProcInfo(partitionInfo = "cache.Key: 0", singlePartition = true)

public class Add extends VoltProcedure
{
    private final SQLStmt clean  = Shared.CleanSQLStmt;
    private final SQLStmt check  = new SQLStmt("SELECT CASVersion FROM cache WHERE Key = ? AND Expires > ?;");
    private final SQLStmt insert = new SQLStmt("INSERT INTO cache ( Key, Expires, Flags, Value ) VALUES( ?, ?, ?, ? );");

    public long run(String key, int flags, int expires, byte[] value)
    {
        final int now = Shared.init(this, key);

        // Pre-check for item existence => Fail w/ NOT_STORED status if the record exists (whether active or queued for deletion)
        voltQueueSQL(check, key, now);
        if (voltExecuteSQL()[1].getRowCount() > 0)
            return VoltCacheResult.NOT_STORED;

        // Insert new item
        voltQueueSQL(insert, key, Shared.expires(this, expires), flags, value);
        voltExecuteSQL(true);
        return VoltCacheResult.STORED;
    }
}
