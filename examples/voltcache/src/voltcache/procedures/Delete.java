/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;

@ProcInfo(partitionInfo = "cache.Key: 0", singlePartition = true)

public class Delete extends VoltCacheProcBase
{
    private final SQLStmt check  = new SQLStmt("SELECT Key FROM cache WHERE Key = ? AND Expires > ? AND CASVersion > -1;");
    private final SQLStmt update = new SQLStmt("UPDATE cache SET Expires = ?, CASVersion = -1 WHERE Key = ?;");
    private final SQLStmt delete = new SQLStmt("DELETE FROM cache WHERE Key = ?;");

    public long run(String key, int expires)
    {
        final int now = baseInit(key);

        // If immediate-deletion request, honor regardless as to whether item was already queued for deletion
        if (expires <= 0)
        {
            voltQueueSQL(delete, key);
            if (voltExecuteSQL()[1].fetchRow(0).getLong(0) == 0)
                return Result.NOT_FOUND;
            return Result.DELETED;
        }
        // Otherwise find item and queue for deletion
        else
        {
            voltQueueSQL(check, key, now);
            if (voltExecuteSQL()[1].getRowCount() == 0)
                return Result.NOT_FOUND;

            voltQueueSQL(update, expirationTimestamp(expires), key);
            voltExecuteSQL(true);
            return Result.DELETED;
        }
    }
}
