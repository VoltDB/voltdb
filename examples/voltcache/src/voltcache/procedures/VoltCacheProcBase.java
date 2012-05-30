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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public abstract class VoltCacheProcBase extends VoltProcedure
{
    public static final SQLStmt cleanSQLStmt = new SQLStmt("DELETE FROM cache WHERE Key = ? AND Expires <= ?;");

    private static final int MAX_EXPIRES = 2592000;

    public int now() {
        return (int) (getTransactionTime().getTime() / 1000);
    }

    public int expirationTimestamp(int expires) {
        if (expires <= 0)
            return now() + MAX_EXPIRES;
        else if (expires <= MAX_EXPIRES)
            return now() + expires;
        return expires;
    }

    public int baseInit(String key) {
        final int now = now();
        voltQueueSQL(cleanSQLStmt, key, now);
        return now;
    }

    public int baseInit(String[] keys) {
        final int now = now();
        for(String key : keys) {
            voltQueueSQL(cleanSQLStmt, key, now);
        }
        return now;
    }
}
