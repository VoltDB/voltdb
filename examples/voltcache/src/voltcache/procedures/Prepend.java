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
import java.io.ByteArrayOutputStream;
import org.voltdb.*;

@ProcInfo(partitionInfo = "cache.Key: 0", singlePartition = true)

public class Prepend extends VoltProcedure
{
    private final SQLStmt clean  = Shared.CleanSQLStmt;
    private final SQLStmt check  = new SQLStmt("SELECT Key, Value FROM cache WHERE Key = ? AND Expires > ? AND CASVersion > -1;");
    private final SQLStmt update = new SQLStmt("UPDATE cache SET Value = ?, CASVersion = CASVersion+1 WHERE Key = ?;");

    public long run(String key, byte[] value)
    {
        final int now = Shared.init(this, key);

        voltQueueSQL(check, key, now);
        VoltTable checkResult = voltExecuteSQL()[1];
        if (checkResult.getRowCount() == 0)
            return VoltCacheResult.NOT_STORED;

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try
        {
            byte[] old = checkResult.fetchRow(0).getVarbinary(1);
            stream.write(value, 0, value.length);
            stream.write(old, 0, old.length);
            voltQueueSQL(update, stream.toByteArray(), key);
            voltExecuteSQL(true);
            return VoltCacheResult.STORED;
        }
        catch(Exception x)
        {
            throw new VoltAbortException(x);
        }
        finally
        {
            try {stream.close();} catch(Exception cx){}
        }
    }
}
