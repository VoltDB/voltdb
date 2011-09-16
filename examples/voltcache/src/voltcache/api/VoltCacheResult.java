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
package voltcache.api;

import java.util.Map;
import java.util.HashMap;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;

public enum VoltCacheResult
{
      STORED       (0l, "STORED")
    , NOT_STORED   (1l, "NOT_STORED")
    , EXISTS       (2l, "EXISTS")
    , NOT_FOUND    (3l, "NOT_FOUND")
    , DELETED      (4l, "DELETED")
    , ERROR        (5l, "ERROR")
    , CLIENT_ERROR (6l, "CLIENT_ERROR")
    , SERVER_ERROR (7l, "SERVER_ERROR")
    , OK           (8l, "OK")
    , SUBMITTED    (9l, "SUBMITTED")
    ;

    public final long Code;
    public final String Name;
    public long IncrDecrValue = Long.MAX_VALUE;
    public Map<String,VoltCacheItem> Data = null;
    VoltCacheResult(long code, String name)
    {
        this.Code = code;
        this.Name = name;
    }

    public enum Type
    {
            CODE    (0)
        ,   DATA    (1)
        ,   IDOP    (2)
        ;
        private int Value;
        Type(int value) { Value = value; }
    }

    public static VoltCacheResult get(Type type, ClientResponse response)
    {
        if (type == Type.CODE)
        {
            final long code = response.getResults()[0].fetchRow(0).getLong(0);
            for (VoltCacheResult result : VoltCacheResult.values())
                if (result.Code == code)
                    return result;
            throw new AssertionError("Unknown type: " + String.valueOf(code));
        }
        else if (type == Type.DATA)
        {
            final VoltTable data = response.getResults()[0];
            final VoltCacheResult result = VoltCacheResult.OK;
            result.Data = new HashMap<String,VoltCacheItem>();
            while(data.advanceRow())
                result.Data.put(
                                 data.getString(0)
                               , new VoltCacheItem(
                                                    data.getString(0)
                                                  , (int)data.getLong(1)
                                                  , data.getVarbinary(2)
                                                  , data.getLong(3)
                                                  )
                               );
            return result;
        }
        else if (type == Type.IDOP)
        {
            final VoltCacheResult result = VoltCacheResult.OK;
            result.IncrDecrValue = response.getResults()[0].fetchRow(0).getLong(0);
            return result;
        }
        else
            throw new RuntimeException("Invalid Result Type: " + type);
    }
}
