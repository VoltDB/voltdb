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

public class VoltCacheResult
{
    public static final long STORED       = 0l;
    public static final long NOT_STORED   = 1l;
    public static final long EXISTS       = 2l;
    public static final long NOT_FOUND    = 3l;
    public static final long DELETED      = 4l;
    public static final long ERROR        = 5l;
    public static final long CLIENT_ERROR = 6l;
    public static final long SERVER_ERROR = 7l;
    public static final long OK           = 8l;
    public static final long SUBMITTED    = 9l;

    public static VoltCacheResult STORED()       { return new VoltCacheResult(0l); }
    public static VoltCacheResult NOT_STORED()   { return new VoltCacheResult(1l); }
    public static VoltCacheResult EXISTS()       { return new VoltCacheResult(2l); }
    public static VoltCacheResult NOT_FOUND()    { return new VoltCacheResult(3l); }
    public static VoltCacheResult DELETED()      { return new VoltCacheResult(4l); }
    public static VoltCacheResult ERROR()        { return new VoltCacheResult(5l); }
    public static VoltCacheResult CLIENT_ERROR() { return new VoltCacheResult(6l); }
    public static VoltCacheResult SERVER_ERROR() { return new VoltCacheResult(7l); }
    public static VoltCacheResult OK()           { return new VoltCacheResult(8l); }
    public static VoltCacheResult SUBMITTED()    { return new VoltCacheResult(9l); }

    private static final String[] Name = new String[] {"STORED","NOT_STORED","EXISTS","NOT_FOUND","DELETED","ERROR","CLIENT_ERROR","SERVER_ERROR","OK","SUBMITTED"};

    public final long Code;
    public long IncrDecrValue = Long.MAX_VALUE;
    public Map<String,VoltCacheItem> Data = null;
    VoltCacheResult(long code)
    {
        this.Code = code;
    }

    public static String getName(long code)
    {
        return Name[(int)code];
    }
    public String getName()
    {
        return Name[(int)this.Code];
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
            return new VoltCacheResult(response.getResults()[0].fetchRow(0).getLong(0));
        }
        else if (type == Type.DATA)
        {
            final VoltTable data = response.getResults()[0];
            final VoltCacheResult result = VoltCacheResult.OK();
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
            final VoltCacheResult result = VoltCacheResult.OK();
            result.IncrDecrValue = response.getResults()[0].fetchRow(0).getLong(0);
            return result;
        }
        else
            throw new RuntimeException("Invalid Result Type: " + type);
    }
}
