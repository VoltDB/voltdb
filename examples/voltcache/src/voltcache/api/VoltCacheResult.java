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
import org.voltdb.VoltType;

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

    public static VoltCacheResult STORED()       { return _CODES[0]; }
    public static VoltCacheResult NOT_STORED()   { return _CODES[1]; }
    public static VoltCacheResult EXISTS()       { return _CODES[2]; }
    public static VoltCacheResult NOT_FOUND()    { return _CODES[3]; }
    public static VoltCacheResult DELETED()      { return _CODES[4]; }
    public static VoltCacheResult ERROR()        { return _CODES[5]; }
    public static VoltCacheResult CLIENT_ERROR() { return _CODES[6]; }
    public static VoltCacheResult SERVER_ERROR() { return _CODES[7]; }
    public static VoltCacheResult OK()           { return new VoltCacheResult(8l); }
    public static VoltCacheResult SUBMITTED()    { return _CODES[9]; }

    private static final VoltCacheResult[] _CODES = new VoltCacheResult[]
    {
      new VoltCacheResult(0l)
    , new VoltCacheResult(1l)
    , new VoltCacheResult(2l)
    , new VoltCacheResult(3l)
    , new VoltCacheResult(4l)
    , new VoltCacheResult(5l)
    , new VoltCacheResult(6l)
    , new VoltCacheResult(7l)
    , new VoltCacheResult(8l)
    , new VoltCacheResult(9l)
    };

    private static final String[] Name = new String[] {"STORED","NOT_STORED","EXISTS","NOT_FOUND","DELETED","ERROR","CLIENT_ERROR","SERVER_ERROR","OK","SUBMITTED"};

    public final long Code;
    public long IncrDecrValue = VoltType.NULL_BIGINT;
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
            return _CODES[(int)response.getResults()[0].asScalarLong()];
        }
        else if (type == Type.DATA)
        {
            final VoltTable data = response.getResults()[0];
            if (data.getRowCount() > 0)
            {
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
                                                      , (int)data.getLong(4)
                                                      )
                                   );
                return result;
            }
            else
                return VoltCacheResult.NOT_FOUND();
        }
        else if (type == Type.IDOP)
        {
            final long value = response.getResults()[0].asScalarLong();
            if (value == VoltType.NULL_BIGINT)
                return VoltCacheResult.NOT_FOUND();
            else
            {
                final VoltCacheResult result = VoltCacheResult.OK();
                result.IncrDecrValue = value;
                return result;
            }
        }
        else
            throw new RuntimeException("Invalid Result Type: " + type);
    }
}
