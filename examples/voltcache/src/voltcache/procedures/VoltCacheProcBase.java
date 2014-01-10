/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.util.Map;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltType;

import voltcache.api.VoltCacheItem;

@ProcInfo(partitionInfo = "cache.Key: 0", singlePartition = true)

public class VoltCacheProcBase extends VoltProcedure
{
    public static final SQLStmt cleanSQLStmt = new SQLStmt("DELETE FROM cache WHERE Key = ? AND Expires <= ?;");

    private static final int MAX_EXPIRES = 2592000;

    public VoltCacheProcBase() {
    }

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

    // Fake run() to get packaged.
    public long run(String key, double d)
    {
        return 0l;
    }

    public static class Result
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

        public static Result STORED()       { return _CODES[0]; }
        public static Result NOT_STORED()   { return _CODES[1]; }
        public static Result EXISTS()       { return _CODES[2]; }
        public static Result NOT_FOUND()    { return _CODES[3]; }
        public static Result DELETED()      { return _CODES[4]; }
        public static Result ERROR()        { return _CODES[5]; }
        public static Result CLIENT_ERROR() { return _CODES[6]; }
        public static Result SERVER_ERROR() { return _CODES[7]; }
        public static Result OK()           { return new Result(8l); }
        public static Result SUBMITTED()    { return _CODES[9]; }

        private static final Result[] _CODES = new Result[]
        {
          new Result(0l)
        , new Result(1l)
        , new Result(2l)
        , new Result(3l)
        , new Result(4l)
        , new Result(5l)
        , new Result(6l)
        , new Result(7l)
        , new Result(8l)
        , new Result(9l)
        };

        private static final String[] Name = new String[] {"STORED","NOT_STORED","EXISTS","NOT_FOUND","DELETED","ERROR","CLIENT_ERROR","SERVER_ERROR","OK","SUBMITTED"};

        public final long code;
        public long incrDecrValue = VoltType.NULL_BIGINT;
        public Map<String,VoltCacheItem> data = null;

        Result(long code)
        {
            this.code = code;
        }

        public static String getName(long code)
        {
            return Name[(int)code];
        }

        public String getName()
        {
            return Name[(int)this.code];
        }

        public enum Type {
           CODE,
           DATA,
           IDOP;
        }

        public static Result getResult(int code)
        {
            return _CODES[code];
        }
    }
}
