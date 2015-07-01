/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
package com.procedures;

import java.nio.ByteBuffer;
import java.util.HashSet;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Put extends VoltProcedure {
    public final SQLStmt selectStmt = new SQLStmt("SELECT value FROM Store WHERE keyspace = ? AND key = ?");
    public final SQLStmt insertStmt = new SQLStmt("INSERT INTO Store VALUES (?, ?, ?)");
    public final SQLStmt updateStmt = new SQLStmt("UPDATE Store SET value = ? WHERE keyspace = ? AND key = ?");

    public long run(byte[] keyspace, String key, byte[] data)
    {
        voltQueueSQL(selectStmt, keyspace, key);
        VoltTable res = voltExecuteSQL()[0];
        if (res.advanceRow())
        {
            voltQueueSQL(updateStmt, merge(res.getVarbinary(0), data), keyspace, key);
        }
        else
        {
            voltQueueSQL(insertStmt, keyspace, key, data);
        }
        voltExecuteSQL(true);
        return 0l;
    }

    private byte[] merge(byte[] dest, byte[] src)
    {
        HashSet<ByteWrapper> mergeSet = new HashSet<ByteWrapper>();
        ByteBuffer buf = ByteBuffer.wrap(src);
        int nSrc = buf.getInt();
        for (int i = 0; i < nSrc; i++)
        {
            int len = buf.getInt();
            int off = buf.position();
            mergeSet.add(new ByteWrapper(src, off, len));
            buf.position(off + len);
            len = buf.getInt();
            buf.position(buf.position() + len);
        }

        byte[] merged = new byte[src.length + dest.length];
        ByteBuffer out = ByteBuffer.wrap(merged);

        buf = ByteBuffer.wrap(dest);
        int nDest = buf.getInt();
        int nFields = nSrc + nDest;
        out.putInt(nFields);

        int blockStart = 4;
        int blockEnd = 4;
        for (int i = 0; i < nDest; i++)
        {
            int len = buf.getInt();
            int off = buf.position();
            boolean flushBlock = mergeSet.contains(new ByteWrapper(dest, off, len));
            buf.position(off + len);
            len = buf.getInt();
            buf.position(buf.position() + len);
            if (flushBlock)
            {
                if (blockStart < blockEnd)
                {
                    out.put(dest, blockStart, blockEnd - blockStart);
                }
                nFields--;
                blockStart = buf.position();
            }
            blockEnd = buf.position();
        }
        if (blockStart < blockEnd)
        {
            out.put(dest, blockStart, blockEnd - blockStart);
        }
        out.put(src, 4, src.length - 4);

        int length = out.position();
        if (nFields != nSrc + nDest)
        {
            out.position(0);
            out.putInt(nFields);
        }

        byte[] res = new byte[length];
        System.arraycopy(merged, 0, res, 0, length);
        return res;
    }

    private static class ByteWrapper
    {
        byte[] m_arr;
        int m_off;
        int m_len;

        ByteWrapper(byte[] arr, int off, int len)
        {
            m_arr = arr;
            m_off = off;
            m_len = len;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof ByteWrapper))
            {
                return false;
            }
            ByteWrapper that = (ByteWrapper) obj;
            if (this.m_len != that.m_len)
            {
                return false;
            }
            for (int i = 0; i < this.m_len; i++)
            {
                if (this.m_arr[this.m_off + i] != that.m_arr[that.m_off + i])
                {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode()
        {
            if (this.m_arr == null)
            {
                return 0;
            }

            int res = 1;
            for (int i = 0; i < m_len; i++)
            {
                res = 31 * res + m_arr[m_off + i];
            }
            return res;
        }
    }
}
