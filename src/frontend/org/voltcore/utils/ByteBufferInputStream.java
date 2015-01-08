/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.utils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.google_voltpatches.common.base.Preconditions;

/**
 * An {@link InputStream} implementation that is backed by a provided
 * {@link ByteBuffer}.
 *
 * It is CATEGORICALLY NOT THREAD SAFE
 */
public class ByteBufferInputStream extends InputStream {

    private final ByteBuffer m_bb;
    private volatile int m_eofCnt = 0;

    public ByteBufferInputStream(final ByteBuffer bb) {
        Preconditions.checkArgument(bb != null, "null byte buffer");
        m_bb = bb;
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (atEOF()) return -1;
        int len = Math.min(b.length, m_bb.remaining());
        m_bb.get(b, 0, len);
        return len;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (atEOF()) return -1;
        len = Math.min(len, m_bb.remaining());
        m_bb.get(b, off, len);
        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        atEOF();
        int len = Math.min((int)n, m_bb.remaining());
        m_bb.position(m_bb.position() + len);
        return len;
    }

    @Override
    public int available() throws IOException {
        return m_bb.remaining();
    }

    @Override
    public synchronized void mark(int readlimit) {
        m_bb.mark();
    }

    @Override
    public synchronized void reset() throws IOException {
        m_bb.reset();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public int read() throws IOException {
        if (atEOF()) return -1;
        return m_bb.get();
    }

    public void rewind() {
        m_bb.rewind();
        m_eofCnt = 0;
    }

    private boolean atEOF() throws EOFException {
        if (m_bb.position() == m_bb.limit()) {
            final int eofCnt = ++m_eofCnt;
            if (eofCnt > 1) throw new EOFException();
            return true;
        }
        return false;
    }
}
