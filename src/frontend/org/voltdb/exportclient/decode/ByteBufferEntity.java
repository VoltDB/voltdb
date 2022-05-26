/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.exportclient.decode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.voltcore.utils.ByteBufferInputStream;

import com.google_voltpatches.common.base.Preconditions;

/**
 * Modeled after {@link org.apache.http.entity.ByteArrayEntity}
 *
 */
public class ByteBufferEntity extends AbstractHttpEntity implements Cloneable {

    private final ByteBuffer m_bb;

    public ByteBufferEntity(final ByteBuffer bb, ContentType contentType) {
        Preconditions.checkArgument(bb != null, "null byte buffer");
        m_bb = bb;
        if (contentType != null) {
            super.setContentType(contentType.toString());
        }
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public long getContentLength() {
        return m_bb.limit();
    }

    @Override
    public InputStream getContent() throws IOException, IllegalStateException {
        return new ByteBufferInputStream(m_bb.asReadOnlyBuffer());
    }

    @Override
    public void writeTo(OutputStream outstream) throws IOException {
        Preconditions.checkArgument(outstream != null, "null output stream");
        WritableByteChannel wchn = Channels.newChannel(outstream);
        ByteBuffer bb = m_bb.asReadOnlyBuffer();
        while (bb.hasRemaining()) {
            wchn.write(bb);
        }
        outstream.flush();
    }

    @Override
    public boolean isStreaming() {
        return false;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
