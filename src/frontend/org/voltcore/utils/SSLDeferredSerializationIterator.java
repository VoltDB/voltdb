/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltdb.common.Constants;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SSLDeferredSerializationIterator implements Iterator<DeferredSerialization> {

    private final SSLEngine sslEngine;
    private final Serializer serializer;
    private Iterator<DeferredSerialization> dsIter;

    public SSLDeferredSerializationIterator(SSLEngine sslEngine, Serializer serializer) {
        this.sslEngine = sslEngine;
        this.serializer = serializer;
    }

    public SSLDeferredSerializationIterator(SSLEngine sslEngine, final DeferredSerialization ds) {
        this.sslEngine = sslEngine;
        this.serializer = new Serializer() {
            @Override
            public ByteBuffer serialize() {
                try {
                    ByteBuffer buf = ByteBuffer.allocate(ds.getSerializedSize());
                    ds.serialize(buf);
                    return buf;
                } catch (IOException e) {
                    return null;
                }
            }
        };
    }

    @Override
    public boolean hasNext() {
        // wait to do the serialization until this is called - hence 'deferred'.
        if (dsIter == null) {
            ByteBuffer buf = serializer.serialize();
            List<DeferredSerialization> dsList = new ArrayList<>();
            while (buf.remaining() > 0) {
                if (buf.remaining() < Constants.SSL_CHUNK_SIZE) {
                    ByteBuffer chunk = buf.slice();
                    dsList.add(new SSLDeferredSerialization(sslEngine, chunk));
                    buf.position(buf.limit());
                } else {
                    int oldLimit = buf.limit();
                    int newPosition = buf.position() + Constants.SSL_CHUNK_SIZE;
                    buf.limit(newPosition);
                    ByteBuffer chunk = buf.slice();
                    dsList.add(new SSLDeferredSerialization(sslEngine, chunk));
                    buf.position(newPosition);
                    buf.limit(oldLimit);
                }
            }
            dsIter = dsList.iterator();
        }
        return dsIter.hasNext();
    }

    @Override
    public DeferredSerialization next() {
        return dsIter.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
