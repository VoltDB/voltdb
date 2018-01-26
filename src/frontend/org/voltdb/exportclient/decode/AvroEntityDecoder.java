/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.voltcore.utils.ByteBufferOutputStream;

import com.google_voltpatches.common.base.Charsets;

/**
 * A {@link BatchDecoder} that produces HttpRequest entities that are not suitable for
 * asynchronous HTTP requests. Once an entity is harvested, it must be wholly consumed
 * before more rows can be added to this decoder (i.e. with synchronous requests)
 *
 */
public class AvroEntityDecoder extends EntityDecoder {

    public static final ContentType AvroContentType = ContentType.create("avro/binary", (Charset)null);
    public static final ContentType AvroSchemaContentType = ContentType.create("application/json", Charsets.UTF_8);

    protected final AvroDecoder m_avroDecoder;
    protected final ByteBufferOutputStream m_bbos = new ByteBufferOutputStream();
    protected final GenericDatumWriter<GenericRecord> m_writer;
    protected final DataFileWriter<GenericRecord> m_fileWriter;

    protected final AbstractHttpEntity m_header;
    protected final List<GenericRecord> m_records = new LinkedList<>();

    protected AvroEntityDecoder(AvroDecoder avroDecoder, boolean compress) {
        m_avroDecoder = avroDecoder;
        m_writer = new GenericDatumWriter<>(getSchema());
        m_fileWriter = new DataFileWriter<GenericRecord>(m_writer);
        if (compress) {
            m_fileWriter.setCodec(CodecFactory.snappyCodec());
        }
        try {
            m_fileWriter.create(getSchema(), m_bbos);
            m_fileWriter.flush();
            m_header = new ByteArrayEntity(m_bbos.toByteArray(), AvroContentType);
        } catch (IOException e) {
            throw new BulkException("failed to initialize avro data container header", e);
        } finally {
            m_bbos.reset();
        }
    }

    /**
     * Produces an HttpRequest entity that is not suitable for asynchronous HTTP requests. Once
     * an entity is harvested, it must be wholly consumed before more rows can be added to this
     * decoder (i.e. with synchronous requests)
     */
    @Override
    public AbstractHttpEntity harvest() {
        ByteBufferEntity enty = null;
        try {
            Iterator<GenericRecord> itr = m_records.iterator();
            while (itr.hasNext()) {
                m_fileWriter.append(itr.next());
                itr.remove();
            }

            m_fileWriter.flush();
            enty = new ByteBufferEntity(m_bbos.toByteBuffer(), AvroContentType);
        } catch (IOException e) {
            throw new BulkException("failed to append to the avro data container", e);
        } finally {
            reset();
        }

        return enty;
    }

    @Override
    public void add(Object[] fields) throws RuntimeException {
        m_records.add(m_avroDecoder.decode(null, fields));
    }

    @Override
    public void discard() {
        try { m_bbos.close(); } catch (Exception ignoreIt) {}
    }

    @Override
    public AbstractHttpEntity getHeaderEntity() {
        return m_header;
    }

    protected void reset() {
        m_bbos.reset();
        m_records.clear();
    }

    public Schema getSchema() {
        return m_avroDecoder.getSchema();
    }

    public StringEntity getSchemaAsEntity() {
        return new StringEntity(m_avroDecoder.getSchema().toString(true), AvroSchemaContentType);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AvroDecoder.DelegateBuilder {
        private final AvroDecoder.Builder m_delegateBuilder;
        private boolean m_compress = false;

        public Builder() {
            super(new AvroDecoder.Builder());
            m_delegateBuilder = super.getDelegateAs(AvroDecoder.Builder.class);
        }

        public Builder compress(boolean compress) {
            m_compress = compress;
            return this;
        }

        public AvroEntityDecoder build() {
            return new AvroEntityDecoder(m_delegateBuilder.build(), m_compress);
        }
    }
}
