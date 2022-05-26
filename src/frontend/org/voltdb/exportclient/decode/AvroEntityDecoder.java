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
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
import org.voltdb.VoltType;

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

    protected final List<GenericRecord> m_records = new LinkedList<>();
    protected final Map <Long, DecoderHelper> m_decoders = new HashMap<>();

    protected final boolean m_compress;
    protected AvroEntityDecoder(AvroDecoder avroDecoder, boolean compress) {
        m_compress = compress;
        m_avroDecoder = avroDecoder;
    }

    class DecoderHelper {
        final ByteBufferOutputStream m_bbos = new ByteBufferOutputStream();
        final GenericDatumWriter<GenericRecord> m_writer;
        final DataFileWriter<GenericRecord> m_fileWriter;
        final AbstractHttpEntity m_headerEntity;

        public DecoderHelper(final Schema schema) throws IOException {
            assert(schema != null);
            m_writer = new GenericDatumWriter<>(schema);
            m_fileWriter = new DataFileWriter<GenericRecord>(m_writer);
            if (m_compress) {
                m_fileWriter.setCodec(CodecFactory.snappyCodec());
            }
            try {
                m_fileWriter.create(schema, m_bbos);
                m_fileWriter.flush();
                m_headerEntity = new ByteArrayEntity(m_bbos.toByteArray(), AvroContentType);
            } catch (IOException e) {
                throw new BulkException("failed to initialize avro data container header", e);
            } finally {
                m_bbos.reset();
            }
        }

    }

    private DecoderHelper decoder(long generation, Schema schema) throws IOException {
        DecoderHelper decoder = m_decoders.get(generation);
        if (decoder != null) {
            return decoder;
        }
        decoder = new DecoderHelper(schema);
        m_decoders.put(generation, decoder);
        return decoder;
    }

    /**
     * Produces an HttpRequest entity that is not suitable for asynchronous HTTP requests. Once
     * an entity is harvested, it must be wholly consumed before more rows can be added to this
     * decoder (i.e. with synchronous requests)
     */
    @Override
    public AbstractHttpEntity harvest(long generation) {
        DecoderHelper decoder = null;
        DataFileWriter<GenericRecord> fileWriter = null;
        ByteBufferEntity entity = null;
        //Now iterate over records and write
        Iterator<GenericRecord> itr = m_records.iterator();
        try {
            while (itr.hasNext()) {
                GenericRecord record = itr.next();
                if (decoder == null) {
                    decoder = decoder(generation, record.getSchema());
                }
                fileWriter = decoder.m_fileWriter;
                fileWriter.append(record);
                itr.remove();
            }

            if (fileWriter != null) {
                fileWriter.flush();
            }
            if (decoder != null) {
                entity = new ByteBufferEntity(decoder.m_bbos.toByteBuffer(), AvroContentType);
            }
        }
        catch (IOException e) {
            throw new BulkException("failed to append to the avro data container", e);
        }
        finally {
            if (decoder != null) {
                decoder.m_bbos.reset();
            }
            m_records.clear();
        }

        return entity;
    }

    @Override
    public void add(long generation, String tableName, List<VoltType> types, List<String> names, Object[] fields) throws RuntimeException {
        GenericRecord record = m_avroDecoder.decode(generation, tableName, types, names, null, fields);
        m_records.add(record);
    }

    @Override
    public void discard(long generation) {
        DecoderHelper decoder = m_decoders.get(generation);
        if (decoder != null) {
            try {
                decoder.m_bbos.close();
            }
            catch (Exception ignore) {}
        }
    }

    @Override
    public AbstractHttpEntity getHeaderEntity(long generation, String tableName, List<VoltType> columnTypes, List<String> columnNames) {
        DecoderHelper decoder = m_decoders.get(generation);
        if (decoder != null) {
            return decoder.m_headerEntity;
        }
        try {
            decoder = new DecoderHelper(getSchema(generation, tableName, columnTypes, columnNames));
            m_decoders.put(generation, decoder);
        } catch (IOException e) {
            throw new BulkException("failed to append to the avro data container", e);
        }
        return decoder.m_headerEntity;
    }

    public Schema getSchema(long generation, String tableName, List<VoltType> types, List<String> names) {
        return m_avroDecoder.getSchema(generation, tableName, types, names);
    }

    public StringEntity getSchemaAsEntity(long generation, String tableName, List<VoltType> types, List<String> names) {
        return new StringEntity(m_avroDecoder.getSchema(generation, tableName, types, names).toString(true), AvroSchemaContentType);
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
