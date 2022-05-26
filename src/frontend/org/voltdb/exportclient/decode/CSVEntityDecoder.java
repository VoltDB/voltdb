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

import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.voltcore.utils.ByteBufferOutputStream;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

import com.google_voltpatches.common.base.Charsets;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.voltdb.VoltType;

/**
 * A {@link BatchDecoder} that produces HttpRequest entities that are not suitable for
 * asynchronous HTTP requests. Once an entity is harvested, it must be wholly consumed
 * before more rows can be added to this decoder (i.e. with synchronous requests)
 *
 */
public class CSVEntityDecoder extends EntityDecoder {

    public static final ContentType CSVContentType = ContentType.create("text/csv", Charsets.UTF_8);

    protected final CSVWriterDecoder m_csvDecoder;
    protected final Map<Long, ByteBufferOutputStream> m_bbos = new HashMap<>();
    protected final Map<Long, CSVWriter> m_writers = new HashMap<>();

    protected CSVEntityDecoder(CSVWriterDecoder csvDecoder) {
        m_csvDecoder = csvDecoder;
    }

    @Override
    public void add(long generation, String tableName, List<VoltType> types, List<String> names, Object[] fields) throws RuntimeException {
        try {
            CSVWriter writer;
            if (!m_bbos.containsKey(generation)) {
                ByteBufferOutputStream bbos = new ByteBufferOutputStream();
                m_bbos.put(generation, bbos);
                writer = new CSVWriter(new OutputStreamWriter(bbos, Charsets.UTF_8));
                m_writers.put(generation, writer);
            } else {
                writer = m_writers.get(generation);
            }
            m_csvDecoder.decode(generation, tableName, types, names, writer, fields);
        } catch (IOException e) {
            throw new BulkException("unable to convert a row into CSV string", e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Produces an HttpRequest entity that is not suitable for asynchronous HTTP requests. Once
     * an entity is harvested, it must be wholly consumed before more rows can be added to this
     * decoder (i.e. with synchronous requests)
     */
    @Override
    public AbstractHttpEntity harvest(long generation) {
        ByteBufferEntity enty;
        try {
            m_writers.get(generation).flush();
            enty = new ByteBufferEntity(m_bbos.get(generation).toByteBuffer(), CSVContentType);
        } catch (IOException e) {
            throw new BulkException("unable to flush CSVWriter", e);
        } finally {
            m_bbos.get(generation).reset();
        }
        return enty;
    }

    @Override
    public void discard(long generation) {
        try { m_bbos.get(generation).close(); } catch (Exception ignoreIt) {}
    }

    @Override
    public AbstractHttpEntity getHeaderEntity(long generation, String tableName, List<VoltType> types, List<String> names) {
        return null;
    }

    public static class Builder extends CSVWriterDecoder.DelegateBuilder {
        private final CSVWriterDecoder.Builder m_csvWriterBuilder;

        public Builder() {
            super(new CSVWriterDecoder.Builder());
            m_csvWriterBuilder = super.getDelegateAs(CSVWriterDecoder.Builder.class);
        }

        public CSVEntityDecoder build() {
            return new CSVEntityDecoder(m_csvWriterBuilder.build());
        }

    }
}
