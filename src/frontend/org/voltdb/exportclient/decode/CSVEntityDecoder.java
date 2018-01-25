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
import java.io.OutputStreamWriter;

import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.voltcore.utils.ByteBufferOutputStream;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

import com.google_voltpatches.common.base.Charsets;

/**
 * A {@link BatchDecoder} that produces HttpRequest entities that are not suitable for
 * asynchronous HTTP requests. Once an entity is harvested, it must be wholly consumed
 * before more rows can be added to this decoder (i.e. with synchronous requests)
 *
 */
public class CSVEntityDecoder extends EntityDecoder {

    public static final ContentType CSVContentType = ContentType.create("text/csv", Charsets.UTF_8);

    protected final CSVWriterDecoder m_csvDecoder;
    protected final ByteBufferOutputStream m_bbos = new ByteBufferOutputStream();
    protected final CSVWriter m_writer = new CSVWriter(
            new OutputStreamWriter(m_bbos, Charsets.UTF_8)
            );

    protected CSVEntityDecoder(CSVWriterDecoder csvDecoder) {
        m_csvDecoder = csvDecoder;
    }

    @Override
    public void add(Object[] fields) throws RuntimeException {
        try {
            m_csvDecoder.decode(m_writer, fields);
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
    public AbstractHttpEntity harvest() {
        ByteBufferEntity enty;
        try {
            m_writer.flush();
            enty = new ByteBufferEntity(m_bbos.toByteBuffer(), CSVContentType);
        } catch (IOException e) {
            throw new BulkException("unable to flush CSVWriter", e);
        } finally {
            m_bbos.reset();
        }
        return enty;
    }

    @Override
    public void discard() {
        try { m_bbos.close(); } catch (Exception ignoreIt) {}
    }

    @Override
    public AbstractHttpEntity getHeaderEntity() {
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
