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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONWriter;
import org.voltcore.utils.ByteBufferOutputStream;
import org.voltdb.VoltType;

public class ElasticSearchJsonEntityDecoder  extends EntityDecoder
{

    public static final ContentType JsonContentType = ContentType.APPLICATION_JSON.withCharset("utf-8");

    protected final JsonWriterDecoder m_jsonDecoder;
    protected final ByteBufferOutputStream m_bbos = new ByteBufferOutputStream();
    protected final Writer m_writer = new BufferedWriter(new OutputStreamWriter(m_bbos, StandardCharsets.UTF_8));

    private String m_index;
    private String m_type;
    private String m_id = null;

    protected ElasticSearchJsonEntityDecoder(JsonWriterDecoder jsonDecoder, String index, String type){
        m_jsonDecoder = jsonDecoder;
        m_index = index;
        m_type = type;
        if (m_index != null && m_index.trim().isEmpty()) {
            throw new IllegalArgumentException("elastic search index can not be blank");
        }
        if (m_type != null && m_type.trim().isEmpty()) {
            throw new IllegalArgumentException("elastic search type can not be blank");
        }

    }

    @Override
    public void add(long generation, String tableName, List<VoltType> types, List<String> names, Object[] fields) throws BulkException{
        try {
            addActionMetaData();
            m_writer.append("\n");
            m_jsonDecoder.decode(generation, tableName, types, names, new JSONWriter(m_writer), fields);
            m_writer.append("\n");
        }
        catch (JSONException | IOException e) {
            throw new BulkException("unable to convert a row into Json string", e);
        }
    }

    @Override
    public AbstractHttpEntity harvest(long generation) {
        ByteBufferEntity entity ;
        try {
            m_writer.flush();
            entity = new ByteBufferEntity(m_bbos.toByteBuffer(), JsonContentType);
        }
        catch (IOException e) {
            throw new BulkException("unable to flush JSON ", e);
        }
        finally {
            m_bbos.reset();
        }
        return entity;
    }
    private void addActionMetaData() throws JSONException, IOException {
        // write out the action-meta-data line
        // e.g.: { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
        String actionType = "index";
        final JSONWriter jsonWriter = new JSONWriter(m_writer);

        jsonWriter.object();
        jsonWriter.key(actionType);
        jsonWriter.object();
        if (StringUtils.isNotBlank(m_id)) {
            jsonWriter.key("_id").value(m_id);
        }
        if (StringUtils.isNotBlank(m_index)) {
            jsonWriter.key("_index").value(m_index);
        }
        if (StringUtils.isNotBlank(m_type)) {
            jsonWriter.key("_type").value(m_type);
        }

        // TODO
        // support other acceptable bulk parameters
        // i.e.  TIMESTAMP, TTL, RETRY_ON_CONFLICT, VERSION etc.
        jsonWriter.endObject();
        jsonWriter.endObject();
    }

    @Override
    public void discard(long generation) {
        try { m_bbos.close(); } catch (Exception ignoreIt) {}
    }

    @Override
    public AbstractHttpEntity getHeaderEntity(long generation, String tableName, List<VoltType> types, List<String> names) {
        return null;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends JsonWriterDecoder.DelegateBuilder
    {
        private final JsonWriterDecoder.Builder m_delegateBuilder;
        private String m_index = null;
        private String m_type = null;

        public Builder() {
            super(new JsonWriterDecoder.Builder());
            m_delegateBuilder = super.getDelegateAs(JsonWriterDecoder.Builder.class);
        }

        public ElasticSearchJsonEntityDecoder build() {
            return new ElasticSearchJsonEntityDecoder(m_delegateBuilder.build(), m_index, m_type);
        }

        public Builder index(String index) {
            m_index  = index;
            return this;
        }

        public Builder type(String type) {
            m_type  = type;
            return this;
        }
    }

}
