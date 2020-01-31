/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.export;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.voltdb.exportclient.ExportRow;
import org.voltdb.exportclient.decode.AvroDecoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serializer for converting {@link ExportRow} to Avro format byte array as well as
 * register the schema in the schema registry.
 */
public class ExportAvroSerializer {
    private final Map<String, AvroDecoder> m_decoderMap = new ConcurrentHashMap<>(); // (topic -> decoder) mapping

    private String m_schemaRegistryUrl;
    private SchemaRegistryClient m_schemaRegistryClient;
    private final EncoderFactory m_encoderFactory = EncoderFactory.get();

    public ExportAvroSerializer(String schemaRegistryUrl) {
        updateConfig(schemaRegistryUrl);
    }

    /**
     * Converting {@link ExportRow} to Avro format byte array as well as register the schema
     * in the schema registry.
     *
     * @param exportRow
     * @param topic
     * @return The serialize byte array in Avro format.
     */
    public byte[] serialize(ExportRow exportRow, String topic) {
        AvroDecoder decoder = m_decoderMap.computeIfAbsent(topic, k -> new AvroDecoder.Builder().build());
        GenericRecord avroRecord = decoder.decode(exportRow.generation, exportRow.tableName, exportRow.types,
                exportRow.names, null, exportRow.values);

        return serialize(topic, avroRecord);
    }

    private byte[] serialize(String topic, GenericRecord avroRecord) {
        if (avroRecord == null) {
            return null;
        }
        String schemaName = "kipling-" + topic + "-value";
        Schema schema = avroRecord.getSchema();
        try {
            // register the schema if not registered, return the schema id.
            int schemaId = m_schemaRegistryClient.register(schemaName, schema);
            // serialize to bytes
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(0);
            out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
            BinaryEncoder encoder = m_encoderFactory.directBinaryEncoder(out, null);
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

            writer.write(avroRecord, encoder);
            encoder.flush();

            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (IOException e) {
            throw new SerializationException("Error serializing Avro message", e);
        } catch (RestClientException e) {
            throw new SerializationException("Error registering Avro schema: " + schema, e);
        }
    }

    public void dropTopic(String topicName) {
        m_decoderMap.remove(topicName);
    }

    /**
     * Handling the change of the {@code SchemaRegistryUrl} in the deployment file.
     */
    public synchronized void updateConfig(String schemaRegistryUrl) {
        // update the serializer config if the schema_register_url in the deployment file changes
        if (!Objects.equals(m_schemaRegistryUrl, schemaRegistryUrl)) {
            m_schemaRegistryUrl = schemaRegistryUrl;
            List<String> baseUrls = Arrays.asList(schemaRegistryUrl.split(","));
            // create a new m_schemaRegistryClient when we have a update on the url, since the cache is outdated
            m_schemaRegistryClient = new CachedSchemaRegistryClient(baseUrls, 10000);
        }
    }
}
