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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.voltdb.compiler.deploymentfile.AvroType;
import org.voltdb.exportclient.ExportRow;
import org.voltdb.exportclient.decode.AvroDecoder;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Serializer for converting {@link ExportRow} to Avro format byte array as well as
 * register the schema in the schema registry.
 */
public class ExportAvroSerializer {
    private final Map<String, AvroDecoder> m_decoderMap = new ConcurrentHashMap<>(); // (topic -> decoder) mapping

    private AvroType m_avro;
    private SchemaRegistryClient m_schemaRegistryClient;
    private final EncoderFactory m_encoderFactory = EncoderFactory.get();

    public ExportAvroSerializer(AvroType avro) {
        updateConfig(avro);
    }

    /**
     * Converting {@link ExportRow} to Avro format byte array as well as register the schema
     * in the schema registry.
     *
     * @param exportRow
     * @param schemaName
     * @return The serialize byte array in Avro format.
     */
    public byte[] serialize(ExportRow exportRow, String schemaName) {
        AvroDecoder decoder = m_decoderMap.computeIfAbsent(schemaName, k -> new AvroDecoder.Builder().build());
        GenericRecord avroRecord = decoder.decode(exportRow.generation, exportRow.tableName, exportRow.types,
                exportRow.names, null, exportRow.values);

        return serialize(schemaName, avroRecord);
    }

    private byte[] serialize(String schemaName, GenericRecord avroRecord) {
        if (avroRecord == null) {
            return null;
        }
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
     * Handling the change of the {@link AvroType} in the deployment file.
     */
    public synchronized void updateConfig(AvroType avro) {
        if (avro == null) {
            // Use a default config talking to no-one
            avro = new AvroType();
            avro.setRegistry("");
        }

        // update the serializer config if the schema_registry url in the deployment file changes
        if (m_avro == null || !Objects.equals(m_avro.getRegistry(), avro.getRegistry())) {
            m_avro = avro;
            // create a new m_schemaRegistryClient when we have a update on the url, since the cache is outdated
            m_schemaRegistryClient = new CachedSchemaRegistryClient(m_avro.getRegistry().trim(), 10000);
        }
    }
}
