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
 * <p>
 * Synchronization: the implementation must be thread-safe as it may be called
 * from multiple {@link SubscriberSession} instances executing in a thread pool.
 */
public class ExportAvroSerializer {
    // Map of <avroSubject, AvroDecoder>
    private final Map<String, AvroDecoder> m_decoderMap = new ConcurrentHashMap<>();

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
        String avroSubject = getAvroSubjectName(schemaName);
        AvroDecoder decoder = m_decoderMap.computeIfAbsent(
                avroSubject, k -> new AvroDecoder.Builder().packageName(getAvroNamespace()).build());
        GenericRecord avroRecord = decoder.decode(exportRow.generation, exportRow.tableName, exportRow.types,
                exportRow.names, null, exportRow.values);

        return serialize(avroSubject, avroRecord);
    }

    private byte[] serialize(String avroSubject, GenericRecord avroRecord) {
        if (avroRecord == null) {
            return null;
        }
        Schema schema = avroRecord.getSchema();
        try {
            // register the schema if not registered, return the schema id.
            int schemaId = m_schemaRegistryClient.register(avroSubject, schema);
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

    /**
     * Cleanup the decoders when a topic is dropped.
     * <p>
     * Note: the map of decoders is keyed by avro subject names: for every topic
     * we have 2 avro subjects (for the value and the key). This method assumes
     * that the key name for a topic has the suffix defined by the implementation
     * of {@link ExportRow#extractKey(java.util.List)}
     *
     * @param topicName
     */
    public synchronized void dropTopic(String topicName) {
        if (m_avro == null) {
            return;
        }
        m_decoderMap.remove(getAvroSubjectName(topicName));
        m_decoderMap.remove(getAvroSubjectName(topicName + ExportRow.KEY_SUFFIX));
    }

    /**
     * Handling the change of the {@link AvroType} in the deployment file.
     */
    public synchronized void updateConfig(AvroType avro) {
        if (avro == null) {
            if (m_schemaRegistryClient != null) {
                m_schemaRegistryClient.reset();
                m_schemaRegistryClient = null;
            }
            m_decoderMap.clear();
            m_avro = avro;
            return;
        }

        // update the serializer config if the schema_registry url in the deployment file changes
        if (m_avro == null || !Objects.equals(m_avro.getRegistry(), avro.getRegistry())) {
            // create a new m_schemaRegistryClient when we have a update on the url, since the cache is outdated
            if (m_schemaRegistryClient != null) {
                m_schemaRegistryClient.reset();
            }
            m_schemaRegistryClient = new CachedSchemaRegistryClient(avro.getRegistry().trim(), 10000);
        }

        // Clear all the decoders if the prefix or schema changes
        if (m_avro == null || !Objects.equals(m_avro.getPrefix(), avro.getPrefix())
                || !Objects.equals(m_avro.getNamespace(), avro.getNamespace())) {
            m_decoderMap.clear();
        }
        m_avro = avro;
    }

    /**
     * Use the configured prefix to build the avro subject name that
     * identifies the schema. Changing the prefix creates new avro subjects.
     *
     * @param schemaName
     * @return
     */
    private synchronized String getAvroSubjectName(String schemaName) {
        return m_avro.getPrefix() + schemaName;
    }

    /**
     * @return the configured namespace
     */
    private synchronized String getAvroNamespace() {
        return m_avro.getNamespace();
    }

}
