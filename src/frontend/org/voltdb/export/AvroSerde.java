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
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.voltcore.logging.VoltLogger;
import org.voltdb.compiler.deploymentfile.AvroType;
import org.voltdb.exportclient.ExportRow;
import org.voltdb.exportclient.decode.AvroDecoder;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * A schema-registry aware serde (serializer/deserializer) for VoltDB exports or topics, that can
 * be used for reading and writing data in a "specific Avro" format.
 *
 * <p>This serde reads and writes data according to the wire format defined at
 * http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format.
 * It requires access to a Confluent Schema Registry endpoint, which you must
 * configure in the <avro> element of the deployment file.</p>
 * <p>
 * Synchronization: the implementation must be thread-safe as it may be called
 * from multiple {@link SubscriberSession} instances executing in a thread pool.
 */
public class AvroSerde {
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    // Map of <avroSubject, AvroDecoder>
    // NOTE: AvroDecoder is a VoltDB misnomer since it is used in the serialize methods (technically, "encoding" to Avro)
    private final Map<String, AvroDecoder> m_decoderMap = new ConcurrentHashMap<>();

    private final DecoderFactory m_decoderFactory = DecoderFactory.get();
    private final EncoderFactory m_encoderFactory = EncoderFactory.get();

    private AvroType m_avro;
    private SchemaRegistryClient m_schemaRegistryClient;

   /**
    * Return a schema name expected by Kafka Connect for the value part of a polled topic
    *
    * @param name topic name
    * @return schema name for value
    */
    public static String getValueSchemaName(String name) {
        return name + "-value";
    }

    /**
     * Return a schema name expected by Kafka Connect for the key part of a polled topic
     *
     * @param name topic name
     * @return schema name for key
     */
    public static String getKeySchemaName(String name) {
        return name + "-key";
    }

    /**
     * Constructor with Avro deployment configuration
     * @param avro Avro config from deployment
     */
    public AvroSerde(AvroType avro) {
        updateConfig(avro);
    }

    /**
     * Serializing {@link ExportRow} to byte array in wire format, as well as registering the schema
     * in the schema registry.
     *
     * @param exportRow     the row to encode in Avro
     * @param schemaName    the name to use for registering the schema
     * @return              the serialized byte array in wire format.
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

            // Write 5-byte header: magic byte and schema id (4 bytes)
            out.write(0);
            out.write(ByteBuffer.allocate(4).putInt(schemaId).array());

            // Serialize row
            BinaryEncoder encoder = m_encoderFactory.directBinaryEncoder(out, null);
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

            writer.write(avroRecord, encoder);
            encoder.flush();

            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        }
        catch (IOException e) {
            throw new SerializationException("Error serializing Avro message", e);
        }
        catch (RestClientException e) {
            throw new SerializationException("Error registering Avro schema: " + schema, e);
        }
    }

    /**
     * Deserializing {@link ByteBuffer} in wire format to {@link Object} array, looking up
     * the schema in the schema registry.
     *
     * @param buf   the buffer to deserialize
     * @return      an array of objects
     * @throws IOException
     */
    public Object[] deserialize(ByteBuffer buf) throws IOException {
        Object[] params = null;

        try {
            byte magic = buf.get();
            if (magic != 0) {
                throw new IOException("Invalid magic value " + magic);
            }

            int schemaId = buf.getInt();
            Schema schema = m_schemaRegistryClient.getById(schemaId);
            if (schema == null) {
                throw new IOException("Unknown avro schema: " + schemaId);
            }
            if (schema.getType() != Schema.Type.RECORD ) {
                throw new IOException("Unsupported avro schema type: " + schema.getType());
            }

            // FIXME: cache schema or reader?

            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
            int start = buf.position() + buf.arrayOffset();
            int length = buf.limit() - 5;

            GenericRecord genRec = datumReader.read(null, m_decoderFactory.binaryDecoder(buf.array(),
                    start, length, null));

            ArrayList<Object> objs = new ArrayList<>();
            for (Schema.Field f : schema.getFields()) {
                Object obj = genRec.get(f.name());
                if (obj == null) {
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("no object for field: " + f.name());
                    }
                    continue;
                }

                if (obj instanceof org.apache.avro.util.Utf8) {
                    objs.add(obj.toString());
                }
                else {
                    objs.add(obj);
                }
            }
            params = objs.toArray();
        }
        catch (IOException ex) {
            throw ex;
        }
        catch (Exception ex) {
            exportLog.error("Failed to decode avro buffer ", ex);
            throw new IOException(ex.getMessage());
        }
        return params;
    }

    /**
     * Cleanup the decoders when a topic is dropped.
     * <p>
     * Note: the map of decoders is keyed by avro subject names: for every topic
     * we have 2 avro subjects (for the value and the key).
     *
     * @param topicName
     */
    public synchronized void dropTopic(String topicName) {
        if (m_avro == null) {
            return;
        }
        m_decoderMap.remove(getAvroSubjectName(getValueSchemaName(topicName)));
        m_decoderMap.remove(getAvroSubjectName(getKeySchemaName(topicName)));
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
