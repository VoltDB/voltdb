/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb.serdes;

import static org.voltdb.serdes.VoltAvroLogicalTypes.SCHEMA_BYTE;
import static org.voltdb.serdes.VoltAvroLogicalTypes.SCHEMA_BYTE_ARRAY;
import static org.voltdb.serdes.VoltAvroLogicalTypes.SCHEMA_DECIMAL;
import static org.voltdb.serdes.VoltAvroLogicalTypes.SCHEMA_GEOGRAPHY_BINARY;
import static org.voltdb.serdes.VoltAvroLogicalTypes.SCHEMA_GEOGRAPHY_POINT_BINARY;
import static org.voltdb.serdes.VoltAvroLogicalTypes.SCHEMA_GEOGRAPHY_POINT_FIXED_BINARY;
import static org.voltdb.serdes.VoltAvroLogicalTypes.SCHEMA_GEOGRAPHY_POINT_STRING;
import static org.voltdb.serdes.VoltAvroLogicalTypes.SCHEMA_GEOGRAPHY_STRING;
import static org.voltdb.serdes.VoltAvroLogicalTypes.SCHEMA_SHORT;
import static org.voltdb.serdes.VoltAvroLogicalTypes.SCHEMA_TIMESTAMP_MICRO;
import static org.voltdb.serdes.VoltAvroLogicalTypes.SCHEMA_TIMESTAMP_MILLI;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation_voltpatches.concurrent.NotThreadSafe;
import javax.annotation_voltpatches.concurrent.ThreadSafe;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.ByteBufferInputStream;
import org.voltdb.CatalogContext;
import org.voltdb.compiler.deploymentfile.AvroType;
import org.voltdb.compiler.deploymentfile.PropertyType;
import org.voltdb.messaging.FastSerializer;

import com.google_voltpatches.common.annotations.VisibleForTesting;
import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableMap;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * A schema-registry aware serde (serializer/deserializer) for VoltDB, that can be used for reading and writing data in
 * a "specific Avro" format.
 *
 * <p>
 * This serde reads and writes data according to the wire format defined at
 * http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format. It requires access to a
 * Confluent Schema Registry endpoint, which you must configure in the <avro> element of the deployment file.
 * <p>
 * {@link #getIdForSchema(String, List)} is used to get the ID of an existing schema or create a new schema
 * <p>
 * {@link #createSerializer(int)} is used to create a new {@link Serializer} which writes records using the schema with
 * the given ID
 * <p>
 * {@link #createDeserializer()} is used to create a new {@link Deserializer} which can deserialize any valid record
 * which starts with a valid ID
 */
@ThreadSafe
public class AvroSerde {
    private static final byte s_avroMagic = 0;
    private static final VoltLogger s_log = new VoltLogger("AVRO");

    // The client implementation does its own caching so no need to do extra caching here

    // Use one immutable volatile reference to hold all of the current configuration and client
    private volatile Internal m_internal = new Internal(null, null);

    static {
        VoltAvroLogicalTypes.addLogicalTypeConversions();
    }

    public static int decodeId(ByteBuffer buffer) throws IOException {
        byte magic = buffer.get();
        if (magic != s_avroMagic) {
            throw new IOException("Invalid magic value " + magic);
        }
        return buffer.getInt();
    }

    public AvroSerde() {
        this(null);
    }

    AvroSerde(AvroType avro) {
        updateConfig(avro);
    }

    /**
     * Update the configuration for this class
     *
     * @param context {@link CatalogContext} with the latest configurations
     */
    public void updateConfig(CatalogContext context) {
        updateConfig(context.getDeployment().getAvro());
    }

    /**
     * Shutdown the client to the schema-registry and prevent any more serialization or deserialization
     */
    public void shutdown() {
        updateRegistryClient(null, null);
    }

    /**
     * Handling the change of the {@link AvroType} in the deployment file
     *
     * @param avro configuration
     */
    synchronized void updateConfig(AvroType avro) {
        if (avro == m_internal.m_avro) {
            return;
        }

        if (avro == null || StringUtils.isBlank(avro.getRegistry())) {
            updateRegistryClient(null, avro);
            if (s_log.isDebugEnabled()) {
                s_log.debug(this.getClass().getSimpleName() + " is set to no registry");
            }
        } else {
            Map<String, String> properties = propertiesToMap(avro);
            if (m_internal.m_avro == null || !Objects.equals(m_internal.m_avro.getRegistry(), avro.getRegistry())
                    || !properties.equals(propertiesToMap(m_internal.m_avro))) {
                // update the client if the schema_registry url or configurations in the deployment file changes
                if (s_log.isDebugEnabled()) {
                    s_log.debug(this.getClass().getSimpleName() + " registry updated to " + avro.getRegistry()
                            + " properties: " + properties);
                }

                // create a new m_schemaRegistryClient when we have a update on the url, since the cache is outdated
                updateRegistryClient(buildClient(avro, properties), avro);
            }
        }

        m_internal = new Internal(m_internal.m_client, avro);
    }

    /**
     * Delete all schemas associated with {@code subject}
     *
     * @param subject of the schema
     * @return {@code true} if schemas with {@code subject} were deleted or {@code false} if the registry is not
     *         configured
     * @throws IOException if the schema could not be deleted
     */
    public boolean deleteSchema(String subject) throws IOException {
        Internal internal = m_internal;
        if (internal.m_avro == null) {
            return false;
        }
        try {
            internal.m_client.deleteSubject(internal.m_avro.getPrefix() + subject);
            return true;
        } catch (RestClientException e) {
            throw new IOException(e);
        }
    }

    /**
     * Retrieve the id for a schema with the {@code subject} and fields described by {@code fields} if the schema does
     * not exist in the registry it will be created
     *
     * @param subject       of the schema
     * @param schemaName    name of the schema
     * @param fields        in the schema
     * @param configuration to be used to generate the schema
     * @return ID for schema in the given subject
     * @throws IOException if the id could not be retrieved or the schema registry is not configured
     */
    @SuppressWarnings("deprecation")
    public int getIdForSchema(String subject, String schemaName, List<FieldDescription> fields,
            Configuration configuration)
            throws IOException {
        Internal internal = getInternal();
        AvroType avro = internal.m_avro;
        FieldAssembler<Schema> schemaFields = SchemaBuilder.record(schemaName).namespace(avro.getNamespace()).fields();

        for (FieldDescription field : fields) {
            schemaFields = addField(schemaFields, field, configuration);
        }

        Schema schema = schemaFields.endRecord();
        try {
            return internal.m_client.register(avro.getPrefix() + subject, schema);
        } catch (RestClientException e) {
            throw new IOException(e);
        }
    }

    /**
     * Create a serializer for serializing records using the schema with {@code id}
     *
     * @param id of the schema
     * @return {@link AvroSerde.Serializer} to serialize the schema
     * @throws IOException If the schema could not be retrieved or the schema registry is not configured
     */
    public Serializer createSerializer(int id) throws IOException {
        return new Serializer(id);
    }

    /**
     * Create a deserializer for deserializing avro records. This {@link Deserializer} can be used to deserialize
     * records with different schemas but is optimized to not have the schema change often
     *
     * @return A new instance of {@link AvroSerde.Deserializer}
     */
    public Deserializer createDeserializer() {
        return new Deserializer();
    }

    @SuppressWarnings("deprecation")
    Schema getById(int id) throws IOException {
        try {
            return getInternal().m_client.getById(id);
        } catch (RestClientException e) {
            throw new IOException(e);
        }
    }

    /**
     * Convert {@link AvroType#getProperties()} to a {@link Map} of properties
     *
     * @param avro instance
     * @return a {@link Map} of properties. Never {@code null}
     */
    private Map<String, String> propertiesToMap(AvroType avro) {
        return avro.getProperties() == null ? ImmutableMap.of()
                : avro.getProperties().getProperty().stream()
                        .collect(Collectors.toMap(PropertyType::getName, PropertyType::getValue));
    }

    @VisibleForTesting
    SchemaRegistryClient buildClient(AvroType avro, Map<String, String> properties) {
        List<String> registries = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(avro.getRegistry());

        return new CachedSchemaRegistryClient(registries, 10000, properties);
    }

    private void updateRegistryClient(SchemaRegistryClient client, AvroType avro) {
        SchemaRegistryClient previous = m_internal.m_client;
        m_internal = new Internal(client, avro);
        if (previous != null) {
            previous.reset();
        }
    }

    private FieldAssembler<Schema> addField(FieldAssembler<Schema> schemaFields, FieldDescription field,
            Configuration configuration) {
        FieldBuilder<Schema> builder = schemaFields.name(field.name());
        switch (field.type()) {
            default:
                throw new IllegalArgumentException("Unsupported type: " + field.type());
            case TINYINT:
                return addFieldType(builder, SCHEMA_BYTE, field);
            case SMALLINT:
                return addFieldType(builder, SCHEMA_SHORT, field);
            case INTEGER:
                return addFieldType(builder, Schema.create(Schema.Type.INT), field);
            case BIGINT:
                return addFieldType(builder, Schema.create(Schema.Type.LONG), field);
            case DECIMAL:
                return addFieldType(builder, SCHEMA_DECIMAL, field);
            case TIMESTAMP:
                return addFieldType(builder, configuration.m_timestamp, field);
            case FLOAT:
                return addFieldType(builder, Schema.create(Schema.Type.DOUBLE), field);
            case STRING:
                return addFieldType(builder, Schema.create(Schema.Type.STRING), field);
            case GEOGRAPHY_POINT:
                return addFieldType(builder, configuration.m_geographyPoint, field);
            case GEOGRAPHY:
                return addFieldType(builder, configuration.m_geography, field);
            case VARBINARY:
                return addFieldType(builder, SCHEMA_BYTE_ARRAY, field);
        }
    }

    /**
     * Add a field type to the {@link FieldAssembler}, ensuring a constant index for nullables
     * <p>
     * Avro inline encoding doesn't interpret schemas and expects nulls to be at index 1.
     *
     * @param builder
     * @param typeSchema
     * @param field
     * @return
     */
    private FieldAssembler<Schema> addFieldType(FieldBuilder<Schema> builder, Schema typeSchema, FieldDescription field) {
        if (field.isNullable()) {
            return builder.type().unionOf().nullType().and().type(typeSchema).endUnion().noDefault();
        }
        return builder.type(typeSchema).noDefault();
    }

    private Internal getInternal() throws IOException {
        Internal internal = m_internal;

        if (internal.m_avro == null) {
            throw new IOException("Avro schema registry is not configured");
        }
        return internal;
    }

    /**
     * Configuration class for dictating how avro schemas are generated
     */
    public static final class Configuration {
        final Schema m_timestamp;
        final Schema m_geographyPoint;
        final Schema m_geography;

        public static Builder builder() {
            return new Builder();
        }

        public static Configuration defaults() {
            return new Builder().build();
        }

        public Configuration(TimestampPrecision timestamp, GeographyPointSerialization geographyPoint,
                GeographySerialization geography) {
            m_timestamp = timestamp.m_schema;
            m_geographyPoint = geographyPoint.m_schema;
            m_geography = geography.m_schema;
        }

        /**
         * Builder to help construct {@link Configuration} with good defaults
         */
        public static final class Builder {
            private TimestampPrecision m_timestamp = TimestampPrecision.MICROSECONDS;
            private GeographyPointSerialization m_geographyPoint = GeographyPointSerialization.FIXED_BINARY;
            private GeographySerialization m_geography = GeographySerialization.BINARY;

            Builder() {}

            public Builder timestampPrecision(TimestampPrecision precision) {
                m_timestamp = precision;
                return this;
            }

            public Builder geographyPoint(GeographyPointSerialization geographyPoint) {
                m_geographyPoint = geographyPoint;
                return this;
            }

            public Builder geography(GeographySerialization geography) {
                m_geography = geography;
                return this;
            }

            public Configuration build() {
                return new Configuration(m_timestamp, m_geographyPoint, m_geography);
            }
        }

        public enum TimestampPrecision {
            MICROSECONDS(SCHEMA_TIMESTAMP_MICRO), MILLISECONDS(SCHEMA_TIMESTAMP_MILLI);

            final Schema m_schema;

            TimestampPrecision(Schema schema) {
                m_schema = schema;
            }
        }

        public enum GeographyPointSerialization {
            STRING(SCHEMA_GEOGRAPHY_POINT_STRING), BINARY(SCHEMA_GEOGRAPHY_POINT_BINARY),
            FIXED_BINARY(SCHEMA_GEOGRAPHY_POINT_FIXED_BINARY);

            final Schema m_schema;

            GeographyPointSerialization(Schema schema) {
                m_schema = schema;
            }
        }

        public enum GeographySerialization {
            STRING(SCHEMA_GEOGRAPHY_STRING), BINARY(SCHEMA_GEOGRAPHY_BINARY);

            final Schema m_schema;

            GeographySerialization(Schema schema) {
                m_schema = schema;
            }
        }
    }

    /**
     * Serialize values using the avro format and a fixed schema
     */
    @NotThreadSafe
    public final class Serializer {
        private final int m_id;
        private final Schema m_schema;
        private final GenericDatumWriter<GenericRecord> m_writer;
        private BinaryEncoder m_encoder;

        Serializer(int id) throws IOException {
            m_id = id;
            m_schema = getById(m_id);
            m_writer = new GenericDatumWriter<>(m_schema);
        }

        /**
         * Serialize the {@code values} to {@code serializer}
         *
         * @param serializer to write the values to
         * @param values     to be serialized
         * @throws IOException If an error occurs while serializing
         */
        public void serialize(FastSerializer serializer, Object[] values) throws IOException {
            GenericData.Record record = new GenericData.Record(m_schema);

            for (int i = 0; i < values.length; ++i) {
                record.put(i, values[i]);
            }

            m_encoder = EncoderFactory.get().directBinaryEncoder(serializer, m_encoder);

            serializer.write(s_avroMagic);
            serializer.writeInt(m_id);
            m_writer.write(record, m_encoder);
            m_encoder.flush();
        }
    }

    /**
     * Deserialize a {@link ByteBuffer} to an {@link Object[]}. This object caches the last used schema so it is best
     * when used to decode records which do not change schema often.
     */
    @NotThreadSafe
    public final class Deserializer {
        // Cache schema and reader for reuse when schema does not change between decoding calls
        private int m_id = -1;
        private Schema m_schema;
        private DatumReader<GenericRecord> m_reader;

        // Save decoder and recorder for reuse as recommended by the avro documentation
        private BinaryDecoder m_decoder;
        private GenericRecord m_record;

        Deserializer() {}

        /**
         * Deserialize the avro encoded record in {@code buffer}
         *
         * @param buffer with serialized record
         * @return values deserialized from {@code buffer}
         * @throws IOException If an error occurs while deserializing
         */
        public Object[] deserialize(ByteBuffer buffer) throws IOException {
            int id = decodeId(buffer);
            if (id != m_id) {
                m_id = id;
                m_schema = getById(m_id);
                m_reader = new GenericDatumReader<>(m_schema);
            }

            int length = buffer.remaining();
            m_decoder = DecoderFactory.get().directBinaryDecoder(new ByteBufferInputStream(buffer), m_decoder);
            m_record = m_reader.read(m_record, m_decoder);

            // Parse generic record for objects to return
            Object[] results = new Object[m_schema.getFields().size()];
            int i = 0;
            for (Schema.Field f : m_schema.getFields()) {
                Object obj = m_record.get(f.name());
                if (obj instanceof org.apache.avro.util.Utf8) {
                    results[i++] = obj.toString();
                } else {
                    // Note: may be null
                    results[i++] = obj;
                }
            }
            if (s_log.isTraceEnabled()) {
                s_log.trace(this.getClass().getSimpleName() + " decoded " + length + " avro bytes to " + results.length
                        + " objects");
            }

            return results;
        }
    }

    /**
     * Internal class to make sure that the client and avro configuration are always kept in sync
     */
    private static final class Internal {
        final SchemaRegistryClient m_client;
        final AvroType m_avro;

        Internal(SchemaRegistryClient client, AvroType avro) {
            m_client = client;
            m_avro = avro;
        }
    }
}
