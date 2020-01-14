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
import org.voltdb.VoltDB;
import org.voltdb.exportclient.ExportRow;
import org.voltdb.exportclient.decode.AvroDecoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Serializer for converting {@link ExportRow} to Avro format byte array as well as
 * register the schema in the schema registry.
 */
public class ExportAvroSerializer {
    private final AvroDecoder m_decoder;
    private static String s_schemaRegistryUrl;
    private static SchemaRegistryClient s_schemaRegistryClient;
    private static final EncoderFactory s_encoderFactory = EncoderFactory.get();

    public ExportAvroSerializer() {
        m_decoder = new AvroDecoder.Builder().build();
        updateConfig();
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
        GenericRecord avroRecord = m_decoder.decode(exportRow.generation, exportRow.tableName, exportRow.types,
                exportRow.names, null, exportRow.values);

        return serialize(topic, avroRecord);
    }

    private byte[] serialize(String topic, GenericRecord avroRecord) {
        if (avroRecord == null) {
            return null;
        }
        topic = "kipling" + topic + "-value";
        Schema schema = avroRecord.getSchema();
        try {
            // register the schema if not registered, return the schema id.
            int schemaId = s_schemaRegistryClient.register(topic, schema);
            // serialize to bytes
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(0);
            out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
            BinaryEncoder encoder = s_encoderFactory.directBinaryEncoder(out, null);
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
     * Handling the change of the {@code SchemaRegistryUrl} in the deployment file.
     */
    public synchronized void updateConfig() {
        String url = VoltDB.instance().getCatalogContext().getDeployment().getSchemaregistryurl().trim();
        // update the serializer config if the schema_register_url in the deployment file changes
        if (!Objects.equals(s_schemaRegistryUrl, url)) {
            s_schemaRegistryUrl = url;
            List<String> baseUrls = Arrays.asList(url.split(","));
            // create a new s_schemaRegistryClient when we have a update on the url, since the cache is outdated
            s_schemaRegistryClient = new CachedSchemaRegistryClient(baseUrls, 10000);
        }
    }
}
