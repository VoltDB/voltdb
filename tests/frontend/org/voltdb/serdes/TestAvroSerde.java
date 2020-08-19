/* This file is part of VoltDB.
 * Copyright (C) 2020 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package org.voltdb.serdes;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.voltdb.VoltType;
import org.voltdb.compiler.deploymentfile.AvroType;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.test.utils.RandomTestRule;

import com.google_voltpatches.common.collect.ImmutableList;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class TestAvroSerde {
    @Rule
    public final RandomTestRule m_random = new RandomTestRule();

    @Rule
    public final TestName m_name = new TestName();

    @Test
    public void serializeDeserialize() throws Exception {
        AvroType avro = new AvroType();
        avro.setRegistry("faker");

        AvroSerde client = new MockAvroClient(avro);

        List<VoltType> types = ImmutableList.of(VoltType.TINYINT, VoltType.SMALLINT, VoltType.INTEGER, VoltType.BIGINT,
                VoltType.FLOAT, VoltType.DECIMAL, VoltType.TIMESTAMP, VoltType.STRING, VoltType.VARBINARY,
                VoltType.GEOGRAPHY_POINT, VoltType.GEOGRAPHY);

        Object[][] values = m_random.nextValues(10, types);

        int id = client.getIdForSchema(m_name.getMethodName(), m_name.getMethodName(),
                types.stream().map(t -> new FieldDescription(t.getName(), t)).collect(Collectors.toList()));

        AvroSerde.Serializer serializer = client.createSerializer(id);
        FastSerializer fs = new FastSerializer();

        // Serialize all original values
        for (Object[] object : values) {
            serializer.serialize(fs, object);
        }

        ByteBuffer serialized = fs.getBuffer();
        fs.discard();

        // Deserialize the records and validate they are the same as the original values
        AvroSerde.Deserializer deserialier = client.createDeserializer();
        for (int i = 0; i < values.length; ++i) {
            assertArrayEquals(values[i], deserialier.deserialize(serialized));
        }

        assertFalse(serialized.hasRemaining());
    }

    private class MockAvroClient extends AvroSerde {
        public MockAvroClient(AvroType avro) {
            super(avro);
        }

        @Override
        SchemaRegistryClient buildClient(AvroType avro) {
            return new MockSchemaRegistryClient();
        }
    }
}
