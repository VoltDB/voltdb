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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;

/**
 * Utility class to hold all of the classes in order to use the {@link LogicalType}s supported by VoltDB. They are
 * divided into three sections {@link LogicalType}s, {@link Schema}s and {@link Conversion}.
 * <p>
 * {@link LogicalType}s are automatically registered when this class is loaded and are used to notify avro which logical
 * types are supported
 * <p>
 * {@link Schema}s are used when constructing a complex schema with a logical type in one or more fields
 * <p>
 * {@link Conversion}s are used convert values to the primitive types supported by avro or from the primitive type to
 * the logical type. In order to use the {@link Conversion}s they have to be registered with {@link GenericData}. The
 * utility method {@link #addLogicalTypeConversions()} will add all of the {@link Conversion}s in this class.
 */
public class VoltAvroLogicalTypes {
    // Custom logical types
    public static final LogicalType TYPE_BYTE = new VoltLogicalType("byte", Schema.Type.INT);
    public static final LogicalType TYPE_SHORT = new VoltLogicalType("short", Schema.Type.INT);
    public static final LogicalType TYPE_GEOGRAPHY_POINT = new VoltLogicalType("geographyPoint", Schema.Type.FIXED,
            Schema.Type.BYTES, Schema.Type.STRING);
    public static final LogicalType TYPE_GEOGRAPHY = new VoltLogicalType("geography", Schema.Type.BYTES,
            Schema.Type.STRING);
    public static final LogicalType TYPE_BYTE_ARRAY = new VoltLogicalType("byteArray", Schema.Type.BYTES);
    public static final LogicalType TYPE_DECIMAL = LogicalTypes.decimal(38, 12);

    // Schemas for logical types
    public static final Schema SCHEMA_TIMESTAMP_MICRO = LogicalTypes.timestampMicros()
            .addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema SCHEMA_TIMESTAMP_MILLI = LogicalTypes.timestampMillis()
            .addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema SCHEMA_BYTE = TYPE_BYTE.addToSchema(Schema.create(Schema.Type.INT));
    public static final Schema SCHEMA_SHORT = TYPE_SHORT.addToSchema(Schema.create(Schema.Type.INT));
    public static final Schema SCHEMA_DECIMAL = TYPE_DECIMAL.addToSchema(Schema.create(Schema.Type.BYTES));
    public static final Schema SCHEMA_GEOGRAPHY_POINT_FIXED_BINARY = TYPE_GEOGRAPHY_POINT
            .addToSchema(SchemaBuilder.builder().fixed("geographyPoint").size(GeographyPointValue.getLengthInBytes()));
    public static final Schema SCHEMA_GEOGRAPHY_POINT_BINARY = TYPE_GEOGRAPHY_POINT
            .addToSchema(Schema.create(Schema.Type.BYTES));
    public static final Schema SCHEMA_GEOGRAPHY_POINT_STRING = TYPE_GEOGRAPHY_POINT
            .addToSchema(Schema.create(Schema.Type.STRING));
    public static final Schema SCHEMA_GEOGRAPHY_BINARY = TYPE_GEOGRAPHY.addToSchema(Schema.create(Schema.Type.BYTES));
    public static final Schema SCHEMA_GEOGRAPHY_STRING = TYPE_GEOGRAPHY.addToSchema(Schema.create(Schema.Type.STRING));
    public static final Schema SCHEMA_BYTE_ARRAY = TYPE_BYTE_ARRAY.addToSchema(Schema.create(Schema.Type.BYTES));

    // Converters for logical types
    public static final class ByteConversion extends Conversion<Byte> {
        @Override
        public Class<Byte> getConvertedType() {
            return Byte.class;
        }

        @Override
        public String getLogicalTypeName() {
            return TYPE_BYTE.getName();
        }

        @Override
        public Integer toInt(Byte value, Schema schema, LogicalType type) {
            return value.intValue();
        }

        @Override
        public Byte fromInt(Integer value, Schema schema, LogicalType type) {
            return value.byteValue();
        }
    };

    public static final class ShortConversion extends Conversion<Short> {
        @Override
        public Class<Short> getConvertedType() {
            return Short.class;
        }

        @Override
        public String getLogicalTypeName() {
            return TYPE_SHORT.getName();
        }

        @Override
        public Integer toInt(Short value, Schema schema, LogicalType type) {
            return value.intValue();
        }

        @Override
        public Short fromInt(Integer value, Schema schema, LogicalType type) {
            return value.shortValue();
        }
    };

    public static final class TimestampMicroConversion extends Conversion<TimestampType> {
        @Override
        public Class<TimestampType> getConvertedType() {
            return TimestampType.class;
        }

        @Override
        public String getLogicalTypeName() {
            return SCHEMA_TIMESTAMP_MICRO.getLogicalType().getName();
        }

        @Override
        public Long toLong(TimestampType value, Schema schema, LogicalType type) {
            return value.getTime();
        }

        @Override
        public TimestampType fromLong(Long value, Schema schema, LogicalType type) {
            return new TimestampType(value);
        }
    };

    public static final class TimestampMilliConversion extends Conversion<TimestampType> {
        @Override
        public Class<TimestampType> getConvertedType() {
            return TimestampType.class;
        }

        @Override
        public String getLogicalTypeName() {
            return SCHEMA_TIMESTAMP_MILLI.getLogicalType().getName();
        }

        @Override
        public Long toLong(TimestampType value, Schema schema, LogicalType type) {
            return value.getTime() / 1000;
        }

        @Override
        public TimestampType fromLong(Long value, Schema schema, LogicalType type) {
            return new TimestampType(value * 1000);
        }
    };

    public static final class ByteArrayConversion extends Conversion<byte[]> {
        @Override
        public Class<byte[]> getConvertedType() {
            return byte[].class;
        }

        @Override
        public String getLogicalTypeName() {
            return TYPE_BYTE_ARRAY.getName();
        }

        @Override
        public ByteBuffer toBytes(byte[] value, Schema schema, LogicalType type) {
            return ByteBuffer.wrap(value);
        }

        @Override
        public byte[] fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
            byte[] result = new byte[value.remaining()];
            value.get(result);
            return result;
        }
    };

    public static final class GeographicPointConversion extends Conversion<GeographyPointValue> {
        @Override
        public Class<GeographyPointValue> getConvertedType() {
            return GeographyPointValue.class;
        }

        @Override
        public String getLogicalTypeName() {
            return TYPE_GEOGRAPHY_POINT.getName();
        }

        @Override
        public GenericFixed toFixed(GeographyPointValue value, Schema schema, LogicalType type) {
            return new GenericData.Fixed(schema, toBytes(value, schema, type).array());
        }

        @Override
        public GeographyPointValue fromFixed(GenericFixed value, Schema schema, LogicalType type) {
            return fromBytes(ByteBuffer.wrap(value.bytes()), schema, type);
        }

        @Override
        public ByteBuffer toBytes(GeographyPointValue value, Schema schema, LogicalType type) {
            ByteBuffer buffer = ByteBuffer.allocate(GeographyPointValue.getLengthInBytes());
            value.flattenToBuffer(buffer);
            buffer.flip();
            return buffer;
        }

        @Override
        public GeographyPointValue fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
            return GeographyPointValue.unflattenFromBuffer(value);
        }

        @Override
        public CharSequence toCharSequence(GeographyPointValue value, Schema schema, LogicalType type) {
            return value.toWKT();
        }

        @Override
        public GeographyPointValue fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
            return GeographyPointValue.fromWKT(value.toString());
        }
    };

    public static final class GeographicConversion extends Conversion<GeographyValue> {
        @Override
        public Class<GeographyValue> getConvertedType() {
            return GeographyValue.class;
        }

        @Override
        public String getLogicalTypeName() {
            return TYPE_GEOGRAPHY.getName();
        }

        @Override
        public ByteBuffer toBytes(GeographyValue value, Schema schema, LogicalType type) {
            ByteBuffer buffer = ByteBuffer.allocate(value.getLengthInBytes());
            value.flattenToBuffer(buffer);
            buffer.flip();
            return buffer;
        }

        @Override
        public GeographyValue fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
            return GeographyValue.unflattenFromBuffer(value);
        }

        @Override
        public CharSequence toCharSequence(GeographyValue value, Schema schema, LogicalType type) {
            return value.toWKT();
        }

        @Override
        public GeographyValue fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
            return GeographyValue.fromWKT(value.toString());
        }
    };

    public static void addLogicalTypeConversions() {
        GenericData gd = GenericData.get();
        gd.addLogicalTypeConversion(new ByteConversion());
        gd.addLogicalTypeConversion(new ShortConversion());
        gd.addLogicalTypeConversion(new Conversions.DecimalConversion());
        gd.addLogicalTypeConversion(new TimestampMicroConversion());
        gd.addLogicalTypeConversion(new ByteArrayConversion());
        gd.addLogicalTypeConversion(new GeographicPointConversion());
        gd.addLogicalTypeConversion(new GeographicConversion());
        gd.addLogicalTypeConversion(new TimestampMilliConversion());
    }

    private static class VoltLogicalType extends LogicalType {
        private final EnumSet<Schema.Type> m_supported;

        VoltLogicalType(String logicalTypeName, Schema.Type... type) {
            super(logicalTypeName);
            m_supported = EnumSet.copyOf(Arrays.asList(type));
            LogicalTypes.register(logicalTypeName, s -> this);
        }

        @Override
        public void validate(Schema schema) {
            super.validate(schema);
            if (!m_supported.contains(schema.getType())) {
                throw new IllegalArgumentException(
                        getName() + " can only be used with one of the underlying types: " + m_supported);
            }
        }
    }

    private VoltAvroLogicalTypes() {}
}
