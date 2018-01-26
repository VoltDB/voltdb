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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldTypeBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.exportclient.decode.DecodeType.SimpleVisitor;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableList;

/**
 * Converts an object array containing an exported row values into an
 * an avro record.
 */
public class AvroDecoder extends RowDecoder<GenericRecord,RuntimeException> {

    protected final String m_tableName;
    protected final String m_packageName;
    protected final Schema m_schema;
    protected final SimpleDateFormat m_dtfmt =
            new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
    protected final FieldNameDecoder [] m_fieldDecoders;

    /**
     * Generate an avro schema with the given table column types, and prepares per column field
     * decoders. All TINYINT, SMALLINT column types are cast to avro integer types. TIMESTAMP
     * is cast to avro long field (containing millis since epoch)
     *
     * @param columnTypes table column types
     * @param columnNames table column names
     * @param firstFieldOffset
     * @param tableName avro record type name
     * @param packageName avro record package name space (equivalent to java packages)
     */
    protected AvroDecoder(List<VoltType> columnTypes,
            List<String> columnNames, int firstFieldOffset,
            String tableName, String packageName, TimeZone tz) {

        super(columnTypes, columnNames, firstFieldOffset);

        Preconditions.checkArgument(
                tableName != null && !tableName.trim().isEmpty(),
                "table name is null or empty"
                );
        Preconditions.checkArgument(
                packageName != null && !packageName.trim().isEmpty(),
                "package name is null or empty"
                );
        Preconditions.checkArgument(
                tz != null,
                "time zone name is null"
                );

        m_tableName = tableName;
        m_packageName = packageName;
        m_dtfmt.setTimeZone(tz);

        FieldAssembler<Schema> fa =
                SchemaBuilder.record(m_tableName).namespace(m_packageName).fields();
        List<FieldNameDecoder> decoders = new ArrayList<>();

        int fieldPos = 0;
        for (Entry<String, DecodeType> e: m_typeMap.entrySet()) {
            e.getValue().accept(typeBuilderVisitor, fa.name(e.getKey()).type(), null);
            decoders.add(e.getValue().accept(decodingVisitor, fieldPos++, null));
        }

        m_schema = fa.endRecord();
        m_fieldDecoders = decoders.toArray(new FieldNameDecoder[0]);
    }

    /**
     * @return the export table associated avro schema
     */
    public Schema getSchema() {
        return m_schema;
    }

    @Override
    public GenericRecord decode(GenericRecord ignored, Object[] fields)
            throws RuntimeException {

        Preconditions.checkArgument(
                fields != null && fields.length > m_firstFieldOffset,
                "null or inapropriately sized export row array"
        );

        GenericData.Record to = new GenericData.Record(m_schema);
        for (
                int i = m_firstFieldOffset, j = 0;
                i < fields.length && j < m_fieldDecoders.length;
                ++i, ++j
        ) {
            m_fieldDecoders[j].decode(to, fields[i]);
        }
        return to;
    }

    final static SimpleVisitor<FieldAssembler<Schema>,FieldTypeBuilder<Schema>> typeBuilderVisitor =
            new SimpleVisitor<FieldAssembler<Schema>,FieldTypeBuilder<Schema>>() {

                @Override
                public FieldAssembler<Schema> visitTinyInt(FieldTypeBuilder<Schema> p,
                        Object v) throws RuntimeException {
                    return p.nullable().intType().noDefault();
                }

                @Override
                public FieldAssembler<Schema> visitSmallInt(FieldTypeBuilder<Schema> p,
                        Object v) throws RuntimeException {
                    return p.nullable().intType().noDefault();
                }

                @Override
                public FieldAssembler<Schema> visitInteger(FieldTypeBuilder<Schema> p,
                        Object v) throws RuntimeException {
                    return p.nullable().intType().noDefault();
                }

                @Override
                public FieldAssembler<Schema> visitBigInt(FieldTypeBuilder<Schema> p,
                        Object v) throws RuntimeException {
                    return p.nullable().longType().noDefault();
                }

                @Override
                public FieldAssembler<Schema> visitFloat(FieldTypeBuilder<Schema> p,
                        Object v) throws RuntimeException {
                    return p.nullable().doubleType().noDefault();
                }

                @Override
                public FieldAssembler<Schema> visitTimestamp(FieldTypeBuilder<Schema> p,
                        Object v) throws RuntimeException {
                    return p.nullable().stringType().noDefault();
                }

                @Override
                public FieldAssembler<Schema> visitString(FieldTypeBuilder<Schema> p,
                        Object v) throws RuntimeException {
                    return p.nullable().stringType().noDefault();
                }

                @Override
                public FieldAssembler<Schema> visitVarBinary(FieldTypeBuilder<Schema> p,
                        Object v) throws RuntimeException {
                    return p.nullable().bytesType().noDefault();
                }

                @Override
                public FieldAssembler<Schema> visitDecimal(FieldTypeBuilder<Schema> p,
                        Object v) throws RuntimeException {
                    return p.nullable().bytesBuilder()
                                .prop("logicalType","decimal")
                                .prop("scale","12")
                                .prop("precision","38")
                            .endBytes().noDefault();
                }

                @Override
                public FieldAssembler<Schema> visitGeographyPoint(FieldTypeBuilder<Schema> p,
                        Object v) throws RuntimeException {
                    return p.nullable().stringType().noDefault();
                }

                @Override
                public FieldAssembler<Schema> visitGeography(FieldTypeBuilder<Schema> p,
                        Object v) throws RuntimeException {
                    return p.nullable().stringType().noDefault();
                }
    };

    static abstract class FieldNameDecoder implements FieldDecoder<GenericData.Record,RuntimeException> {
        final int m_fieldPos;
        FieldNameDecoder(final int fieldPos) {
            m_fieldPos = fieldPos;
        }
    }

    final SimpleVisitor<FieldNameDecoder,Integer> decodingVisitor = new SimpleVisitor<FieldNameDecoder,Integer>() {

        @Override
        public FieldNameDecoder visitTinyInt(Integer p, Object v)
                throws RuntimeException {
            return new FieldNameDecoder(p) {
                @Override
                final public void decode(Record to, Object field) throws RuntimeException {
                    if (field == null) return;
                    to.put(m_fieldPos, Byte.valueOf((byte)field).intValue());
                }
            };
        }

        @Override
        public FieldNameDecoder visitSmallInt(Integer p, Object v)
                throws RuntimeException {
            return new FieldNameDecoder(p) {
                @Override
                final public void decode(Record to, Object field) throws RuntimeException {
                    if (field == null) return;
                    to.put(m_fieldPos, Short.valueOf((short)field).intValue());
                }
            };
        }

        @Override
        public FieldNameDecoder visitInteger(Integer p, Object v)
                throws RuntimeException {
            return new FieldNameDecoder(p) {
                @Override
                final public void decode(Record to, Object field) throws RuntimeException {
                    if (field == null) return;
                    to.put(m_fieldPos, (int)field);
                }
            };
        }

        @Override
        public FieldNameDecoder visitBigInt(Integer p, Object v)
                throws RuntimeException {
            return new FieldNameDecoder(p) {
                @Override
                final public void decode(Record to, Object field) throws RuntimeException {
                    if (field == null) return;
                    to.put(m_fieldPos, (long)field);
                }
            };
        }

        @Override
        public FieldNameDecoder visitFloat(Integer p, Object v)
                throws RuntimeException {
            return new FieldNameDecoder(p) {
                @Override
                final public void decode(Record to, Object field) throws RuntimeException {
                    if (field == null) return;
                    to.put(m_fieldPos, (double)field);
                }
            };
        }

        @Override
        public FieldNameDecoder visitTimestamp(Integer p, Object v)
                throws RuntimeException {
            return new FieldNameDecoder(p) {
                final SimpleDateFormat m_df = (SimpleDateFormat)m_dtfmt.clone();
                @Override
                final public void decode(Record to, Object field) throws RuntimeException {
                    if (field == null) return;
                    Date timestamp = ((TimestampType)field).asApproximateJavaDate();
                    to.put(m_fieldPos, m_df.format(timestamp));
                }
            };
        }

        @Override
        public FieldNameDecoder visitString(Integer p, Object v)
                throws RuntimeException {
            return new FieldNameDecoder(p) {
                @Override
                final public void decode(Record to, Object field) throws RuntimeException {
                    if (field == null) return;
                    to.put(m_fieldPos, field);
                }
            };
        }

        @Override
        public FieldNameDecoder visitVarBinary(Integer p, Object v)
                throws RuntimeException {
            return new FieldNameDecoder(p) {
                @Override
                final public void decode(Record to, Object field) throws RuntimeException {
                    if (field == null) return;
                    to.put(m_fieldPos, ByteBuffer.wrap((byte[])field));
                }
            };
        }

        @Override
        public FieldNameDecoder visitDecimal(Integer p, Object v)
                throws RuntimeException {
            return new FieldNameDecoder(p) {
                @Override
                final public void decode(Record to, Object field) throws RuntimeException {
                    if (field == null) return;
                    byte [] serialized = VoltDecimalHelper.serializeBigDecimal((BigDecimal)field);
                    to.put(m_fieldPos, ByteBuffer.wrap(serialized));
                }
            };
        }

        @Override
        public FieldNameDecoder visitGeographyPoint(Integer p, Object v)
                throws RuntimeException {
            return new FieldNameDecoder(p) {
                @Override
                final public void decode(Record to, Object field) throws RuntimeException {
                    if (field == null) return;
                    to.put(m_fieldPos, ((GeographyPointValue)field).toWKT());
                }
            };
        }

        @Override
        public FieldNameDecoder visitGeography(Integer p, Object v)
                throws RuntimeException {
            return new FieldNameDecoder(p) {
                @Override
                final public void decode(Record to, Object field) throws RuntimeException {
                    if (field == null) return;
                    to.put(m_fieldPos, ((GeographyValue)field).toWKT());
                }
            };
        }
    };

    public static class Builder extends RowDecoder.Builder {

        protected String m_packageName = "volt.db";
        protected String m_tableName;
        protected TimeZone m_timeZone = TimeZone.getDefault();

        public Builder tableName(String tableName) {
            m_tableName = convertToCamelCase(tableName, true);
            return this;
        }

        @Override
        public Builder columnNames(List<String> columnNames) {
            super.columnNames(FluentIterable.from(columnNames).transform(camelCaseNameLowerFirst).toList());
            return this;
        }

        @Override
        public Builder columnTypeMap(LinkedHashMap<String, VoltType> map) {
            columnNames(ImmutableList.copyOf(map.keySet()));
            super.columnTypes(ImmutableList.copyOf(map.values()));
            return this;
        }

        public Builder packageName(String packageName) {
            m_packageName = packageName;
            return this;
        }

        public Builder timeZone(String timeZoneID) {
            return timeZone(TimeZone.getTimeZone(timeZoneID));
        }

        public Builder timeZone(TimeZone timeZone) {
            m_timeZone = timeZone;
            return this;
        }

        public AvroDecoder build() {
            return new AvroDecoder(
                    m_columnTypes,
                    m_columnNames,
                    m_firstFieldOffset,
                    m_tableName,
                    m_packageName,
                    m_timeZone
                    );
        }

        @Override
        public String toString() {
            return "Builder [m_packageName=" + m_packageName + ", m_tableName="
                    + m_tableName + ", m_columnTypes=" + m_columnTypes
                    + ", m_columnNames=" + m_columnNames
                    + ", m_firstFieldOffset=" + m_firstFieldOffset + "]";
        }
    }

    public static class DelegateBuilder extends RowDecoder.DelegateBuilder {
        final AvroDecoder.Builder m_delegateBuilder;

        protected DelegateBuilder(Builder builder) {
            super(builder);
            m_delegateBuilder = builder;
        }

        protected DelegateBuilder(DelegateBuilder delegateBuilder) {
            super(delegateBuilder.getDelegateAs(Builder.class));
            m_delegateBuilder = delegateBuilder.getDelegateAs(Builder.class);
        }

       public DelegateBuilder tableName(String tableName) {
            m_delegateBuilder.tableName(tableName);
            return this;
        }

        public DelegateBuilder packageName(String packageName) {
            m_delegateBuilder.packageName(packageName);
            return this;
        }

        public DelegateBuilder timeZone(TimeZone timeZone) {
            m_delegateBuilder.timeZone(timeZone);
            return this;
        }

        public DelegateBuilder timeZone(String timeZoneID) {
            m_delegateBuilder.timeZone(timeZoneID);
            return this;
        }

        @Override
        protected <TT extends RowDecoder.Builder> TT getDelegateAs(Class<TT> clazz) {
            return clazz.cast(m_delegateBuilder);
        }

    }
}
