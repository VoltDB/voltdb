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

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.exportclient.ExportDecoderBase.BinaryEncoding;
import org.voltdb.exportclient.decode.DecodeType.SimpleVisitor;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ImmutableList;

/**
 * Converts an object array containing an exported row values into an
 * array of their string representations
 */
public class StringArrayDecoder extends RowDecoder<String[], RuntimeException> {

    protected final SimpleDateFormat m_dateFormatter;
    protected final BinaryEncoding m_binaryEncoding;
    protected final String m_nullRepresentation;
    protected final Map<Long, StringFieldDecoder []> m_fieldDecoders = new HashMap<>();

    protected StringArrayDecoder(
            int firstFieldOffset,
            SimpleDateFormat dateFormatter,
            BinaryEncoding binaryEncoding,
            String nullRepresentation)
    {
        super(firstFieldOffset);

        Preconditions.checkArgument(dateFormatter != null, "date formatter is null");
        Preconditions.checkArgument(binaryEncoding != null, "binary encoding is null");

        m_dateFormatter = dateFormatter;
        m_binaryEncoding = binaryEncoding;
        m_nullRepresentation = nullRepresentation;
    }

    /**
     * Converts an object array containing an exported row values into an
     * array of their string representations
     */
    @Override
    public String[] decode(long generation, String tableName, List<VoltType> types, List<String> names, String[] to, Object[] fields) throws RuntimeException {
        Preconditions.checkArgument(
                fields != null && fields.length > m_firstFieldOffset,
                "null or inapropriately sized export row array"
        );
        /*
         * Builds a list of string formatters that reflects the row
         * column types.
         */
        StringFieldDecoder [] fieldDecoders;
        if (!m_fieldDecoders.containsKey(generation)) {
            int fieldCount = 0;
            Map<String, DecodeType> typeMap = getTypeMap(generation, types, names);
            ImmutableList.Builder<StringFieldDecoder> lb = ImmutableList.builder();
            for (org.voltdb.exportclient.decode.DecodeType dt: typeMap.values()) {
                lb.add(dt.accept(decodingVisitor, fieldCount++, null));
            }

            fieldDecoders = lb.build().toArray(new StringFieldDecoder[0]);
            m_fieldDecoders.put(generation, fieldDecoders);
        } else {
            fieldDecoders = m_fieldDecoders.get(generation);
        }
        if (to == null || to.length < fieldDecoders.length) {
            to = new String[fieldDecoders.length];
        }
        for (
                int i = m_firstFieldOffset, j = 0;
                i < fields.length && j < fieldDecoders.length;
                ++i, ++j
        ) {
            fieldDecoders[j].decode(to, fields[i]);
        }

        return to;
    }

    static abstract class StringFieldDecoder implements FieldDecoder<String[], RuntimeException> {
        protected final int m_fieldIndex;

        StringFieldDecoder(int fieldIndex) {
            m_fieldIndex = fieldIndex;
        }
    }

    final SimpleVisitor<StringFieldDecoder,Integer> decodingVisitor = new SimpleVisitor<StringFieldDecoder,Integer>() {

        @Override
        public StringFieldDecoder visitTinyInt(Integer p, Object v) throws RuntimeException {
            return new StringFieldDecoder(p) {
                @Override
                public final void decode(String[] to, Object field) throws RuntimeException {
                    to[m_fieldIndex] = field != null ? field.toString() : m_nullRepresentation;
                }
            };
        }

        @Override
        public StringFieldDecoder visitSmallInt(Integer p, Object v) throws RuntimeException {
            return new StringFieldDecoder(p) {
                @Override
                public final void decode(String[] to, Object field) throws RuntimeException {
                    to[m_fieldIndex] = field != null ? field.toString() : m_nullRepresentation;
                }
            };
        }

        @Override
        public StringFieldDecoder visitInteger(Integer p, Object v) throws RuntimeException {
            return new StringFieldDecoder(p) {
                @Override
                public final void decode(String[] to, Object field) throws RuntimeException {
                    to[m_fieldIndex] = field != null ? field.toString() : m_nullRepresentation;
                }
            };
        }

        @Override
        public StringFieldDecoder visitBigInt(Integer p, Object v) throws RuntimeException {
            return new StringFieldDecoder(p) {
                @Override
                public final void decode(String[] to, Object field) throws RuntimeException {
                    to[m_fieldIndex] = field != null ? field.toString() : m_nullRepresentation;
                }
            };
        }

        @Override
        public StringFieldDecoder visitFloat(Integer p, Object v) throws RuntimeException {
            return new StringFieldDecoder(p) {
                @Override
                public final void decode(String[] to, Object field) throws RuntimeException {
                    to[m_fieldIndex] = field != null ? field.toString() : m_nullRepresentation;
                }
            };
        }

        @Override
        public StringFieldDecoder visitTimestamp(Integer p, Object v) throws RuntimeException {
            return new StringFieldDecoder(p) {
                final SimpleDateFormat m_df = (SimpleDateFormat)m_dateFormatter.clone();
                @Override
                public final void decode(String[] to, Object field) throws RuntimeException {
                    if (field == null) {
                        to[m_fieldIndex] = m_nullRepresentation;
                        return;
                    }
                    TimestampType ts = (TimestampType)field;
                    to[m_fieldIndex] = m_df.format(ts.asApproximateJavaDate());
                }
            };
        }

        @Override
        public StringFieldDecoder visitString(Integer p, Object v) throws RuntimeException {
            return new StringFieldDecoder(p) {
                @Override
                public final void decode(String[] to, Object field) throws RuntimeException {
                    to[m_fieldIndex] = field != null ? (String)field : m_nullRepresentation;
                }
            };
        }

        @Override
        public StringFieldDecoder visitVarBinary(Integer p, Object v) throws RuntimeException {
            if (m_binaryEncoding == BinaryEncoding.BASE64) {
                return new StringFieldDecoder(p) {
                    @Override
                    public final void decode(String[] to, Object field) throws RuntimeException {
                        byte [] bytes = (byte[])field;
                        to[m_fieldIndex] = field != null ? Encoder.base64Encode(bytes) : m_nullRepresentation;
                    }
                };
            } else {
                return new StringFieldDecoder(p) {
                    @Override
                    public final void decode(String[] to, Object field) throws RuntimeException {
                        byte [] bytes = (byte[])field;
                        to[m_fieldIndex] = field != null ? Encoder.hexEncode(bytes) : m_nullRepresentation;
                    }
                };
            }
        }

        @Override
        public StringFieldDecoder visitDecimal(Integer p, Object v) throws RuntimeException {
            return new StringFieldDecoder(p) {
                @Override
                public final void decode(String[] to, Object field) throws RuntimeException {
                    to[m_fieldIndex] = field != null ? field.toString() : m_nullRepresentation;
                }
            };
        }

        @Override
        public StringFieldDecoder visitGeographyPoint(Integer p, Object v) throws RuntimeException {
            return new StringFieldDecoder(p) {
                @Override
                public final void decode(String[] to, Object field) throws RuntimeException {
                    GeographyPointValue gpv = (GeographyPointValue)field;
                    to[m_fieldIndex] = field != null ? gpv.toWKT() : m_nullRepresentation;
                }
            };
        }

        @Override
        public StringFieldDecoder visitGeography(Integer p, Object v) throws RuntimeException {
            return new StringFieldDecoder(p) {
                @Override
                public final void decode(String[] to, Object field) throws RuntimeException {
                    GeographyValue gv = (GeographyValue)field;
                    to[m_fieldIndex] = field != null ? gv.toWKT() : m_nullRepresentation;
                }
            };
        }
    };

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends RowDecoder.Builder {
        protected SimpleDateFormat m_dateFormatter = new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
        protected BinaryEncoding m_binaryEncoding = BinaryEncoding.BASE64;
        protected String m_nullRepresentation = "NULL";
        protected TimeZone m_timeZone = TimeZone.getDefault();

        public Builder dateFormatter(String pattern) {
            m_dateFormatter = new SimpleDateFormat(pattern);
            return this;
        }

        public Builder dateFormatter(SimpleDateFormat dateFormatter) {
            m_dateFormatter = dateFormatter;
            return this;
        }

        public Builder timeZone(String timeZoneID) {
            return timeZone(TimeZone.getTimeZone(timeZoneID));
        }

        public Builder timeZone(TimeZone timeZone) {
            m_timeZone = timeZone;
            return this;
        }

        public Builder binaryEncoding(BinaryEncoding binaryEncoding) {
            m_binaryEncoding = binaryEncoding;
            return this;
        }

        public Builder nullRepresentation(String nullRepresentation) {
            m_nullRepresentation = nullRepresentation;
            return this;
        }

        public StringArrayDecoder build() {
            m_dateFormatter.setTimeZone(m_timeZone);
            return new StringArrayDecoder(
                    m_firstFieldOffset,
                    m_dateFormatter,
                    m_binaryEncoding,
                    m_nullRepresentation
                    );
        }
    }

    public static class DelegateBuilder extends RowDecoder.DelegateBuilder {
        private final Builder m_stringArrayBuilderDelegate;

        protected DelegateBuilder(Builder builder) {
            super(builder);
            m_stringArrayBuilderDelegate = builder;
        }

        protected DelegateBuilder(DelegateBuilder delegateBuilder) {
            super(delegateBuilder.getDelegateAs(Builder.class));
            m_stringArrayBuilderDelegate = delegateBuilder.getDelegateAs(Builder.class);
        }

        public DelegateBuilder dateFormatter(String pattern) {
            m_stringArrayBuilderDelegate.dateFormatter(pattern);
            return this;
        }

        public DelegateBuilder dateFormatter(SimpleDateFormat dateFormatter) {
            m_stringArrayBuilderDelegate.dateFormatter(dateFormatter);
            return this;
        }

        public DelegateBuilder timeZone(String timeZoneID) {
            m_stringArrayBuilderDelegate.timeZone(timeZoneID);
            return this;
        }

        public DelegateBuilder timeZone(TimeZone timeZone) {
            m_stringArrayBuilderDelegate.timeZone(timeZone);
            return this;
        }

        public DelegateBuilder binaryEncoding(BinaryEncoding binaryEncoding) {
            m_stringArrayBuilderDelegate.binaryEncoding(binaryEncoding);
            return this;
        }

        public DelegateBuilder nullRepresentation(String nullRepresentation) {
            m_stringArrayBuilderDelegate.nullRepresentation(nullRepresentation);
            return this;
        }

        @Override
        protected <TT extends RowDecoder.Builder> TT getDelegateAs(Class<TT> clazz) {
            return clazz.cast(m_stringArrayBuilderDelegate);
        }

    }
}
