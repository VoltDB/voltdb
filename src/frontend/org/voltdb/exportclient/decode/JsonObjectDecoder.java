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

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.exportclient.decode.DecodeType.SimpleVisitor;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.collect.FluentIterable;

public class JsonObjectDecoder extends RowDecoder<JSONObject, JSONException> {

    final protected SimpleDateFormat m_dateFormatter =
            new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
    protected final String [] m_columnNames;
    protected final JsonFieldDecoder [] m_fieldDecoders;

    protected JsonObjectDecoder(List<VoltType> columnTypes,
            List<String> columnNames, int firstFieldOffset,
            TimeZone timeZone) {
        super(columnTypes, columnNames, firstFieldOffset);

        m_dateFormatter.setTimeZone(timeZone);
        m_columnNames = new String[m_typeMap.size()];
        m_fieldDecoders = new JsonFieldDecoder[m_typeMap.size()];

        int i = 0;
        for (Entry<String, DecodeType> e: m_typeMap.entrySet()) {
            final String columnName = e.getKey().intern();
            m_columnNames[i] = columnName;
            m_fieldDecoders[i++] = e.getValue()
                    .accept(decodingVisitor, columnName, null)
                    ;
        }
    }

    @Override
    public JSONObject decode(JSONObject to, Object[] fields)
            throws JSONException {
        if (to == null) {
            to = new JSONObject();
        }
        for (
                int i = m_firstFieldOffset, j = 0;
                i < fields.length && j < m_fieldDecoders.length;
                ++i, ++j
        ) {
            m_fieldDecoders[j].decode(to, fields[i]);
        }
        return to;
    }

    static abstract class JsonFieldDecoder implements FieldDecoder<JSONObject, JSONException> {
        protected final String m_fieldName;

        JsonFieldDecoder(String fieldName) {
            m_fieldName = fieldName;
        }
    }

    final SimpleVisitor<JsonFieldDecoder, String> decodingVisitor =
            new SimpleVisitor<JsonFieldDecoder, String>() {

        JsonFieldDecoder defaultDecoder(String p) {
            return new JsonFieldDecoder(p) {
                @Override
                public final void decode(JSONObject to, Object v) throws JSONException {
                    to.put(m_fieldName, v);
                }
            };
        }

        @Override
        public JsonFieldDecoder visitTinyInt(String p, Object v) {
            return new JsonFieldDecoder(p) {
                @Override
                public final void decode(JSONObject to, Object o)
                        throws JSONException {
                    Integer v = o != null ? Byte.valueOf((byte)o).intValue() : null;
                    to.put(m_fieldName, v);
                }
            };
        }

        @Override
        public JsonFieldDecoder visitSmallInt(String p, Object v) {
            return new JsonFieldDecoder(p) {
                @Override
                public final void decode(JSONObject to, Object o) throws JSONException {
                    Integer v = o != null ? Short.valueOf((short)o).intValue() : null;
                    to.put(m_fieldName, v);
                }
            };
        }

        @Override
        public JsonFieldDecoder visitInteger(String p, Object v) {
            return defaultDecoder(p);
        }

        @Override
        public JsonFieldDecoder visitBigInt(String p, Object v) {
            return defaultDecoder(p);
        }

        @Override
        public JsonFieldDecoder visitFloat(String p, Object v) {
            return defaultDecoder(p);
        }

        @Override
        public JsonFieldDecoder visitTimestamp(String p, Object v) {
            return new JsonFieldDecoder(p) {
                final SimpleDateFormat m_df = (SimpleDateFormat)m_dateFormatter.clone();
                @Override
                public final void decode(JSONObject to, Object v)
                        throws JSONException {
                    String formatted = null;
                    if (v != null) {
                        TimestampType ts = (TimestampType)v;
                        formatted = m_df.format(ts.asApproximateJavaDate());
                    }
                    to.put(m_fieldName, formatted);
                }
            };
        }

        @Override
        public JsonFieldDecoder visitString(String p, Object v) {
            return defaultDecoder(p);
        }

        @Override
        public JsonFieldDecoder visitVarBinary(String p, Object v) {
            return new JsonFieldDecoder(p) {
                @Override
                public final void decode(JSONObject to, Object v)
                        throws JSONException {
                    String encoded = null;
                    if (v != null) {
                        byte [] bytes = (byte[])v;
                        encoded = Encoder.base64Encode(bytes);
                    }
                    to.put(m_fieldName, encoded);
                }
            };
        }

        @Override
        public JsonFieldDecoder visitDecimal(String p, Object v) {
            return defaultDecoder(p);
        }

        @Override
        public JsonFieldDecoder visitGeographyPoint(String p, Object v) {
            return new JsonFieldDecoder(p) {
                @Override
                public final void decode(JSONObject to, Object v)
                        throws JSONException {
                    String encoded = null;
                    if (v != null) {
                        GeographyPointValue gpv = (GeographyPointValue)v;
                        encoded = gpv.toWKT();
                    }
                    to.put(m_fieldName, encoded);
                }
            };
        }

        @Override
        public JsonFieldDecoder visitGeography(String p, Object v) {
            return new JsonFieldDecoder(p) {
                @Override
                public final void decode(JSONObject to, Object v)
                        throws JSONException {
                    String encoded = null;
                    if (v != null) {
                        GeographyValue gv = (GeographyValue)v;
                        encoded = gv.toWKT();
                    }
                    to.put(m_fieldName, encoded);
                }
            };
        }
    };

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends RowDecoder.Builder {
        protected boolean m_camelCaseFieldNames = true;
        protected TimeZone m_timeZone = TimeZone.getDefault();

        public Builder camelCaseFieldNames(boolean doit) {
            m_camelCaseFieldNames = doit;
            return this;
        }

        public Builder timeZone(TimeZone tz) {
            if (tz != null) {
                m_timeZone = tz;
            }
            return this;
        }

        public JsonObjectDecoder build() {
            List<String> columnNames = m_columnNames;
            if (m_camelCaseFieldNames) {
                columnNames = FluentIterable.from(columnNames)
                      .transform(camelCaseNameLowerFirst)
                      .toList();
            }
            return new JsonObjectDecoder(
                    m_columnTypes, columnNames,
                    m_firstFieldOffset, m_timeZone
                    );
        }
    }

    public static class DelegateBuilder extends RowDecoder.DelegateBuilder {
        private final Builder m_jsonObjectBuilderDelegate;

        protected DelegateBuilder(Builder builder) {
            super(builder);
            m_jsonObjectBuilderDelegate = builder;
        }

        protected DelegateBuilder(DelegateBuilder delegateBuilder) {
            super(delegateBuilder.getDelegateAs(Builder.class));
            m_jsonObjectBuilderDelegate = delegateBuilder.getDelegateAs(Builder.class);
        }

        public DelegateBuilder camelCaseFieldNames(boolean doit) {
            m_jsonObjectBuilderDelegate.camelCaseFieldNames(doit);
            return this;
        }

        public DelegateBuilder timeZone(TimeZone tz) {
            m_jsonObjectBuilderDelegate.timeZone(tz);
            return this;
        }

        @Override
        protected <TT extends RowDecoder.Builder> TT getDelegateAs(Class<TT> clazz) {
            return clazz.cast(m_jsonObjectBuilderDelegate);
        }
    }
}
