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

import static java.lang.Character.isLowerCase;
import static java.lang.Character.toLowerCase;
import static java.lang.Character.toUpperCase;
import static org.voltdb.exportclient.ExportDecoderBase.INTERNAL_FIELD_COUNT;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltType;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Base class for export row decoders whose function is to convert
 * a volt row of values into a target type. This class allows writing
 * decoders via composition. For example one decoder may convert a
 * row of objects into an array of string values, while another decoder
 * may use the array of strings to create a map, or a CSV string
 *
 * @param <T> the type to which an array of volt field values is converted to
 * @param <E> the exception that conversion may incur
 */
public abstract class RowDecoder<T, E extends Exception> {

    /**
     * column name to decode type map. Entries order are important
     * as they reflect the column name and type layout of volt
     * table row
     */
    private final Map<Long, Map<String, DecodeType>> m_generationTypeMap = new HashMap<>();
    protected final int m_firstFieldOffset;

    /**
     * Constructors are protected because inner Builder classes have
     * the responsibility to construct decoders
     *
     * @param firstFieldOffset field offset at which the decoder converts field values
     */
    protected RowDecoder(int firstFieldOffset) {
        m_firstFieldOffset = firstFieldOffset;
    }

    protected Map<String, DecodeType> getTypeMap(long generation, List<VoltType> columnTypes, List<String> columnNames) {
        Map<String,DecodeType> typeMap = m_generationTypeMap.get(generation);
        if (typeMap != null) {
            return typeMap;
        }
        ImmutableMap.Builder<String, DecodeType> mb = ImmutableMap.builder();
        Iterator<String> nameItr =
                columnNames.subList(m_firstFieldOffset, columnNames.size()).iterator();
        Iterator<VoltType> typeItr =
                columnTypes.subList(m_firstFieldOffset, columnTypes.size()).iterator();
        while (nameItr.hasNext() && typeItr.hasNext()) {
            mb.put(nameItr.next(), DecodeType.forType(typeItr.next()));
        }
        typeMap = mb.build();
        m_generationTypeMap.put(generation, typeMap);
        return typeMap;
    }

    /**
     * Copy constructor
     *
     * @param src the source decoder to copy from
     */
    protected <TT,EE extends Exception> RowDecoder(RowDecoder<TT,EE> src) {
        m_firstFieldOffset = src.m_firstFieldOffset;
    }

    public void close() throws E {
    }

    /**
     * It converts an exported volt row of values into a target type
     *
     * @param generation generation id of the row getting decoded
     * @param tableName table name to which this row belongs to
     * @param types list of column types of the row, in order
     * @param names list of column names of the row, in order
     * @param to may be used as an accumulator (byte buffers, lists, maps)
     * @param fields and array of objects containing values from an exported row
     * @return the conversion target type
     * @throws E the exception that this conversion may incur
     */
    public abstract T decode(long generation, String tableName, List<VoltType> types, List<String> names, T to, Object[] fields) throws E;

    /**
     * Responsible to build and instantiate row decoders.
     */
    public static abstract class Builder {
        protected int m_firstFieldOffset = INTERNAL_FIELD_COUNT;

        /**
         * Helps when converting column names to their camel case form
         * i.e. PRINCIPAL_ADDRESS becomes PrincipalAddress
         */
        public final static Function<String, String> camelCaseNameUpperFirst = new Function<String, String>() {
            @Override
            public final String apply(String input) {
                return Builder.convertToCamelCase(input, true);
            }
        };

        /**
         * Helps when converting column names to their camel case form
         * i.e. PRINCIPAL_ADDRESS becomes principalAddress
         */
        public final static Function<String, String> camelCaseNameLowerFirst = new Function<String, String>() {
            @Override
            public final String apply(String input) {
                return Builder.convertToCamelCase(input, false);
            }
        };

        private static String convertToCamelCase(CharSequence input, boolean upperCaseFirst) {
            StringBuilder cName = new StringBuilder(input);

            boolean upperCaseIt = upperCaseFirst;
            boolean hasLowerCase = false;

            for (int i = 0; i < cName.length();) {
                char chr = cName.charAt(i);

                if (chr == '_' || chr == '.' || chr == '$') {

                    cName.deleteCharAt(i);
                    upperCaseIt = true;
                    hasLowerCase = false;

                } else {

                    if (upperCaseIt) {
                        chr = toUpperCase(chr);
                    } else if (!(hasLowerCase = hasLowerCase || isLowerCase(chr))) {
                        chr = toLowerCase(chr);
                    }
                    cName.setCharAt(i++, chr);
                    upperCaseIt = false;

                }
            }

            return cName.toString();
        }

        public Builder skipInternalFields(boolean skipThem) {
            m_firstFieldOffset = skipThem ? INTERNAL_FIELD_COUNT : 0;
            return this;
        }

        protected Builder use(Builder other) {
            m_firstFieldOffset = other.m_firstFieldOffset;
            return this;
        }
    }

    public static class DelegateBuilder extends RowDecoder.Builder {
        private final RowDecoder.Builder m_delegateBuilder;

        protected DelegateBuilder(RowDecoder.Builder delegateBuilder) {
            m_delegateBuilder = delegateBuilder;
        }


        @Override
        public DelegateBuilder skipInternalFields(boolean skipThem) {
            m_delegateBuilder.skipInternalFields(skipThem);
            return this;
        }

        protected <TT extends RowDecoder.Builder> TT getDelegateAs(Class<TT> clazz) {
            return clazz.cast(m_delegateBuilder);
        }
    }

}
