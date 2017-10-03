/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltType;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Predicates;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableList;
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
    protected final Map<String,DecodeType> m_typeMap;
    protected final int m_firstFieldOffset;

    /**
     * Constructors are protected because inner Builder classes have
     * the responsibility to construct decoders
     *
     * @param columnTypes a list of column field types
     * @param columnNames a list of column names
     * @param firstFieldOffset field offset at which the decoder converts field values
     */
    protected RowDecoder(List<VoltType> columnTypes, List<String> columnNames, int firstFieldOffset) {

        Preconditions.checkArgument(
                columnTypes != null && !columnTypes.isEmpty(),
                "column types is null or empty"
                );
        Preconditions.checkArgument(
                columnNames != null && !columnNames.isEmpty(),
                "column names is null or empty"
                );
        Preconditions.checkArgument(
                columnTypes.size() == columnNames.size(),
                "column names and types differ in size"
                );
        Preconditions.checkArgument(
                !FluentIterable.from(columnTypes).anyMatch(Predicates.isNull()),
                "column types has null elements"
                );
        Preconditions.checkArgument(
                !FluentIterable.from(columnNames).anyMatch(Predicates.isNull()),
                "column names has null elements"
                );
        Preconditions.checkArgument(
                firstFieldOffset == 0 || firstFieldOffset == INTERNAL_FIELD_COUNT,
                "specified invalid value for firstFieldOffset"
                );
        Preconditions.checkArgument(
                firstFieldOffset < columnTypes.size(),
                "first field offset is larger then specified number of columns"
                );
        m_firstFieldOffset = firstFieldOffset;

        ImmutableMap.Builder<String, DecodeType> mb = ImmutableMap.builder();
        Iterator<String> nameItr =
                columnNames.subList(firstFieldOffset, columnNames.size()).iterator();
        Iterator<VoltType> typeItr =
                columnTypes.subList(firstFieldOffset, columnTypes.size()).iterator();
        while (nameItr.hasNext() && typeItr.hasNext()) {
            mb.put(nameItr.next(), DecodeType.forType(typeItr.next()));
        }
        m_typeMap = mb.build();
    }

    /**
     * Copy constructor
     *
     * @param src the source decoder to copy from
     */
    protected <TT,EE extends Exception> RowDecoder(RowDecoder<TT,EE> src) {
        m_typeMap = ImmutableMap.<String,DecodeType>copyOf(src.m_typeMap);
        m_firstFieldOffset = src.m_firstFieldOffset;
    }

    public void close() throws E {
    }

    /**
     * It converts an exported volt row of values into a target type
     *
     * @param to may be used as an accumulator (byte buffers, lists, maps)
     * @param fields and array of objects containing values from an exported row
     * @return the conversion target type
     * @throws E the exception that this conversion may incur
     */
    public abstract T decode(T to, Object[] fields) throws E;

    /**
     * Responsible to build and instantiate row decoders.
     */
    public static abstract class Builder {
        protected List<VoltType> m_columnTypes = null;
        protected List<String> m_columnNames = null;
        protected int m_firstFieldOffset = INTERNAL_FIELD_COUNT;

        /**
         * Helps when converting column names to their camel case form
         * i.e. PRINCIPAL_ADDRESS becomes PrincipalAddress
         */
        protected final static Function<String, String> camelCaseNameUpperFirst = new Function<String, String>() {
            @Override
            public final String apply(String input) {
                return Builder.convertToCamelCase(input, true);
            }
        };

        /**
         * Helps when converting column names to their camel case form
         * i.e. PRINCIPAL_ADDRESS becomes principalAddress
         */
        protected final static Function<String, String> camelCaseNameLowerFirst = new Function<String, String>() {
            @Override
            public final String apply(String input) {
                return Builder.convertToCamelCase(input, false);
            }
        };

        public static String convertToCamelCase(CharSequence input, boolean upperCaseFirst) {
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

        public Builder columnNames(List<String> columnNames) {
            m_columnNames = columnNames;
            return this;
        }

        public Builder columnTypes(List<VoltType> columnTypes) {
            m_columnTypes = columnTypes;
            return this;
        }

        public Builder columnTypeMap(LinkedHashMap<String, VoltType> map) {
            m_columnNames = ImmutableList.copyOf(map.keySet());
            m_columnTypes = ImmutableList.copyOf(map.values());
            return this;
        }

        public Builder skipInternalFields(boolean skipThem) {
            m_firstFieldOffset = skipThem ? INTERNAL_FIELD_COUNT : 0;
            return this;
        }

        protected Builder use(Builder other) {
            m_columnNames = ImmutableList.copyOf(other.m_columnNames);
            m_columnTypes = ImmutableList.copyOf(other.m_columnTypes);
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
        public DelegateBuilder columnTypes(List<VoltType> columnTypes) {
            m_delegateBuilder.columnTypes(columnTypes);
            return this;
        }
        @Override
        public DelegateBuilder columnNames(List<String> columnNames) {
            m_delegateBuilder.columnNames(columnNames);
            return this;
        }

        @Override
        public DelegateBuilder columnTypeMap(LinkedHashMap<String, VoltType> map) {
            m_delegateBuilder.columnTypeMap(map);
            return this;
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
