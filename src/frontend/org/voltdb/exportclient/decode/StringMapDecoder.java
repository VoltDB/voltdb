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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltType;

import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;

public class StringMapDecoder extends RowDecoder<Map<String,String>, RuntimeException>{

    protected final StringArrayDecoder m_stringArrayDecoder;
    protected final String [] m_columnNames;

    protected StringMapDecoder(StringArrayDecoder stringArrayDecoder) {
        super(stringArrayDecoder);
        m_stringArrayDecoder = stringArrayDecoder;
        m_columnNames = ImmutableList.copyOf(m_typeMap.keySet()).toArray(new String[0]);
    }

    @Override
    public Map<String,String> decode(Map<String,String> to, Object[] fields) throws RuntimeException {
        ImmutableMap.Builder<String,String> mb = null;
        if (to == null) {
            mb = ImmutableMap.builder();
        }
        String [] strings = m_stringArrayDecoder.decode(null, fields);
        for (int i = 0; i < strings.length && i < m_columnNames.length; ++i) {
            if (mb != null) {
                mb.put(m_columnNames[i], strings[i]);
            } else {
                to.put(m_columnNames[i], strings[i]);
            }
        }
        return mb != null ? mb.build() : to;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends StringArrayDecoder.DelegateBuilder {
        protected final StringArrayDecoder.Builder m_delegateBuilder;

        public Builder() {
            super(new StringArrayDecoder.Builder());
            m_delegateBuilder = getDelegateAs(StringArrayDecoder.Builder.class);
            m_delegateBuilder.nullRepresentation("");
        }

        @Override
        public DelegateBuilder columnNames(List<String> columnNames) {
            return super.columnNames(FluentIterable.from(columnNames).transform(camelCaseNameLowerFirst).toList());
        }

        @Override
        public DelegateBuilder columnTypeMap(LinkedHashMap<String, VoltType> map) {
            columnNames(ImmutableList.copyOf(map.keySet()));
            return super.columnTypes(ImmutableList.copyOf(map.values()));
        }

        public StringMapDecoder build() {
            return new StringMapDecoder(m_delegateBuilder.build());
        }
    }
}
