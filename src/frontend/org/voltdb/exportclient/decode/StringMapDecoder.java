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

import java.util.List;
import java.util.Map;

import org.voltdb.VoltType;

import com.google_voltpatches.common.collect.ImmutableMap;

public class StringMapDecoder extends RowDecoder<Map<String,String>, RuntimeException>{

    protected final StringArrayDecoder m_stringArrayDecoder;

    protected StringMapDecoder(StringArrayDecoder stringArrayDecoder) {
        super(stringArrayDecoder);
        m_stringArrayDecoder = stringArrayDecoder;
    }

    @Override
    public Map<String,String> decode(long generation, String tableName, List<VoltType> types, List<String> names, Map<String,String> to, Object[] fields) throws RuntimeException {
        ImmutableMap.Builder<String,String> mb = null;
        if (to == null) {
            mb = ImmutableMap.builder();
        }
        String [] strings = m_stringArrayDecoder.decode(generation, tableName, types, names, null, fields);
        int i = 0;
        for (String name : names) {
            if (mb != null) {
                mb.put(name, strings[i]);
            } else {
                to.put(name, strings[i]);
            }
            i++;
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

        public StringMapDecoder build() {
            return new StringMapDecoder(m_delegateBuilder.build());
        }
    }
}
