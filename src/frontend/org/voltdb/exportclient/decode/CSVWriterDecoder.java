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

import java.io.IOException;
import java.util.List;

import org.voltdb.VoltType;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

public class CSVWriterDecoder extends RowDecoder<CSVWriter, IOException> {

    protected final StringArrayDecoder m_stringArrayDecoder;

    protected CSVWriterDecoder(StringArrayDecoder stringArrayDecoder) {
        super(stringArrayDecoder);
        m_stringArrayDecoder = stringArrayDecoder;
    }

    @Override
    public CSVWriter decode(long generation, String tableName, List<VoltType> types, List<String> names, CSVWriter to, Object[] fields) throws IOException {
        to.writeNext(m_stringArrayDecoder.decode(generation, tableName, types, names, null, fields));
        return to;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends StringArrayDecoder.DelegateBuilder {
        protected final StringArrayDecoder.Builder m_stringArrayBuilder;

        public Builder() {
            super(new StringArrayDecoder.Builder());
            m_stringArrayBuilder = super.getDelegateAs(StringArrayDecoder.Builder.class);
            m_stringArrayBuilder.nullRepresentation("NULL");
        }

        protected Builder(Builder delegateBuilder) {
            super(delegateBuilder.getDelegateAs(StringArrayDecoder.Builder.class));
            m_stringArrayBuilder = delegateBuilder.getDelegateAs(StringArrayDecoder.Builder.class);
        }

        public CSVWriterDecoder build() {
            return new CSVWriterDecoder(m_stringArrayBuilder.build());
        }
    }

    public static class DelegateBuilder extends StringArrayDecoder.DelegateBuilder {
        protected final Builder m_csvWriterBuilderDelegate;

        protected DelegateBuilder(Builder delegateBuilder) {
            super(delegateBuilder);
            m_csvWriterBuilderDelegate = delegateBuilder;
        }

        protected DelegateBuilder(DelegateBuilder delegateBuilder) {
            super(delegateBuilder.getDelegateAs(Builder.class));
            m_csvWriterBuilderDelegate = delegateBuilder.getDelegateAs(Builder.class);
        }

        @Override
        protected <TT extends RowDecoder.Builder> TT getDelegateAs(Class<TT> clazz) {
            return clazz.cast(m_csvWriterBuilderDelegate);
        }

    }
}
