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

import java.io.PrintWriter;
import java.io.StringWriter;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;
import java.util.List;
import org.voltdb.VoltType;

public class CSVStringDecoder extends RowDecoder<String, RuntimeException> {

    protected final StringArrayDecoder m_stringArrayDecoder;
    protected final CSVWriter m_writer;
    protected final StringBuffer m_writerDestination;

    protected CSVStringDecoder(StringArrayDecoder stringArrayDecoder) {
        super(stringArrayDecoder);
        m_stringArrayDecoder = stringArrayDecoder;
        StringWriter sw = new StringWriter(2048);
        m_writerDestination = sw.getBuffer();
        PrintWriter pw = new PrintWriter(sw, true);
        //We use no line ending CSVWriter so that no extra data appears after CSV fields.
        m_writer = new CSVWriter(pw, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER, "");
    }

    @Override
    public String decode(long generation, String tableName, List<VoltType> types, List<String> names, String ignoreIt, Object[] fields) throws RuntimeException {
        m_writer.writeNext(m_stringArrayDecoder.decode(generation, tableName, types, names, null,fields));
        String csvLine = m_writerDestination.toString();
        m_writerDestination.setLength(0);
        return csvLine;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends StringArrayDecoder.DelegateBuilder {
        protected final StringArrayDecoder.Builder m_delegateBuilder;

        public Builder() {
            super(new StringArrayDecoder.Builder());
            m_delegateBuilder = getDelegateAs(StringArrayDecoder.Builder.class);
            m_delegateBuilder.nullRepresentation("NULL");
        }

        public CSVStringDecoder build() {
            return new CSVStringDecoder(m_delegateBuilder.build());
        }
    }
}
