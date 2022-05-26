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

package org.voltdb.importer.formatter.builtin;

import java.io.IOException;
import java.util.Properties;

import org.voltdb.common.Constants;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;

import au.com.bytecode.opencsv_voltpatches.CSVParser;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class VoltCSVFormatter implements Formatter {
    final CSVParser m_parser;

    public VoltCSVFormatter (String formatName, Properties prop) {

        if (!("csv".equalsIgnoreCase(formatName) || "tsv".equalsIgnoreCase(formatName))) {
            throw new IllegalArgumentException("Invalid format " + formatName + ", choices are either \"csv\" or \"tsv\".");
        }
        char separator = "csv".equalsIgnoreCase(formatName) ? ',' : '\t';

        String separatorProp = prop.getProperty("separator", "");
        if (!separatorProp.isEmpty() && separatorProp.length() == 1) {
            separator = separatorProp.charAt(0);
        }

        char quotechar = CSVParser.DEFAULT_QUOTE_CHARACTER;
        String quoteCharProp = prop.getProperty("quotechar", "");
        if (!quoteCharProp.isEmpty() && quoteCharProp.length() == 1) {
            quotechar = quoteCharProp.charAt(0);
        }

        char escape = CSVParser.DEFAULT_ESCAPE_CHARACTER;
        String escapeProp = prop.getProperty("escape", "");
        if (!escapeProp.isEmpty() && escapeProp.length() == 1) {
            escape = escapeProp.charAt(0);
        }

        boolean strictQuotes = CSVParser.DEFAULT_STRICT_QUOTES;
        String strictQuotesProp = prop.getProperty("strictquotes", "");
        if (!strictQuotesProp.isEmpty()) {
            strictQuotes = Boolean.parseBoolean(strictQuotesProp);
        }

        boolean ignoreLeadingWhiteSpace = CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE;
        String ignoreLeadingWhiteSpaceProp = prop.getProperty("ignoreleadingwhitespace", "");
        if (!ignoreLeadingWhiteSpaceProp.isEmpty()) {
            ignoreLeadingWhiteSpace = Boolean.parseBoolean(ignoreLeadingWhiteSpaceProp);
        }

        m_parser = new CSVParser(separator, quotechar, escape, strictQuotes, ignoreLeadingWhiteSpace);
    }

    @Override
    public Object[] transform(ByteBuffer payload) throws FormatException {
        String line = null;
        try {
            if (payload == null) {
                return null;
            }
            line = new String(payload.array(), payload.arrayOffset(), payload.limit(), StandardCharsets.UTF_8);
            Object list[] = m_parser.parseLine(line);
            if (list != null) {
                for (int i = 0; i < list.length; i++) {
                    if ("NULL".equals(list[i])
                            || Constants.CSV_NULL.equals(list[i])
                            || Constants.QUOTED_CSV_NULL.equals(list[i])) {
                        list[i] = null;
                    }
                }
            }
            return list;
        } catch (IOException e) {
            throw new FormatException("failed to format " + line, e);
        }
    }
}
