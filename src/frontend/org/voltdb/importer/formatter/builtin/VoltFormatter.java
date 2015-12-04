/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import org.voltdb.importer.formatter.AbstractFormatter;
import org.voltdb.importer.formatter.FormatException;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public class VoltFormatter implements AbstractFormatter {
    String m_format;

    @Override
    public void configure(Properties p) {
        m_format = p.getProperty("format", "").trim();
        if (!("csv".equalsIgnoreCase(m_format) || "tsv".equalsIgnoreCase(m_format))) {
            throw new RuntimeException("Invalid format " + m_format + ", choices are either \"csv\" or \"tsv\".");
        }
    };

    @Override
    public Object[] transform(Object b) throws FormatException {
        if (b == null || !b.getClass().equals(String.class))
            throw new FormatException("Transform takes an argument of type String.");
        String line = (String) b;
        try {
            Object list[] = new CSVParser("csv".equalsIgnoreCase(m_format) ? ',' : '\t').parseLine(line);
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
            throw new FormatException(e);
        }
    }
}
