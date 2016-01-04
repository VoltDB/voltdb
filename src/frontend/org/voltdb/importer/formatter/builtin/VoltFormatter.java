/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

public class VoltFormatter implements Formatter<String> {
    final CSVParser m_parser;

    VoltFormatter (String name, Properties prop) {
        if (!("csv".equalsIgnoreCase(name) || "tsv".equalsIgnoreCase(name))) {
            throw new IllegalArgumentException("Invalid format " + name + ", choices are either \"csv\" or \"tsv\".");
        }
        m_parser = new CSVParser("csv".equalsIgnoreCase(name) ? ',' : '\t');
    }

    @Override
    public Object[] transform(String sourceData) throws FormatException {
        try {
            Object list[] = m_parser.parseLine(sourceData);
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
            throw new FormatException("failed to format " + sourceData, e);
        }
    }
}
