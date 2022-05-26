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

package org.voltdb.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

/**
 * CSV output formatter for SQLCommand.
 */
class SQLCommandOutputFormatterCSV implements SQLCommandOutputFormatter
{
    class OutputStreamWriter extends Writer
    {
        OutputStream m_stream;

        OutputStreamWriter(OutputStream stream)
        {
            m_stream = stream;
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException
        {
            m_stream.write(new String(cbuf, off, len).getBytes());
        }

        @Override
        public void flush() throws IOException
        {
            m_stream.flush();
        }

        @Override
        public void close() throws IOException
        {
            m_stream.close();
        }
    }

    @Override
    public void printTable(PrintStream stream, VoltTable t, boolean includeColumnNames)
            throws IOException
    {
        final int columnCount = t.getColumnCount();
        List<VoltType> columnTypes = new ArrayList<VoltType>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            columnTypes.add(t.getColumnType(i));
        }
        CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(stream));
        if (includeColumnNames) {
            String[] columnNames = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnNames[i] = t.getColumnName(i);
            }
            csvWriter.writeNext(columnNames);
        }
        VoltTableUtil.toCSVWriter(csvWriter, t, columnTypes);
    }
}
