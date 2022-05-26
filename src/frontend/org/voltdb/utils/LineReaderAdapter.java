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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * Wraps a reader into an SQLCommandLineReader. Can also keep track of line numbers.
 *
 * @author jcrump
 */
public class LineReaderAdapter implements SQLCommandLineReader {
    private final BufferedReader m_reader;
    private int m_lineNum = 0;

    @Override
    public int getLineNumber() {
        return m_lineNum;
    }

    public LineReaderAdapter(Reader reader) {
        m_reader = new BufferedReader(reader);
    }

    @Override
    public String readBatchLine() throws IOException {
        m_lineNum++;
        return m_reader.readLine();
    }

    public void close() {
        try {
            m_reader.close();
        } catch (IOException e) { }
    }
}
