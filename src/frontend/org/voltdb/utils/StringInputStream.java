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
import java.io.InputStream;

import org.voltdb.common.Constants;

/**
 * Create an <code>InputStream</code> from a Java string so it can
 * be used where <code>InputStream</code>s are needed, like in XML parsers.
 *
 */
public class StringInputStream extends InputStream {
    final byte[] m_strBytes;
    int m_position = 0;

    /**
     * Create the stream from a string.
     * @param value The string value to turn into an InputStream.
     */
    public StringInputStream(String value) {
        m_strBytes = value.getBytes(Constants.UTF8ENCODING);
    }

    /**
     * Read a single byte from the stream (string).
     */
    @Override
    public int read() throws IOException {
        if (m_position == m_strBytes.length)
            return -1;
        return m_strBytes[m_position++];
    }
}
