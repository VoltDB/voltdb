/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.io.*;

/**
 * Create an <code>InputStream</code> from a Java string so it can
 * be used where <code>InputStream</code>s are needed, like in XML parsers.
 *
 */
public class StringInputStream extends InputStream {
    StringReader sr;

    /**
     * Create the stream from a string.
     * @param value The string value to turn into an InputStream.
     */
    public StringInputStream(String value) { sr = new StringReader(value); }

    /**
     * Read a single byte from the stream (string).
     */
    public int read() throws IOException { return sr.read(); }
}
