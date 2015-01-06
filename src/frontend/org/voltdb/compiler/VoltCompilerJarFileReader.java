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

package org.voltdb.compiler;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;

import org.voltdb.utils.InMemoryJarfile;

/**
 * Catalog compilation reader for individual jar file items.
 */
public class VoltCompilerJarFileReader extends VoltCompilerReader
{
    private final InMemoryJarfile m_jarFile;
    private final String m_path;
    private StringReader m_stringReader = null;
    private byte[] m_bytes = null;

    /**
     * @param jarFile  in-memory jar file
     * @param path  DDL "path" within in-memory jar file
     */
    public VoltCompilerJarFileReader(InMemoryJarfile jarFile, String path)
    {
        m_jarFile = jarFile;
        m_path = path;
    }

    /* (non-Javadoc)
     * @see java.io.Reader#read(char[], int, int)
     */
    @Override
    public int read(char[] cbuf, int off, int len) throws IOException
    {
        if (m_stringReader == null) {
            m_bytes = m_jarFile.get(m_path);
            if (m_bytes == null) {
                // A stubborn caller will get StringReader exceptions.
                m_stringReader = new StringReader("");
                throw new IOException(String.format(
                        "DDL file \"%s\" not found in in-memory jar file.",
                        m_path));
            }
            m_stringReader = new StringReader(new String(m_bytes, "UTF-8"));
        }
        return m_stringReader.read(cbuf, off, len);
    }

    /* (non-Javadoc)
     * @see java.io.Reader#close()
     */
    @Override
    public void close() throws IOException
    {
        m_stringReader = null;
    }

    @Override
    public String getName()
    {
        return new File(m_path).getName();
    }

    @Override
    public String getPath()
    {
        return m_path;
    }

    @Override
    public void putInJar(InMemoryJarfile jarFile, String name) throws IOException
    {
        jarFile.put(name, m_bytes);
    }
}
