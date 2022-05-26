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

package org.voltdb.compiler;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;

import org.voltdb.utils.InMemoryJarfile;

/**
 * Used for providing fixed DDL text without involving the filesystem.
 */
public class VoltCompilerStringReader extends VoltCompilerReader
{
    private final String m_path;
    private final String m_text;
    private final StringReader m_reader;

    public VoltCompilerStringReader(final String path, final String text)
    {
        m_path = path;
        m_text = text;
        m_reader = new StringReader(m_text);
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
        jarFile.put(name, m_text.getBytes());
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException
    {
        return m_reader.read(cbuf, off, len);
    }

    @Override
    public void close() throws IOException
    {
        m_reader.close();
    }
}
