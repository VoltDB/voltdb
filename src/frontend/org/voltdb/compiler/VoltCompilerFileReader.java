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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.voltdb.utils.InMemoryJarfile;

/**
 * VoltCompiler file-based reader.
 */
public class VoltCompilerFileReader extends VoltCompilerReader
{
    private final File m_file;
    private FileReader m_fileReader;

    /**
     * @param path  file path
     */
    public VoltCompilerFileReader(String path) throws IOException
    {
        m_file = new File(path);
        try {
            m_fileReader = new FileReader(m_file);
        }
        catch (FileNotFoundException e) {
            throw new IOException(String.format("Unable to open \"%s\" for reading", path));
        }
    }

    /* (non-Javadoc)
     * @see java.io.Reader#read(char[], int, int)
     */
    @Override
    public int read(char[] cbuf, int off, int len) throws IOException
    {
        return m_fileReader.read(cbuf, off, len);
    }

    /* (non-Javadoc)
     * @see java.io.Reader#close()
     */
    @Override
    public void close() throws IOException
    {
        if (m_fileReader != null) {
            m_fileReader.close();
            m_fileReader = null;
        }
    }

    @Override
    public String getName()
    {
        return m_file.getName();
    }

    @Override
    public String getPath()
    {
        return m_file.getPath();
    }

    @Override
    public void putInJar(InMemoryJarfile jarFile, String name) throws IOException
    {
        jarFile.put(name, m_file);
    }

    /**
     * Get the path of a schema file, optionally relative to a project.xml file's path.
     */
    static String getSchemaPath(String projectFilePath, String path) throws IOException
    {
        File file = null;

        if (path.contains(".jar!")) {
            String ddlText = null;
            ddlText = VoltCompilerUtils.readFileFromJarfile(path);
            file = VoltProjectBuilder.writeStringToTempFile(ddlText);
        }
        else {
            file = new File(path);
        }

        if (!file.isAbsolute()) {
            // Resolve schemaPath relative to either the database definition xml file
            // or the working directory.
            if (projectFilePath != null) {
                file = new File(new File(projectFilePath).getParent(), path);
            }
            else {
                file = new File(path);
            }
        }

        return file.getPath();
    }
}
