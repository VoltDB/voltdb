/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

/**
 * VoltCompiler DDL file-based reader.
 */
public class VoltCompilerDDLFileReader extends VoltCompilerFileReader
{
    /**
     * @param path  ddl file path
     * @param projectFilePath  optional path to project.xml
     */
    public VoltCompilerDDLFileReader(String path, String projectFilePath) throws IOException
    {
        super(getSchemaPath(projectFilePath, path));
    }

    private static String getSchemaPath(String projectFilePath, String path) throws IOException
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
