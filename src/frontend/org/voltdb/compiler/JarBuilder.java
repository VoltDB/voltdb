/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.compiler;

import java.io.*;
import java.util.*;
import java.util.jar.*;

import org.voltdb.compiler.VoltCompiler.VoltCompilerException;

/**
 * Builds jar files given paths and arrays of bytes.
 *
 * <br/><br/>Key features:
 * <ul>
 * <li>Integrated with the VoltDB build system.</li>
 * <li>Builds a map of paths to byte arrays in memory.</li>
 * <li>Ignores duplicate byte arrays for the same path.</li>
 * <li>Throws exceptions if byte arrays don't match for the same path.</li>
 * <li>Only writes to disk in one go.</li>
 * </ul>
 */
public class JarBuilder {

    HashMap<String, byte[]> dataInJar = new HashMap<String, byte[]>();
    VoltCompiler m_compiler;

    /**
     * Initialize with a reference to the current compiler.
     * @param compiler Current VoltCompiler instance used for error collection.
     */
    JarBuilder(VoltCompiler compiler) {
        this.m_compiler = compiler;
    }

    /**
     * Queue an entry to be added to the jarfile being built.
     * @param key The in-jar path to the resource being stored.
     * @param bytes The bytes representing the object being stored.
     * @throws VoltCompiler.VoltCompilerException Throws an exception
     * if the object being stored will overwrite a different object.
     */
    public void addEntry(String key, byte[] bytes)
    throws VoltCompiler.VoltCompilerException {
        byte[] existing = dataInJar.get(key);
        if (existing != null) {
            if (existing.equals(bytes))
                return;
            String msg = "Tring to put the same content in a jar file twice.";
            throw m_compiler.new VoltCompilerException(msg);
        }
        dataInJar.put(key, bytes);
    }

    public void addEntry(String key, File file) throws VoltCompilerException {
        byte[] bytes = null;

        try {
            bytes = new byte[(int) file.length()];
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
            int bytesRead = in.read(bytes);
            assert(bytesRead != -1);

        } catch (FileNotFoundException e) {
            String msg = "JarBuilder can't find file: " + file.getName();
            throw m_compiler.new VoltCompilerException(msg);
        } catch (IOException e) {
            String msg = "IO Exception reading file: " + file.getName();
            throw m_compiler.new VoltCompilerException(msg);
        }

        addEntry(key, bytes);
    }

    /**
     * Write the in-memory jar data structure to a real jarfile in a specific location.
     * <br/>Note: This method can be repeatedly called.
     * @param path The destination path on disk for the jarfile.
     * @throws VoltCompiler.VoltCompilerException Throws and exception when there
     * is a file i/o error.
     */
    public void writeJarToDisk(String path)
    throws VoltCompiler.VoltCompilerException {
        FileOutputStream output = null;
        try {
            output = new FileOutputStream(path);
        } catch (FileNotFoundException e) {
            String msg = "Unable to open destination jarfile for writing";
            throw m_compiler.new VoltCompilerException(msg);
        }

        JarOutputStream jarOut = null;
        try {
            jarOut = new JarOutputStream(output);
        } catch (IOException e) {
            String msg = "Error writing to destination jarfile";
            throw m_compiler.new VoltCompilerException(msg);
        }

        for (String key : dataInJar.keySet()) {
            byte[] bytes = dataInJar.get(key);
            assert(bytes != null);

            JarEntry entry = new JarEntry(key);
            try {
                entry.setSize(bytes.length);
                jarOut.putNextEntry(entry);
                jarOut.write(bytes);
                jarOut.flush();
                jarOut.closeEntry();
            } catch (IOException e) {
                String msg = "Unable to write file: " + key + " to destination jarfile";
                throw m_compiler.new VoltCompilerException(msg);
            }
        }

        try {
            jarOut.close();
        } catch (IOException e) {
            String msg = "Error finishing writing destination jarfile to disk";
            throw m_compiler.new VoltCompilerException(msg);
        }
    }

}
