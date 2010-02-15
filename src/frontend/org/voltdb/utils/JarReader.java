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

package org.voltdb.utils;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.jar.*;

/**
 * Read a text file from within a Jarfile and return the contents as
 * a string value. This is used by the Catalog deserialization
 * currently.
 *
 */
public class JarReader {

    protected final JarFile m_jarFile;
    protected final String m_jarPath;

    public JarReader(String jarPath) throws IOException {
        m_jarPath = jarPath;
        m_jarFile = new JarFile(m_jarPath);
    }

    /**
     * Read filename's contents from m_jarPath's jar file.
     * @param filename
     */
    public byte[] readFileFromJar(String filename) {
        return readFileFromJarAtURL(m_jarPath, filename);
    }

    /**
     * Read jar contents from an HTTP URL or a local file.
     * @param filename The path of the file within the jar to read.
     * @return An array of bytes representing the contents of the file.
     */
    static public byte[] readFileFromJarAtURL(String jarpath, String filename) {
        JarInputStream jarIn = JarReader.openJar(jarpath);
        if (jarIn == null) return (null);

        byte[] bytes = null;
        try {
            JarEntry catEntry = jarIn.getNextJarEntry();
            while ((catEntry != null) && (catEntry.getName().equals(filename) == false)) {
                catEntry = jarIn.getNextJarEntry();
            }
            if (catEntry == null) {
                return null;
            }

            int totalRead = 0;
            int maxToRead = 4096 << 10;
            byte buffer[] = new byte[maxToRead];
            bytes = new byte[maxToRead * 2];

            // Keep reading until we run out of bytes for this entry
            // We will resize our return value byte array if we run out of space
            while (jarIn.available() == 1) {
                int readSize = jarIn.read(buffer, 0, buffer.length);
                if (readSize > 0) {
                    totalRead += readSize;
                    if (totalRead > bytes.length) {
                        byte temp[] = new byte[bytes.length * 2];
                        System.arraycopy(bytes, 0, temp, 0, bytes.length);
                        bytes = temp;
                    }
                    System.arraycopy(buffer, 0, bytes, totalRead - readSize, readSize);
                }
            }

            // Trim bytes to proper size
            byte temp[] = new byte[totalRead];
            System.arraycopy(bytes, 0, temp, 0, totalRead);
            bytes = temp;
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return bytes;
    }

    /**
     *
     * @param jarpath
     * @return
     */
    static JarInputStream openJar(String jarpath) {
        URL jar_url = null;
        InputStream fin = null;
        JarInputStream jarIn = null;
        try {
            jar_url = new URL(jarpath);
            fin = jar_url.openStream();
        } catch (MalformedURLException ex) {
            // Invalid URL. Try as a file.
            try {
                fin = new FileInputStream(jarpath);
            } catch (FileNotFoundException e) {
                return null;
            }
        } catch (IOException ioex) {
            return null;
        }
        try {
            jarIn = new JarInputStream(fin);
        } catch (IOException ioex) {
            return null;
        }
        return jarIn;
    }

    /**
     * @return The list of paths of files stored in this Jar.
     */
    public List<String> getContentsFromJarfile() {
        JarInputStream jarIn = JarReader.openJar(m_jarPath);
        List<String> files = new ArrayList<String>();
        try {
            JarEntry catEntry = null;
            while ((catEntry = jarIn.getNextJarEntry()) != null) {
                files.add(catEntry.getName());
            }
        } catch (Exception ex) {
            return null;
        }
        return files;
    }

    /**
     * Read a text file from within a Jarfile and return the contents as
     * a string value. This is used by the Catalog deserialization
     * currently. Jarpath can be an HTTP URL or a local filename.
     *
     * @param jarpath Path to the jarfile
     * @param filename Filename within the jarfile
     * @return A string of the contents of the embedded file or null on failure.
     */
    public static String readFileFromJarfile(String jarpath, String filename) throws IOException {
        String retval = null;
        byte[] bytes = JarReader.readFileFromJarAtURL(jarpath, filename);
        retval = new String(bytes, 0, bytes.length, "UTF-8");
        return retval;
    }

    /**
     * Read a file from a jar in the form path/to/jar.jar!/path/to/file.ext
     */
    public static String readFileFromJarfile(String fulljarpath) throws IOException {
        assert (fulljarpath.contains(".jar!"));
        String[] paths = fulljarpath.split("!");
        if (paths[0].startsWith("file:"))
            paths[0] = paths[0].substring("file:".length());
        paths[1] = paths[1].substring(1);
        return JarReader.readFileFromJarfile(paths[0], paths[1]);
    }
}
