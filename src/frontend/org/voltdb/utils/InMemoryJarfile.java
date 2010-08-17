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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.CRC32;

/**
 * Given a jarfile, construct a map of entry name => byte array representing
 * the contents. Allow it to be modified and written out in flexible ways.
 *
 */
public class InMemoryJarfile extends TreeMap<String, byte[]> {

    private static final long serialVersionUID = 1L;

    ///////////////////////////////////////////////////////
    // CONSTRUCTION
    ///////////////////////////////////////////////////////

    public InMemoryJarfile() {}

    public InMemoryJarfile(String pathOrURL) throws IOException {
        InputStream fin = null;
        try {
            URL url = new URL(pathOrURL);
            fin = url.openStream();
        } catch (MalformedURLException ex) {
            // Invalid URL. Try as a file.
            fin = new FileInputStream(pathOrURL);
        }
        loadFromStream(fin);
    }

    public InMemoryJarfile(URL url) throws IOException {
        loadFromStream(url.openStream());
    }

    public InMemoryJarfile(File file) throws IOException {
        loadFromStream(new FileInputStream(file));
    }

    private void loadFromStream(InputStream in) throws IOException {
        JarInputStream jarIn = new JarInputStream(in);
        JarEntry catEntry = null;
        while ((catEntry = jarIn.getNextJarEntry()) != null) {
            byte[] value = readFromJarEntry(jarIn, catEntry);
            String key = catEntry.getName();
            super.put(key, value);
        }
    }

    public static byte[] readFromJarEntry(JarInputStream jarIn, JarEntry entry) throws IOException {
        int totalRead = 0;
        int maxToRead = 4096 << 10;
        byte[] buffer = new byte[maxToRead];
        byte[] bytes = new byte[maxToRead * 2];

        // Keep reading until we run out of bytes for this entry
        // We will resize our return value byte array if we run out of space
        while (jarIn.available() == 1) {
            int readSize = jarIn.read(buffer, 0, buffer.length);
            if (readSize > 0) {
                totalRead += readSize;
                if (totalRead > bytes.length) {
                    byte[] temp = new byte[bytes.length * 2];
                    System.arraycopy(bytes, 0, temp, 0, bytes.length);
                    bytes = temp;
                }
                System.arraycopy(buffer, 0, bytes, totalRead - readSize, readSize);
            }
        }

        // Trim bytes to proper size
        byte retval[] = new byte[totalRead];
        System.arraycopy(bytes, 0, retval, 0, totalRead);
        return retval;
    }

    ///////////////////////////////////////////////////////
    // OUTPUT
    ///////////////////////////////////////////////////////

    public void writeToFile(File file) throws IOException {
        FileOutputStream output = new FileOutputStream(file);
        writeToOutputStream(output);
    }

    public byte[] getFullJarBytes() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeToOutputStream(output);
        return output.toByteArray();
    }

    protected void writeToOutputStream(OutputStream output) throws IOException {
        JarOutputStream jarOut = new JarOutputStream(output);

        for (Entry<String, byte[]> e : super.entrySet()) {
            assert(e.getValue() != null);

            JarEntry entry = new JarEntry(e.getKey());
            entry.setSize(e.getValue().length);
            entry.setTime(System.currentTimeMillis());
            jarOut.putNextEntry(entry);
            jarOut.write(e.getValue());
            jarOut.flush();
            jarOut.closeEntry();
        }
    }

    ///////////////////////////////////////////////////////
    // UTILITY
    ///////////////////////////////////////////////////////

    public long getCRC() throws IOException {

        CRC32 crc = new CRC32();

        for (Entry<String, byte[]> e : super.entrySet()) {
            crc.update(e.getKey().getBytes("UTF-8"));
            crc.update(e.getValue());
        }

        return crc.getValue();
    }

    public byte[] put(String key, File value) throws IOException {
        byte[] bytes = null;

        bytes = new byte[(int) value.length()];
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(value));
        int bytesRead = in.read(bytes);
        assert(bytesRead != -1);

        return put(key, bytes);
    }

    ///////////////////////////////////////////////////////
    // OVERRIDDEN TREEMAP OPERATIONS
    ///////////////////////////////////////////////////////

    @Override
    public byte[] put(String key, byte[] value) {
        if (value == null)
            throw new RuntimeException("InMemoryJarFile cannon contain null entries.");
        return super.put(key, value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends byte[]> m) {
        for (Entry<? extends String, ? extends byte[]> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }
}
