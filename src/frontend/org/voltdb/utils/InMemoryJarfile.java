/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32;

/**
 * Given a jarfile, construct a map of entry name => byte array representing
 * the contents. Allow it to be modified and written out in flexible ways.
 *
 */
public class InMemoryJarfile extends TreeMap<String, byte[]> {

    private static final long serialVersionUID = 1L;
    protected final JarLoader m_loader = new JarLoader();;

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

    public InMemoryJarfile(byte[] bytes) throws IOException {
        loadFromStream(new ByteArrayInputStream(bytes));
    }

    private void loadFromStream(InputStream in) throws IOException {
        JarInputStream jarIn = new JarInputStream(in);
        JarEntry catEntry = null;
        while ((catEntry = jarIn.getNextJarEntry()) != null) {
            byte[] value = readFromJarEntry(jarIn, catEntry);
            String key = catEntry.getName();
            put(key, value);
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

    public Runnable writeToFile(File file) throws IOException {
        final FileOutputStream output = new FileOutputStream(file);
        writeToOutputStream(output);
        return new Runnable() {
            @Override
            public void run() {
                try {
                    output.getFD().sync();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    try {
                        output.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

    public byte[] getFullJarBytes() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeToOutputStream(output);
        output.close();
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

        jarOut.finish();
    }

    ///////////////////////////////////////////////////////
    // UTILITY
    ///////////////////////////////////////////////////////

    public long getCRC() throws IOException {

        PureJavaCrc32 crc = new PureJavaCrc32();

        for (Entry<String, byte[]> e : super.entrySet()) {
            if (e.getKey().equals("buildinfo.txt")) {
                continue;
            }
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
    // CLASSLOADING
    ///////////////////////////////////////////////////////

    public class JarLoader extends ClassLoader {
        final Map<String, Class<?>> m_cache = new HashMap<String, Class<?>>();
        final Set<String> m_classNames = new HashSet<String>();

        void noteUpdated(String key) {
            if (!key.endsWith(".class"))
                return;
            String javaClassName = key.replace(File.separatorChar, '.');
            javaClassName = javaClassName.substring(0, javaClassName.length() - 6);
            m_classNames.add(javaClassName);
        }

        void noteRemoved(String key) {
            if (!key.endsWith(".class"))
                return;
            String javaClassName = key.replace(File.separatorChar, '.');
            javaClassName = javaClassName.substring(0, javaClassName.length() - 6);
            m_classNames.remove(javaClassName);
            m_cache.remove(javaClassName);
        }

        // prevent this from being publicly called
        JarLoader() {}

        @Override
        public synchronized Class<?> loadClass(String className) throws ClassNotFoundException {
            // try the fast cache first
            Class<?> result;
            if (m_cache.containsKey(className)) {
                //System.out.println("found in cache.");
                return m_cache.get(className);
            }

            // now look through the list
            if (m_classNames.contains(className)) {
                String classPath = className.replace('.', File.separatorChar) + ".class";

                byte bytes[] = get(classPath);
                if (bytes == null)
                    throw new ClassNotFoundException(className);

                result = this.defineClass(className, bytes, 0, bytes.length);

                resolveClass(result);
                m_cache.put(className, result);
                return result;
            }

            // default to parent
            //System.out.println("deferring to parent.");
            return getParent().loadClass(className);
        }
    }

    public JarLoader getLoader() {
        return m_loader;
    }

    ///////////////////////////////////////////////////////
    // OVERRIDDEN TREEMAP OPERATIONS
    ///////////////////////////////////////////////////////

    @Override
    public byte[] put(String key, byte[] value) {
        if (value == null)
            throw new RuntimeException("InMemoryJarFile cannon contain null entries.");
        byte[] retval = super.put(key, value);
        m_loader.noteUpdated(key);
        return retval;
    }

    @Override
    public void putAll(Map<? extends String, ? extends byte[]> m) {
        for (Entry<? extends String, ? extends byte[]> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public byte[] remove(Object key) {
        String realKey = null;
        try {
            realKey = (String) key;
        }
        catch (Exception e) {
            return null;
        }

        m_loader.noteRemoved(realKey);
        return super.remove(key);
    }

    @Override
    public void clear() {
        for (String key : keySet())
            m_loader.noteRemoved(key);
        super.clear();
    }

    @Override
    public Object clone() {
        throw new UnsupportedOperationException();
    }

    @Override
    public java.util.Map.Entry<String, byte[]> pollFirstEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public java.util.Map.Entry<String, byte[]> pollLastEntry() {
        throw new UnsupportedOperationException();
    }
}
