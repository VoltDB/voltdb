/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltCompiler;

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

    // A class that can be used to read from a JarInputStream where the size of entries
    // is not known.  Can be reused multiple times to avoid excessive memory allocations.
    private static class JarInputStreamReader {

        // A best guess at an upper bound for the size of entries in
        // jar files, since we do not know the uncompressed size of
        // the entries ahead of time.  This number is used to size the
        // buffers used to read from jar streams.
        private static final int JAR_ENTRY_SIZE_GUESS = 1024 * 1024; // 1MB

        private byte[] m_array;

        JarInputStreamReader() {
            m_array = new byte[JAR_ENTRY_SIZE_GUESS];
        }

        byte[] readEntryFromStream(JarInputStream jarIn) throws IOException {
            int totalRead = 0;

            while (jarIn.available() == 1) {
                int bytesToRead = m_array.length - totalRead;
                assert (bytesToRead > 0);
                int readSize = jarIn.read(m_array, totalRead, bytesToRead);
                if (readSize > 0) {
                    totalRead += readSize;
                    // If we have filled up our buffer and there is
                    // still more to read...
                    if (readSize == bytesToRead && (jarIn.available() == 1)) {
                        // Make a new array double the size, and copy what we've read so far
                        // in there.
                        byte[] tmpArray = new byte[2 * m_array.length];
                        System.arraycopy(m_array, 0, tmpArray, 0, totalRead);
                        m_array = tmpArray;
                    }
                }
            }

            byte trimmedBytes[] = new byte[totalRead];
            System.arraycopy(m_array, 0, trimmedBytes, 0, totalRead);
            return trimmedBytes;
        }
    }

    public static byte[] readFromJarEntry(JarInputStream jarIn) throws IOException {
        JarInputStreamReader reader = new JarInputStreamReader();
        return reader.readEntryFromStream(jarIn);
    }

    private void loadFromStream(InputStream in) throws IOException {
        JarInputStream jarIn = new JarInputStream(in);
        JarEntry catEntry = null;
        JarInputStreamReader reader = new JarInputStreamReader();
        while ((catEntry = jarIn.getNextJarEntry()) != null) {
            byte[] value = reader.readEntryFromStream(jarIn);
            String key = catEntry.getName();
            put(key, value);
        }
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

    // This method should be able to be killed and all usage replaced with
    // getSha1Hash, in theory.  We serialize this to pass it between the master
    // and replica for DR, so there's probably some extra work to do beyond
    // just replacing one method call with another, though.
    public long getCRC() {

        PureJavaCrc32 crc = new PureJavaCrc32();

        for (Entry<String, byte[]> e : super.entrySet()) {
            if (e.getKey().equals("buildinfo.txt") || e.getKey().equals("catalog-report.html")) {
                continue;
            }
            // Hacky way to skip the first line of the autogenerated ddl, which
            // has a date which changes and causes test failures
            if (e.getKey().equals(VoltCompiler.AUTOGEN_DDL_FILE_NAME)) {
                byte[] ddlbytes = e.getValue();
                int index = 0;
                while (ddlbytes[index] != '\n') {
                    index++;
                }
                byte[] newddlbytes = Arrays.copyOfRange(ddlbytes, index, ddlbytes.length);
                crc.update(e.getKey().getBytes(Constants.UTF8ENCODING));
                crc.update(newddlbytes);
            }
            else {
                crc.update(e.getKey().getBytes(Constants.UTF8ENCODING));
                crc.update(e.getValue());
            }
        }

        return crc.getValue();
    }

    public byte[] getSha1Hash() {

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            VoltDB.crashLocalVoltDB("Bad JVM has no SHA-1 hash.", true, e);
        }

        for (Entry<String, byte[]> e : super.entrySet()) {
            if (e.getKey().equals("buildinfo.txt") || e.getKey().equals("catalog-report.html")) {
                continue;
            }
            // Hacky way to skip the first line of the autogenerated ddl, which
            // has a date which changes and causes test failures
            if (e.getKey().equals(VoltCompiler.AUTOGEN_DDL_FILE_NAME)) {
                byte[] ddlbytes = e.getValue();
                int index = 0;
                while (ddlbytes[index] != '\n') {
                    index++;
                }
                byte[] newddlbytes = Arrays.copyOfRange(ddlbytes, index, ddlbytes.length);
                md.update(e.getKey().getBytes(Constants.UTF8ENCODING));
                md.update(newddlbytes);
            }
            else {
                md.update(e.getKey().getBytes(Constants.UTF8ENCODING));
                md.update(e.getValue());
            }
        }

        return md.digest();
    }

    public byte[] put(String key, File value) throws IOException {
        byte[] bytes = null;

        int bytesRead = 0;
        bytes = new byte[(int) value.length()];
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(value));
        try {
            bytesRead = in.read(bytes);
        }
        finally {
            in.close();
        }
        assert(bytesRead != -1);

        return put(key, bytes);
    }

    private String fileToClassName(String filename)
    {
        return filename.replace(File.separatorChar, '.').substring(0, filename.length() - ".class".length());
    }

    private String classToFileName(String classname)
    {
        return classname.replace('.', File.separatorChar) + ".class";
    }

    /**
     * Remove the provided classname and all inner classes from the jarfile and the classloader
     */
    public void removeClassFromJar(String classname)
    {
        for (String innerclass : getLoader().getInnerClassesForClass(classname)) {
            remove(classToFileName(innerclass));
        }
        remove(classToFileName(classname));
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
            m_classNames.add(fileToClassName(key));
        }

        void noteRemoved(String key) {
            if (!key.endsWith(".class"))
                return;
            m_classNames.remove(fileToClassName(key));
            m_cache.remove(fileToClassName(key));
        }

        // prevent this from being publicly called
        JarLoader() {}

        /**
         * @return The InMemoryJarFile instance owning this loader.
         */
        public InMemoryJarfile getInMemoryJarfile() {
            return InMemoryJarfile.this;
        }

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
                String classPath = classToFileName(className);

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

        /**
         * For a given class, find all
         */
        public String[] getInnerClassesForClass(String className) {
            List<String> matches = new ArrayList<>();
            for (String potential : m_classNames) {
                if (potential.startsWith(className + "$")) {
                    matches.add(potential);
                }
            }
            return matches.toArray(new String[0]);
        }

        public Set<String> getClassNames() {
            return m_classNames;
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
