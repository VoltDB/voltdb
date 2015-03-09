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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.InMemoryJarfile.JarLoader;

public class VoltCompilerUtils
{
    /**
     * Read a file from a jar in the form path/to/jar.jar!/path/to/file.ext
     */
    static String readFileFromJarfile(String fulljarpath) throws IOException {
        assert (fulljarpath.contains(".jar!"));

        String[] paths = fulljarpath.split("!");
        if (paths[0].startsWith("file:"))
            paths[0] = paths[0].substring("file:".length());
        paths[1] = paths[1].substring(1);

        return readFileFromJarfile(paths[0], paths[1]);
    }

    static String readFileFromJarfile(String jarfilePath, String entryPath) throws IOException {
        InputStream fin = null;
        try {
            URL jar_url = new URL(jarfilePath);
            fin = jar_url.openStream();
        } catch (MalformedURLException ex) {
            // Invalid URL. Try as a file.
            fin = new FileInputStream(jarfilePath);
        }
        JarInputStream jarIn = new JarInputStream(fin);

        JarEntry catEntry = jarIn.getNextJarEntry();
        while ((catEntry != null) && (catEntry.getName().equals(entryPath) == false)) {
            catEntry = jarIn.getNextJarEntry();
        }
        if (catEntry == null) {
            jarIn.close();
            return null;
        }

        byte[] bytes = InMemoryJarfile.readFromJarEntry(jarIn, catEntry);

        return new String(bytes, "UTF-8");
    }

    public static boolean addClassToJar(InMemoryJarfile jarOutput, final Class<?> cls)
            throws IOException
    {
        String packagePath = cls.getName();
        packagePath = packagePath.replace('.', '/');
        packagePath += ".class";

        String realName = cls.getName();
        realName = realName.substring(realName.lastIndexOf('.') + 1);
        realName += ".class";

        byte [] classBytes = null;
        try {
            classBytes = getClassAsBytes(cls);
        } catch (Exception e) {
            final String msg = "Unable to locate classfile for " + realName;
            throw new IOException(msg);
        }

        jarOutput.put(packagePath, classBytes);
        return true;
    }

    public static byte[] getClassAsBytes(final Class<?> c) throws IOException {

        ClassLoader cl = c.getClassLoader();
        if (cl == null) {
            cl = Thread.currentThread().getContextClassLoader();
        }

        String classAsPath = c.getName().replace('.', '/') + ".class";

        if (cl instanceof JarLoader) {
            InMemoryJarfile memJar = ((JarLoader) cl).getInMemoryJarfile();
            return memJar.get(classAsPath);
        }
        else {
            BufferedInputStream   cis = null;
            ByteArrayOutputStream baos = null;
            try {
                cis  = new BufferedInputStream(cl.getResourceAsStream(classAsPath));
                baos =  new ByteArrayOutputStream();

                byte [] buf = new byte[1024];

                int rsize = 0;
                while ((rsize=cis.read(buf)) != -1) {
                    baos.write(buf, 0, rsize);
                }

            } finally {
                try { if (cis != null)  cis.close();}   catch (Exception ignoreIt) {}
                try { if (baos != null) baos.close();}  catch (Exception ignoreIt) {}
            }

            return baos.toByteArray();
        }
    }

    public static InMemoryJarfile createClassesJar(Class<?>... classes) throws IOException {
        InMemoryJarfile jarMem = new InMemoryJarfile();
        for (Class<?> cls : classes) {
            addClassToJar(jarMem, cls);
        }
        return jarMem;
    }
}
