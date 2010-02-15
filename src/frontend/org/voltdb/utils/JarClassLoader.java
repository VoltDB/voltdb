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
import java.util.*;
import java.util.jar.*;

public class JarClassLoader extends ClassLoader {
    final Map<String, Class<?>> m_cache = new HashMap<String, Class<?>>();
    final Set<String> m_classNames = new HashSet<String>();
    final String m_jarFilePath;

    public JarClassLoader(String jarFilePath) {
        m_jarFilePath = jarFilePath;
        loadAllClassNamesFromJar();
    }

    @Override
    public synchronized Class<?> loadClass(String className) throws ClassNotFoundException {
        System.out.printf("Loading %s... ", className);

        // try the fast cache first
        Class<?> result;
        if (m_cache.containsKey(className)) {
            System.out.println("found in cache.");
            return m_cache.get(className);
        }

        // now look through the list
        if (m_classNames.contains(className)) {
            String classPath = className.replace('.', File.separatorChar) + ".class";

            System.out.println("loading from jar.");

            byte bytes[] = JarReader.readFileFromJarAtURL(m_jarFilePath, classPath);
            if (bytes == null)
                throw new ClassNotFoundException(className);

            result = this.defineClass(className, bytes, 0, bytes.length);

            resolveClass(result);
            m_cache.put(className, result);
            return result;
        }

        // default to parent
        System.out.println("deferring to parent.");
        return getParent().loadClass(className);
    }

    void loadAllClassNamesFromJar() {
        JarInputStream jarIn = JarReader.openJar(m_jarFilePath);
        if (jarIn == null)
            throw new RuntimeException("Failed to locate jarfile.");

        try {
            for (JarEntry catEntry = jarIn.getNextJarEntry(); catEntry != null; catEntry = jarIn.getNextJarEntry()) {
                String classFileName = catEntry.getName();
                if (!classFileName.endsWith(".class"))
                    continue;
                String javaClassName = classFileName.replace(File.separatorChar, '.');
                javaClassName = javaClassName.substring(0, javaClassName.length() - 6);
                m_classNames.add(javaClassName);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
