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

import java.net.URL;
import java.net.URLClassLoader;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Need a custom class loader so that user classes can be found, either when
 * the catalog is being compiled for the first time and a classpath is specified
 * or when recompiling an existing catalog to upgrade the version. In the latter
 * case the classes have already been loaded into an in-memory file and should
 * be resolved from there.
 */
class VoltCompilerClassLoader extends URLClassLoader
{
    public VoltCompilerClassLoader(URL... userURLs)
    {
        super(initializeURLs(userURLs));
    }

    private static URL[] initializeURLs(URL[] userURLs)
    {
        URL[] systemURLs = ((URLClassLoader) ClassLoader.getSystemClassLoader()).getURLs();
        return ArrayUtils.addAll(systemURLs, userURLs);
    }
}