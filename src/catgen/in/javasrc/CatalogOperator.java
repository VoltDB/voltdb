/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

import org.voltdb.NativeLibraryLoader;

/**
 * The operator class to make changes to the catalog.
 * @author Yiqun Zhang
 * @since 8.4
 */
public class CatalogOperator {

    static {
        NativeLibraryLoader.loadCatalogAPIs();
    }

    static final char MAP_SEPARATOR = '#';

    final Catalog m_catalog;

    private CatalogType m_prevUsedPath = null;

    public CatalogOperator(Catalog catalog) {
        m_catalog = catalog;
    }

    /**
     * Get a catalog item using a path string.
     * @param path the absolute path to the wanted node.
     * @return the catalog item at the specified path.
     */
    CatalogType getItemForPath(String path) {
        CatalogType retval = m_catalog.m_pathCache.getIfPresent(path);
        if (retval != null) {
            return retval;
        }

        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == -1) {
            return getItemForPathPart(m_catalog, path);
        }

        // The recursive case
        String immediateParentPath = path.substring(0, lastSlash);
        String subPath = path.substring(lastSlash);
        CatalogType immediateParent = getItemForPath(immediateParentPath);
        if (immediateParent == null) {
            return null;
        }
        // Cache all parents
        m_catalog.m_pathCache.put(immediateParentPath, immediateParent);

        return getItemForPathPart(immediateParent, subPath);
    }

    /**
     * Get a catalog item under a given parent node using a path string.
     * @param parent the parent node.
     * @param path the path from the parent node to the wanted node.
     * @return the catalog item at the specified path under the parent node.
     */
    private static CatalogType getItemForPathPart(CatalogType parent, String path) {
        if (path.length() == 0) {
            return parent;
        }

        boolean hasStartSlash = path.charAt(0) == '/';
        // The path is just "/"
        if ((path.length() == 1) && hasStartSlash) {
            return parent;
        }
        int mapSeparatorIndex = path.lastIndexOf(MAP_SEPARATOR);
        assert(mapSeparatorIndex > 0);

        String collectionPath = path.substring(hasStartSlash ? 1 : 0, mapSeparatorIndex);
        String entryName = path.substring(mapSeparatorIndex + 1, path.length());

        return parent.getCollection(collectionPath).get(entryName);
    }

    /**
     * Run one or more single-line catalog commands separated by newlines.
     * See the docs for more info on catalog statements.
     * @param commands a string containing one or more catalog commands
     * separated by newlines.
     */
    public native void execute(final String commands);
}
