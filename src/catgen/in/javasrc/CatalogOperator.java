/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
 * @since 9.0
 */
public class CatalogOperator {

    static {
        NativeLibraryLoader.loadCatalogAPIs();
    }

    final Catalog m_catalog;

    public CatalogOperator(Catalog catalog) {
        m_catalog = catalog;
    }

    /**
     * Get a catalog item using a path string.
     * @param path the absolute path to the wanted node.
     * @return the catalog item at the specified path.
     */
    native CatalogType getItemForPath(String path);

    /**
     * Run one or more single-line catalog commands separated by newlines.
     * See the docs for more info on catalog statements.
     * @param commands a string containing one or more catalog commands
     * separated by newlines.
     */
    public native void execute(final String commands);
}
