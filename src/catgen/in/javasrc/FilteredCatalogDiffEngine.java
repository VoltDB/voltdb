/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

/*
 * This class provides additional filtering to the CatalogDiffEngine. This class
 * is used to verify that Generated DDL is consistent with the catalog that it was
 * generated from. However, some elements in the two catalogs such as the schema or
 * the export entries may be different due to ordering of DDL statements or catalog
 * state not processed with DDL statements. This filter class is used to provide the
 * incremental filtering to allow two subtly different catalogs to be considered identical.
 */
public class FilteredCatalogDiffEngine extends CatalogDiffEngine {

    public FilteredCatalogDiffEngine(Catalog prev, Catalog next, boolean forceVerbose) {
        super(prev, next, forceVerbose);
    }

    /**
     * @return true if this change may be ignored
     */
    @Override
    protected boolean checkModifyIgnoreList(final CatalogType suspect,
                                            final CatalogType prevType,
                                            final String field)
    {
        if (super.checkModifyIgnoreList(suspect, prevType, field))
            return true;
        if (suspect instanceof Database) {
            if ("schema".equals(field))
                // Since the schema field is generated in the order of the DDL commands,
                // the generated DDL will have a different command order and therefore
                // a different schema string.
                return true;
        }
        return false;
    }

    /**
     * @return true if this delete may be ignored
     */
    @Override
    protected boolean checkDeleteIgnoreList(final CatalogType prevType,
                                            final CatalogType newlyChildlessParent,
                                            final String mapName,
                                            final String name)
    {
        if (super.checkDeleteIgnoreList(prevType, newlyChildlessParent, mapName, name)) {
            return true;
        }
        if ("connectors".equals(mapName)) {
            // Export specific catalog elements do not affect the DDL
            return true;
        }
        return false;
    }

}
