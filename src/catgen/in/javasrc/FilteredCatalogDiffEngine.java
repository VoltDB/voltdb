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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

public class FilteredCatalogDiffEngine extends CatalogDiffEngine {

    public FilteredCatalogDiffEngine(Catalog prev, Catalog next) {
        super(prev, next);
        // TODO Auto-generated constructor stub
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
                // Since Epoch dates and other possible fields may be different
                // Schema will also be different
                return true;
        }
        return false;
    }

    /**
     * @return true if this addition may be ignored
     */
    @Override
    protected boolean checkAddIgnoreList(final CatalogType suspect)
    {
        if (super.checkAddIgnoreList(suspect)) {
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
