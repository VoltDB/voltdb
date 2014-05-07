/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 */

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
