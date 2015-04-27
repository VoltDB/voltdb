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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

import org.voltdb.utils.CatalogUtil;

public class DRCatalogDiffEngine extends CatalogDiffEngine {
    public DRCatalogDiffEngine(Catalog prev, Catalog next) {
        super(prev, next);
    }

    public static String serializeCatalogCommandsForDr(Database catalog) {
        StringBuilder sb = new StringBuilder();
        for (Table t : catalog.getTables()) {
            if (t.getIsdred() && t.getMaterializer() == null && !CatalogUtil.isTableExportOnly(catalog, t)) {
                t.writeCreationCommand(sb);
                t.writeFieldCommands(sb);
                t.writeChildCommands(sb);
            }
        }
        return sb.toString();
    }

    @Override
    protected String checkAddDropWhitelist(final CatalogType suspect, final ChangeType changeType) {
        if (ChangeType.ADDITION == changeType && suspect instanceof Table) {
            assert ((Boolean)suspect.getField("isDRed"));
            return "Missing DR table " + suspect.getTypeName() + " on replica cluster";
        }
        if (suspect instanceof Column || isUniqueIndex(suspect) || isUniqueIndexColumn(suspect) || isTableLimitDeleteStmt(suspect)) {
            return "Missing " + suspect + " from " + suspect.getParent() + " on " + (ChangeType.ADDITION == changeType ? "replica" : "master");
        }
        return null;
    }

    @Override
    protected String checkModifyWhitelist(final CatalogType suspect, final CatalogType prevType, final String field) {
        if (suspect instanceof Table) {
            if ("isdred".equalsIgnoreCase(field)) {
                assert ((Boolean)suspect.getField("isDRed"));
                return "Table " + suspect.getTypeName() + " has DR enabled on the master but not the replica";
            }
            if ("estimatedtuplecount".equals(field)) {
                return null;
            }
        } else if (suspect instanceof Column) {
            if ("defaultvalue".equals(field) ||
                "defaulttype".equals(field) ||
                "matview".equals(field) ||
                "aggregatetype".equals(field) ||
                "matviewsource".equals(field)) {
                return null;
            }
        } else if (isTableLimitDeleteStmt(suspect)) {
            if ("cost".equals(field) ||
                "seqscancount".equals(field) ||
                "explainplan".equals(field) ||
                "indexesused".equals(field) ||
                "cachekeyprefix".equals(field)) {
                return null;
            }
        } else if (isUniqueIndex(suspect) == isUniqueIndex(prevType) && !isUniqueIndexColumn(suspect)) {
            return null;
        }
        return "Incompatible schema between master and replica: field " + field + " in schema object " + suspect;
    }

    private boolean isUniqueIndexColumn(CatalogType suspect) {
        return suspect instanceof ColumnRef && isUniqueIndex(suspect.getParent());
    }

    private boolean isUniqueIndex(CatalogType suspect) {
        return suspect instanceof Index && (Boolean)suspect.getField("unique");
    }

    @Override
    protected String[] checkAddDropIfTableIsEmptyWhitelist(final CatalogType suspect, final ChangeType changeType) { return null; }

    @Override
    public String[] checkModifyIfTableIsEmptyWhitelist(final CatalogType suspect,
            final CatalogType prevType,
            final String field) { return null; }
}
