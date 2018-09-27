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

package org.voltdb.catalog.org.voltdb.calciteadaptor;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.voltdb.calciteadapter.rel.VoltDBIndex;
import org.voltdb.calciteadapter.rel.VoltDBMatViewInfo;
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.catalog.Database;


/**
 * This is the common adaptor that VoltDB should query any catalog object from.
 * It is built around the <code>org.voltdb.catalog.Database</code> instance in sync
 * with any DDL operations. Taken/adapted from Mike A.
 *
 * NOTE that VoltDB creates a new Catalog/Database instance on every DDL stmt. (See
 * <code>org.voltdb.compiler.VoltCompiler.loadSchema</code> method. In future, we
 * might save some trouble by avoid creating all catalog objects (tables, views, indexes,
 * etc.) from scratch upon a new DDL batch/stmt.
 */
public class CatalogAdapter {
    /**
     * Creates a brand new SchemaPlus instance upon new VoltDB Database catalog.
     *
     * CalciteSchema is capable of caching; but we disable it for now since
     * <code>org.apache.calcite.schema.SchemaPlus</code> instance is forced
     * refreshed upon every new DDL batch/stmt.
     */
    public static SchemaPlus schemaPlusFromDatabase(Database db) {
        final SchemaPlus rootSchema =
                CalciteSchema.createRootSchema(false, false, db.getSchema()).plus();
        // Get all tables from database
        db.getTables().forEach(table -> {
            rootSchema.add(table.getTypeName(), new VoltDBTable(table));
            // Lift all table-associated catalog objects to global visible level, might be convenient for planning use.
            table.getIndexes().forEach(index -> rootSchema.add(index.getTypeName(), new VoltDBIndex(index)));
            table.getViews().forEach(mv -> rootSchema.add(mv.getTypeName(), new VoltDBMatViewInfo(mv)));
            // TODO: Get all functions, etc. from database
        });
        return rootSchema;
    }

}
