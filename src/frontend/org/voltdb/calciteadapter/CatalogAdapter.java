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

package org.voltdb.calciteadapter;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.voltdb.calciteadapter.rel.VoltTable;
import org.voltdb.catalog.Database;

/**
 * This is the common adapter that VoltDB should query any catalog object from.
 * It is built around the <code>org.voltdb.catalog.Database</code> instance in sync
 * with any DDL operations. Taken/adapted from Mike A.
 *
 * NOTE that VoltDB creates a new Catalog/Database instance on every DDL stmt. (See the
 * <code>org.voltdb.compiler.VoltCompiler.loadSchema</code> method. In future, we
 * might save some troubles by avoiding creating all catalog objects (tables, views, indexes,
 * etc.) from scratch upon a new DDL batch/stmt.
 *
 * @author Lukai Liu
 * @since 8.4
 */
public class CatalogAdapter {

    /**
     * Creates a brand new SchemaPlus instance upon new VoltDB Database catalog.
     * CalciteSchema is capable of caching (CachingCalciteSchema), but we disabled it for now to
     * use SimpleCalciteSchema since the {@link org.apache.calcite.schema.SchemaPlus} instance is
     * forced to be refreshed upon every new DDL batch/stmt.
     */
    public static SchemaPlus schemaPlusFromDatabase(Database db) {
        final SchemaPlus rootSchema =
                CalciteSchema.createRootSchema(false /*no adding the metadata schema*/,
                                               false /*no caching*/, "catalog").plus();
        // Get all tables from the database
        db.getTables().forEach(table -> {
            rootSchema.add(table.getTypeName(), new VoltTable(table));
            // TODO: Get all functions, etc. from database
        });
        return rootSchema;
    }

}
