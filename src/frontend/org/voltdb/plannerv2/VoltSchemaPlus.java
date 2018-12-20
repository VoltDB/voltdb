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

package org.voltdb.plannerv2;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.voltdb.catalog.Database;
import org.voltdb.plannerv2.rel.VoltTable;

/**
 * This is the common adapter that VoltDB should query any catalog object from.
 * It is built around the <code>org.voltdb.catalog.Database</code> instance in sync
 * with any DDL operations. Taken/adapted from Mike Alexeev.
 *
 * NOTE that VoltDB creates a new Catalog/Database instance on every DDL statement.
 * (See <code>org.voltdb.compiler.VoltCompiler.loadSchema()</code>.
 * In future, we might save some troubles by avoiding creating all catalog objects
 * (tables, views, indexes, etc.) from scratch upon a new DDL batch/statement.
 *
 * @author Lukai Liu
 * @since 8.4
 */
public class VoltSchemaPlus {

    /**
     * Creates a brand new SchemaPlus instance upon new VoltDB Database catalog.
     * @param db the VoltDB database catalog object.
     */
    public static SchemaPlus from(Database db) {
        final SchemaPlus schema = CalciteSchema.createRootSchema(false /*no adding the metadata schema*/,
                                                                 false /*no caching*/, "catalog").plus();
        // Get all tables from the database
        db.getTables().forEach(table -> {
            schema.add(table.getTypeName(), new VoltTable(table));
            // TODO: Get all functions, etc. from database
        });
        return schema;
    }
}
