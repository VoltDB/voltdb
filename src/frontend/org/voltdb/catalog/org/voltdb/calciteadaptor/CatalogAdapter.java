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
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

public class CatalogAdapter {
    // NOTE: VoltDB creates a new Catalog/Database instance on every DDL stmt. See VoltCompiler.loadSchema()
    public static SchemaPlus schemaPlusFromDatabase(Database db) {
        // addMetadataSchema = false; cache = true
        final SchemaPlus rootSchema =
                CalciteSchema.createRootSchema(false, false, db.getSchema()).plus();
        // Get all tables from database
        db.getTables().forEach(table -> rootSchema.add(table.getTypeName(), new VoltDBTable(table)));
        // Get all indexes from database
        return rootSchema;
    }

}
