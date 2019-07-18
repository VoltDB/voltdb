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

package org.voltdb.plannerv2;

import java.util.Map;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.voltdb.catalog.Database;
import org.voltdb.plannerv2.sqlfunctions.VoltSqlFunctions;
import org.voltdb.plannerv2.sqlfunctions.VoltSqlFunctions.ScalarFunctionDescriptor;

/**
 * This is the common adapter that VoltDB should query any catalog object from.
 * It is built around the {@link Database} instance in sync
 * with any DDL operations. Taken/adapted from <b>Michael Alexeev</b>.
 * </br></br>
 * <b>NOTE</b> VoltDB creates a new Catalog/Database instance on every DDL statement.
 * In future, we might save some troubles by avoiding creating all catalog objects
 * (tables, views, indexes, etc.) from scratch upon a new DDL batch/statement.
 *
 * @see org.voltdb.compiler.VoltCompiler#loadSchema(org.hsqldb_voltpatches.HSQLInterface,
 *      org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad, String...)
 * @author Lukai Liu
 * @since 9.0
 */
public class VoltSchemaPlus {

    /**
     * Creates a brand new {@link SchemaPlus} instance following a VoltDB Database catalog.
     *
     * @param db the VoltDB database catalog object.
     */
    public static SchemaPlus from(Database db) {
        final SchemaPlus schema = CalciteSchema.createRootSchema(
                false /*no adding the metadata schema*/,
                false /*no caching*/, VoltFrameworkConfig.DEFAULT_SCHEMA_NAME).plus();

        // Get all tables from the database and add them to the SchemaPlus.
        db.getTables().forEach(table -> {
            schema.add(table.getTypeName(), new VoltTable(table));
        });

        // add Volt extend SQL functions to the SchemaPlus
        for (Map.Entry<Class, VoltSqlFunctions.FunctionDescriptor> function :
                VoltSqlFunctions.VOLT_SQL_FUNCTIONS.entries()) {
            switch(function.getValue().getType()){
                case SCALAR:
                    ScalarFunctionDescriptor scalarFunction = (ScalarFunctionDescriptor) function.getValue();
                    schema.add(scalarFunction.getFunctionName().toUpperCase(),
                            ScalarFunctionImpl.create(
                                    function.getKey(),
                                    scalarFunction.getFunctionName(),
                                    scalarFunction.isExactArgumentTypes(),
                                    scalarFunction.getFunctionId(),
                                    scalarFunction.getArgumentTypes()));
                    break;
                case AGGREGATE:
                    // TODO
                    break;
                default:
                    break;
            }
        }

        return schema;
    }
}
