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

package org.voltdb.plannerv2.utils;

import java.util.stream.StreamSupport;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.PlanningErrorException;

/**
 * Utilities that serve the package only.
 */
abstract class CalciteUtils {

    static void except(String msg) {
        throw new PlanningErrorException(msg, 0);
    }

    static void exceptWhen(boolean when, String format, Object... args) {
        if (when) {
            throw new PlanningErrorException(String.format(format, args), 0);
        }
    }

    /**
     * Add a table present in previous database version to current database.
     * We need this weird piece of logic because, in executing "CREATE INDEX indx ON t(i, j, k)",
     * current database does not have any table in ad-hoc mode (it does in batch mode); but HSQL node
     * is a create-table node with the new index added, and we would create the target table from scratch,
     * via DDLCompiler.addTableToCatalog() method.
     * @param prevDb old database to find other related dependencies
     * @param currentDb new database to check and add target table to.
     * @param table target table found in old database version.
     */
    static Table addTableToDatabase(Database prevDb, Database currentDb, Table table) {
        if (table == null) {
            return null;
        } else {
            final String tableName = table.getTypeName();
            Table targetTable = currentDb.getTables().get(tableName);
            if (targetTable == null) {
                final Table copy = currentDb.getTables().add(tableName);
                table.copyFields(copy);
                // When table is a mat view, also clone materializer.
                final Table materializer = table.getMaterializer();
                copy.setMaterializer(addTableToDatabase(prevDb, currentDb, materializer));
                if (materializer != null) {
                    // Pull in all views' (children) whose materializer is the target table into current database.
                    StreamSupport.stream(((Iterable<MaterializedViewInfo>)
                            materializer.getViews()::iterator).spliterator(), false)
                            .map(MaterializedViewInfo::getTypeName)
                            .forEach(tblName ->
                                    addTableToDatabase(prevDb, currentDb, prevDb.getTables().get(tblName)));
                }
                final Column partCol = table.getPartitioncolumn();
                if (partCol != null) {
                    final Column copyPartCol = copy.getColumns().get(partCol.getTypeName());
                    copyPartCol.setNullable(false);
                    copy.setPartitioncolumn(copyPartCol);
                    copy.setIsreplicated(false);
                }
                assert table.getMaterializer() == null ||
                        table.getMaterializer().getViews().size() == copy.getMaterializer().getViews().size();
                StreamSupport.stream(((Iterable<Column>)table.getColumns()::iterator).spliterator(), false)
                        .forEach(col -> {
                            final Column copyColumn = copy.getColumns().get(col.getTypeName());
                            final Column mvSource = col.getMatviewsource();
                            if (mvSource != null) { // column from a different table
                                copyColumn.setMatviewsource(
                                        copy.getMaterializer().getColumns().get(mvSource.getTypeName()));
                            }
                            final MaterializedViewInfo info = col.getMatview();
                            if (info != null) {
                                copyColumn.setMatview(new MaterializedViewInfo());
                                info.copyFields(copyColumn.getMatview());
                            }
                        });
                targetTable = copy;
            } // otherwise, the named table already exists in target database.
            return targetTable;
        }
    }

    static void migrateAllTables(Database prevDb, Database currentDb) {
        StreamSupport.stream(((Iterable<Table>) prevDb.getTables()::iterator).spliterator(), false)
                .forEach(tbl -> addTableToDatabase(prevDb, currentDb, tbl));
    }
}
