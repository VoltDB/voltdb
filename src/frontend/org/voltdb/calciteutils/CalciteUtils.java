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

package org.voltdb.calciteutils;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.PlanningErrorException;

import java.util.Set;
import java.util.function.Predicate;

/**
 * Utilities that serve the package only.
 */
class CalciteUtils {
    private CalciteUtils() {}

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
     * @param db new database to check and add target table to.
     * @param table target table found in old database version.
     */
    static Table addTableToDatabase(Database db, Table table) {
        if (table == null) {
            return null;
        } else {
            final String tableName = table.getTypeName();
            if (db.getTables().get(tableName) == null) {
                final Table copy = db.getTables().add(tableName);
                table.copyFields(copy);
                // When table is a mat view, also clone materializer.
                copy.setMaterializer(addTableToDatabase(db, table.getMaterializer()));
                final Column partCol = table.getPartitioncolumn();
                if (partCol != null) {
                    copy.setPartitioncolumn(copy.getColumns().get(partCol.getTypeName()));
                }
                assert table.getMaterializer() == null ||
                        table.getMaterializer().getViews().size() == copy.getMaterializer().getViews().size();
                return copy;
            } else {    // the named table already exists in target database.
                return table;
            }
        }
    }

    /**
     * Collect all terminal expression types from an expression aggregate.
     * @param src src expression to collect from
     * @param pred whether current expression need to be collected.
     * @param collections collected expressions
     */
    static void collectSubExpressions(
            AbstractExpression src, Predicate<AbstractExpression> pred, Set<AbstractExpression> collections) {
        if (src != null) {
            if (pred.test(src)) {
                collections.add(src);
            }
            if (src.getLeft() != null) {
                collectSubExpressions(src.getLeft(), pred, collections);
            }
            if (src.getRight() != null) {
                collectSubExpressions(src.getRight(), pred, collections);
            }
            if (src.getArgs() != null) {
                src.getArgs().forEach(arg -> collectSubExpressions(arg, pred, collections));
            }
        }
    }
}
