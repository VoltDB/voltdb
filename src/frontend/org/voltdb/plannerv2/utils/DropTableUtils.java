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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.plannerv2.VoltFastSqlParser;
import org.voltdb.utils.Encoder;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Helper utility to execute a DROP TABLE query.
 */
public class DropTableUtils {
    private DropTableUtils() {}

    /**
     * Convert a SQL statement to a SQLNode, ignoring any Calcite parse errors.
     * Eventually, when Calcite is patched to be fully compatible with VoltDB syntax,
     * we shall not silence any exceptions.
     * @param sql SQL statement
     * @return parsed SqlNode
     */
    private static SqlNode toSqlNode(String sql) {
        try {
            return VoltFastSqlParser.parse(sql);
        } catch (SqlParseException e) {
            return null;    // TODO: do not catch when Calcite eventually supports all Volt DDL syntax.
        }
    }

    /**
     * Collect names of all materialized views depending on the target table.
     * @param t target table
     * @return names of materialized views whose materialization depend on the table.
     */
    private static Set<String> collectMaterializedViews(Table t) {
        return StreamSupport.stream(((Iterable<MaterializedViewInfo>) t.getViews()::iterator).spliterator(), false)
                .map(mv -> mv.getDest().getTypeName())
                .collect(Collectors.toSet());
    }

    /**
     * Check if target table's name is in a list of tables we are concerned with.
     * @param table target table under inspection
     * @param tables a list of table names we are concerned
     * @return whether the table is among our concerned table list.
     */
    private static boolean tableIn(Table table, Set<String> tables) {
        return table != null && tables.contains(table.getTypeName());
    }

    /**
     * Check if the target column comes from any of tables of interest
     * @param col target column
     * @param tables tables of interest
     * @return whether the column comes from any table provided.
     */
    private static boolean columnConcernsWith(Column col, Set<String> tables) {
        if (col == null) {
            return false;
        } else {
            final CatalogType parent = col.getParent();
            return parent instanceof Table && tableIn((Table) parent, tables);
        }
    }

    /**
     * Checks whether the given Catalog statement concerns any tables of interest.
     * @param stmt Catalog statement for the query
     * @param tables tables of interest
     * @return whether the statement mentioned any tables we are interested
     */
    private static boolean statementConcernsWith(Statement stmt, Set<String> tables) {
        return ! new HashSet<String>() {{
            addAll(Arrays.asList(stmt.getTablesread().split(",")));
            addAll(Arrays.asList(stmt.getTablesupdated().split(",")));
            retainAll(tables);
        }}.isEmpty();
    }

    /**
     * Checks whether a given collection of statements has anything to do with the list of tables of interest.
     * @param stmts a collection of statements
     * @param tables table names of interest
     * @return whether the collection of statements concerns with any of the tables provided.
     */
    private static boolean statementsConcernsWith(CatalogMap<Statement> stmts, Set<String> tables) {
        return StreamSupport.stream(((Iterable<Statement>) stmts::iterator).spliterator(), false)
                .anyMatch(stmt -> statementConcernsWith(stmt, tables));
    }

    /**
     * Search database for all stored procedures that is involved with the list of table names.
     * @param db database catalog
     * @param tables list of table names of interest
     * @return a list of stored procedures that involves with the list of given table names.
     */
    private static Set<String> collectStoredProcedures(Database db, Set<String> tables) {
        return StreamSupport.stream(((Iterable< Procedure>) db.getProcedures()::iterator).spliterator(), false)
                .filter(proc ->
                        tableIn(proc.getPartitiontable(), tables) ||
                                tableIn(proc.getPartitiontable2(), tables) ||
                                columnConcernsWith(proc.getPartitioncolumn(), tables) ||
                                columnConcernsWith(proc.getPartitioncolumn2(), tables) ||
                        statementsConcernsWith(proc.getStatements(), tables))
                .map(Procedure::getTypeName)
                .collect(Collectors.toSet());
    }

    /**
     * Execute a DROP TABLE statement, and effect changes to the VOltXMLElement for HSQL session and VoltCompiler.
     * @param db database catalog
     * @param sql SQL query for the DROP TABLE statement
     * @param dropStmt Calcite drop table object
     * @param schema HSQL session that we need to effect on.
     * @param compiler VoltCompiler instance
     * @return Generated encoded SQL drop-table statement if successfully executed; empty string otherwise (when
     * drop table request is legitimately rejected).
     */
    private static String execDropTable(
            Database db, String sql, SqlDropTable dropStmt, VoltXMLElement schema, VoltCompiler compiler) {
        final String tableName = dropStmt.getOperandList().get(0).toString();
        final boolean ignoreNotFound = dropStmt.getIfExists(), cascaded = dropStmt.getCascade();
        final Optional<Table> table =
                db == null ? Optional.empty() :
                        StreamSupport.stream(((Iterable<Table>) db.getTables()::iterator).spliterator(), false)
                                .filter(tbl -> tbl.getTypeName().equals(tableName)).findAny();
        CalciteUtils.exceptWhen(!ignoreNotFound && !table.isPresent(),
                String.format("Table %s not found", tableName));
        // found table: check dependencies
        // Exec "DROP TABLE t IF EXISTS;" and t does not exist
        return table.map(value -> execDropTable(db, sql, tableName, value, cascaded, schema, compiler))
                .orElse("");
    }

    private static String execDropTable(
            Database db, String sql, String tableName, Table tbl, boolean cascaded,
            VoltXMLElement schema, VoltCompiler compiler) {
        final Set<String> materializedViews = collectMaterializedViews(tbl),
                procs = collectStoredProcedures(db,
                        new HashSet<String>(){{
                            addAll(materializedViews);
                            add(tableName);
                        }});
        // We decline drop table when some stored procedure depend on it, regardless cascaded or not.
        // In future (ENG-7542, ENG-6353, doc), we will drop those stored procedures in cascade mode.
        CalciteUtils.exceptWhen(! procs.isEmpty(),
                String.format("Cannot drop table %s: stored procedure(s) %s depend on it.",
                        tableName, String.join(", ", procs)));
        final String viewNames = String.join(", ", materializedViews);
        CalciteUtils.exceptWhen(!cascaded && !materializedViews.isEmpty(),
                String.format("Dependent object exists: PUBLIC.%s in statement [%s]", viewNames, sql));
        materializedViews.add(tableName);
        final VoltXMLElement.VoltXMLDiff od = new VoltXMLElement.VoltXMLDiff("databaseschemadatabaseschema");
        for (int index = schema.children.size() - 1; index >= 0; --index) {
            final VoltXMLElement elm = schema.children.get(index);
            final String dirtyTable = elm.attributes.get("name");
            if (elm.name.equals("table") && materializedViews.contains(dirtyTable)) {
                compiler.markTableAsDirty(dirtyTable);
                od.getRemovedNodes().add(elm);
                schema.children.remove(index);
            }
        }
        schema.applyDiff(od);
        return Encoder.hexEncode(sql) + "\n";
    }

    /**
     * Check whether the given SQL query is DROP TABLE statement, and execute it here if it is.
     * @param db Database catalog.
     * @param sql SQL statement, possibly ends with semi-colon
     * @param schema HSQL session to effect on when executing DROP TABLE syntax
     * @param compiler the Volt compiler
     * @return encoded SQL statement (later appended, encoded and set database's schema to the batch) when the statement
     * is a DROP TABLE statement; null otherwise.
     */
    public static String run(Database db, String sql, VoltXMLElement schema, VoltCompiler compiler) {
        final SqlNode query = toSqlNode(sql);
        if (query == null) {    // Calcite parse error
           return null;
        } else {
            switch (query.getKind()) {
                case DROP_TABLE:
                    return execDropTable(db, sql, (SqlDropTable) query, schema, compiler);
                case CREATE_TABLE:
                    /*final String tableName = ((SqlCreateTable) query).getOperandList().get(0).toString();
                    Database db, String sql, String tableName, Table tbl, boolean cascaded,
                    VoltXMLElement schema, VoltCompiler compiler)
                    return execDropTable(db, sql, tableName, tb, )*/
                default:
                    return null;
            }
        }
    }
}
