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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateIndex;
import org.json_voltpatches.JSONException;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.calciteadapter.CatalogAdapter;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.IndexType;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Util class to help with "CREATE INDEX" DDL statement through Calcite
 */
final public class CreateIndexUtils {
    private CreateIndexUtils() {}

    /**
     * Validate that the table and columns referred to from the index do exist in the database
     *
     * Note that the checks here are redundant for now, as VoltCompiler.compileDatabase()'s
     * loadSchema method checks first.
     * Eventually we will get rid of that check from DDLCompiler.
     */
    private static final class ValidateAndExtractFromCreateIndexNode {
        private Table table;
        private List<Column> columns;
        private AbstractExpression filter;

        /**
         * Constructor from database to refer to, and Calcite CreateIndex node.
         * @param db Database that should contain all catalog objects required from CREATE INDEX node.
         * @param node Calcite CREATE INDEX node
         */
        ValidateAndExtractFromCreateIndexNode(Database db, SqlCreateIndex node) {
            final String tableName = node.getTable().toString();
            table = db.getTables().get(tableName);
            CalciteUtils.exceptWhen(table == null, String.format("Table %s not found", tableName));
            final CatalogMap<Column> columnsInTable = table.getColumns();
            columns = node.getColumnList().getList().stream()
                    .map(SqlNode::toString).map(columnsInTable::get)
                    .collect(Collectors.toList());
            if (columns.size() < node.getColumnList().size()) {
                CalciteUtils.except(String.format("Columns %s not found from table %s",
                        node.getColumnList().getList().stream().map(SqlNode::toString)
                                .filter(name -> columnsInTable.get(name) == null)
                                .collect(Collectors.joining(", ")),
                        tableName));
                filter = null;
            } else if (node.getFilter() == null) {
                filter = null;
            } else {
                filter = ExpressionTranslator.translate((SqlBasicCall) node.getFilter(), table);
            }
        }

        /**
         * Refresh to point to a new table from a different database.
         * @param table new table from a different database
         * @return refreshed current instance
         */
        ValidateAndExtractFromCreateIndexNode refreshUsing(Table table) {
            CalciteUtils.exceptWhen(! this.table.getTypeName().equals(table.getTypeName()),
                    "Cannot refresh to a different table version: table names are different. " +
                            "Source table name %s, dest table name %s",
                    table.getTypeName(), this.table.getTypeName());
            this.table = table;
            for(int index = 0; index < columns.size(); ++index) {
                final String columnName = columns.get(index).getTypeName();
                final Column dstColumn = table.getColumns().get(columnName);
                CalciteUtils.exceptWhen(dstColumn == null,
                        "Internal error: table does not have column %s", columnName);
                columns.set(index, dstColumn);
            }
            return this;
        }
        Table getTable()  {
            return table;
        }
        List<Column> getColumns() {
            return columns;
        }
        AbstractExpression getFilter() {
            return filter;
        }
    }

    /**
     * Validate that a column declared to be PRIMARY KEY is actually capable of being a PRIMARY KEY
     * @param pkeyName name of the primary key
     * @param col column declared to be primary key
     */
    static void validatePKeyColumnType(String pkeyName, Column col) {
        final VoltType vt = VoltType.get((byte) col.getType());
        CalciteUtils.exceptWhen(! vt.isIndexable(),
                String.format("Cannot create index \"%s\" because %s values " +
                                "are not currently supported as index keys: \"%s\"",
                        pkeyName, vt.getName(), col.getName()));
        CalciteUtils.exceptWhen(! vt.isUniqueIndexable(),
                String.format("Cannot create index \"%s\" because %s values " +
                                "are not currently supported as unique index keys: \"%s\"",
                        pkeyName, vt.getName(), col.getName()));
    }

    /**
     * Validate that the **filter** declared to be UNIQUE or ASSUMEUNIQUE cannot contain any GEOGRAPHY column.
     * e.g. CREATE TABLE foo(i smallint, j bigint, unique(i + j)) is OK; while
     * CREATE TABLE foo(i GEOGRAPHY, UNIQUE(NumInteriorRings(i)) is not
     * @param indexName index name
     * @param exprs UNIQUE/ASSUMEUNIQUE filter
     */
    static void validateGeogInExprs(String indexName, List<AbstractExpression> exprs) {
        exprs.stream().filter(expr -> expr.getValueType() == VoltType.GEOGRAPHY)
                .findAny().ifPresent(expr -> {
            CalciteUtils.exceptWhen(exprs.size() > 1,
                    String.format("Cannot create index \"%s\" because %s values must be the only component of an index key.",
                            indexName, expr.getValueType().getName()));
            CalciteUtils.exceptWhen(! (expr instanceof TupleValueExpression),
                    String.format("Cannot create index \"%s\" because %s expressions must be simple value expressions.",
                            indexName, expr.getValueType().getName()));
        });
    }

    /**
     * Validate that the list of columns declared to be primary key, when containing more than one column, shall not
     * contain any GEOGRAPHY column.
     * @param indexName index name
     * @param cols list of columns declared to be primary key
     */
    static void validateGeogInColumns(String indexName, List<Column> cols) {
        if (cols.size() > 1) {
            cols.stream().filter(column -> column.getType() == VoltType.GEOGRAPHY.getValue()).findAny()
                    .ifPresent(column -> CalciteUtils.except(String.format(
                            "Cannot create index %s because %s values must be the only component of an index key: \"%s\"",
                            indexName, VoltType.GEOGRAPHY.getName(), column.getName())));
        }
    }

    /**
     * Validate that a UNIQUE or ASSUMEUNIQUE column/filter is actually capable of assuming such index.
     * @param indexName name of the index
     * @param e filter containing the list of column(s) that is UNIQUE or ASSUMEUNIQUE
     */
    static void validateIndexColumnType(String indexName, AbstractExpression e) {
        StringBuffer sb = new StringBuffer();
        CalciteUtils.exceptWhen(! e.isValueTypeIndexable(sb),
                String.format("Cannot create index \"%s\" because it contains %s, which is not supported.",
                        indexName, sb));
        CalciteUtils.exceptWhen(! e.isValueTypeUniqueIndexable(sb),
                String.format("Cannot create unique index \"%s\" because it contains %s, which is not supported",
                        indexName, sb));
    }

    /**
     * Sets the type of constraint depending on SqlNode content.
     * @param cons Catalog object for the constraint
     * @param index Catalog object for the index
     * @param type Calcite SqlKind specifying the nature of the constraint
     * @param indexName name of the catalog index
     * @param tableName name of the catalog table
     */
    private static void setConstraintType(
            Constraint cons, Index index, SqlKind type, String indexName, String tableName) {
        cons.setIndex(index);
        final ConstraintType constraintType;
        switch (type) {
            case PRIMARY_KEY:
                constraintType = ConstraintType.PRIMARY_KEY;
                break;
            case UNIQUE:
            case ASSUME_UNIQUE:
                constraintType = ConstraintType.UNIQUE;
                break;
            default:
                constraintType = null;
                CalciteUtils.except(String.format("Unsupported index type %s of index %s on table %s",
                        type.name(), indexName, tableName));
        }
        cons.setType(constraintType.getValue());
    }

    /**
     * Generate index name and constraint name for the given SqlTyppe and, either a list of columns, or an filter.
     * We need two names here to be compatible with what VoltDB had been doing; but having the index and constraint to
     * be the same name does not seems to cause any trouble: indexes and constraints are in different catalog spaces.
     * We need two names with same naming scheme to pass all Jenkins tests.
     * @param type constraint type
     * @param tableName name of table of the constraint
     * @param cols list of columns for the index
     * @param hasExpression whether the constraint contains an filter, e.g. CREATE TABLE foo(i SMALLINT, j BIGINT, UNIQUE(i, j - i));
     * @return generated (IndexName, ConstraintName)
     */
    static Pair<String, String> genIndexAndConstraintName(
            String constraintName, SqlKind type, String tableName, List<Column> cols, boolean hasExpression) {
        if (constraintName != null) {
            return Pair.of("VOLTDB_AUTOGEN_CONSTRAINT_IDX_" + constraintName, constraintName);
        } else {
            final String suffix,
                    type_str = type == SqlKind.PRIMARY_KEY ? "PK" : "CT",
                    index = cols.stream().map(Column::getName)
                            .reduce(String.format("VOLTDB_AUTOGEN_IDX_%s_%s", type_str, tableName),
                                    (acc, colName) -> acc + "_" + colName),
                    constr = cols.stream().map(Column::getName)
                            .reduce(String.format("VOLTDB_AUTOGEN_CT__%s_%s", type_str, tableName),
                                    (acc, colName) -> acc + "_" + colName);
            if (hasExpression) {        // For index involving filter(s), generate a UID string as suffix, and pretending that
                // filter-groups for different constraints are never the same
                // (If they are, user will suffer some additional time cost when upserting table.)
                suffix = String.format("_%s", UUID.randomUUID().toString()).replace('-', '_');
            } else {
                suffix = "";
            }
            return Pair.of(index + suffix, constr + suffix);
        }
    }

    private static String toJSON(List<AbstractExpression> exprs, String indexName, String tableName) {
        try {
            return DDLCompiler.convertToJSONArray(exprs);
        } catch (JSONException e) {
            CalciteUtils.except(String.format(
                    "Unexpected error serializing non-column expressions for index '%s' on type '%s': %s",
                    indexName, tableName, e.toString()));
            return null;
        }
    }

    private static String toJSON(AbstractExpression expr, String indexName, String tableName) {
        try {
            return DDLCompiler.convertToJSONObject(expr);
        } catch (JSONException e) {
            CalciteUtils.except(String.format(
                    "Unexpected error serializing non-column expressions for index '%s' on type '%s': %s",
                    indexName, tableName, e.toString()));
            return null;
        }
    }

    /**
     * Create a catalog object for the constraint given all its details
     * @param type constraint type, used for checking whether it is ASSUMEUNIQUE.
     * @param t table for the index.
     * @param indexName name of the index.
     * @param constraintName name of the constraint. Different from name of the index, as constraint and index
     *                       are separate catalog objects.
     * @param hasGeog whether the index contains a Geography column.
     * @param indexCols list of columns contained in the index. e.g. CREATE TABLE foo(int i, bigint j, UNIQUE(i, j - i)); ==> [i]
     * @param exprs List of expressions contained in the index. e.g. ... => [j - i]
     * @return the catalog object for the index
     */
    static Index genIndex(SqlKind type, Table t, String indexName, String constraintName, boolean hasGeog,
                                  List<Column> indexCols, List<AbstractExpression> exprs, AbstractExpression predicate) {
        // ENG-12475: Hash index is not officially removed.
        final Index index = t.getIndexes().add(indexName);
        if (hasGeog) {
            index.setCountable(false);
            index.setType(IndexType.COVERING_CELL_INDEX.getValue());
        } else {
            index.setCountable(true);
            index.setType(IndexType.BALANCED_TREE.getValue());
        }
        index.setUnique(true);
        index.setAssumeunique(type == SqlKind.ASSUME_UNIQUE);
        setConstraintType(t.getConstraints().add(constraintName), index, type, constraintName, t.getTypeName());
        AtomicInteger i = new AtomicInteger(0);
        indexCols.forEach(column -> {
            final ColumnRef cref = index.getColumns().add(column.getName());
            cref.setColumn(column);
            cref.setIndex(i.getAndIncrement());
        });
        if (! exprs.isEmpty()) {
            index.setExpressionsjson(toJSON(exprs, indexName, t.getTypeName()));
        }
        if (predicate != null) {
            index.setPredicatejson(toJSON(predicate, indexName, t.getTypeName()));
        }
        final AbstractExpression.UnsafeOperatorsForDDL unsafeOp = new AbstractExpression.UnsafeOperatorsForDDL();
        exprs.forEach(expr -> expr.findUnsafeOperatorsForDDL(unsafeOp));
        index.setIssafewithnonemptysources(! unsafeOp.isUnsafe());
        return index;
    }

    /**
     * Get the list of columns that an index is on. Used in index dedup upon index creation time.
     * Remember that an index on column a, b is different from an index on column b, a.
     * @param index catalog index object
     * @return a list of columns, possibly empty (when index is purely made up of expressions),
     * that is contained by the index.
     */
    static List<Integer> colIndicesOfIndex(Index index) {
        final Iterable<ColumnRef> iterCref = () -> index.getColumns().iterator();
        return StreamSupport
                .stream(iterCref.spliterator(), false)
                .mapToInt(cref -> cref.getColumn().getIndex())
                .boxed()
                .collect(Collectors.toList());
    }

    static boolean equals(Index left, Index right) {
        return (left == null && right == null) || (left != null && right != null &&
                ! left.equals(right) &&        // don't compare with itself
                left.getType() == right.getType() &&
                left.getCountable() == right.getCountable() &&
                left.getUnique() == right.getUnique() &&
                left.getAssumeunique() == right.getAssumeunique() &&
                left.getColumns().size() == right.getColumns().size() &&
                // skip concrete filter comparisons
                left.getExpressionsjson().isEmpty() == right.getExpressionsjson().isEmpty() &&
                left.getPredicatejson().isEmpty() == right.getPredicatejson().isEmpty() &&
                colIndicesOfIndex(left).equals(colIndicesOfIndex(right)));
    }

    /**
     * Map SqlCreateIndex.IndexType enum type to SqlKind enum type. Used only for "CREATE INDEX" stmt
     */
    private static SqlKind toSqlKind(SqlCreateIndex.IndexType type) {
        return type == SqlCreateIndex.IndexType.UNIQUE ? SqlKind.UNIQUE : SqlKind.ASSUME_UNIQUE;
    }

    private static ValidateAndExtractFromCreateIndexNode getOrCreateTable(
            Database prevDb, Database currentDb, SqlCreateIndex node) {
        try {
            return new ValidateAndExtractFromCreateIndexNode(currentDb, node);
        } catch (PlanningErrorException ex) {
            if (prevDb == null) {
                throw ex;
            } else {
                final ValidateAndExtractFromCreateIndexNode info =
                        new ValidateAndExtractFromCreateIndexNode(prevDb, node);
                return info.refreshUsing(CalciteUtils.addTableToDatabase(currentDb, info.getTable()));
            }
        }
    }

    /**
     * API to run for "CREATE INDEX" statement
     * @param node Calcite parsed SQL node
     * @param prevDb previous version of the database (from last batch or last statement of AdHoc query).
     *               We should be first looking for the target table from the new database, and when absent,
     *               from old database, then clone it to the latest currentDb, then add index.
     * @param currentDb current database version to update, i.e. to add index/constraint to target table of
     *           the previous database version.
     * @return updated SchemaPlus from database when the SqlNode is a "CREATE INDEX" node; null otherwise.
     */
    public static SchemaPlus run(SqlNode node, Database prevDb, Database currentDb) {
        if (node.getKind() == SqlKind.CREATE_INDEX) {
            final SqlCreateIndex indexNode = (SqlCreateIndex) node;
            final ValidateAndExtractFromCreateIndexNode info = getOrCreateTable(prevDb, currentDb, indexNode);
            // Now that we know that prevDb has the table referred to, make sure that current version of currentDb should also
            // have it. Clone if it doesn't
            final Table table = info.getTable();
            final CatalogMap<Index> indicesOnTable = table.getIndexes();
            final String tableName = table.getTypeName();
            final List<Column> columns = info.getColumns();
            final AbstractExpression filter = info.getFilter();
            final String constraintName = indexNode.getName().toString();

            CalciteUtils.exceptWhen(indicesOnTable.get(constraintName) != null,
                    String.format("A constraint named %s already exists on table %s", constraintName, tableName));
            final Pair<String, String> indexAndConstraintNames = genIndexAndConstraintName(
                    constraintName, SqlKind.UNIQUE, tableName, columns, filter != null);
            final String indexName = indexAndConstraintNames.getFirst();
            columns.forEach(column -> validatePKeyColumnType(indexName, column));
            validateGeogInColumns(indexName, columns);
            final StringBuffer msg = new StringBuffer("Partial index \"" + indexName + "\" ");
            if (filter != null && ! filter.isValidExprForIndexesAndMVs(msg, false)) {
                CalciteUtils.except(msg.toString());
            }
            final boolean hasGeogType =
                    columns.stream().anyMatch(column -> column.getType() == VoltType.GEOGRAPHY.getValue());
            final Index indexConstraint = genIndex(toSqlKind(indexNode.getType()), table, indexName, constraintName,
                    hasGeogType, columns, Collections.emptyList(), filter);
            CalciteUtils.exceptWhen(
                    StreamSupport.stream(((Iterable<Index>) indicesOnTable::iterator).spliterator(), false)
                            .anyMatch(index -> equals(index, indexConstraint)),
                    String.format("A constraint already exists on table %s", tableName));
            return CatalogAdapter.schemaPlusFromDatabase(currentDb);
        } else {
            return null;
        }
    }
}
