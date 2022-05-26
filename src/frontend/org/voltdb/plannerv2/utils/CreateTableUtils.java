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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlColumnDeclarationWithExpression;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TimeToLive;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compilereport.TableAnnotation;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannerv2.ColumnTypes;
import org.voltdb.utils.CatalogSchemaTools;
import org.voltdb.utils.CatalogUtil;

/**
 * Util class to help with "CREATE TABLE" DDL statements through Calcite.
 */
public class CreateTableUtils {
    /**
     * Validate a column with variable length, e.g. VARCHAR/VARBINARY/GEOGRAPHY.
     * Checks that the length of the column is reasonable.
     *
     * @param vt column type
     * @param tableName table name
     * @param colName column name
     * @param size user provided size of the column, as VARCHAR(1024)
     * @param inBytes whether VARCHAR was specified to be in BYTES, as `VARCHAR(1024 BYTES)`. May get set even when user
     *                did not explicitly state that the column is in bytes: when user provided length exceeds maximum
     *                number of CHARACTERs a VARCHAR column could hold.
     * @return column size and whether the column is in bytes.
     */
    private static Pair<Integer, Boolean> validateVarLenColumn(
            VoltType vt, String tableName, String colName, int size, boolean inBytes) {
        if (size < 0) {        // user did not provide, e.g. CREATE TABLE t(i VARCHAR);
            size = vt.defaultLengthForVariableLengthType();
        }
        CalciteUtils.exceptWhen(size > VoltType.MAX_VALUE_LENGTH,
                "%s column %s in table %s has unsupported length %s",
                vt.toSQLString(), colName, tableName, VoltType.humanReadableSize(size));
        if (vt == VoltType.STRING && size > VoltType.MAX_VALUE_LENGTH_IN_CHARACTERS) {
            System.err.println(String.format(       // in place of giving a warning
                    "The size of VARCHAR column %s in table %s greater than %d " +
                            "will be enforced as byte counts rather than UTF8 character counts. " +
                            "To eliminate this warning, specify \"VARCHAR(%s BYTES)\"",
                    colName, tableName, VoltType.MAX_VALUE_LENGTH_IN_CHARACTERS,
                    VoltType.humanReadableSize(size)));
            inBytes = true;
        }
        CalciteUtils.exceptWhen(size < vt.getMinLengthInBytes(),
                "%s column %s in table %s has length of %d which is shorter than %d, the minimum length allowed for the type.",
                vt.toSQLString(), colName, tableName, size, vt.getMinLengthInBytes());
        CalciteUtils.exceptWhen(vt == VoltType.STRING && ! inBytes &&
                        size * DDLCompiler.MAX_BYTES_PER_UTF8_CHARACTER > VoltType.MAX_VALUE_LENGTH,
                "Column %s.%s specifies a maixmum size of %d characters but the maximum supported size is %s characters or %s bytes",
                tableName, colName, size,
                VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH / DDLCompiler.MAX_BYTES_PER_UTF8_CHARACTER),
                VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH));
        return Pair.of(size, inBytes);
    }

    /**
     * Checks that the function expression for DEFAULT expr is allowed, and translates into format FUNCTION_NAME:FUNCTION_ID
     * that is understood by EE. Only "NOW"/"CURRENT_TIMESTAMP" functions w/o parenthesis are allowed.
     * @param vt column type
     * @param funName name of function for DEFAULT expr
     * @param colName name of the column with DEFAULT expr
     * @return EE-understood form of function call.
     */
    private static String defaultFunctionValue(VoltType vt, String funName, String colName) {
        CalciteUtils.exceptWhen(! funName.equals("NOW") && ! funName.equals("CURRENT_TIMESTAMP"),
                "Function %s not allowed for DEFAULT value of column declaration", funName);
        CalciteUtils.exceptWhen(vt != VoltType.TIMESTAMP,
                "Function %s not allowed for DEFAULT value of column \"%s\" of %s type",
                        funName, colName, vt.getName());
        return "CURRENT_TIMESTAMP:43";
    }

    /**
     * Manually extract VARBINARY default value in form of "x'03'" into "03"
     * @param src raw default string
     * @return extracted string
     */
    private static String extractVarBinaryValue(String src) {
        final int indexQuote1 = src.indexOf('\''), indexQuote2 = src.lastIndexOf('\'');
        assert indexQuote1 > 0 && indexQuote2 > indexQuote1;
        return src.substring(indexQuote1 + 1, indexQuote2 - indexQuote1 + 1);
    }

    /**
     * Add a column to the table under construction
     * @param col column declaration
     * @param t Table containing the column
     * @param tableName name of the table
     * @param index 0-based index of the column
     * @param columnTypes column index -> column type pair that gets inserted per successful column addition
     * @return size of the column that affects the size of the whole table
     */
    static int addColumn(SqlColumnDeclarationWithExpression col, Table t, String tableName,
                                 AtomicInteger index, Map<Integer, VoltType> columnTypes) {
        final List<SqlNode> nameAndType = col.getOperandList();
        final String colName = nameAndType.get(0).toString();
        final Column column = t.getColumns().add(colName);
        column.setName(colName);

        final SqlDataTypeSpec type = (SqlDataTypeSpec) nameAndType.get(1);
        int colSize = type.getPrecision();       // -1 when user did not provide.
        final VoltType vt = ColumnTypes.getVoltType(type.getTypeName().toString());
        column.setType(vt.getValue());
        column.setNullable(type.getNullable());
        // Validate user-supplied size (SqlDataTypeSpec.precision)
        final SqlDataTypeSpec dspec = col.getDataType();
        boolean inBytes = dspec.getInBytes();
        if (vt.isVariableLength()) {     // user did not specify a size. Set to default value
            final Pair<Integer, Boolean> r = validateVarLenColumn(vt, tableName, colName, colSize, inBytes);
            colSize = r.getFirst();
            inBytes = r.getSecond();
        } else if (colSize < 0) {
            colSize = vt.getLengthInBytesForFixedTypesWithoutCheck();
        }
        column.setSize(colSize);
        column.setIndex(index.getAndIncrement());
        columnTypes.put(index.get(), vt);
        column.setInbytes(inBytes);
        final SqlNode expr = col.getExpression();
        if (expr != null) {
            column.setDefaulttype(vt.getValue());
            if (expr instanceof SqlBasicCall) {
                column.setDefaultvalue(defaultFunctionValue(vt, ((SqlBasicCall) expr).getOperator().getName(), colName));
            } else if (expr instanceof SqlIdentifier) {
                column.setDefaultvalue(defaultFunctionValue(vt, ((SqlIdentifier) expr).getSimple(), colName));
            } else if (expr.getKind() == SqlKind.LITERAL) {
                final String defaultValue;
                if (((SqlLiteral) expr).getValue() == null) {
                    column.setDefaulttype(VoltType.NULL.getValue());        // reset default type to NULL
                    defaultValue = null;
                } else if (expr instanceof SqlNumericLiteral) {
                    defaultValue = ((SqlNumericLiteral) expr).getValue().toString();
                } else if (expr instanceof SqlBinaryStringLiteral) {
                    // NOTE: cannot call toValue() method, which Calcite will happily change into binary representation,
                    // e.g. x'00' -> '0'; x'06' -> '110', etc.
                    // But, HSQL will be unhappy because Scanner#convertToBinary expects the raw form (which it converts).
                    // TODO: use toValue() when we deprecate HSQL.
                    defaultValue = extractVarBinaryValue(expr.toString());
                } else {
                    assert expr instanceof SqlCharStringLiteral;
                    defaultValue = ((SqlCharStringLiteral) expr).toValue();
                }
                column.setDefaultvalue(defaultValue);
            } else {
                CalciteUtils.except(String.format(
                        "Unsupported default expression for column \"%s\": \"%s\"", colName, expr.toString()));
            }
        } else {
            column.setDefaultvalue(null);
        }
        if (vt.isVariableLength()) {
            return  4 + colSize * (vt != VoltType.STRING || inBytes ? 1 : 4);
        } else {
            return colSize;
        }
    }

    /**
     * Process a TTL statement in CREATE TABLE, e.g. CREATE TABLE foo(i int, ...) USING TTL 5 hours on COLUMN i;
     * @param t table for the TTL statement
     * @param constraint Calcite object containing all we need to know for the TTL constraint
     */
    public static void procTTLStmt(Table t, SqlCreateTable.TtlConstraint constraint) {
        if (constraint != null) {
            CalciteUtils.exceptWhen(constraint.getDuration() <= 0,
                    "Error: TTL on table %s must be positive: got %d",
                    t.getTypeName(), constraint.getDuration());
            final String ttlColumnName = constraint.getColumn().toString();
            final Column ttlColumn = t.getColumns().get(ttlColumnName);
            CalciteUtils.exceptWhen(ttlColumn == null,
                    "Error: TTL column %s does not exist in table %s", ttlColumnName, t.getTypeName());
            CalciteUtils.exceptWhen(!VoltType.get((byte) ttlColumn.getType()).isBackendIntegerType(),
                    "Error: TTL column %s of type %s in table %s is not allowed. Allowable column types are: %s, %s, %s or %s.",
                    ttlColumnName, VoltType.get((byte) ttlColumn.getType()).getName(), t.getTypeName(),
                    VoltType.INTEGER.getName(), VoltType.TINYINT.getName(),
                    VoltType.BIGINT.getName(), VoltType.TIMESTAMP.getName());
            CalciteUtils.exceptWhen(ttlColumn.getNullable(),
                    "Error: TTL column %s in table %s is required to be NOT NULL.",
                    ttlColumnName, t.getTypeName());
            // was set to TimeToLiveVoltDB.TTL_NAME: TimeToLiveVoltDB was a patch to HSQL
            final TimeToLive ttl = t.getTimetolive().add("ttl");
            ttl.setTtlcolumn(ttlColumn);
            ttl.setTtlvalue(constraint.getDuration());
            // The TTL batch size and max frequencies were set by HSQL to be these fixed values, and are never used anywhere.
            ttl.setBatchsize(1000);
            ttl.setMaxfrequency(1);
            ttl.setTtlunit(constraint.getUnit().name());
        }
    }

    /**
     * Set table annotation
     * @param t source table
     */
    private static void addAnnotation(Table t) {
        // TODO: assuming that no mat view is associated with t; and t is not a streaming table
        // The matview need to reset annotation.ddl with the query, yada yada yada.
        final TableAnnotation annotation = new TableAnnotation();
        annotation.ddl = CatalogSchemaTools.toSchema(new StringBuilder(), t,
                null, false, null, null, null);
        t.setAnnotation(annotation);
    }

    /**
     * API to use for CREATE TABLE ddl stmt.
     * @param node Calcite SqlNode for parsed CREATE TABLE stmt
     * @param db catalog database object to insert created table
     */
    public static void addTable(SqlNode node, Database db) {
        if (node.getKind() != SqlKind.CREATE_TABLE) {           // for now, only partially support CREATE TABLE stmt
            return;
        }
        final List<SqlNode> nameAndColListAndQuery = ((SqlCreateTable) node).getOperandList();
        final String tableName = nameAndColListAndQuery.get(0).toString();
        final SqlNodeList nodeTableList = (SqlNodeList) nameAndColListAndQuery.get(1);
        final Table t = db.getTables().add(tableName);
        t.setIsreplicated(true);

        final int numCols = nodeTableList.getList().size();
        CalciteUtils.exceptWhen(numCols > DDLCompiler.MAX_COLUMNS,
                "Table %s has %d columns (max is %d)", tableName, numCols, DDLCompiler.MAX_COLUMNS);
        int rowSize = 0;
        final AtomicInteger index = new AtomicInteger(0);
        final SortedMap<Integer, VoltType> columnTypes = new TreeMap<>();
        final Map<String, Index> indexMap = new HashMap<>();
        for (SqlNode col : nodeTableList) {
            switch (col.getKind()) {
                case PRIMARY_KEY:        // PKey and Unique are treated almost in the same way, except naming and table constraint type.
                case UNIQUE:
                case ASSUME_UNIQUE:
                    final List<SqlNode> nodes = ((SqlKeyConstraint) col).getOperandList();
                    assert(nodes.size() == 2);      // for un-named constraint, the 1st elm is null; else 1st is SqlIdentifier storing constraint name.
                    Pair<List<Column>, List<AbstractExpression>> columsAndExpressions =
                            CreateIndexUtils.splitSqlNodes((SqlNodeList) nodes.get(1), t);
                    final List<AbstractExpression> voltExprs = columsAndExpressions.getSecond();
                    final List<Column> columns = columsAndExpressions.getFirst();
                    final Pair<String, String> indexAndConstraintNames = CreateIndexUtils.genIndexAndConstraintName(
                            nodes.get(0) == null ? null : nodes.get(0).toString(),
                            col.getKind(), tableName, columns, ! voltExprs.isEmpty());
                    final String indexName = indexAndConstraintNames.getFirst(), constraintName = indexAndConstraintNames.getSecond();
                    CalciteUtils.exceptWhen(nodes.get(1) != null && indexMap.containsKey(indexName),  // detection of duplicated constraint names
                            "A constraint named %s already exists on table %s.", indexName, tableName);
                    columns.forEach(column -> CreateIndexUtils.validateIndexColumnType(indexName, column));
                    CreateIndexUtils.validateGeogInColumns(indexName, columns);
                    voltExprs.forEach(e -> CreateIndexUtils.validateIndexColumnType(indexName, e));
                    final StringBuffer sb = new StringBuffer(String.format("index %s", indexName));
                    if(! AbstractExpression.validateExprsForIndexesAndMVs(voltExprs, sb, false)) {
                        CalciteUtils.except(sb.toString());
                    }
                    CreateIndexUtils.validateGeogInExprs(indexName, voltExprs);
                    final boolean hasGeogType =
                            columns.stream().anyMatch(column -> column.getType() == VoltType.GEOGRAPHY.getValue()) ||
                                    voltExprs.stream().anyMatch(expr -> expr.getValueType() == VoltType.GEOGRAPHY);
                    final Index indexConstraint = CreateIndexUtils.genIndex(
                            col.getKind(), t, indexName, constraintName, hasGeogType, columns, voltExprs, null);
                    if (indexMap.values().stream().noneMatch(other -> CreateIndexUtils.equals(other, indexConstraint))) {
                        indexMap.put(indexName, indexConstraint);
                    }
                    break;
                case COLUMN_DECL:
                    rowSize += addColumn(new SqlColumnDeclarationWithExpression((SqlColumnDeclaration) col),
                            t, tableName, index, columnTypes);
                    CalciteUtils.exceptWhen(rowSize > DDLCompiler.MAX_ROW_SIZE,       // fails when the accumulative row size exeeds limit
                            "Error: table %s has a maximum row size of %s but the maximum supported size is %s",
                                    tableName, VoltType.humanReadableSize(rowSize), VoltType.humanReadableSize(DDLCompiler.MAX_ROW_SIZE));
                    break;
                default:
            }
        }
        t.setSignature(CatalogUtil.getSignatureForTable(tableName, columnTypes));
        procTTLStmt(t, ((SqlCreateTable) node).getTtlConstraint());
        addAnnotation(t);
    }
}
