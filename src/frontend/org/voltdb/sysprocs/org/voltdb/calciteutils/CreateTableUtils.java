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

package org.voltdb.sysprocs.org.voltdb.calciteutils;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.*;
import org.apache.calcite.sql.dialect.VoltSqlDialect;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.hsqldb_voltpatches.FunctionCustom;
import org.hsqldb_voltpatches.FunctionSQL;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.catalog.*;
import org.voltdb.catalog.org.voltdb.calciteadaptor.CatalogAdapter;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compilereport.TableAnnotation;
import org.voltdb.expressions.*;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.sysprocs.org.voltdb.calciteadapter.ColumnType;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.CatalogSchemaTools;
import org.voltdb.utils.CatalogUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Util class to help with "CREATE TABLE" DDL statements through Calcite.
 */
public class CreateTableUtils {
    private static Pair<Integer, Boolean> validateVarLenColumn(VoltType vt, String tableName, String colName, int size, boolean inBytes) {
        if (size < 0) {        // user did not provide, e.g. CREATE TABLE t(i VARCHAR);
            size = vt.defaultLengthForVariableLengthType();
        } else if (size > VoltType.MAX_VALUE_LENGTH) {
            throw new PlanningErrorException(String.format("%s column %s in table %s has unsupported length %s",
                    vt.toSQLString(), colName, tableName, VoltType.humanReadableSize(size)));
        } else if (vt == VoltType.STRING && size > VoltType.MAX_VALUE_LENGTH_IN_CHARACTERS) {
            System.err.println(String.format(       // in place of giving a warning
                    "The size of VARCHAR column %s in table %s greater than %d " +
                            "will be enforced as byte counts rather than UTF8 character counts. " +
                            "To eliminate this warning, specify \"VARCHAR(%s BYTES)\"",
                    colName, tableName, VoltType.MAX_VALUE_LENGTH_IN_CHARACTERS,
                    VoltType.humanReadableSize(size)));
            inBytes = true;
        } else if (size < vt.getMinLengthInBytes()) {
            throw new PlanningErrorException(String.format(
                    "%s column %s in table %s has length of %d which is shorter than %d, the minimum length allowed for the type.",
                    vt.toSQLString(), colName, tableName, size, vt.getMinLengthInBytes()));
        }
        if (vt == VoltType.STRING && ! inBytes &&
                size * DDLCompiler.MAX_BYTES_PER_UTF8_CHARACTER > VoltType.MAX_VALUE_LENGTH) {
            throw new PlanningErrorException(String.format(
                    "Column %s.%s specifies a maixmum size of %d characters but the maximum supported size is %s characters or %s bytes",
                    tableName, colName, size,
                    VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH / DDLCompiler.MAX_BYTES_PER_UTF8_CHARACTER),
                    VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH)));
        }
        return Pair.of(size, inBytes);
    }

    // Allow only NOW/CURRENT_TIMESTAMP functions w/o parenthesis on TIMESTAMP column
    private static String defaultFunctionValue(VoltType vt, String funName, String colName) {
        if (! funName.equals("NOW") && ! funName.equals("CURRENT_TIMESTAMP")) {
            throw new PlanningErrorException(String.format(
                    "Function %s not allowed for DEFAULT value of column declaration",
                    funName));
        } else if (vt != VoltType.TIMESTAMP) {
            throw new PlanningErrorException(String.format(
                    "Function %s not allowed for DEFAULT value of column \"%s\" of %s type",
                    funName, colName, vt.getName()));
        } else {
            return "CURRENT_TIMESTAMP:43";
        }
    }

    private static Optional<SqlKind> isColumnIndexed(SqlDataTypeSpec dspec) {
        if (dspec.getIsUnique() || dspec.getIsPKey() || dspec.getIsAssumeUnique()) {
            final SqlKind dtype;
            if (dspec.getIsPKey()) {
                dtype = SqlKind.PRIMARY_KEY;
            } else if (dspec.getIsUnique()) {
                dtype = SqlKind.UNIQUE;
            } else {
                dtype = SqlKind.ASSUME_UNIQUE;
            }
            return Optional.of(dtype);
        } else {
            return Optional.empty();
        }
    }

    private static int addColumn(SqlColumnDeclarationWithExpression col, Table t, String tableName,
                                 AtomicInteger index, Map<Integer, VoltType> columnTypes, Map<String, Index> indexes) {
        final List<SqlNode> nameAndType = col.getOperandList();
        final String colName = nameAndType.get(0).toString();
        final Column column = t.getColumns().add(colName);
        column.setName(colName);

        final SqlDataTypeSpec type = (SqlDataTypeSpec) nameAndType.get(1);
        //final int scale = type.getScale();
        int colSize = type.getPrecision();       // -1 when user did not provide.
        final VoltType vt = ColumnType.getVoltType(type.getTypeName().toString());
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
                column.setDefaultvalue(expr.toString());
            } else {
                throw new PlanningErrorException(String.format(
                        "Unsupported default expression for column \"%s\": \"%s\"", colName, expr.toString()));
            }
        } else {
            column.setDefaultvalue(null);
        }
        isColumnIndexed(dspec).ifPresent(dtype -> {
            final List<Column> cols = Collections.singletonList(column);
            final String indexName = genIndexName(dtype, tableName, cols, false);
            indexes.put(indexName, genIndex(dtype, t, indexName, vt == VoltType.GEOGRAPHY, cols, new ArrayList<>()));
        });
        if (vt.isVariableLength()) {
            return  4 + colSize * (inBytes ? 4 : 1);
        } else {
            return colSize;
        }
    }

    private static void validatePKeyColumnType(String pkeyName, Column col) {
        final VoltType vt = VoltType.get((byte) col.getType());
        if (! vt.isIndexable()) {
            throw new PlanningErrorException(String.format("Cannot create index \"%s\" because %s values " +
                            "are not currently supported as index keys: \"%s\"",
                    pkeyName, vt.getName(), col.getName()));
        } else if (! vt.isUniqueIndexable()) {
            throw new PlanningErrorException(String.format("Cannot create index \"%s\" because %s values " +
                            "are not currently supported as unique index keys: \"%s\"",
                    pkeyName, vt.getName(), col.getName()));
        }
    }

    private static void validateIndexColumnType(String indexName, AbstractExpression e) {
        StringBuffer sb = new StringBuffer();
        if (! e.isValueTypeIndexable(sb)) {
            throw new PlanningErrorException(String.format(
                    "Cannot create index \"%s\" because it contains %s, which is not supported.",
                    indexName, sb));
        } else if (! e.isValueTypeUniqueIndexable(sb)) {
            throw new PlanningErrorException(String.format(
                    "Cannot create unique index \"%s\" because it contains %s, which is not supported",
                    indexName, sb));
        }
    }

    private static void validateGeogPKey(String pkeyName, List<Column> cols) {
        if (cols.size() > 1) {
            cols.stream()
                    .filter(column -> column.getType() == VoltType.GEOGRAPHY.getValue()).findFirst()
                    .ifPresent(column -> {
                        throw new PlanningErrorException(String.format(
                                "Cannot create index %s because %s values must be the only component of an index key: \"%s\"",
                                pkeyName, VoltType.GEOGRAPHY.getName(), column.getName()));
                    });
        }
    }

    private static void validateGeogIndex(String indexName, List<AbstractExpression> exprs) {
        exprs.stream()
                .filter(expr -> expr.getValueType() == VoltType.GEOGRAPHY)
                .findFirst()
                .ifPresent(expr -> {
                    if (exprs.size() > 1) {
                        throw new PlanningErrorException(String.format(
                                "Cannot create index \"%s\" because %s values must be the only component of an index key.",
                                indexName, expr.getValueType().getName()));
                    } else if (! (expr instanceof TupleValueExpression)) {
                        throw new PlanningErrorException(String.format(
                                "Cannot create index \"%s\" because %s expressions must be simple value expressions.",
                                indexName, expr.getValueType().getName()));
                    }
                });
    }

    private static List<Integer> colIndicesOfIndex(Index index) {
        final Iterable<ColumnRef> iterCref = () -> index.getColumns().iterator();
        return StreamSupport
                .stream(iterCref.spliterator(), false)
                .mapToInt(cref -> cref.getColumn().getIndex())
                .boxed()
                .collect(Collectors.toList());
    }

    private static String genIndexName(SqlKind type, String tableName, List<Column> cols, boolean hasExpression) {
        final String prefix = type == SqlKind.PRIMARY_KEY ? "VOLTDB_AUTOGEN_IDX_PK" : "SYS_IDX";
        final String s =
                cols.stream().map(Column::getName).reduce(
                        String.format("%s_%s", prefix, tableName),
                        (acc, colName) -> acc + "_" + colName),
                suffix;
        if (hasExpression) {        // For index involving expression(s), generate a UID string as suffix, and pretending that
            // expression-groups for different constraints are never the same
            // (If they are, user will suffer some additional time cost when upserting table.)
            suffix = String.format("_%s", UUID.randomUUID().toString()).replace('-', '_');
        } else {
            suffix = "";
        }
        return s + suffix;
    }

    private static void updateConstraint(Constraint cons, Index index, SqlKind type, String indexName, String tableName) {
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
                throw new PlanningErrorException(String.format(
                        "Unsupported index type %s of index %s on table %s",
                        type.name(), indexName, tableName));
        }
        cons.setType(constraintType.getValue());
    }

    private static Index genIndex(SqlKind type, Table t, String indexName, boolean hasGeog,
                                  List<Column> indexCols, List<AbstractExpression> exprs) {
        final Index index = t.getIndexes().add(indexName);
        index.setIssafewithnonemptysources(false);    // TODO
        if (hasGeog) {
            index.setCountable(false);
            index.setType(IndexType.COVERING_CELL_INDEX.getValue());
        } else {
            index.setCountable(true);
            index.setType(IndexType.BALANCED_TREE.getValue());
        }
        index.setUnique(true);
        index.setAssumeunique(type == SqlKind.ASSUME_UNIQUE);
        updateConstraint(t.getConstraints().add(indexName), index, type, indexName, t.getTypeName());
        AtomicInteger i = new AtomicInteger(0);
        indexCols.forEach(column -> {
            final ColumnRef cref = index.getColumns().add(column.getName());
            cref.setColumn(column);
            cref.setIndex(i.getAndIncrement());
        });
        if (! exprs.isEmpty()) {
            try {
                index.setExpressionsjson(DDLCompiler.convertToJSONArray(exprs));
            } catch (JSONException e) {
                throw new PlanningErrorException(String.format(
                        "Unexpected error serializing non-column expressions for index '%s' on type '%s': %s",
                        indexName, t.getTypeName(), e.toString()));
            }
        }
        return index;
    }

    // NOTE/TODO: we get function ID from HSql, which we aim to get rid of.
    private static int getSqlFunId(String funName) {
        int funId = FunctionSQL.regularFuncMap.get(funName, -1);
        if (funId < 0) {
            funId = FunctionCustom.getFunctionId(funName);
        }
        return funId;
    }

    private static TupleValueExpression genIndexFromExpr(SqlIdentifier id, Table t) {
        final String colName = id.toString();
        assert(t.getColumns().get(colName) != null);
        return new TupleValueExpression(t.getTypeName(), colName, t.getColumns().get(colName).getIndex());
    }

    // Generate index from an expression
    private static AbstractExpression genIndexFromExpr(SqlBasicCall call, Table t) {
        final SqlOperator operator = call.getOperator();
        final AbstractExpression result;
        if (operator instanceof SqlFunction) {
            final SqlFunction calciteFunc = (SqlFunction) operator;
            final FunctionExpression expr = new FunctionExpression();
            final String funName = operator.getName();
            final int funId = getSqlFunId(funName);
            expr.setAttributes(funName, null, funId);
            expr.setArgs(call.getOperandList().stream().map(node -> {
                if (node instanceof SqlBasicCall) {
                    return genIndexFromExpr((SqlBasicCall) node, t);
                } else if (node instanceof SqlIdentifier){
                    return genIndexFromExpr((SqlIdentifier) node, t);
                } else {
                    throw new PlanningErrorException(String.format("Error parsing the function for index: %s",
                            node.toString()));
                }
            }).collect(Collectors.toList()));
            final VoltType returnType;
            switch (calciteFunc.getFunctionType()) {
                case STRING:
                    returnType = VoltType.STRING;
                    break;
                case NUMERIC:       // VoltDB does not have NUMERIC(len, prec), so this type promotion is safe.
                    returnType = VoltType.FLOAT;
                    break;
                case TIMEDATE:
                    returnType = VoltType.TIMESTAMP;
                    break;
                default:
                    throw new PlanningErrorException(String.format("Unsupported function return type %s for function %s",
                            calciteFunc.getFunctionType().toString(), funName));
            }
            expr.setValueType(returnType);
            result = expr;
        } else {
            List<AbstractExpression> exprs = call.getOperandList().stream().map(node -> {
                if (node instanceof SqlIdentifier) {
                    return genIndexFromExpr((SqlIdentifier) node, t);
                } else if (node instanceof SqlNumericLiteral) {
                    final SqlNumericLiteral literal = (SqlNumericLiteral) node;
                    final ConstantValueExpression e = new ConstantValueExpression();
                    e.setValue(literal.toValue());
                    e.setValueType(literal.isInteger() ? VoltType.BIGINT : VoltType.FLOAT);
                    return e;
                } else {
                    assert (node instanceof SqlBasicCall);
                    return genIndexFromExpr((SqlBasicCall) node, t);
                }
            }).collect(Collectors.toList());
            final ExpressionType op;
            if (operator instanceof SqlMonotonicBinaryOperator) {
                switch (call.getKind()) {
                    case PLUS:
                        op = ExpressionType.OPERATOR_PLUS;
                        break;
                    case MINUS:
                        op = ExpressionType.OPERATOR_MINUS;
                        break;
                    case TIMES:
                        op = ExpressionType.OPERATOR_MULTIPLY;
                        break;
                    case DIVIDE:
                        op = ExpressionType.OPERATOR_DIVIDE;
                        break;
                    case MOD:
                        op = ExpressionType.OPERATOR_MOD;
                        break;
                    default:
                        throw new PlanningErrorException(String.format("Found unexpected binary expression operator \"%s\" in %s",
                                call.getOperator().getName(), call.toString()));
                }
                result = new OperatorExpression(op, exprs.get(0), exprs.get(1));
            } else if (operator instanceof SqlPrefixOperator) {
                switch (call.getKind()) {
                    case MINUS_PREFIX:
                        result = new OperatorExpression(ExpressionType.OPERATOR_UNARY_MINUS, exprs.get(0), null);
                        break;
                    case PLUS_PREFIX:       // swallow unary plus
                        result = exprs.get(0);
                        break;
                    default:
                        throw new PlanningErrorException(String.format("Found unexpected unary expression operator \"%s\" in %s",
                                call.getOperator().getName(), call.toString()));
                }
            } else {
                throw new PlanningErrorException(String.format("Found Unknown expression operator \"%s\" in %s",
                        call.getOperator().getName(), call.toString()));
            }
        }
        result.resolveForTable(t);
        result.finalizeValueTypes();
        return result;
    }

    public static void procTTLStmt(Table t, SqlCreateTable.TtlConstraint constraint) {
        if (constraint == null) {
            return;
        } else if (constraint.getDuration() <= 0) {
            throw new PlanningErrorException(String.format("Error: TTL on table %s must be positive: got %d",
                    t.getTypeName(), constraint.getDuration()));
        } else {
            final String ttlColumnName = constraint.getColumn().toString();
            final Column ttlColumn = t.getColumns().get(ttlColumnName);
            if (ttlColumn == null) {
                throw new PlanningErrorException(String.format(
                        "Error: TTL column %s does not exist in table %s", ttlColumnName, t.getTypeName()));
            } else if (! VoltType.get((byte) ttlColumn.getType()).isBackendIntegerType()) {
                throw new PlanningErrorException(String.format(
                        "Error: TTL column %s of type %s in table %s is not allowed. Allowable column types are: %s, %s, %s or %s.",
                        ttlColumnName, VoltType.get((byte) ttlColumn.getType()).getName(), t.getTypeName(),
                        VoltType.INTEGER.getName(), VoltType.TINYINT.getName(),
                        VoltType.BIGINT.getName(), VoltType.TIMESTAMP.getName()));
            } else if (ttlColumn.getNullable()) {
                throw new PlanningErrorException(String.format(
                        "Error: TTL column %s in table %s is required to be NOT NULL.", ttlColumnName, t.getTypeName()));
            }
            final TimeToLive ttl = t.getTimetolive().add("ttl"); // was set to TimeToLiveVoltDB.TTL_NAME: TimeToLiveVoltDB was a patch to HSQL
            ttl.setTtlcolumn(ttlColumn);
            ttl.setTtlvalue(constraint.getDuration());
            // The TTL batch size and max frequencies were set by HSQL to be these fixed values, and are never used anywhere.
            ttl.setBatchsize(1000);
            ttl.setMaxfrequency(1);
            ttl.setTtlunit(constraint.getUnit().name());
        }
    }

    private static Pair<Statement, VoltXMLElement> addLimitPartitionRowsConstraint(
            Table t, HSQLInterface hsql, SqlLimitPartitionRowsConstraint constraint) {
        final int rowCount = constraint.getRowCount();
        final SqlDelete delete = constraint.getDelStmt();
        final String sql = delete.toSqlString(VoltSqlDialect.DEFAULT)
                .toString().replace('\n', ' ');
        assert (delete.getTargetTable().toString().equals(t.getTypeName()));
        t.setTuplelimit(rowCount);
        final Statement stmt = t.getTuplelimitdeletestmt().add("limit_delete");
        stmt.setSqltext(sql);
        try { // Note: this ugliness came from how StatementCompiler.compileStatementAndUpdateCatalog()
            // does its bidding; and from how HSQLInterface.getXMLCompiledStatement() needs the HSQL session
            // for its work.
            return Pair.of(stmt, hsql.getXMLCompiledStatement(stmt.getSqltext()));
        } catch (HSQLInterface.HSQLParseException ex) {
            throw new PlanningErrorException(ex.getMessage());
        }
    }

    private static void addTableAnnotation(Table t) {
        // TODO: assuming that no mat view is associated with t; and t is not a streaming table
        final TableAnnotation annotation = new TableAnnotation();
        annotation.ddl = CatalogSchemaTools.toSchema(new StringBuilder(), t,
                null, false, null, null);
        t.setAnnotation(annotation);
    }

    // NOTE: sadly, HSQL is pretty deeply ingrained and it's hard to make this method independent of HSQL.
    public static Pair<SchemaPlus, Pair<Statement, VoltXMLElement>> addTable(
            SqlNode node, HSQLInterface hsql, Database db) {
        if (node.getKind() != SqlKind.CREATE_TABLE) {           // for now, only partially support CREATE TABLE stmt
            return Pair.of(CatalogAdapter.schemaPlusFromDatabase(db), null);
        }
        final List<SqlNode> nameAndColListAndQuery = ((SqlCreateTable) node).getOperandList();
        final String tableName = nameAndColListAndQuery.get(0).toString();
        final SqlNodeList nodeTableList = (SqlNodeList) nameAndColListAndQuery.get(1);
        final Table t = db.getTables().add(tableName);
        t.setTuplelimit(Integer.MAX_VALUE);
        t.setIsreplicated(true);

        final int numCols = nodeTableList.getList().size();
        if (numCols > DDLCompiler.MAX_COLUMNS) {
            throw new PlanningErrorException(String.format("Table %s has %d columns (max is %d)",
                    tableName, numCols, DDLCompiler.MAX_COLUMNS));
        }
        int rowSize = 0;
        final AtomicInteger index = new AtomicInteger(0);
        final SortedMap<Integer, VoltType> columnTypes = new TreeMap<>();
        final Map<String, Index> indexMap = new HashMap<>();
        Pair<Statement, VoltXMLElement> limitRowCatalogUpdate = null;
        for (SqlNode col : nodeTableList) {
            switch (col.getKind()) {
                case PRIMARY_KEY:        // PKey and Unique are treated almost in the same way, except naming and table constraint type.
                case UNIQUE:
                case ASSUME_UNIQUE:
                    final List<SqlNode> nodes = ((SqlKeyConstraint) col).getOperandList();
                    assert(nodes.size() == 2);      // for un-named constraint, the 1st elm is null; else 1st is SqlIdentifier storing constraint name.
                    final List<SqlNode> sqls = ((SqlNodeList) nodes.get(1)).getList(),
                            exprs = sqls.stream().filter(n -> n instanceof SqlBasicCall).collect(Collectors.toList()),
                            cols = sqls.stream().filter(n -> ! (n instanceof SqlBasicCall) && ! (n instanceof SqlLiteral))   // ignore constant
                                    .collect(Collectors.toList());
                    final List<AbstractExpression> voltExprs = exprs.stream()
                            .map(call -> genIndexFromExpr((SqlBasicCall) call, t))
                            .collect(Collectors.toList());
                    final List<Column> pkCols = cols.stream()
                            .map(c -> t.getColumns().get(c.toString()))
                            .collect(Collectors.toList());
                    final String indexName = nodes.get(0) == null ?
                            genIndexName(col.getKind(), tableName, pkCols, ! voltExprs.isEmpty()) :
                            nodes.get(0).toString();
                    if (nodes.get(1) != null && indexMap.containsKey(indexName)) {  // On detection of duplicated constraint name: bail
                        throw new PlanningErrorException(String.format(
                                "A constraint named %s already exists on table %s.", indexName, tableName));
                    }
                    pkCols.forEach(column -> validatePKeyColumnType(indexName, column));
                    validateGeogPKey(indexName, pkCols);
                    voltExprs.forEach(e -> validateIndexColumnType(indexName, e));
                    final StringBuffer sb = new StringBuffer(String.format("index %s", indexName));
                    if (! AbstractExpression.validateExprsForIndexesAndMVs(voltExprs, sb, false)) {
                        throw new PlanningErrorException(sb.toString());
                    }
                    validateGeogIndex(indexName, voltExprs);
                    final boolean hasGeogType =
                            pkCols.stream().anyMatch(column -> column.getType() == VoltType.GEOGRAPHY.getValue()) ||
                                    voltExprs.stream().anyMatch(expr -> expr.getValueType() == VoltType.GEOGRAPHY);
                    final Index indexConstraint = genIndex(col.getKind(), t, indexName, hasGeogType, pkCols, voltExprs);
                    if (indexMap.values().stream()       // find dup index
                            .noneMatch(
                                    other -> other.getType() == indexConstraint.getType() &&
                                            other.getCountable() == indexConstraint.getCountable() &&
                                            other.getUnique() == indexConstraint.getUnique() &&
                                            other.getAssumeunique() == indexConstraint.getAssumeunique() &&
                                            other.getColumns().size() == indexConstraint.getColumns().size() && // skip expression comparison
                                            colIndicesOfIndex(other).equals(colIndicesOfIndex(indexConstraint)))) {
                        indexMap.put(indexName, indexConstraint);
                    }
                    break;
                case LIMIT_PARTITION_ROWS:
                    limitRowCatalogUpdate = addLimitPartitionRowsConstraint(t, hsql, (SqlLimitPartitionRowsConstraint) col);
                    break;
                case COLUMN_DECL:
                    rowSize += addColumn(new SqlColumnDeclarationWithExpression((SqlColumnDeclaration) col),
                            t, tableName, index, columnTypes, indexMap);
                    if (rowSize > DDLCompiler.MAX_ROW_SIZE) {       // fails when the accumulative row size exeeds limit
                        throw new PlanningErrorException(String.format(
                                "Error: table %s has a maximum row size of %s but the maximum supported size is %s",
                                tableName, VoltType.humanReadableSize(rowSize), VoltType.humanReadableSize(DDLCompiler.MAX_ROW_SIZE)));
                    }
                    break;
                default:
            }
        }
        t.setSignature(CatalogUtil.getSignatureForTable(tableName, columnTypes));
        procTTLStmt(t, ((SqlCreateTable) node).getTtlConstraint());
        addTableAnnotation(t);
        return Pair.of(CatalogAdapter.schemaPlusFromDatabase(db), limitRowCatalogUpdate);
    }
}
