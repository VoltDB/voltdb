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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlColumnDeclarationWithExpression;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.ddl.SqlLimitPartitionRowsConstraint;
import org.apache.calcite.sql.dialect.VoltSqlDialect;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.hsqldb_voltpatches.FunctionCustom;
import org.hsqldb_voltpatches.FunctionSQL;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TimeToLive;
import org.voltdb.catalog.org.voltdb.calciteadaptor.CatalogAdapter;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compilereport.TableAnnotation;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.sysprocs.org.voltdb.calciteadapter.ColumnType;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.CatalogSchemaTools;
import org.voltdb.utils.CatalogUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Util class to help with "CREATE TABLE" DDL statements through Calcite.
 */
public class CreateTableUtils {

    private static void except(String msg) {
        throw new PlanningErrorException(msg, 0);
    }
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
        } else if (size > VoltType.MAX_VALUE_LENGTH) {
            except(String.format("%s column %s in table %s has unsupported length %s",
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
            except(String.format(
                    "%s column %s in table %s has length of %d which is shorter than %d, the minimum length allowed for the type.",
                    vt.toSQLString(), colName, tableName, size, vt.getMinLengthInBytes()));
        }
        if (vt == VoltType.STRING && ! inBytes &&
                size * DDLCompiler.MAX_BYTES_PER_UTF8_CHARACTER > VoltType.MAX_VALUE_LENGTH) {
            except(String.format(
                    "Column %s.%s specifies a maixmum size of %d characters but the maximum supported size is %s characters or %s bytes",
                    tableName, colName, size,
                    VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH / DDLCompiler.MAX_BYTES_PER_UTF8_CHARACTER),
                    VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH)));
        }
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
        if (! funName.equals("NOW") && ! funName.equals("CURRENT_TIMESTAMP")) {
            except(String.format("Function %s not allowed for DEFAULT value of column declaration", funName));
        } else if (vt != VoltType.TIMESTAMP) {
            except(String.format("Function %s not allowed for DEFAULT value of column \"%s\" of %s type",
                    funName, colName, vt.getName()));
        }
        return "CURRENT_TIMESTAMP:43";
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
    private static int addColumn(SqlColumnDeclarationWithExpression col, Table t, String tableName,
                                 AtomicInteger index, Map<Integer, VoltType> columnTypes) {
        final List<SqlNode> nameAndType = col.getOperandList();
        final String colName = nameAndType.get(0).toString();
        final Column column = t.getColumns().add(colName);
        column.setName(colName);

        final SqlDataTypeSpec type = (SqlDataTypeSpec) nameAndType.get(1);
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
                final String defaultValue;
                if (expr instanceof SqlNumericLiteral) {
                    defaultValue = ((SqlNumericLiteral) expr).getValue().toString();
                } else {
                    assert expr instanceof SqlCharStringLiteral;
                    defaultValue = ((SqlCharStringLiteral) expr).toValue();
                }
                column.setDefaultvalue(defaultValue);
            } else {
                except(String.format(
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
     * Validate that a column declared to be PRIMARY KEY is actually capable of being a PRIMARY KEY
     * @param pkeyName name of the primary key
     * @param col column declared to be primary key
     */
    private static void validatePKeyColumnType(String pkeyName, Column col) {
        final VoltType vt = VoltType.get((byte) col.getType());
        if (! vt.isIndexable()) {
            except(String.format("Cannot create index \"%s\" because %s values " +
                            "are not currently supported as index keys: \"%s\"",
                    pkeyName, vt.getName(), col.getName()));
        } else if (! vt.isUniqueIndexable()) {
            except(String.format("Cannot create index \"%s\" because %s values " +
                            "are not currently supported as unique index keys: \"%s\"",
                    pkeyName, vt.getName(), col.getName()));
        }
    }

    /**
     * Validate that a UNIQUE or ASSUMEUNIQUE column/expression is actually capable of assuming such index.
     * @param indexName name of the index
     * @param e expression containing the list of column(s) that is UNIQUE or ASSUMEUNIQUE
     */
    private static void validateIndexColumnType(String indexName, AbstractExpression e) {
        StringBuffer sb = new StringBuffer();
        if (! e.isValueTypeIndexable(sb)) {
            except(String.format(
                    "Cannot create index \"%s\" because it contains %s, which is not supported.",
                    indexName, sb));
        } else if (! e.isValueTypeUniqueIndexable(sb)) {
            except(String.format(
                    "Cannot create unique index \"%s\" because it contains %s, which is not supported",
                    indexName, sb));
        }
    }

    /**
     * Validate that the list of columns declared to be primary key, when containing more than one column, shall not
     * contain any GEOGRAPHY column.
     * @param pkeyName primary key name
     * @param cols list of columns declared to be primary key
     */
    private static void validateGeogPKey(String pkeyName, List<Column> cols) {
        if (cols.size() > 1) {
            cols.stream()
                    .filter(column -> column.getType() == VoltType.GEOGRAPHY.getValue()).findFirst()
                    .ifPresent(column -> {
                        except(String.format(
                                "Cannot create index %s because %s values must be the only component of an index key: \"%s\"",
                                pkeyName, VoltType.GEOGRAPHY.getName(), column.getName()));
                    });
        }
    }

    /**
     * Validate that the **expression** declared to be UNIQUE or ASSUMEUNIQUE cannot contain any GEOGRAPHY column.
     * e.g. CREATE TABLE foo(i smallint, j bigint, unique(i + j)) is OK; while
     * CREATE TABLE foo(i GEOGRAPHY, UNIQUE(NumInteriorRings(i)) is not
     * @param indexName index name
     * @param exprs UNIQUE/ASSUMEUNIQUE expression
     */
    private static void validateGeogIndex(String indexName, List<AbstractExpression> exprs) {
        exprs.stream()
                .filter(expr -> expr.getValueType() == VoltType.GEOGRAPHY)
                .findFirst()
                .ifPresent(expr -> {
                    if (exprs.size() > 1) {
                        except(String.format(
                                "Cannot create index \"%s\" because %s values must be the only component of an index key.",
                                indexName, expr.getValueType().getName()));
                    } else if (! (expr instanceof TupleValueExpression)) {
                        except(String.format(
                                "Cannot create index \"%s\" because %s expressions must be simple value expressions.",
                                indexName, expr.getValueType().getName()));
                    }
                });
    }

    /**
     * Get the list of columns that an index is on. Used in index dedup upon index creation time.
     * Remember that an index on column a, b is different from an index on column b, a.
     * @param index catalog index object
     * @return a list of columns, possibly empty (when index is purely made up of expressions),
     * that is contained by the index.
     */
    private static List<Integer> colIndicesOfIndex(Index index) {
        final Iterable<ColumnRef> iterCref = () -> index.getColumns().iterator();
        return StreamSupport
                .stream(iterCref.spliterator(), false)
                .mapToInt(cref -> cref.getColumn().getIndex())
                .boxed()
                .collect(Collectors.toList());
    }

    /**
     * Generate index name and constraint name for the given SqlTyppe and, either a list of columns, or an expression.
     * We need two names here to be compatible with what VoltDB had been doing; but having the index and constraint to
     * be the same name does not seems to cause any trouble: indexes and constraints are in different catalog spaces.
     * We need two names with same naming scheme to pass all Jenkins tests.
     * @param type constraint type
     * @param tableName name of table of the constraint
     * @param cols list of columns for the index
     * @param hasExpression whether the constraint contains an expression, e.g. CREATE TABLE foo(i SMALLINT, j BIGINT, UNIQUE(i, j - i));
     * @return generated (IndexName, ConstraintName)
     */
    private static Pair<String, String> genIndexAndConstraintName(
            SqlNode constraintName, SqlKind type, String tableName, List<Column> cols, boolean hasExpression) {
        if (constraintName != null) {
            return Pair.of("VOLTDB_AUTOGEN_CONSTRAINT_IDX_" + constraintName.toString(), constraintName.toString());
        } else {
            final String suffix,
                    type_str = type == SqlKind.PRIMARY_KEY ? "PK" : "CT",
                    index = cols.stream().map(Column::getName)
                            .reduce(String.format("VOLTDB_AUTOGEN_IDX_%s_%s", type_str, tableName),
                                    (acc, colName) -> acc + "_" + colName),
                    constr = cols.stream().map(Column::getName)
                            .reduce(String.format("VOLTDB_AUTOGEN_CT__%s_%s", type_str, tableName),
                                    (acc, colName) -> acc + "_" + colName);
            if (hasExpression) {        // For index involving expression(s), generate a UID string as suffix, and pretending that
                // expression-groups for different constraints are never the same
                // (If they are, user will suffer some additional time cost when upserting table.)
                suffix = String.format("_%s", UUID.randomUUID().toString()).replace('-', '_');
            } else {
                suffix = "";
            }
            return Pair.of(index + suffix, constr + suffix);
        }
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
                except(String.format("Unsupported index type %s of index %s on table %s",
                        type.name(), indexName, tableName));
        }
        cons.setType(constraintType.getValue());
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
    private static Index genIndex(SqlKind type, Table t, String indexName, String constraintName, boolean hasGeog,
                                  List<Column> indexCols, List<AbstractExpression> exprs) {
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
            try {
                index.setExpressionsjson(DDLCompiler.convertToJSONArray(exprs));
            } catch (JSONException e) {
                except(String.format(
                        "Unexpected error serializing non-column expressions for index '%s' on type '%s': %s",
                        indexName, t.getTypeName(), e.toString()));
            }
        }
        final AbstractExpression.UnsafeOperatorsForDDL unsafeOp = new AbstractExpression.UnsafeOperatorsForDDL();
        exprs.forEach(expr -> expr.findUnsafeOperatorsForDDL(unsafeOp));
        index.setIssafewithnonemptysources(! unsafeOp.isUnsafe());
        return index;
    }

    /**
     * Get the SQL function id recognized by EE.
     * TODO: Now we get function ID from HSql, which we should get rid of eventually.
     * @param funName name of SQL function
     * @return function ID, or -1 if not found.
     */
    private static int getSqlFunId(String funName) {
        int funId = FunctionSQL.regularFuncMap.get(funName, -1);
        if (funId < 0) {
            funId = FunctionCustom.getFunctionId(funName);
        }
        return funId;
    }

    /**
     * Convert a Calcite SqlIdentifier referring to a columne, and the table, to a TupleValueExpression.
     * @param id identifier for the column
     * @param t table Catalog object
     * @return create TVE object
     */
    private static TupleValueExpression toTVE(SqlIdentifier id, Table t) {
        final String colName = id.toString();
        assert(t.getColumns().get(colName) != null);
        return new TupleValueExpression(t.getTypeName(), colName, t.getColumns().get(colName).getIndex());
    }

    /**
     * Convert Calcite expression on a table (e.g. default/unique/pkey expression) to a VoltDB expression.
     * @param call Calcite expression
     * @param t the table that the expression is based on
     * @return VoltDB expression
     */
    private static AbstractExpression genIndexFromExpr(SqlBasicCall call, Table t) {
        final SqlOperator operator = call.getOperator();
        final AbstractExpression result;
        if (operator instanceof SqlFunction) {
            final SqlFunction calciteFunc = (SqlFunction) operator;
            final FunctionExpression expr = new FunctionExpression();
            final String funName = operator.getName();
            final int funId = getSqlFunId(funName);
            assert funId > 0 : String.format("Unrecognized function %s", funName);
            expr.setAttributes(funName, null, funId);
            expr.setArgs(call.getOperandList().stream().flatMap(node -> {
                if (node instanceof SqlBasicCall) {
                    return Stream.of(genIndexFromExpr((SqlBasicCall) node, t));
                } else if (node instanceof SqlIdentifier){
                    return Stream.of(toTVE((SqlIdentifier) node, t));
                } else {
                    except(String.format("Error parsing the function for index: %s", node.toString()));
                    return Stream.empty();
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
                    returnType = null;
                    except(String.format("Unsupported function return type %s for function %s",
                            calciteFunc.getFunctionType().toString(), funName));
            }
            expr.setValueType(returnType);
            result = expr;
        } else {
            List<AbstractExpression> exprs = call.getOperandList().stream().map(node -> {
                if (node instanceof SqlIdentifier) {
                    return toTVE((SqlIdentifier) node, t);
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
                        op = null;
                        except(String.format("Found unexpected binary expression operator \"%s\" in %s",
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
                        result = null;
                        except(String.format("Found unexpected unary expression operator \"%s\" in %s",
                                call.getOperator().getName(), call.toString()));
                }
            } else {
                result = null;
                except(String.format("Found Unknown expression operator \"%s\" in %s",
                        call.getOperator().getName(), call.toString()));
            }
        }
        result.resolveForTable(t);
        result.finalizeValueTypes();
        return result;
    }

    /**
     * Process a TTL statement in CREATE TABLE, e.g. CREATE TABLE foo(i int, ...) USING TTL 5 hours on COLUMN i;
     * @param t table for the TTL statement
     * @param constraint Calcite object containing all we need to know for the TTL constraint
     */
    public static void procTTLStmt(Table t, SqlCreateTable.TtlConstraint constraint) {
        if (constraint == null) {
            return;
        } else if (constraint.getDuration() <= 0) {
            except(String.format("Error: TTL on table %s must be positive: got %d",
                    t.getTypeName(), constraint.getDuration()));
        } else {
            final String ttlColumnName = constraint.getColumn().toString();
            final Column ttlColumn = t.getColumns().get(ttlColumnName);
            if (ttlColumn == null) {
                except(String.format(
                        "Error: TTL column %s does not exist in table %s", ttlColumnName, t.getTypeName()));
            } else if (! VoltType.get((byte) ttlColumn.getType()).isBackendIntegerType()) {
                except(String.format(
                        "Error: TTL column %s of type %s in table %s is not allowed. Allowable column types are: %s, %s, %s or %s.",
                        ttlColumnName, VoltType.get((byte) ttlColumn.getType()).getName(), t.getTypeName(),
                        VoltType.INTEGER.getName(), VoltType.TINYINT.getName(),
                        VoltType.BIGINT.getName(), VoltType.TIMESTAMP.getName()));
            } else if (ttlColumn.getNullable()) {
                except(String.format(
                        "Error: TTL column %s in table %s is required to be NOT NULL.", ttlColumnName, t.getTypeName()));
            }
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
     * Helper function
     * @param sql Calcite SQL node under inspection
     * @param init accumulator
     * @return collected SQL function calls in the Calcite SQL node
     */
    private static List<SqlBasicCall> collectFilterFunctions(SqlNode sql, List<SqlBasicCall> init) {
        if (sql instanceof SqlBasicCall) {
            final SqlBasicCall call = (SqlBasicCall) sql;
            init.add(call);
            call.getOperandList().forEach(op -> collectFilterFunctions(op, init));
        }
        return init;
    }

    /**
     * Collect all SQL function calls contained inside DELETE statement of LIMIT PARTITION ROWS constraint.
     * @param delete the DELETE statement of LIMIT PARTITION ROWS constraint
     * @return a list of all SQL function calls in the DELETE statement.
     */
    private static List<SqlBasicCall> collectFilterFunctions(SqlDelete delete) {
        final SqlNode condition = delete.getCondition();
        if (condition == null) {
            return Collections.emptyList();
        } else {
            return collectFilterFunctions(condition, new ArrayList<>());
        }
    }

    /**
     * Add a LIMIT PARTITION ROWS constraint to table, as in
     * CREATE TABLE foo(i int, .., LIMIT PARTITION ROWS 1000 EXECUTE(DELETE FROM foo WHERE i > 500));
     * @param t Source table for the constraint
     * @param hsql HSQL session. We should eventually get rid of this; but the call stack is deeply winded how it is
     *             passed down and used.
     * @param constraint Calcite constraint for LIMIT PARTITION ROWS
     * @return values necessary to pass down to the call chain to do the actual settings for the constraint.
     */
    private static Pair<Statement, VoltXMLElement> addLimitPartitionRowsConstraint(
            Table t, HSQLInterface hsql, SqlLimitPartitionRowsConstraint constraint) {
        final SqlDelete delete = constraint.getDelStmt();
        final String sql = delete.toSqlString(VoltSqlDialect.DEFAULT).toString().replace('\n', ' ');
        if (! delete.getTargetTable().toString().equals(t.getTypeName())) {
            except(String.format(
                    "Error: the source table (%s) of DELETE statement of LIMIT PARTITION constraint (%s) does not match" +
                            " the table being created (%s)", delete.getTargetTable().toString(), t.getTypeName(), sql));
        }
        collectFilterFunctions(delete).stream().flatMap(call -> {
            if (call.getOperator() instanceof SqlFunction &&
                    ((SqlFunction) call.getOperator()).getFunctionType() == SqlFunctionCategory.USER_DEFINED_FUNCTION) {
                final SqlFunction function = (SqlFunction) call.getOperator();
                return Stream.of(function.getSqlIdentifier().getSimple());
            } else {
                return Stream.empty();
            }
        }).findAny().ifPresent(name -> {
            except(String.format(
                    "Error: Table %s has invalid DELETE statement for LIMIT PARTITION ROWS constraint: " +
                            "user defined function calls are not supported: \"%s\"",
                    t.getTypeName(), name.toLowerCase()));
        });
        t.setTuplelimit(constraint.getRowCount());
        final Statement stmt = t.getTuplelimitdeletestmt().add("limit_delete");
        stmt.setSqltext(sql);
        try { // Note: this ugliness came from how StatementCompiler.compileStatementAndUpdateCatalog()
            // does its bidding; and from how HSQLInterface.getXMLCompiledStatement() needs the HSQL session
            // for its work.
            return Pair.of(stmt, hsql.getXMLCompiledStatement(sql));
        } catch (HSQLInterface.HSQLParseException ex) {
            except(ex.getMessage());
            return null;
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
                null, false, null, null);
        t.setAnnotation(annotation);
    }

    // NOTE: sadly, HSQL is pretty deeply ingrained and it's hard to make this method independent of HSQL.

    /**
     * API to use for CREATE TABLE ddl stmt.
     * @param node Calcite SqlNode for parsed CREATE TABLE stmt
     * @param hsql HSQL session used for LIMIT PARTITION ROWS constraint
     * @param db catalog database object to insert created table
     * @return brand new SchemaPlus object containing the table, together with all other pre-existing catalog objects
     * found in the database; and arguments needed by DDLCompiler.addTableToCatalog() method as this API's only caller.
     */
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
            except(String.format("Table %s has %d columns (max is %d)", tableName, numCols, DDLCompiler.MAX_COLUMNS));
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
                    final Pair<String, String> indexAndConstraintNames = genIndexAndConstraintName(
                            nodes.get(0), col.getKind(), tableName, pkCols, ! voltExprs.isEmpty());
                    final String indexName = indexAndConstraintNames.getFirst(), constraintName = indexAndConstraintNames.getSecond();
                    if (nodes.get(1) != null && indexMap.containsKey(indexName)) {  // On detection of duplicated constraint name: bail
                        except(String.format(
                                "A constraint named %s already exists on table %s.", indexName, tableName));
                    }
                    pkCols.forEach(column -> validatePKeyColumnType(indexName, column));
                    validateGeogPKey(indexName, pkCols);
                    voltExprs.forEach(e -> validateIndexColumnType(indexName, e));
                    final StringBuffer sb = new StringBuffer(String.format("index %s", indexName));
                    if (! AbstractExpression.validateExprsForIndexesAndMVs(voltExprs, sb, false)) {
                        except(sb.toString());
                    }
                    validateGeogIndex(indexName, voltExprs);
                    final boolean hasGeogType =
                            pkCols.stream().anyMatch(column -> column.getType() == VoltType.GEOGRAPHY.getValue()) ||
                                    voltExprs.stream().anyMatch(expr -> expr.getValueType() == VoltType.GEOGRAPHY);
                    final Index indexConstraint = genIndex(col.getKind(), t, indexName, constraintName,
                            hasGeogType, pkCols, voltExprs);
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
                            t, tableName, index, columnTypes);
                    if (rowSize > DDLCompiler.MAX_ROW_SIZE) {       // fails when the accumulative row size exeeds limit
                        except(String.format(
                                "Error: table %s has a maximum row size of %s but the maximum supported size is %s",
                                tableName, VoltType.humanReadableSize(rowSize), VoltType.humanReadableSize(DDLCompiler.MAX_ROW_SIZE)));
                    }
                    break;
                default:
            }
        }
        t.setSignature(CatalogUtil.getSignatureForTable(tableName, columnTypes));
        procTTLStmt(t, ((SqlCreateTable) node).getTtlConstraint());
        addAnnotation(t);
        return Pair.of(CatalogAdapter.schemaPlusFromDatabase(db), limitRowCatalogUpdate);
    }
}
