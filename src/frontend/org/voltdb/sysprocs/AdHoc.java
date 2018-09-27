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

package org.voltdb.sysprocs;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlColumnDeclarationWithExpression;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.voltcore.utils.Pair;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.org.voltdb.calciteadaptor.CatalogAdapter;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compilereport.TableAnnotation;
import org.voltdb.newplanner.SqlBatch;
import org.voltdb.newplanner.SqlTask;
import org.voltdb.parser.SQLLexer;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.sysprocs.org.voltdb.calciteadapter.ColumnType;
import org.voltdb.utils.CatalogUtil;

public class AdHoc extends AdHocNTBase {

    /**
     * Run an AdHoc query batch through the Calcite planner.
     * @param params the user parameters. The first parameter is always the query text.
     * The rest parameters are the ones used in the queries. </br>
     * Some notes:
     * <ul>
     *   <li>AdHoc DDLs do not take parameters ("?" will be treated as an unexpected token);</li>
     *   <li>Currently, a non-DDL batch can take parameters only if the batch has one query.</li>
     *   <li>We do not handle large query mode now. The special flag for swap tables is also
     *       eliminated. They both need to be re-designed in the new Calcite framework.</li>
     * </ul>
     * @return the client response.
     * @since 8.4
     * @author Yiqun Zhang
     */
    public CompletableFuture<ClientResponse> runThroughCalcite(ParameterSet params) {
        // TRAIL [Calcite:1] [entry] AdHoc.runThroughCalcite()
        SqlBatch batch;
        try {
            // We do not need to worry about the ParameterSet,
            // AdHocAcceptancePolicy will sanitize the parameters ahead of time.
            batch = SqlBatch.fromParameterSet(params);
            if (batch.isDDLBatch()) {
                return runDDLBatchThroughCalcite(batch);
            } else {
                // Large query mode should be set to m_backendTargetType.isLargeTempTableTarget
                // But for now let's just disable it.
                return runNonDDLBatchThroughCalcite(batch);
            }
        } catch (Exception ex) {
            // For now, let's just fail the batch if any error happens.
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE,
                                     ex.getLocalizedMessage());
        }
    }

    private boolean shouldBeUsingCalcite() {
        if (m_backendTargetType.isLargeTempTableTarget) {
            return false;
        }
        return true;
    }

    public CompletableFuture<ClientResponse> run(ParameterSet params) {
        // TRAIL [Calcite:0] [entry] AdHoc.run()
        if (shouldBeUsingCalcite()) {
            return runThroughCalcite(params);
        }
        if (params.size() == 0) {
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE,
                    "Adhoc system procedure requires at least the query parameter.");
        }

        Object[] paramArray = params.toArray();
        String sql = (String) paramArray[0];
        Object[] userParams = null;
        if (params.size() > 1) {
            userParams = Arrays.copyOfRange(paramArray, 1, paramArray.length);
        }

        List<String> sqlStatements = new ArrayList<>();
        AdHocSQLMix mix = processAdHocSQLStmtTypes(sql, sqlStatements);

        if (mix == AdHocSQLMix.EMPTY) {
            // we saw neither DDL or DQL/DML.  Make sure that we get a
            // response back to the client
            return makeQuickResponse(
                    ClientResponse.GRACEFUL_FAILURE,
                    "Failed to plan, no SQL statement provided.");
        }

        else if (mix == AdHocSQLMix.MIXED) {
            // No mixing DDL and DML/DQL.  Turn this into an error returned to client.
            return makeQuickResponse(
                    ClientResponse.GRACEFUL_FAILURE,
                    "DDL mixed with DML and queries is unsupported.");
        }

        else if (mix == AdHocSQLMix.ALL_DML_OR_DQL) {
            // this is where we run non-DDL sql statements
            return runNonDDLAdHoc(VoltDB.instance().getCatalogContext(),
                                  sqlStatements,
                                  true, // infer partitioning
                                  null, // no partition key
                                  ExplainMode.NONE,
                                  m_backendTargetType.isLargeTempTableTarget, // back end dependent.
                                  false, // is not swap tables
                                  userParams);
        }

        // at this point assume all DDL
        assert(mix == AdHocSQLMix.ALL_DDL);
        // Since we are not going through Calcite, there is no need to update CalciteSchema.
        return runDDLBatch(sqlStatements, new ArrayList<>());
    }

    /**
     * Run a DDL batch through Calcite.
     * @param batch the batch to run.
     * @return the client response.
     * @since 8.4
     * @author Yiqun Zhang
     */
    private CompletableFuture<ClientResponse> runDDLBatchThroughCalcite(SqlBatch batch) {
        // We batch SqlNode with original SQL DDL stmts together, because we need both.
        final List<Pair<String, SqlNode>> args =
                StreamSupport.stream(((Iterable<SqlTask>) () -> batch.iterator()).spliterator(), false)
                .map(task -> Pair.of(task.getSQL(), task.getParsedQuery()))
                .collect(Collectors.toList());
        return runDDLBatch(args.stream().map(Pair::getFirst).collect(Collectors.toList()),
                args.stream().map(Pair::getSecond).collect(Collectors.toList()));
    }

    private static Pair<Integer, Boolean> validateVarLenColumn(VoltType vt, String tableName, String colName, int size, boolean inBytes) {
        if (size < 0) {        // user did not provide, e.g. CREATE TABLE t(i VARCHAR);
            size = vt.defaultLengthForVariableLengthType();
        } else if (size > VoltType.MAX_VALUE_LENGTH) {
            throw new PlanningErrorException(String.format("%s column %s in table %s has unsupported length %s",
                    vt.toSQLString(), colName, VoltType.humanReadableSize(size)));
        } else if (vt == VoltType.STRING && size > VoltType.MAX_VALUE_LENGTH_IN_CHARACTERS) {
            System.err.println(String.format(
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

    private static int addColumn(SqlColumnDeclarationWithExpression col, Table t, String tableName,
                                 AtomicInteger index, Map<Integer, VoltType> columnTypes) {
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
        boolean inBytes = col.getDataType().getInBytes();
        if (vt.isVariableLength()) {     // user did not specify a size. Set to default value
            final Pair<Integer, Boolean> r = validateVarLenColumn(vt, tableName, colName, colSize, inBytes);
            colSize = r.getFirst();
            inBytes = r.getSecond();
        } else if (colSize < 0) {
            colSize = vt.getLengthInBytesForFixedTypesWithoutCheck();
        }
        final int rowSizeDelta;
        if (vt.isVariableLength()) {
            rowSizeDelta = 4 + colSize * (inBytes ? 4 : 1);
        } else {
            rowSizeDelta = colSize;
        }
        column.setSize(colSize);
        column.setIndex(index.getAndIncrement());
        columnTypes.put(index.get(), vt);
        column.setInbytes(inBytes);
        final SqlNode expr = col.getExpression();
        if (expr != null) {
            column.setDefaulttype(vt.getValue());
            column.setDefaultvalue(expr.toString());
        } else {
            column.setDefaultvalue(null);
        }
        return rowSizeDelta;
    }

    public static SchemaPlus addTable(SqlNode node, Database db) {
        if (node.getKind() != SqlKind.CREATE_TABLE) {           // for now, only patially support CREATE TABLE stmt
            return CatalogAdapter.schemaPlusFromDatabase(db);
        }
        final List<SqlNode> nameAndColListAndQuery = ((SqlCreateTable) node).getOperandList();
        final String tableName = nameAndColListAndQuery.get(0).toString();
        final SqlNodeList nodeTableList = (SqlNodeList) nameAndColListAndQuery.get(1);
        final Table t = db.getTables().add(tableName);
        t.setAnnotation(new TableAnnotation());
        ((TableAnnotation) t.getAnnotation()).ddl =      // NOTE: Calcite dialect double-quotes all table/column names. Might not be compatible with our canonical DDL?
                node.toSqlString(CalciteSqlDialect.DEFAULT).toString();
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

        for (SqlNode c : nodeTableList) {
            switch (c.getKind()) {
                case PRIMARY_KEY:        // For now, skip constraint entries.
                    continue;
                case COLUMN_DECL:
                    rowSize += addColumn(new SqlColumnDeclarationWithExpression((SqlColumnDeclaration) c),
                            t, tableName, index, columnTypes);
                    if (rowSize > DDLCompiler.MAX_ROW_SIZE) {       // fails when the accumulative row size exeeds limit
                        throw new PlanningErrorException(String.format(
                                "Error: table %s has a maximum row size of %s but the maximum supported size is %s",
                                tableName, VoltType.humanReadableSize(rowSize), VoltType.humanReadableSize(DDLCompiler.MAX_ROW_SIZE)));
                    }
            }
        }
        t.setSignature(CatalogUtil.getSignatureForTable(tableName, columnTypes));
        return CatalogAdapter.schemaPlusFromDatabase(db);
    }


    private CompletableFuture<ClientResponse> runDDLBatch(List<String> sqlStatements, List<SqlNode> sqlNodes) {
        // conflictTables tracks dropped tables before removing the ones that don't have CREATEs.
        SortedSet<String> conflictTables = new TreeSet<>();
        Set<String> createdTables = new HashSet<>();

        for (String stmt : sqlStatements) {
            // check that the DDL is allowed
            String rejectionExplanation = SQLLexer.checkPermitted(stmt);
            if (rejectionExplanation != null) {
                return makeQuickResponse(
                        ClientResponse.GRACEFUL_FAILURE,
                        rejectionExplanation);
            }

            String ddlToken = SQLLexer.extractDDLToken(stmt);
            // make sure not to mix drop and create in the same batch for the same table
            if (ddlToken.equals("drop")) {
                String tableName = SQLLexer.extractDDLTableName(stmt);
                if (tableName != null) {
                    conflictTables.add(tableName);
                }
            }
            else if (ddlToken.equals("create")) {
                String tableName = SQLLexer.extractDDLTableName(stmt);
                if (tableName != null) {
                    createdTables.add(tableName);
                }
            }
        }

        // check for conflicting DDL create/drop table statements.
        // unhappy if the intersection is empty
        conflictTables.retainAll(createdTables);
        if (!conflictTables.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("AdHoc DDL contains both DROP and CREATE statements for the following table(s):");
            for (String tableName : conflictTables) {
                sb.append(" ");
                sb.append(tableName);
            }
            sb.append("\nYou cannot DROP and ADD a table with the same name in a single batch "
                    + "(via @AdHoc). Issue the DROP and ADD statements as separate commands.");
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, sb.toString());
        }

        if (!allowPausedModeWork(false, isAdminConnection())) {
            return makeQuickResponse(
                    ClientResponse.SERVER_UNAVAILABLE,
                    "Server is paused and is available in read-only mode - please try again later.");
        }

        boolean useAdhocDDL = VoltDB.instance().getCatalogContext().cluster.getUseddlschema();
        if (!useAdhocDDL) {
            return makeQuickResponse(
                    ClientResponse.GRACEFUL_FAILURE,
                    "Cluster is configured to use @UpdateApplicationCatalog " +
                    "to change application schema.  AdHoc DDL is forbidden.");
        }

        logCatalogUpdateInvocation("@AdHoc");

        return updateApplication("@AdHoc",
                                null,
                                null,
                                sqlStatements.toArray(new String[0]), sqlNodes,
                                null,
                                false,
                                true);
    }
}
