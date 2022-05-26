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

package org.voltdb.compiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.TableType;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.IndexRef;
import org.voltdb.catalog.MaterializedViewHandlerInfo;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TableRef;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractExpression.UnsafeOperatorsForDDL;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.ParsedColInfo;
import org.voltdb.planner.ParsedSelectStmt;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.planner.SubPlanAssembler;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.PlanNodeTree;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

public class MaterializedViewProcessor {

    private final VoltCompiler m_compiler;
    private final HSQLInterface m_hsql;

    public MaterializedViewProcessor(VoltCompiler compiler,
                                     HSQLInterface hsql) {
        assert(compiler != null);
        assert(hsql != null);
        m_compiler = compiler;
        m_hsql = hsql;
    }

    /**
     * Add materialized view info to the catalog for the tables that are
     * materialized views.
     * @throws VoltCompilerException
     */
    public void startProcessing(Database db, HashMap<Table, String> matViewMap, TreeSet<String> exportTableNames)
            throws VoltCompilerException {
        HashSet <String> viewTableNames = new HashSet<>();
        for (Entry<Table, String> entry : matViewMap.entrySet()) {
            viewTableNames.add(entry.getKey().getTypeName());
        }

        for (Entry<Table, String> entry : matViewMap.entrySet()) {
            Table destTable = entry.getKey();
            String query = entry.getValue();

            // get the xml for the query
            VoltXMLElement xmlquery = null;
            try {
                xmlquery = m_hsql.getXMLCompiledStatement(query);
            }
            catch (HSQLParseException e) {
                e.printStackTrace();
            }
            assert(xmlquery != null);

            // parse the xml like any other sql statement
            ParsedSelectStmt stmt = null;
            try {
                stmt = (ParsedSelectStmt) AbstractParsedStmt.parse(null, query, xmlquery, null, db, null);
            }
            catch (Exception e) {
                throw m_compiler.new VoltCompilerException(e.getMessage());
            }
            assert(stmt != null);
            // NOTE: it is hard to check for display columns at DDLCompiler time when we get from HSQL, so we
            // have to guard in here (the actual parsing is done in ParseSelectStmt)
            if (stmt.m_displayColumns.stream().anyMatch(col ->
                    col.m_expression.getExpressionType() == ExpressionType.AGGREGATE_COUNT &&
                            ((AggregateExpression) col.m_expression).isDistinct())) {
                throw new PlanningErrorException(String.format("View does not support COUNT(DISTINCT) expression: \"%s\"",
                        stmt.m_sql));
            }

            String viewName = destTable.getTypeName();
            // throw an error if the view isn't within voltdb's limited world view
            checkViewMeetsSpec(viewName, stmt);

            // Allow only non-unique indexes other than the primary key index.
            // The primary key index is yet to be defined (below).
            for (Index destIndex : destTable.getIndexes()) {
                if (destIndex.getUnique() || destIndex.getAssumeunique()) {
                    String msg = "A UNIQUE or ASSUMEUNIQUE index is not allowed on a materialized view. " +
                                 "Remove the qualifier from the index " + destIndex.getTypeName() +
                                 "defined on the materialized view \"" + viewName + "\".";
                    throw m_compiler.new VoltCompilerException(msg);
                }
            }

            // Check source tables
            boolean migView = TableType.isPersistentMigrate(destTable.getTabletype());
            for (Table srcTable : stmt.m_tableList) {

                // A Materialized view cannot depend on another view.
                if (viewTableNames.contains(srcTable.getTypeName())) {
                    String msg = String.format("A materialized view (%s) can not be defined on another view (%s).",
                            viewName, srcTable.getTypeName());
                    throw m_compiler.new VoltCompilerException(msg);
                }

                // A Migrating view can only depend on STREAM.
                if (migView && !TableType.isStream(srcTable.getTabletype())) {
                    String msg = String.format("Table %s cannot be used in migrating view %s, only streams are allowed.",
                            srcTable.getTypeName(), viewName);
                    throw m_compiler.new VoltCompilerException(msg);
                }
            }

            // The existing code base still need this materializer field to tell if a table
            // is a materialized view table. Leaving this for future refactoring.
            destTable.setMaterializer(stmt.m_tableList.get(0));

            List<Column> destColumnArray = CatalogUtil.getSortedCatalogItems(destTable.getColumns(), "index");
            List<AbstractExpression> groupbyExprs = null;
            if (stmt.hasComplexGroupby()) {
                groupbyExprs = new ArrayList<>();
                for (ParsedColInfo col: stmt.groupByColumns()) {
                    groupbyExprs.add(col.m_expression);
                }
            }

            // Generate query XMLs for min/max recalculation (ENG-8641)
            boolean isMultiTableView = stmt.m_tableList.size() > 1;
            MatViewFallbackQueryXMLGenerator xmlGen = new MatViewFallbackQueryXMLGenerator(xmlquery, stmt.groupByColumns(), stmt.m_displayColumns, isMultiTableView);
            List<VoltXMLElement> fallbackQueryXMLs = xmlGen.getFallbackQueryXMLs();

            // create an index and constraint for the table
            // After ENG-7872 is fixed if there is no group by column then we will not create any
            // index or constraint in order to avoid error and crash.
            if (stmt.groupByColumns().size() != 0) {
                Index pkIndex = destTable.getIndexes().add(HSQLInterface.AUTO_GEN_MATVIEW_IDX);
                pkIndex.setType(IndexType.BALANCED_TREE.getValue());
                pkIndex.setUnique(true);
                // add the group by columns from the src table
                // assume index 1 throuh #grpByCols + 1 are the cols
                for (int i = 0; i < stmt.groupByColumns().size(); i++) {
                    ColumnRef c = pkIndex.getColumns().add(String.valueOf(i));
                    c.setColumn(destColumnArray.get(i));
                    c.setIndex(i);
                }
                Constraint pkConstraint = destTable.getConstraints().add(HSQLInterface.AUTO_GEN_MATVIEW_CONST);
                pkConstraint.setType(ConstraintType.PRIMARY_KEY.getValue());
                pkConstraint.setIndex(pkIndex);
            }
            // If we have an unsafe MV message, then
            // remember it here.  We don't really know how
            // to transfer the message through the catalog, but
            // we can transmit the existence of the message.
            boolean isSafeForDDL = (stmt.getUnsafeMVMessage() == null);
            // Here we will calculate the maximum allowed size for variable-length columns in the view.
            // The maximum row size is 2MB. We will first subtract the sum of
            // all fixed-size data type column sizes from this 2MB allowance then
            // divide it by the number of variable-length columns.
            int maximumColumnSize = DDLCompiler.MAX_ROW_SIZE;
            int varLengthColumnCount = stmt.m_displayColumns.size();
            for (int i = 0; i < stmt.m_displayColumns.size(); i++) {
                ParsedColInfo col = stmt.m_displayColumns.get(i);
                if ( ! col.m_expression.getValueType().isVariableLength()) {
                    varLengthColumnCount--;
                    maximumColumnSize -= col.m_expression.getValueSize();
                }
            }
            if (varLengthColumnCount > 0) {
                maximumColumnSize /= varLengthColumnCount;
            }
            // Note that the size of a single column cannot be larger than 1MB.
            if (maximumColumnSize > DDLCompiler.MAX_VALUE_LENGTH) {
                maximumColumnSize = DDLCompiler.MAX_VALUE_LENGTH;
            }

            // Here the code path diverges for different kinds of views (single table view and joined table view)
            if (isMultiTableView) {
                // Materialized view on joined tables
                // Add mvHandlerInfo to the destTable:
                MaterializedViewHandlerInfo mvHandlerInfo = destTable.getMvhandlerinfo().add("mvHandlerInfo");
                mvHandlerInfo.setDesttable(destTable);
                for (Table srcTable : stmt.m_tableList) {
                    // Now we do not support having a view on persistent tables joining streamed tables.
                    if (exportTableNames.contains(srcTable.getTypeName())) {
                        String msg = String.format("A materialized view (%s) on joined tables cannot have streamed table (%s) as its source.",
                                                   viewName, srcTable.getTypeName());
                        throw m_compiler.new VoltCompilerException(msg);
                    }
                    // The view table will need to keep a list of its source tables.
                    // The list is used to install / uninstall the view reference on the source tables when the
                    // view handler is constructed / destroyed.
                    TableRef tableRef = mvHandlerInfo.getSourcetables().add(srcTable.getTypeName());
                    tableRef.setTable(srcTable);

                    // Try to find a partition column for the view table.
                    // There could be more than one partition column candidate, but we will only use the first one we found.
                    if (destTable.getPartitioncolumn() == null && srcTable.getPartitioncolumn() != null) {
                        Column partitionColumn = srcTable.getPartitioncolumn();
                        String partitionColName = partitionColumn.getTypeName();
                        String srcTableName = srcTable.getTypeName();
                        destTable.setIsreplicated(false);
                        if (stmt.hasComplexGroupby()) {
                            for (int i = 0; i < groupbyExprs.size(); i++) {
                                AbstractExpression groupbyExpr = groupbyExprs.get(i);
                                if (groupbyExpr instanceof TupleValueExpression) {
                                    TupleValueExpression tve = (TupleValueExpression) groupbyExpr;
                                    if (tve.getTableName().equals(srcTableName) && tve.getColumnName().equals(partitionColName)) {
                                        // The partition column is set to destColumnArray.get(i), because we have the restriction
                                        // that the non-aggregate columns must come at the very begining, and must exactly match
                                        // the group-by columns.
                                        // If we are going to remove this restriction in the future, then we need to do more work
                                        // in order to find a proper partition column.
                                        destTable.setPartitioncolumn(destColumnArray.get(i));
                                        break;
                                    }
                                }
                            }
                        }
                        else {
                            for (int i = 0; i < stmt.groupByColumns().size(); i++) {
                                ParsedColInfo gbcol = stmt.groupByColumns().get(i);
                                if (gbcol.m_tableName.equals(srcTableName) && gbcol.m_columnName.equals(partitionColName)) {
                                    destTable.setPartitioncolumn(destColumnArray.get(i));
                                    break;
                                }
                            }
                        }
                    } // end find partition column
                } // end for each source table

                compileFallbackQueriesAndUpdateCatalog(db, query, fallbackQueryXMLs, mvHandlerInfo);
                compileCreateQueryAndUpdateCatalog(db, query, xmlquery, mvHandlerInfo);
                mvHandlerInfo.setGroupbycolumncount(stmt.groupByColumns().size());

                for (int i=0; i<stmt.m_displayColumns.size(); i++) {
                    ParsedColInfo col = stmt.m_displayColumns.get(i);
                    Column destColumn = destColumnArray.get(i);
                    setTypeAttributesForColumn(destColumn, col.m_expression, maximumColumnSize);

                    // Set the expression type here to determine the behavior of the merge function.
                    destColumn.setAggregatetype(col.m_expression.getExpressionType().getValue());
                }
                mvHandlerInfo.setIssafewithnonemptysources(isSafeForDDL);
            }
            else { // =======================================================================================
                // Materialized view on single table
                // create the materializedviewinfo catalog node for the source table
                Table srcTable = stmt.m_tableList.get(0);
                MaterializedViewInfo matviewinfo = srcTable.getViews().add(viewName);
                matviewinfo.setDest(destTable);

                AbstractExpression where = stmt.getSingleTableFilterExpression();
                if (where != null) {
                    String hex = Encoder.hexEncode(where.toJSONString());
                    matviewinfo.setPredicate(hex);
                }
                else {
                    matviewinfo.setPredicate("");
                }

                List<Column> srcColumnArray = CatalogUtil.getSortedCatalogItems(srcTable.getColumns(), "index");
                if (stmt.hasComplexGroupby()) {
                    // Parse group by expressions to json string
                    String groupbyExprsJson = null;
                    try {
                        groupbyExprsJson = DDLCompiler.convertToJSONArray(groupbyExprs);
                    } catch (JSONException e) {
                        throw m_compiler.new VoltCompilerException ("Unexpected error serializing non-column " +
                                "expressions for group by expressions: " + e.toString());
                    }
                    matviewinfo.setGroupbyexpressionsjson(groupbyExprsJson);
                }
                else {
                    // add the group by columns from the src table
                    for (int i = 0; i < stmt.groupByColumns().size(); i++) {
                        ParsedColInfo gbcol = stmt.groupByColumns().get(i);
                        Column srcCol = srcColumnArray.get(gbcol.m_index);
                        ColumnRef cref = matviewinfo.getGroupbycols().add(srcCol.getTypeName());
                        // groupByColumns is iterating in order of groups. Store that grouping order
                        // in the column ref index. When the catalog is serialized, it will, naturally,
                        // scramble this order like a two year playing dominos, presenting the data
                        // in a meaningless sequence.
                        cref.setIndex(i);           // the column offset in the view's grouping order
                        cref.setColumn(srcCol);     // the source column from the base (non-view) table

                        // parse out the group by columns into the dest table
                        ParsedColInfo col = stmt.m_displayColumns.get(i);
                        Column destColumn = destColumnArray.get(i);
                        processMaterializedViewColumn(srcTable, destColumn,
                                ExpressionType.VALUE_TUPLE, (TupleValueExpression)col.m_expression);
                    }
                }

                // prepare info for aggregation columns and COUNT(*) column(s)
                List<AbstractExpression> aggregationExprs = new ArrayList<>();
                boolean hasAggregationExprs = false;
                ArrayList<AbstractExpression> minMaxAggs = new ArrayList<>();
                for (int i = stmt.groupByColumns().size(); i < stmt.m_displayColumns.size(); i++) {
                    ParsedColInfo col = stmt.m_displayColumns.get(i);
                    // skip COUNT(*)
                    if ( col.m_expression.getExpressionType() == ExpressionType.AGGREGATE_COUNT_STAR ) {
                        continue;
                    }
                    AbstractExpression aggExpr = col.m_expression.getLeft();
                    if (aggExpr.getExpressionType() != ExpressionType.VALUE_TUPLE) {
                        hasAggregationExprs = true;
                    }
                    aggregationExprs.add(aggExpr);
                    if (col.m_expression.getExpressionType() ==  ExpressionType.AGGREGATE_MIN ||
                            col.m_expression.getExpressionType() == ExpressionType.AGGREGATE_MAX) {
                        minMaxAggs.add(aggExpr);
                    }
                }

                compileFallbackQueriesAndUpdateCatalog(db, query, fallbackQueryXMLs, matviewinfo);

                // set Aggregation Expressions.
                if (hasAggregationExprs) {
                    String aggregationExprsJson = null;
                    try {
                        aggregationExprsJson = DDLCompiler.convertToJSONArray(aggregationExprs);
                    } catch (JSONException e) {
                        throw m_compiler.new VoltCompilerException ("Unexpected error serializing non-column " +
                                "expressions for aggregation expressions: " + e.toString());
                    }
                    matviewinfo.setAggregationexpressionsjson(aggregationExprsJson);
                }

                // Find index for each min/max aggCol/aggExpr (ENG-6511 and ENG-8512)
                for (Integer i = 0; i < minMaxAggs.size(); ++i) {
                    Index found = findBestMatchIndexForMatviewMinOrMax(matviewinfo, srcTable, groupbyExprs, minMaxAggs.get(i));
                    IndexRef refFound = matviewinfo.getIndexforminmax().add(i.toString());
                    if (found != null) {
                        refFound.setName(found.getTypeName());
                    } else {
                        refFound.setName("");
                    }
                }

                // This is to fix the data type mismatch of the group by columns (and potentially other columns).
                // The COUNT(*) should return a BIGINT column, whereas we found here the COUNT(*) was assigned a INTEGER column is fixed below loop.
                for (int i = 0; i < stmt.groupByColumns().size(); i++) {
                    ParsedColInfo col = stmt.m_displayColumns.get(i);
                    Column destColumn = destColumnArray.get(i);
                    setTypeAttributesForColumn(destColumn, col.m_expression, maximumColumnSize);
                }

                // parse out the aggregation columns into the dest table
                for (int i = stmt.groupByColumns().size(); i < stmt.m_displayColumns.size(); i++) {
                    ParsedColInfo col = stmt.m_displayColumns.get(i);
                    Column destColumn = destColumnArray.get(i);

                    AbstractExpression colExpr = col.m_expression.getLeft();
                    TupleValueExpression tve = null;

                    if ( col.m_expression.getExpressionType() != ExpressionType.AGGREGATE_COUNT_STAR
                            && colExpr.getExpressionType() == ExpressionType.VALUE_TUPLE) {
                        tve = (TupleValueExpression)colExpr;
                    }

                    processMaterializedViewColumn(srcTable, destColumn,
                            col.m_expression.getExpressionType(), tve);
                    setTypeAttributesForColumn(destColumn, col.m_expression, maximumColumnSize);
                }

                if (srcTable.getPartitioncolumn() != null) {
                    // Set the partitioning of destination tables of associated views.
                    // If a view's source table is replicated, then a full scan of the
                    // associated view is single-sited. If the source is partitioned,
                    // a full scan of the view must be distributed, unless it is filtered
                    // by the original table's partitioning key, which, to be filtered,
                    // must also be a GROUP BY key.
                    destTable.setIsreplicated(false);
                    setGroupedTablePartitionColumn(matviewinfo, srcTable.getPartitioncolumn());
                }
                matviewinfo.setIssafewithnonemptysources(isSafeForDDL);
            } // end if single table view materialized view.
        }
    }

    private void setGroupedTablePartitionColumn(MaterializedViewInfo mvi, Column partitionColumn)
            throws VoltCompilerException {
        // A view of a replicated table is replicated.
        // A view of a partitioned table is partitioned -- regardless of whether it has a partition key
        // -- it certainly isn't replicated!
        // If the partitioning column is grouped, its counterpart is the partitioning column of the view table.
        // Otherwise, the view table just doesn't have a partitioning column
        // -- it is seemingly randomly distributed,
        // and its grouped columns are only locally unique but not globally unique.
        Table destTable = mvi.getDest();
        // Get the grouped columns in "index" order.
        // This order corresponds to the iteration order of the MaterializedViewInfo's group by columns.
        List<Column> destColumnArray = CatalogUtil.getSortedCatalogItems(destTable.getColumns(), "index");
        String partitionColName = partitionColumn.getTypeName(); // Note getTypeName gets the column name -- go figure.

        if (mvi.getGroupbycols().size() > 0) {
            int index = 0;
            for (ColumnRef cref : CatalogUtil.getSortedCatalogItems(mvi.getGroupbycols(), "index")) {
                Column srcCol = cref.getColumn();
                if (srcCol.getName().equals(partitionColName)) {
                    Column destCol = destColumnArray.get(index);
                    destTable.setPartitioncolumn(destCol);
                    return;
                }
                ++index;
            }
        } else {
            String complexGroupbyJson = mvi.getGroupbyexpressionsjson();
            if (complexGroupbyJson.length() > 0) {
                int partitionColIndex =  partitionColumn.getIndex();

                  List<AbstractExpression> mvComplexGroupbyCols = null;
                  try {
                      mvComplexGroupbyCols = AbstractExpression.fromJSONArrayString(complexGroupbyJson, null);
                  } catch (JSONException e) {
                      e.printStackTrace();
                  }
                  int index = 0;
                  for (AbstractExpression expr: mvComplexGroupbyCols) {
                      if (expr instanceof TupleValueExpression) {
                          TupleValueExpression tve = (TupleValueExpression) expr;
                          if (tve.getColumnIndex() == partitionColIndex) {
                              Column destCol = destColumnArray.get(index);
                              destTable.setPartitioncolumn(destCol);
                              return;
                          }
                      }
                      ++index;
                  }
            }
        }
    }

    /**
     * If the view is defined on joined tables (>1 source table),
     * check if there are self-joins.
     *
     * @param tableList The list of view source tables.
     * @param compiler The VoltCompiler
     * @throws VoltCompilerException
     */
    private void checkViewSources(List<Table> tableList) throws VoltCompilerException {
        Set<String> tableSet = new HashSet<>();
        for (Table tbl : tableList) {
            if (! tableSet.add(tbl.getTypeName())) {
                String errMsg = "Table " + tbl.getTypeName() + " appeared in the table list more than once: " +
                                "materialized view does not support self-join.";
                throw m_compiler.new VoltCompilerException(errMsg);
            }
        }
    }

    /**
     * Verify the materialized view meets our arcane rules about what can and can't
     * go in a materialized view. Throw hopefully helpful error messages when these
     * rules are inevitably borked.
     *
     * @param viewName The name of the view being checked.
     * @param stmt The output from the parser describing the select statement that creates the view.
     * @throws VoltCompilerException
     */

    private void checkViewMeetsSpec(String viewName, ParsedSelectStmt stmt) throws VoltCompilerException {
        int groupColCount = stmt.groupByColumns().size();
        int displayColCount = stmt.m_displayColumns.size();
        StringBuffer msg = new StringBuffer();
        msg.append("Materialized view \"").append(viewName).append("\" ");

        if (stmt.getParameters().length > 0) {
            msg.append("contains placeholders (?), which are not allowed in the SELECT query for a view.");
            throw m_compiler.new VoltCompilerException(msg.toString());
        }

        List <AbstractExpression> checkExpressions = new ArrayList<>();
        int i;
        // First, check the group by columns.  They are at
        // the beginning of the display list.
        for (i = 0; i < groupColCount; i++) {
            ParsedColInfo gbcol = stmt.groupByColumns().get(i);
            ParsedColInfo outcol = stmt.m_displayColumns.get(i);
            // The columns must be equal.
            if (!outcol.m_expression.equals(gbcol.m_expression)) {
                msg.append("must exactly match the GROUP BY clause at index ").append(i).append(" of SELECT list.");
                throw m_compiler.new VoltCompilerException(msg.toString());
            }
            // check if the expression return type is not unique indexable
            StringBuffer exprMsg = new StringBuffer();
            if (!outcol.m_expression.isValueTypeUniqueIndexable(exprMsg)) {
                msg.append("with ").append(exprMsg).append(" in GROUP BY clause not supported.");
                throw m_compiler.new VoltCompilerException(msg.toString());
            }

            // collect all the expressions and we will check
            // for other guards on all of them together
            checkExpressions.add(outcol.m_expression);
        }

        // check for count star in the display list
        boolean countStarFound = false;

        UnsafeOperatorsForDDL unsafeOps = new UnsafeOperatorsForDDL();

        // Finally, the display columns must have aggregate
        // calls.  But these are not any aggregate calls. They
        // must be count(), min(), max() or sum().
        for (; i < displayColCount; i++) {
            ParsedColInfo outcol = stmt.m_displayColumns.get(i);
            // Note that this expression does not catch all aggregates.
            // An instance of avg() would cause the exception.
            // ENG-10945 - We can have count(*) anywhere after the group by columns and multiple count(*)(s)
            if (outcol.m_expression.getExpressionType() == ExpressionType.AGGREGATE_COUNT_STAR) {
                if (! countStarFound) {
                    countStarFound = true;
                }
                continue;
            }

            if ((outcol.m_expression.getExpressionType() != ExpressionType.AGGREGATE_COUNT) &&
                    (outcol.m_expression.getExpressionType() != ExpressionType.AGGREGATE_SUM) &&
                    (outcol.m_expression.getExpressionType() != ExpressionType.AGGREGATE_MIN) &&
                    (outcol.m_expression.getExpressionType() != ExpressionType.AGGREGATE_MAX)) {
                msg.append("must have non-group by columns aggregated by sum, count, min or max.");
                throw m_compiler.new VoltCompilerException(msg.toString());
            }
            // Don't push the expression, though.  Push the argument.
            // We will check for aggregate calls and fail, and we don't
            // want to fail on legal aggregate expressions.
            if (outcol.m_expression.getLeft() != null) {
                checkExpressions.add(outcol.m_expression.getLeft());
            }
            // Check if the aggregation is safe for non-empty view source table.
            outcol.m_expression.findUnsafeOperatorsForDDL(unsafeOps);
            assert(outcol.m_expression.getRight() == null);
            assert(outcol.m_expression.getArgs() == null || outcol.m_expression.getArgs().size() == 0);
        }

        // Users can create SINGLE TABLE VIEWS without declaring count(*) in the stmt.
        // Multiple table views still need this restriction.
        if (stmt.m_tableList.size() > 1 && ! countStarFound) {
            msg.append("joins multiple tables, therefore must include COUNT(*) after any GROUP BY columns.");
            throw m_compiler.new VoltCompilerException(msg.toString());
        }

        AbstractExpression where = stmt.getSingleTableFilterExpression();
        if (where != null) {
            checkExpressions.add(where);
        }

        /*
         * Gather up all the join expressions.  The ParsedSelectStatement
         * has not been analyzed yet, so it's not clear where these are.  But
         * the stmt knows.
         */
        stmt.gatherJoinExpressions(checkExpressions);
        if (stmt.getHavingPredicate() != null) {
            checkExpressions.add(stmt.getHavingPredicate());
        }
        // Check all the subexpressions we gathered up.
        if (!AbstractExpression.validateExprsForIndexesAndMVs(checkExpressions, msg, true)) {
            // The error message will be in the StringBuffer msg.
            throw m_compiler.new VoltCompilerException(msg.toString());
        }

        // Check some other materialized view specific things.
        //
        // Check to see if the expression is safe for creating
        // views on nonempty tables.
        for (AbstractExpression expr : checkExpressions) {
            expr.findUnsafeOperatorsForDDL(unsafeOps);
        }
        if (unsafeOps.isUnsafe()) {
            stmt.setUnsafeDDLMessage(unsafeOps.toString());
        }

        if (stmt.hasSubquery()) {
            msg.append("cannot contain subquery sources.");
            throw m_compiler.new VoltCompilerException(msg.toString());
        } else if (! stmt.m_joinTree.allInnerJoins()) {
            throw m_compiler.new VoltCompilerException("Materialized view only supports INNER JOIN.");
        } else if (stmt.orderByColumns().size() != 0) {
            msg.append("with an ORDER BY clause is not supported.");
            throw m_compiler.new VoltCompilerException(msg.toString());
        } else if (stmt.hasLimitOrOffset()) {
            msg.append("with a LIMIT or OFFSET clause is not supported.");
            throw m_compiler.new VoltCompilerException(msg.toString());
        } else if (stmt.getHavingPredicate() != null) {
            msg.append("with a HAVING clause is not supported.");
            throw m_compiler.new VoltCompilerException(msg.toString());
        } else if ((stmt.m_tableList.size() > 1 && displayColCount <= groupColCount) ||
                displayColCount < groupColCount) { // ENG-10892, since count(*) can be removed from SV table
            msg.append("has too few columns.");
            throw m_compiler.new VoltCompilerException(msg.toString());
        } else {
            checkViewSources(stmt.m_tableList);
        }
     }

    private static void processMaterializedViewColumn(
            Table srcTable, Column destColumn, ExpressionType type, TupleValueExpression colExpr) {

        if (colExpr != null) {
            assert(colExpr.getTableName().equalsIgnoreCase(srcTable.getTypeName()));
            String srcColName = colExpr.getColumnName();
            Column srcColumn = srcTable.getColumns().getIgnoreCase(srcColName);
            destColumn.setMatviewsource(srcColumn);
        }
        destColumn.setAggregatetype(type.getValue());
    }

    // Compile the fallback query XMLs, add the plans into the catalog statement (ENG-8641).
    private void compileFallbackQueriesAndUpdateCatalog(Database db, String query, List<VoltXMLElement> fallbackQueryXMLs,
                                                        MaterializedViewInfo matviewinfo) throws VoltCompilerException {
        DatabaseEstimates estimates = new DatabaseEstimates();
        for (int i=0; i<fallbackQueryXMLs.size(); ++i) {
            String key = String.valueOf(i);
            Statement fallbackQueryStmt = matviewinfo.getFallbackquerystmts().add(key);
            VoltXMLElement fallbackQueryXML = fallbackQueryXMLs.get(i);
            fallbackQueryStmt.setSqltext(query);
            compileStatementAndUpdateCatalog(db, estimates, fallbackQueryStmt, fallbackQueryXML,
                    StatementPartitioning.forceSP());
        }
    }

    // Compile the fallback query XMLs, add the plans into the catalog statement (ENG-8641).
    private void compileFallbackQueriesAndUpdateCatalog(Database db,
                                                        String query,
                                                        List<VoltXMLElement> fallbackQueryXMLs,
                                                        MaterializedViewHandlerInfo mvHandlerInfo)
                                                        throws VoltCompilerException {
        DatabaseEstimates estimates = new DatabaseEstimates();
        for (int i=0; i<fallbackQueryXMLs.size(); ++i) {
            String key = String.valueOf(i);
            Statement fallbackQueryStmt = mvHandlerInfo.getFallbackquerystmts().add(key);
            VoltXMLElement fallbackQueryXML = fallbackQueryXMLs.get(i);
            fallbackQueryStmt.setSqltext(query);
            compileStatementAndUpdateCatalog(db, estimates, fallbackQueryStmt, fallbackQueryXML,
                    StatementPartitioning.forceSP());
        }
    }

    private void compileCreateQueryAndUpdateCatalog(Database db,
                                                    String query,
                                                    VoltXMLElement xmlquery,
                                                    MaterializedViewHandlerInfo mvHandlerInfo)
                                                    throws VoltCompilerException {
        DatabaseEstimates estimates = new DatabaseEstimates();
        // Here we are compiling the query twice:
        //   In the first round, we will use inferPartitioning.
        // The purpose is to use the planner to check if the join query is plannable.
        // Some multi-partition join queries are not plannable because they are not
        // joining tables on the partition columns.
        //   In the second round, we will use forceSP to get the single partition
        // version of the query plan.
        Statement createQueryInfer = mvHandlerInfo.getCreatequery().add("createQueryInfer");
        Statement createQuery = mvHandlerInfo.getCreatequery().add("createQuery");
        createQueryInfer.setSqltext(query);
        createQuery.setSqltext(query);
        compileStatementAndUpdateCatalog(db, estimates, createQueryInfer, xmlquery,
                StatementPartitioning.inferPartitioning());

        mvHandlerInfo.getCreatequery().delete("createQueryInfer");
        compileStatementAndUpdateCatalog(db, estimates, createQuery, xmlquery, StatementPartitioning.forceSP());
    }

    private void compileStatementAndUpdateCatalog(Database db, DatabaseEstimates estimates, Statement createQuery,
            VoltXMLElement xmlquery, StatementPartitioning partitioning) throws VoltCompilerException {
        // no user-supplied join order
        StatementCompiler.compileStatementAndUpdateCatalog(m_compiler, m_hsql, db, estimates, createQuery, xmlquery,
                createQuery.getSqltext(), null, DeterminismMode.FASTER, partitioning, true);
    }

    private PlanNodeTree getPlanNodeTreeFromCatalogStatement(Database db, Statement stmt) {
        PlanNodeTree pnt = new PlanNodeTree();
        try {
            JSONObject jsonPlan = new JSONObject(org.voltdb.utils.CompressionService.decodeBase64AndDecompress(
                                                    stmt.getFragments().get("0").getPlannodetree()));
            pnt.loadFromJSONPlan(jsonPlan, db);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return pnt;
    }

    private boolean needsWarningForSingleTableView(PlanNodeTree pnt) {
        for (AbstractPlanNode apn : pnt.getNodeList()) {
            if (apn instanceof IndexScanPlanNode) {
                return false;
            }
        }
        return true;
    }

    private boolean needsWarningForJoinQueryView(PlanNodeTree pnt) {
        for (AbstractPlanNode apn : pnt.getNodeList()) {
            if (apn instanceof NestLoopPlanNode) {
                return true;
            }
        }
        return false;
    }

    /**
     * Process materialized view warnings.
     */
    public void processMaterializedViewWarnings(Database db, HashMap<Table, String> matViewMap) throws VoltCompilerException {
        for (Table table : db.getTables()) {
            for (MaterializedViewInfo mvInfo : table.getViews()) {
                for (Statement stmt : mvInfo.getFallbackquerystmts()) {
                    // If there is any statement in the fallBackQueryStmts map, then
                    // there must be some min/max columns.
                    // Only check if the plan uses index scan.
                    if (needsWarningForSingleTableView( getPlanNodeTreeFromCatalogStatement(db, stmt))) {
                        // If we are using IS NOT DISTINCT FROM as our equality operator (which is necessary
                        // to get correct answers), then there will often be no index scans in the plan,
                        // since we cannot optimize IS NOT DISTINCT FROM.
                        m_compiler.addWarn(
                                "No index found to support UPDATE and DELETE on some of the min() / max() columns " +
                                "in the materialized view " + mvInfo.getTypeName() +
                                ", and a sequential scan might be issued when current min / max value is updated / deleted.");
                        break;
                    }
                }
            }
            // If it's a view on join query case, we check if the join can utilize indices.
            // We throw out warning only if no index scan is used in the plan (ENG-10864).
            MaterializedViewHandlerInfo mvHandlerInfo = table.getMvhandlerinfo().get("mvHandlerInfo");
            if (mvHandlerInfo != null) {
                Statement createQueryStatement = mvHandlerInfo.getCreatequery().get("createQuery");
                if (needsWarningForJoinQueryView( getPlanNodeTreeFromCatalogStatement(db, createQueryStatement))) {
                    m_compiler.addWarn(
                            "No index found to support some of the join operations required to refresh the materialized view " +
                            table.getTypeName() +
                            ". The refreshing may be slow.");
                }
            }
        }
    }

    private enum MatViewIndexMatchingGroupby {GB_COL_IDX_COL, GB_COL_IDX_EXP,  GB_EXP_IDX_EXP}

    // if the materialized view has MIN / MAX, try to find an index defined on the source table
    // covering all group by cols / exprs to avoid expensive tablescan.
    // For now, the only acceptable index is defined exactly on the group by columns IN ORDER.
    // This allows the same key to be used to do lookups on the grouped table index and the
    // base table index.
    // TODO: More flexible (but usually less optimal*) indexes may be allowed here and supported
    // in the EE in the future including:
    //   -- *indexes on the group keys listed out of order
    //   -- *indexes on the group keys as a prefix before other indexed values.
    //   -- (ENG-6511) indexes on the group keys PLUS the MIN/MAX argument value (to eliminate post-filtering)
    // This function is mostly re-written for the fix of ENG-6511. --yzhang
    private static Index findBestMatchIndexForMatviewMinOrMax(MaterializedViewInfo matviewinfo,
            Table srcTable, List<AbstractExpression> groupbyExprs, AbstractExpression minMaxAggExpr) {
        CatalogMap<Index> allIndexes = srcTable.getIndexes();
        StmtTableScan tableScan = new StmtTargetTableScan(srcTable);

        // Candidate index. If we can find an index covering both group-by columns and aggExpr (optimal) then we will
        // return immediately.
        // If the index found covers only group-by columns (sub-optimal), we will first cache it here.
        Index candidate = null;
        for (Index index : allIndexes) {
            // indexOptimalForMinMax == true if the index covered both the group-by columns and the min/max aggExpr.
            boolean indexOptimalForMinMax = false;
            // If minMaxAggExpr is not null, the diff can be zero or one.
            // Otherwise, for a usable index, its number of columns must agree with that of the group-by columns.
            final int diffAllowance = minMaxAggExpr == null ? 0 : 1;

            // Get all indexed exprs if there is any.
            String expressionjson = index.getExpressionsjson();
            List<AbstractExpression> indexedExprs = null;
            if ( ! expressionjson.isEmpty() ) {
                try {
                    indexedExprs = AbstractExpression.fromJSONArrayString(expressionjson, tableScan);
                } catch (JSONException e) {
                    e.printStackTrace();
                    assert(false);
                    return null;
                }
            }
            // Get source table columns.
            List<Column> srcColumnArray = CatalogUtil.getSortedCatalogItems(srcTable.getColumns(), "index");
            MatViewIndexMatchingGroupby matchingCase = null;

            if (groupbyExprs == null) {
                // This means group-by columns are all simple columns.
                // It also means we can only access the group-by columns by colref.
                List<ColumnRef> groupbyColRefs =
                    CatalogUtil.getSortedCatalogItems(matviewinfo.getGroupbycols(), "index");
                if (indexedExprs == null) {
                    matchingCase = MatViewIndexMatchingGroupby.GB_COL_IDX_COL;

                    // All the columns in the index are also simple columns, EASY! colref vs. colref
                    List<ColumnRef> indexedColRefs =
                        CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
                    // The number of columns in index can never be less than that in the group-by column list.
                    // If minMaxAggExpr == null, they must be equal (diffAllowance == 0)
                    // Otherwise they may be equal (sub-optimal) or
                    // indexedColRefs.size() == groupbyColRefs.size() + 1 (optimal, diffAllowance == 1)
                    if (isInvalidIndexCandidate(indexedColRefs.size(), groupbyColRefs.size(), diffAllowance)) {
                        continue;
                    } else if (! isGroupbyMatchingIndex(matchingCase, groupbyColRefs, null,
                            indexedColRefs, null, null)) {
                        continue;
                    } else if (isValidIndexCandidateForMinMax(indexedColRefs.size(), groupbyColRefs.size(), diffAllowance)) {
                        if(! isIndexOptimalForMinMax(matchingCase, minMaxAggExpr, indexedColRefs,
                                null, srcColumnArray)) {
                            continue;
                        } else {
                            indexOptimalForMinMax = true;
                        }
                    }
                } else {
                    matchingCase = MatViewIndexMatchingGroupby.GB_COL_IDX_EXP;
                    // In this branch, group-by columns are simple columns, but the index contains complex columns.
                    // So it's only safe to access the index columns from indexedExprs.
                    // You can still get something from indexedColRefs, but they will be inaccurate.
                    // e.g.: ONE index column (a+b) will get you TWO separate entries {a, b} in indexedColRefs.
                    // In order to compare columns: for group-by columns: convert colref => col
                    //                              for    index columns: convert    tve => col
                    if (isInvalidIndexCandidate(indexedExprs.size(), groupbyColRefs.size(), diffAllowance)) {
                        continue;
                    } else if (! isGroupbyMatchingIndex(matchingCase, groupbyColRefs,
                            null, null, indexedExprs, srcColumnArray)) {
                        continue;
                    } else if (isValidIndexCandidateForMinMax(indexedExprs.size(), groupbyColRefs.size(), diffAllowance)) {
                        if(! isIndexOptimalForMinMax(matchingCase, minMaxAggExpr, null,
                                indexedExprs, null)) {
                            continue;
                        } else {
                            indexOptimalForMinMax = true;
                        }
                    }
                }
            } else {
                matchingCase = MatViewIndexMatchingGroupby.GB_EXP_IDX_EXP;
                // This means group-by columns have complex columns.
                // It's only safe to access the group-by columns from groupbyExprs.
                // AND, indexedExprs must not be null in this case. (yeah!)
                if ( indexedExprs == null ) {
                    continue;
                } else if (isInvalidIndexCandidate(indexedExprs.size(), groupbyExprs.size(), diffAllowance)) {
                    continue;
                } else if (! isGroupbyMatchingIndex(matchingCase, null, groupbyExprs,
                        null, indexedExprs, null)) {
                    continue;
                } else if (isValidIndexCandidateForMinMax(indexedExprs.size(), groupbyExprs.size(), diffAllowance)) {
                    if (! isIndexOptimalForMinMax(matchingCase, minMaxAggExpr, null, indexedExprs, null)) {
                        continue;
                    }
                    indexOptimalForMinMax = true;
                }
            }

            // NOW index at least covered all group-by columns (sub-optimal candidate)
            if (!index.getPredicatejson().isEmpty()) {
                // Additional check for partial indexes to make sure matview WHERE clause
                // covers the partial index predicate
                List<AbstractExpression> coveringExprs = new ArrayList<>();
                List<AbstractExpression> exactMatchCoveringExprs = new ArrayList<>();
                try {
                    String encodedPredicate = matviewinfo.getPredicate();
                    if (!encodedPredicate.isEmpty()) {
                        String predicate = Encoder.hexDecodeToString(encodedPredicate);
                        AbstractExpression matViewPredicate = AbstractExpression.fromJSONString(predicate, tableScan);
                        coveringExprs.addAll(ExpressionUtil.uncombineAny(matViewPredicate));
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                    assert(false);
                    return null;
                }
                String predicatejson = index.getPredicatejson();
                if ( ! predicatejson.isEmpty() && ! SubPlanAssembler.evaluatePartialIndexPredicate(
                        tableScan, coveringExprs, predicatejson, exactMatchCoveringExprs).getFirst()) {
                    // the partial index predicate does not match the MatView's
                    // where clause -- give up on this index
                    continue;
                }
            }
            // if the index already covered group by columns and the aggCol/aggExpr,
            // it is already the best index we can get, return immediately.
            if (indexOptimalForMinMax) {
                return index;
            }
            // otherwise wait to see if we can find something better!
            candidate = index;
        }
        return candidate;
    }

    private static void setTypeAttributesForColumn(Column column, AbstractExpression expr,
                                                   int maximumDefaultColumnSize) {
        VoltType voltTy = expr.getValueType();
        column.setType(voltTy.getValue());

        if (expr.getValueType().isVariableLength()) {
            int viewColumnLength = expr.getValueSize();
            int lengthInBytes = expr.getValueSize();
            lengthInBytes = expr.getInBytes() ? lengthInBytes : lengthInBytes * 4;

            // We don't create a view column that is wider than the default.
            if (lengthInBytes < maximumDefaultColumnSize) {
                column.setSize(viewColumnLength);
                column.setInbytes(expr.getInBytes());
            } else {
                // Declining to create a view column that is wider than the default.
                // This ensures that if there are a large number of aggregates on a string
                // column that we have a reasonable chance of not exceeding the static max row size limit.
                column.setSize(maximumDefaultColumnSize);
                column.setInbytes(true);
            }
        } else {
            column.setSize(voltTy.getMaxLengthInBytes());
        }
    }

    private static boolean isInvalidIndexCandidate(int idxSize, int gbSize, int diffAllowance) {
        return idxSize < gbSize || idxSize > gbSize + diffAllowance;
    }

    private static boolean isGroupbyMatchingIndex(
            MatViewIndexMatchingGroupby matchingCase,
            List<ColumnRef> groupbyColRefs, List<AbstractExpression> groupbyExprs,
            List<ColumnRef> indexedColRefs, List<AbstractExpression> indexedExprs,
            List<Column> srcColumnArray) {
        // Compare group-by columns/expressions for different cases
        switch(matchingCase) {
            case GB_COL_IDX_COL:
                for (int i = 0; i < groupbyColRefs.size(); ++i) {
                    int groupbyColIndex = groupbyColRefs.get(i).getColumn().getIndex();
                    int indexedColIndex = indexedColRefs.get(i).getColumn().getIndex();
                    if (groupbyColIndex != indexedColIndex) {
                        return false;
                    }
                }
                break;
            case GB_COL_IDX_EXP:
                for (int i = 0; i < groupbyColRefs.size(); ++i) {
                    AbstractExpression indexedExpr = indexedExprs.get(i);
                    if (! (indexedExpr instanceof TupleValueExpression)) {
                        // Group-by columns are all simple columns, so indexedExpr must be tve.
                        return false;
                    }
                    int indexedColIdx = ((TupleValueExpression)indexedExpr).getColumnIndex();
                    Column indexedColumn = srcColumnArray.get(indexedColIdx);
                    Column groupbyColumn = groupbyColRefs.get(i).getColumn();
                    if ( ! indexedColumn.equals(groupbyColumn) ) {
                        return false;
                    }
                }
                break;
            case GB_EXP_IDX_EXP:
                for (int i = 0; i < groupbyExprs.size(); ++i) {
                    if (! indexedExprs.get(i).equals(groupbyExprs.get(i))) {
                        return false;
                    }
                }
                break;
            default:
                assert(false);
                // invalid option
                return false;
        }

        // group-by columns/expressions are matched with the corresponding index
        return true;
    }

    private static boolean isValidIndexCandidateForMinMax(int idxSize, int gbSize, int diffAllowance) {
        return diffAllowance == 1 && idxSize == gbSize + 1;
    }

    private static boolean isIndexOptimalForMinMax(
            MatViewIndexMatchingGroupby matchingCase, AbstractExpression minMaxAggExpr,
            List<ColumnRef> indexedColRefs, List<AbstractExpression> indexedExprs,
            List<Column> srcColumnArray) {
        // We have minMaxAggExpr and the index also has one extra column
        switch(matchingCase) {
        case GB_COL_IDX_COL:
            if ( ! (minMaxAggExpr instanceof TupleValueExpression) ) {
                // Here because the index columns are all simple columns (indexedExprs == null)
                // so the minMaxAggExpr must be TupleValueExpression.
                return false;
            }
            int aggSrcColIdx = ((TupleValueExpression)minMaxAggExpr).getColumnIndex();
            Column aggSrcCol = srcColumnArray.get(aggSrcColIdx);
            Column lastIndexCol = indexedColRefs.get(indexedColRefs.size() - 1).getColumn();
            // Compare the two columns, if they are equal as well, then this is the optimal index! Congrats!
            if (aggSrcCol.equals(lastIndexCol)) {
                return true;
            }
            break;
        case GB_COL_IDX_EXP:
        case GB_EXP_IDX_EXP:
            if (indexedExprs.get(indexedExprs.size()-1).equals(minMaxAggExpr)) {
                return true;
            }
            break;
        default:
            assert(false);
        }

        // If the last part of the index does not match the MIN/MAX expression
        // this is not the optimal index candidate for now
        return false;
    }

    /**
     * If the argument table is a single-table materialized view,
     * then return the attendant MaterializedViewInfo object.  Otherwise
     * return null.
     */
    public static MaterializedViewInfo getMaterializedViewInfo(Table tbl) {
        MaterializedViewInfo mvInfo = null;
        Table source = tbl.getMaterializer();
        if (source != null) {
            mvInfo = source.getViews().get(tbl.getTypeName());
        }
        return mvInfo;
    }
}
