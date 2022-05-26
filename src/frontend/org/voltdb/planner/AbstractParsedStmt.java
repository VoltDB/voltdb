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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import com.google_voltpatches.common.base.Preconditions;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.RowSubqueryExpression;
import org.voltdb.expressions.SelectSubqueryExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.VectorValueExpression;
import org.voltdb.expressions.WindowFunctionExpression;
import org.voltdb.planner.parseinfo.BranchNode;
import org.voltdb.planner.parseinfo.JoinNode;
import org.voltdb.planner.parseinfo.StmtCommonTableScan;
import org.voltdb.planner.parseinfo.StmtCommonTableScanShared;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;
import org.voltdb.types.QuantifierType;
import org.voltdb.types.SortDirectionType;
import org.voltdb.types.VoltDecimalHelper;

public abstract class AbstractParsedStmt {

    public static final String TEMP_TABLE_NAME = "$$_VOLT_TEMP_TABLE_$$";
    public static final String WINDOWED_AGGREGATE_COLUMN_NAME = "WINAGG_COLUMN";

    protected String m_contentDeterminismMessage = null;

     // Internal statement counter
    public static int NEXT_STMT_ID = 0;

    // The unique id to identify the statement
    private int m_stmtId;

    public String m_sql;

    // The initial value is a safety net for the case of parameter-less statements.
    private Map<Integer, ParameterValueExpression> m_paramsByIndex = new TreeMap<>();

    protected Map<Long, ParameterValueExpression> m_paramsById = new HashMap<>();

    // The parameter expression from the correlated expressions. The key is the parameter index.
    // This map acts as intermediate storage for the parameter TVEs found while planning a subquery
    // until they can be distributed to the parent's subquery expression where they originated.
    public Map<Integer, AbstractExpression> m_parameterTveMap = new HashMap<>();

    public List<Table> m_tableList = new ArrayList<>();

    private Table m_DDLIndexedTable = null;

    public List<AbstractExpression> m_noTableSelectionList = new ArrayList<>();

    protected List<AbstractExpression> m_aggregationList = null;

    // Hierarchical join representation
    public JoinNode m_joinTree = null;

    // User specified join order, null if none is specified
    public String m_joinOrder = null;

    protected final Map<String, StmtTableScan> m_tableAliasMap = new HashMap<>();

    // This list is used to identify the order of the table aliases returned by
    // the parser for possible use as a default join order.
    protected List<String> m_tableAliasListAsJoinOrder = new ArrayList<>();

    protected String[] m_paramValues;
    public final Database m_db;

    // Parent statement if any
    public AbstractParsedStmt m_parentStmt;
    boolean m_isUpsert = false;

    // mark whether the statement's parent is UNION clause or not
    private boolean m_isChildOfUnion = false;

    protected static final Collection<String> m_nullUDFNameList = new ArrayList<>();

    private static final String INSERT_NODE_NAME = "insert";
    private static final String UPDATE_NODE_NAME = "update";
    private static final String DELETE_NODE_NAME = "delete";
    private static final String MIGRATE_NODE_NAME = "migrate";
    static final String SELECT_NODE_NAME = "select";
    static final String UNION_NODE_NAME  = "union";
    private static final String SWAP_NODE_NAME = "swap";

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    protected AbstractParsedStmt(AbstractParsedStmt parent, String[] paramValues, Database db) {
        m_parentStmt = parent;
        m_paramValues = paramValues;
        m_db = db;
    }

    /**
     * Test if any parent statement satisfies the predicate.
     * @return whether any parent statement satisfies the predicate
     */
    public boolean anyAncester(Predicate<AbstractParsedStmt> pred) {
        return m_parentStmt != null && (pred.test(m_parentStmt) || m_parentStmt.anyAncester(pred));
    }

    public void setDDLIndexedTable(Table tbl) {
        m_DDLIndexedTable = tbl;
        // Add this table to the cache
        assert(tbl.getTypeName() != null);
        addTableToStmtCache(tbl, tbl.getTypeName());
    }

    /**
     *
     * @param stmtTypeElement
     * @param paramValues
     * @param db
     */
    protected static AbstractParsedStmt getParsedStmt(
            AbstractParsedStmt parent,
            VoltXMLElement stmtTypeElement,
            String[] paramValues,
            Database db) {
        AbstractParsedStmt retval;

        if (stmtTypeElement == null) {
            System.err.println("Unexpected error parsing hsql parsed stmt xml");
            throw new RuntimeException("Unexpected error parsing hsql parsed stmt xml");
        }

        // create non-abstract instances
        if (stmtTypeElement.name.equalsIgnoreCase(INSERT_NODE_NAME)) {
            retval = new ParsedInsertStmt(parent, paramValues, db);
            if (stmtTypeElement.attributes.containsKey(QueryPlanner.UPSERT_TAG)) {
                retval.m_isUpsert = true;
            }
        } else if (stmtTypeElement.name.equals(UPDATE_NODE_NAME)) {
            retval = new ParsedUpdateStmt(parent, paramValues, db);
        } else if (stmtTypeElement.name.equals(DELETE_NODE_NAME)) {
            retval = new ParsedDeleteStmt(parent, paramValues, db);
        } else if (stmtTypeElement.name.equals(SELECT_NODE_NAME)) {
            retval = new ParsedSelectStmt(parent, paramValues, db);
        } else if (stmtTypeElement.name.equals(UNION_NODE_NAME)) {
            retval = new ParsedUnionStmt(parent, paramValues, db);
        } else if (stmtTypeElement.name.equals(SWAP_NODE_NAME)) {
            retval = new ParsedSwapStmt(parent, paramValues, db);
        } else if (stmtTypeElement.name.equalsIgnoreCase(MIGRATE_NODE_NAME)) {
            retval = new ParsedMigrateStmt(parent, paramValues, db);
        } else {
            throw new RuntimeException("Unexpected Element: " + stmtTypeElement.name);
        }
        // Set the unique id
        retval.m_stmtId = NEXT_STMT_ID++;
        return retval;
    }

    /**
     * @param parsedStmt
     * @param sql
     * @param xmlSQL
     * @param joinOrder
     */
    protected static void parse(AbstractParsedStmt parsedStmt, String sql,
            VoltXMLElement stmtTypeElement, String joinOrder) {
        // parse tables and parameters
        parsedStmt.parseTablesAndParams(stmtTypeElement);

        // parse specifics
        parsedStmt.parse(stmtTypeElement);

        // post parse action
        parsedStmt.postParse(sql, joinOrder);
    }

    /**
     *
     * @param sql
     * @param stmtTypeElement
     * @param paramValues
     * @param db
     * @param joinOrder
     */
    public static AbstractParsedStmt parse(
            AbstractParsedStmt parent, String sql, VoltXMLElement stmtTypeElement, String[] paramValues,
            Database db, String joinOrder) {

        // reset the statement counters
        NEXT_STMT_ID = 0;
        AbstractParsedStmt retval = getParsedStmt(parent, stmtTypeElement, paramValues, db);

        parse(retval, sql, stmtTypeElement, joinOrder);
        return retval;
    }

    /**
     *
     * @param stmtElement
     * @param db
     */
    abstract void parse(VoltXMLElement stmtElement);

    void parseTargetColumns(VoltXMLElement columnsNode, Table table,
            HashMap<Column, AbstractExpression> columns) {
        for (VoltXMLElement child : columnsNode.children) {
            assert(child.name.equals("column"));

            String name = child.attributes.get("name");
            assert(name != null);
            Column col = table.getColumns().getExact(name.trim());

            // May be no children of column node in the case of
            //   INSERT INTO ... SELECT ...

            AbstractExpression expr = null;
            if (child.children.size() != 0) {
                assert(child.children.size() == 1);
                VoltXMLElement subChild = child.children.get(0);
                expr = parseExpressionTree(subChild);
                assert(expr != null);
                try {
                    expr.refineValueType(VoltType.get((byte)col.getType()), col.getSize());
                } catch (PlanningErrorException ex) {
                    String errorMsg = ex.getMessage()
                                      + " for column '" + col.getTypeName()
                                      + "' in the table '" + table.getTypeName() + "'";
                    throw new PlanningErrorException(errorMsg);
                }
                ExpressionUtil.finalizeValueTypes(expr);
            }
            columns.put(col, expr);
        }
    }

    /**Parse tables and parameters
     * .
     * @param root
     * @param db
     */
    void parseTablesAndParams(VoltXMLElement root) {

        // Parse parameters first to satisfy a dependency of expression parsing
        // which happens during table scan parsing.
        parseParameters(root);

        parseCommonTableExpressions(root);

        for (VoltXMLElement node : root.children) {
            if (node.name.equalsIgnoreCase("tablescan")) {
                parseTable(node);
            } else if (node.name.equalsIgnoreCase("tablescans")) {
                parseTables(node);
            }
        }
    }

    /**
     * Parse the common table expressions.  These are the
     * tables found in the with clauses, if there are any.
     * For now these CTEs are found only in select statements.
     * So they are a noop for insert and delete.
     * we may eventually have insert into select statements whose
     * select subquery is a common table query.
     *
     * @param root
     * @param stmtId TODO
     */
    abstract protected void parseCommonTableExpressions(VoltXMLElement root);

    /**Miscellaneous post parse activity
     * .
     * @param sql
     * @param joinOrder
     */
    void postParse(String sql, String joinOrder) {
        m_sql = sql;
        m_joinOrder = joinOrder;
    }

    /**
     * Convert a HSQL VoltXML expression to an AbstractExpression tree.
     * @param root
     * @return configured AbstractExpression
     */
    // -- the function is now also called by DDLCompiler with no AbstractParsedStmt in sight --
    // so, the methods COULD be relocated to class AbstractExpression or ExpressionUtil.
    protected AbstractExpression parseConditionTree(VoltXMLElement root) {
        AbstractExpression expr = parseExpressionNode(root);

        // If there were any IN expressions optionally joined by ANDs and ORs
        // at the top of a condition, it is safe to rewrite them as
        // easier-to-optimize EXISTS expressions.
        // The assumption is that FALSE boolean results can replace NULL
        // boolean results in this context without a change in behavior.
        expr = optimizeInExpressions(expr);
        rejectDisallowedRowOpExpressions(expr);
        // If there were any subquery expressions appearing in a scalar context,
        // we must wrap them in ScalarValueExpressions to avoid wrong answers.
        // See ENG-8226.
        return ExpressionUtil.wrapScalarSubqueries(expr);
    }

    /**
     * Convert a HSQL VoltXML expression to an AbstractExpression tree.
     * @param root
     * @return configured AbstractExpression
     */
    // -- the function is now also called by DDLCompiler with no AbstractParsedStmt in sight --
    // so, the methods COULD be relocated to class AbstractExpression or ExpressionUtil.
    public AbstractExpression parseExpressionTree(VoltXMLElement root) {
        AbstractExpression expr = parseExpressionNode(root);

        // If there were any subquery expressions appearing in a scalar context,
        // we must wrap them in ScalarValueExpressions to avoid wrong answers.
        // See ENG-8226.
        expr = ExpressionUtil.wrapScalarSubqueries(expr);
        return expr;
    }

    private interface XMLElementExpressionParser {
        AbstractExpression parse(AbstractParsedStmt stmt, VoltXMLElement element);
    }

    private static Map<String, XMLElementExpressionParser> m_exprParsers =
            new HashMap<String, XMLElementExpressionParser>() {{
                put("value",
                        AbstractParsedStmt::parseValueExpression);
                put("vector",
                        AbstractParsedStmt::parseVectorExpression);
                put("columnref",
                        AbstractParsedStmt::parseColumnRefExpression);
                put("operation",
                        AbstractParsedStmt::parseOperationExpression);
                put("aggregation",
                        AbstractParsedStmt::parseAggregationExpression);
                put("asterisk",
                        (AbstractParsedStmt stmt, VoltXMLElement element) -> null);
                put("win_aggregation",
                        AbstractParsedStmt::parseWindowedAggregationExpression);
                put("function",
                        AbstractParsedStmt::parseFunctionExpression);
                put("tablesubquery",
                        AbstractParsedStmt::parseSubqueryExpression);
                put("row",
                        AbstractParsedStmt::parseRowExpression);

            }};

    /**
     * Given a VoltXMLElement expression node, translate it into an
     * AbstractExpression.  This is mostly a lookup in the table
     * m_exprParsers.
     */
    private AbstractExpression parseExpressionNode(VoltXMLElement exprNode) {
        String elementName = exprNode.name.toLowerCase();

        XMLElementExpressionParser parser = m_exprParsers.get(elementName);
        if (parser == null) {
            throw new PlanningErrorException("Unsupported expression node '" + elementName + "'", 0);
        }
        AbstractExpression retval = parser.parse(this, exprNode);
        assert("asterisk".equals(elementName) || retval != null);
        return retval;
    }

    /**
     * Parse a Vector value for SQL-IN
     */
    private AbstractExpression parseVectorExpression(VoltXMLElement exprNode) {
        ArrayList<AbstractExpression> args = new ArrayList<>();
        for (VoltXMLElement argNode : exprNode.children) {
            assert(argNode != null);
            // recursively parse each argument subtree (could be any kind of expression).
            AbstractExpression argExpr = parseExpressionNode(argNode);
            assert(argExpr != null);
            args.add(argExpr);
        }

        VectorValueExpression vve = new VectorValueExpression();
        vve.setValueType(VoltType.VOLTTABLE);
        vve.setArgs(args);
        return vve;
    }

    /**
     *
     * @param paramsById
     * @param exprNode
     * @return
     */
    private AbstractExpression parseValueExpression(VoltXMLElement exprNode) {
        String isParam = exprNode.attributes.get("isparam");
        String isPlannerGeneratedAttr = exprNode.attributes.get("isplannergenerated");
        boolean isPlannerGenerated;
        if (isPlannerGeneratedAttr != null) {
            isPlannerGenerated = isPlannerGeneratedAttr.equalsIgnoreCase("true");
        } else {
            isPlannerGenerated = false;
        }

        // A ParameterValueExpression is needed to represent any user-provided or planner-injected parameter.
        boolean needParameter = isParam != null && isParam.equalsIgnoreCase("true");

        // A ConstantValueExpression is needed to represent a constant in the statement,
        // EVEN if that constant has been "parameterized" by the plan caching code.
        ConstantValueExpression cve = null;
        boolean needConstant = !needParameter || isPlannerGenerated;

        if (needConstant) {
            String type = exprNode.attributes.get("valuetype");
            VoltType vt = VoltType.typeFromString(type);
            assert(vt != VoltType.VOLTTABLE);

            cve = new ConstantValueExpression();
            cve.setValueType(vt);
            if ((vt != VoltType.NULL) && (vt != VoltType.NUMERIC)) {
                int size = vt.getMaxLengthInBytes();
                cve.setValueSize(size);
            }
            if ( ! needParameter && vt != VoltType.NULL) {
                String valueStr = exprNode.attributes.get("value");
                // Verify that this string can represent the
                // desired type, by converting it into the
                // given type.
                if (valueStr != null) {
                    try {
                        switch (vt) {
                            case BIGINT:
                            case TIMESTAMP:
                                Long.valueOf(valueStr);
                                break;
                            case FLOAT:
                                Double.valueOf(valueStr);
                                break;
                            case DECIMAL:
                                VoltDecimalHelper.stringToDecimal(valueStr);
                            default:
                        }
                    } catch (PlanningErrorException ex) {
                        // We're happy with these.
                        throw ex;
                    } catch (NumberFormatException ex) {
                        throw new PlanningErrorException("Numeric conversion error to type "
                                + vt.name() + " " + ex.getMessage().toLowerCase());
                    } catch (Exception ex) {
                        throw new PlanningErrorException(ex.getMessage());
                    }
                }
                cve.setValue(valueStr);
            }
        }

        if (needParameter) {
            long id = Long.parseLong(exprNode.attributes.get("id"));
            ParameterValueExpression expr = m_paramsById.get(id);
            assert(expr != null);
            if (needConstant) {
                expr.setOriginalValue(cve);
                cve.setValue(m_paramValues[expr.getParameterIndex()]);
            }
            return expr;
        } else {
            return cve;
        }
    }

    /**
     *
     * @param exprNode
     * @return
     */
    private AbstractExpression parseColumnRefExpression(VoltXMLElement exprNode) {

        String tableName = exprNode.attributes.get("table");
        if (tableName == null) {
            assert(m_DDLIndexedTable != null);
            tableName = m_DDLIndexedTable.getTypeName();
        }
        assert(tableName != null);

        String tableAlias = exprNode.attributes.get("tablealias");
        if (tableAlias == null) {
            tableAlias = tableName;
        }
        String columnName = exprNode.attributes.get("column");
        String columnAlias = exprNode.attributes.get("alias");

        // Whether or not this column is the coalesced column produced by a join with a
        // USING predicate.
        String usingAttr = exprNode.attributes.get("using");
        boolean isUsingColumn = Boolean.parseBoolean(usingAttr);

        // Use the index produced by HSQL as a way to differentiate columns that have
        // the same name with a single table (which can happen for subqueries containing joins).
        int differentiator = Integer.parseInt(exprNode.attributes.get("index"));
        if (differentiator == -1 && isUsingColumn) {
            for (VoltXMLElement usingElem : exprNode.children) {
                String usingTableAlias = usingElem.attributes.get("tablealias");
                if (usingTableAlias != null && usingTableAlias.equals(tableAlias)) {
                    differentiator = Integer.parseInt(usingElem.attributes.get("index"));
                }
            }
        }

        TupleValueExpression tve = new TupleValueExpression(tableName, tableAlias,
                columnName, columnAlias, -1, differentiator);
        // Collect the unique columns used in the plan for a given scan.

        // Resolve the tve and add it to the scan's cache of referenced columns
        // Get tableScan where this TVE is originated from. In case of the
        // correlated queries or common table queries it may not be THIS statement
        // but its parent.  For example, in the statement
        //   with recursive rt as (
        //       select ... base case
        //     union all
        //       select ... from data join rt on ... <---
        //   ) select id from rt;  <===
        // The reference to rt (marked <---) will be in the recursive
        // parsed statement, but the reference to rt will be in the main
        // parsed statement.  The scan is defined in the main parsed
        // statement, and is in the main parsed statement's scan list.
        StmtTableScan tableScan = resolveStmtTableScanByAlias(tableAlias);
        if (tableScan == null) {
            // This never used to happen.  HSQL should make sure all the
            // identifiers are defined.  But something has gone wrong.
            // The query is "create index bidx2 on books ( cash + ( select cash from books as child where child.title < books.title ) );"
            // from TestVoltCompler.testScalarSubqueriesExpectedFailures.
            throw new PlanningErrorException("Object not found: " + tableAlias);
        }
        AbstractExpression resolvedExpr = tableScan.resolveTVE(tve);

        if (tableScan instanceof StmtCommonTableScan || m_stmtId == tableScan.getStatementId()) {
            return resolvedExpr;
        }

        // This is a TVE from the correlated expression
        int paramIdx = ParameterizationInfo.getNextParamIndex();
        ParameterValueExpression pve = new ParameterValueExpression(paramIdx, resolvedExpr);
        m_parameterTveMap.put(paramIdx, resolvedExpr);
        return pve;
    }

    /**
     * Parse an expression subquery
     */
    private SelectSubqueryExpression parseSubqueryExpression(VoltXMLElement exprNode) {
        assert(exprNode.children.size() == 1);
        VoltXMLElement subqueryElmt = exprNode.children.get(0);
        AbstractParsedStmt subqueryStmt = parseSubquery(subqueryElmt);
        // add table to the query cache
        String withoutAlias = null;
        StmtSubqueryScan stmtSubqueryScan = addSubqueryToStmtCache(subqueryStmt, withoutAlias);
        // Set to the default SELECT_SUBQUERY. May be overridden depending on the context
        return new SelectSubqueryExpression(ExpressionType.SELECT_SUBQUERY, stmtSubqueryScan);
    }

    // It turns out to be interesting to store this as a list.  We
    // really only want one of them, but it helps to check for multiple
    // windowed expressions in a different place than parsing.
    protected List<WindowFunctionExpression> m_windowFunctionExpressions = new ArrayList<>();

    public List<WindowFunctionExpression> getWindowFunctionExpressions() {
        return m_windowFunctionExpressions;
    }
    /**
     * Parse a windowed expression.  This actually just returns a TVE.  The
     * WindowFunctionExpression is squirreled away in the m_windowFunctionExpressions
     * object, though, because we will need it later.
     *
     * @param exprNode
     * @return
     */
    private AbstractExpression parseWindowedAggregationExpression(VoltXMLElement exprNode) {
        int id = Integer.parseInt(exprNode.attributes.get("id"));
        String optypeName = exprNode.attributes.get("optype");
        ExpressionType optype = ExpressionType.get(optypeName);
        if (optype == ExpressionType.INVALID) {
            throw new PlanningErrorException("Undefined windowed function call " + optypeName);
        }

        // If this is not in the display column list, and the id is not the id of
        // the windowed expression, then this is an error.
        if (!m_parsingInDisplayColumns) {
            if (m_windowFunctionExpressions.size() > 0) {
                WindowFunctionExpression we = m_windowFunctionExpressions.get(0);
                if (we.getXMLID() == id) {
                    // This is the same as a windowed expression we saw in the
                    // display list.  This can happen if we see an alias for
                    // a display list element in the order by expressions.  If
                    // this happens we just want to return the TVE we squirreled
                    // away in the windowed expression.
                    return we.getDisplayListExpression();
                }
            }
            throw new PlanningErrorException("Windowed function call expressions can only appear in the selection list of a query or subquery.");
        }
        // Parse individual aggregate expressions
        List<AbstractExpression> partitionbyExprs = new ArrayList<>();
        List<AbstractExpression> orderbyExprs = new ArrayList<>();
        List<SortDirectionType>  orderbyDirs  = new ArrayList<>();
        List<AbstractExpression> aggParams    = new ArrayList<>();

        for (VoltXMLElement childEle : exprNode.children) {
            if (childEle.name.equals("winspec")) {
                for (VoltXMLElement ele : childEle.children) {
                    if (ele.name.equals("partitionbyList")) {
                        for (VoltXMLElement childNode : ele.children) {
                            AbstractExpression expr = parseExpressionNode(childNode);
                            if (expr.hasSubquerySubexpression()) {
                                throw new PlanningErrorException(
                                        "SQL window functions cannot be partitioned by subquery expression arguments.");
                            }
                            ExpressionUtil.finalizeValueTypes(expr);
                            partitionbyExprs.add(expr);
                        }
                    } else if (ele.name.equals("orderbyList")) {
                        for (VoltXMLElement childNode : ele.children) {
                            SortDirectionType sortDir =
                                    Boolean.parseBoolean(childNode.attributes.get("descending")) ?
                                    SortDirectionType.DESC : SortDirectionType.ASC;

                            AbstractExpression expr = parseExpressionNode(childNode.children.get(0));
                            if (expr.hasSubquerySubexpression()) {
                                throw new PlanningErrorException(
                                        "SQL window functions cannot be ordered by subquery expression arguments.");
                            }
                            ExpressionUtil.finalizeValueTypes(expr);
                            orderbyExprs.add(expr);
                            orderbyDirs.add(sortDir);
                        }
                    }
                }
            } else {
                AbstractExpression aggParam = parseExpressionNode(childEle);
                if (aggParam != null) {
                    aggParam.finalizeValueTypes();
                    aggParams.add(aggParam);
                }
            }
        }

        String alias = WINDOWED_AGGREGATE_COLUMN_NAME;
        if (exprNode.attributes.containsKey("alias")) {
            alias = exprNode.attributes.get("alias");
        }
        WindowFunctionExpression rankExpr = new WindowFunctionExpression(
                optype, partitionbyExprs, orderbyExprs, orderbyDirs, aggParams, id);
        ExpressionUtil.finalizeValueTypes(rankExpr);
        // Only offset 0 is useful.  But we keep the index anyway.
        int offset = m_windowFunctionExpressions.size();
        m_windowFunctionExpressions.add(rankExpr);
        TupleValueExpression tve = new TupleValueExpression(
                TEMP_TABLE_NAME, TEMP_TABLE_NAME,
                alias, alias, rankExpr, offset);
        // This tve does not ever need a differentiator.
        tve.setNeedsNoDifferentiation();
        rankExpr.setDisplayListExpression(tve);
        return tve;
    }

    /**
     *
     * @param exprNode
     * @return
     */
    private AbstractExpression parseRowExpression(VoltXMLElement exprNode) {
        // Parse individual columnref expressions from the IN output schema
        // Short-circuit for COL IN (LIST) and COL IN (SELECT COL FROM ..)
        if (exprNode.children.size() == 1) {
            return parseExpressionNode(exprNode.children.get(0));
        } else { // (COL1, COL2) IN (SELECT C1, C2 FROM...)
            return parseRowExpression(exprNode.children);
        }
    }

    /**
     *
     * @param exprNodes
     * @return
     */
    private AbstractExpression parseRowExpression(List<VoltXMLElement> exprNodes) {
        // Parse individual columnref expressions from the IN output schema
        List<AbstractExpression> exprs = new ArrayList<>();
        for (VoltXMLElement exprNode : exprNodes) {
            AbstractExpression expr = parseExpressionNode(exprNode);
            exprs.add(expr);
        }
        return new RowSubqueryExpression(exprs);
    }

    /**
     * @return How many scans there are.
     */
    public int getScanCount() {
        return m_tableAliasMap.size();
    }

    /**
     * @return What are the aliases of all the scans.
     */
    public Set<String> getScanAliases() {
        return m_tableAliasMap.keySet();
    }

    public Collection<StmtTableScan> allScans() {
        return m_tableAliasMap.values();
    }

    /**
     * Return locally defined StmtTableScan by table alias.
     * @param tableAlias
     */
    public StmtTableScan getStmtTableScanByAlias(String tableAlias) {
        return m_tableAliasMap.get(tableAlias);
    }

    protected Set<Entry<String, StmtTableScan>> getScanEntrySet() {
        return m_tableAliasMap.entrySet();
    }

    /**
     * Return StmtTableScan by table alias. In case of correlated queries,
     * may need to walk up the statement tree.
     * @param tableAlias
     */
    private StmtTableScan resolveStmtTableScanByAlias(String tableAlias) {
        StmtTableScan tableScan = getStmtTableScanByAlias(tableAlias);
        if (tableScan != null) {
            return tableScan;
        } else if (m_parentStmt != null) {
            // This may be a correlated subquery
            return m_parentStmt.resolveStmtTableScanByAlias(tableAlias);
        } else {
            return null;
        }
    }

    /**
     * Add a table to the statement cache.
     * @param table
     * @param tableAlias
     * @return the cache entry
     */
    protected StmtTableScan addTableToStmtCache(Table table, String tableAlias) {
        // Create an index into the query Catalog cache
        StmtTableScan tableScan = m_tableAliasMap.get(tableAlias);
        if (tableScan == null) {
            tableScan = new StmtTargetTableScan(table, tableAlias, m_stmtId);
            m_tableAliasMap.put(tableAlias, tableScan);
        }
        return tableScan;
    }

    /**
     * Add a sub-query to the statement cache.
     * @param subquery
     * @param tableAlias
     * @return the cache entry
     */
    protected StmtSubqueryScan addSubqueryToStmtCache(
            AbstractParsedStmt subquery, String tableAlias) {
        assert(subquery != null);
        // If there is no usable alias because the subquery is inside an expression,
        // generate a unique one for internal use.
        if (tableAlias == null) {
            tableAlias = AbstractParsedStmt.TEMP_TABLE_NAME + "_" + subquery.m_stmtId;
        }
        StmtSubqueryScan subqueryScan =
                new StmtSubqueryScan(subquery, tableAlias, m_stmtId);
        StmtTableScan prior = m_tableAliasMap.put(tableAlias, subqueryScan);
        assert(prior == null);
        return subqueryScan;
    }

    /**
     * Verify if a subquery can be replaced with a direct select from table(s)
     * @param subquery
     * @return TRUE/FALSE
     */
    private StmtTargetTableScan simplifierForSubquery(AbstractParsedStmt subquery) {
        // Must be a SELECT statement (not a SET operation)
        if (!(subquery instanceof ParsedSelectStmt)) {
            return null;
        }
        ParsedSelectStmt selectSubquery = (ParsedSelectStmt) subquery;
        // No aggregation and/or GROUP BY is allowed
        if (selectSubquery.hasAggregateOrGroupby()) {
            return null;
        } else if (selectSubquery.hasAggregateDistinct()) { // No DISTINCT
            return null;
        } else if (selectSubquery.hasWindowFunctionExpression()) { // No windowed aggregate functions like RANK.
            return null;
        } else if (selectSubquery.hasLimitOrOffset() || selectSubquery.hasLimitOrOffsetParameters()) {
            // No LIMIT/OFFSET
            return null;
        }
        // Only SELECT from a single TARGET TABLE is allowed
        int tableCount = 0;
        StmtTargetTableScan simpler = null;
        for (Map.Entry<String, StmtTableScan> entry : selectSubquery.getScanEntrySet()) {
            if (entry.getKey().startsWith(AbstractParsedStmt.TEMP_TABLE_NAME)) {
                // This is an artificial table for a subquery expression
                continue;
            } else if (++tableCount > 1) {
                return null;
            }
            // Only allow one TARGET TABLE, not a nested subquery.
            StmtTableScan scan = entry.getValue();
            if (scan instanceof StmtTargetTableScan) {
                simpler = (StmtTargetTableScan) scan;
            } else {
                return null;
            }
        }
        return simpler;
    }

    /**
     * Replace an existing subquery scan with its underlying table scan. The subquery
     * has already passed all the checks from the canSimplifySubquery method.
     * Subquery ORDER BY clause is ignored if such exists.
     *
     * @param subqueryScan subquery scan to simplify
     * @param tableAlias
     * @return StmtTargetTableScan
     */
    private StmtTargetTableScan addSimplifiedSubqueryToStmtCache(
            StmtSubqueryScan subqueryScan, StmtTargetTableScan tableScan) {
        String tableAlias = subqueryScan.getTableAlias();
        assert(tableAlias != null);
        // It is guaranteed by the canSimplifySubquery that there is
        // one and only one TABLE in the subquery's FROM clause.
        Table promotedTable = tableScan.getTargetTable();
        StmtTargetTableScan promotedScan =
                new StmtTargetTableScan(promotedTable, tableAlias, m_stmtId);
        // Keep the original subquery scan to be able to tie the parent
        // statement column/table names and aliases to the table's.
        promotedScan.setOriginalSubqueryScan(subqueryScan);
        // Replace the subquery scan with the table scan
        StmtTableScan prior = m_tableAliasMap.put(tableAlias, promotedScan);
        assert(prior == subqueryScan);
        m_tableList.add(promotedTable);
        return promotedScan;
    }

    /**
     *
     * @param paramsById
     * @param exprNode
     * @return
     */
    private AbstractExpression parseOperationExpression(VoltXMLElement exprNode) {
        String optype = exprNode.attributes.get("optype");
        ExpressionType exprType = ExpressionType.get(optype);
        AbstractExpression expr;

        if (exprType == ExpressionType.INVALID) {
            throw new PlanningErrorException("Unsupported operation type '" + optype + "'");
        }

        try {
            expr = exprType.getExpressionClass().newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }
        expr.setExpressionType(exprType);

        if (exprType == ExpressionType.OPERATOR_CASE_WHEN || exprType == ExpressionType.OPERATOR_ALTERNATIVE) {
            String valueType = exprNode.attributes.get("valuetype");
            expr.setValueType(VoltType.typeFromString(valueType));
        }

        if (expr instanceof ComparisonExpression) {
            String opsubtype = exprNode.attributes.get("opsubtype");
            if (opsubtype != null) {
                QuantifierType quantifier = QuantifierType.get(opsubtype);
                if (quantifier != QuantifierType.NONE) {
                    ((ComparisonExpression)expr).setQuantifier(quantifier);
                }
            }
        }

        // get the first (left) node that is an element
        VoltXMLElement leftExprNode = exprNode.children.get(0);
        assert(leftExprNode != null);

        // recursively parse the left subtree (could be another operator or
        // a constant/tuple/param value operand).
        AbstractExpression leftExpr = parseExpressionNode(leftExprNode);
        assert((leftExpr != null) || (exprType == ExpressionType.AGGREGATE_COUNT));
        expr.setLeft(leftExpr);

        // get the second (right) node that is an element (might be null)
        VoltXMLElement rightExprNode = null;
        if (exprNode.children.size() > 1) {
            rightExprNode = exprNode.children.get(1);
        }

        if (expr.needsRightExpression()) {
            assert(rightExprNode != null);

            // recursively parse the right subtree
            AbstractExpression rightExpr = parseExpressionNode(rightExprNode);
            assert(rightExpr != null);
            expr.setRight(rightExpr);
        } else {
            assert(rightExprNode == null);
            if (exprType == ExpressionType.OPERATOR_CAST) {
                String valuetype = exprNode.attributes.get("valuetype");
                assert(valuetype != null);
                VoltType voltType = VoltType.typeFromString(valuetype);
                expr.setValueType(voltType);
                // We don't support parameterized casting, such as specifically to "VARCHAR(3)" vs. VARCHAR,
                // so assume max length for variable-length types (VARCHAR and VARBINARY).
                int size = voltType.getMaxLengthInBytes();
                expr.setValueSize(size);
            }
        }

        if (exprType == ExpressionType.COMPARE_EQUAL && QuantifierType.ANY == ((ComparisonExpression) expr).getQuantifier()) {
            // Break up UNION/INTERSECT (ALL) set ops into individual selects connected by
            // AND/OR operator
            // col IN ( queryA UNION queryB ) - > col IN (queryA) OR col IN (queryB)
            // col IN ( queryA INTERSECTS queryB ) - > col IN (queryA) AND col IN (queryB)
            expr = ParsedUnionStmt.breakUpSetOpSubquery(expr);
        } else if (exprType == ExpressionType.OPERATOR_EXISTS) {
            expr = optimizeExistsExpression(expr);
        }
        return expr;
    }

    private void rejectDisallowedRowOpExpressions(AbstractExpression expr) {
        ExpressionType exprType = expr.getExpressionType();

        // Recurse into the operands of logical operators
        // searching for row op subquery expressions.
        if (ExpressionType.CONJUNCTION_AND == exprType || ExpressionType.CONJUNCTION_OR == exprType) {
            rejectDisallowedRowOpExpressions(expr.getLeft());
            rejectDisallowedRowOpExpressions(expr.getRight());
            return;
        } else if (ExpressionType.OPERATOR_NOT == exprType) {
            rejectDisallowedRowOpExpressions(expr.getLeft());
        }

        // The problem cases are all comparison ops.
        if ( ! (expr instanceof ComparisonExpression)) {
            return;
        }

        // The problem cases all have row expressions as an operand.
        AbstractExpression rowExpression = expr.getLeft();
        if (rowExpression instanceof RowSubqueryExpression) {
            rejectDisallowedRowColumns(rowExpression);
        }
        rowExpression = expr.getRight();
        if (rowExpression instanceof RowSubqueryExpression) {
            rejectDisallowedRowColumns(rowExpression);
        }
    }

    private void rejectDisallowedRowColumns(AbstractExpression rowExpression) {
        // Verify that the ROW OP SUBQUERY expression's ROW operand only
        // contains column arguments that reference exactly one column.
        // I (--paul) don't know if it's possible -- in spite of the name
        // for a RowSubqueryExpression to be used in a context other than
        // a comparison with a subquery, AND I don't know if that context
        // needs to have the same guards as the normal subquery comparison
        // case, but let's err on the side of caution and use the same
        // guard condition for RowSubqueryExpressions until the current
        // problematic cases see ENG-9380 can be fully supported and have
        // been validated with tests.
        // The only known-safe case is where each (column) argument to a
        // RowSubqueryExpression is based on exactly one column value.
        for (AbstractExpression arg : rowExpression.getArgs()) {
            Collection<TupleValueExpression> tves = arg.findAllTupleValueSubexpressions();
            if (tves.size() != 1) {
                if (tves.isEmpty()) {
                    throw new PlanningErrorException(
                            "Unsupported use of a constant value in a row column expression.");
                } else {
                    throw new PlanningErrorException(
                            "Unsupported combination of column values in a row column expression.");
                }
            }
        }
    }

    /**
     * Perform various optimizations for IN/EXISTS subqueries if possible
     *
     * @param expr to optimize
     * @return optimized expression
     */
    private AbstractExpression optimizeInExpressions(AbstractExpression expr) {
        ExpressionType exprType = expr.getExpressionType();
        if (ExpressionType.CONJUNCTION_AND == exprType || ExpressionType.CONJUNCTION_OR == exprType) {
            AbstractExpression optimizedLeft = optimizeInExpressions(expr.getLeft());
            expr.setLeft(optimizedLeft);
            AbstractExpression optimizedRight = optimizeInExpressions(expr.getRight());
            expr.setRight(optimizedRight);
            return expr;
        } else if (ExpressionType.COMPARE_EQUAL != exprType) {
            return expr;
        }

        assert(expr instanceof ComparisonExpression);
        if (((ComparisonExpression)expr).getQuantifier() != QuantifierType.ANY) {
            return expr;
        }

        /*
         * Verify that an IN expression can be safely converted to an EXISTS one
         * IN (SELECT" forms e.g. "(A, B) IN (SELECT X, Y, FROM ...) =>
         * EXISTS (SELECT 42 FROM ... AND|WHERE|HAVING A=X AND|WHERE|HAVING B=Y)
         */
        AbstractExpression inColumns = expr.getLeft();
        if (inColumns instanceof SelectSubqueryExpression) {
            // If the left child is a (SELECT ...) expression itself we can't convert it
            // to the EXISTS expression because the mandatory run time scalar check -
            // (expression must return a single row at most)
            return expr;
        }

        // The right hand operand of the equality operation must be a SELECT statement
        AbstractExpression rightExpr = expr.getRight();
        if (!(rightExpr instanceof SelectSubqueryExpression)) {
            return expr;
        }

        SelectSubqueryExpression subqueryExpr = (SelectSubqueryExpression) rightExpr;
        AbstractParsedStmt subquery = subqueryExpr.getSubqueryStmt();
        if (!(subquery instanceof ParsedSelectStmt)) {
            return expr;
        }

        ParsedSelectStmt selectStmt = (ParsedSelectStmt) subquery;

        // Must not have OFFSET or LIMIT set
        // EXISTS (select * from T where T.X = parent.X order by T.Y offset 10 limit 5)
        //      seems to require 11 matches
        // parent.X IN (select T.X from T order by T.Y offset 10 limit 5)
        //      seems to require 1 match that has exactly 10-14 rows (matching or not) with lesser or equal values of Y.
        if (selectStmt.hasLimitOrOffset()) {
            return expr;
        }

        ParsedSelectStmt.rewriteInSubqueryAsExists(selectStmt, inColumns);
        subqueryExpr.resolveCorrelations();
        AbstractExpression existsExpr = new OperatorExpression();
        existsExpr.setExpressionType(ExpressionType.OPERATOR_EXISTS);
        existsExpr.setLeft(subqueryExpr);
        return optimizeExistsExpression(existsExpr);
    }

    /**
     * Simplify the EXISTS expression:
     *  1. EXISTS ( table-agg-without-having-groupby) => TRUE
     *  2. Replace the display columns with a single dummy column "1" and GROUP BY expressions
     *  3. Drop DISTINCT expression
     *  4. Add LIMIT 1
     *  5. Remove ORDER BY expressions if HAVING expression is not present
     *
     * @param existsExpr
     * @return optimized exists expression
     */
    private AbstractExpression optimizeExistsExpression(AbstractExpression existsExpr) {
        assert(ExpressionType.OPERATOR_EXISTS == existsExpr.getExpressionType());
        assert(existsExpr.getLeft() != null);

        if (existsExpr.getLeft() instanceof SelectSubqueryExpression) {
            SelectSubqueryExpression subqueryExpr = (SelectSubqueryExpression) existsExpr.getLeft();
            AbstractParsedStmt subquery = subqueryExpr.getSubqueryStmt();
            if (subquery instanceof ParsedSelectStmt) {
                ParsedSelectStmt selectSubquery = (ParsedSelectStmt) subquery;
                return selectSubquery.simplifyExistsSubqueryStmt(existsExpr);
            }
        }
        return existsExpr;
    }

    /**
     * Helper method to replace all TVEs and aggregated expressions with the corresponding PVEs.
     * The original expressions are placed into the map to be propagated to the EE.
     * The key to the map is the parameter index.
     *
     *
     * @param stmt - subquery statement
     * @param expr - expression with parent TVEs
     * @return Expression with parent TVE replaced with PVE
     */
    protected AbstractExpression replaceExpressionsWithPve(AbstractExpression expr) {
        assert(expr != null);
        if (expr instanceof TupleValueExpression) {
            int paramIdx = ParameterizationInfo.getNextParamIndex();
            ParameterValueExpression pve = new ParameterValueExpression(paramIdx, expr);
            m_parameterTveMap.put(paramIdx, expr);
            return pve;
        }

        if (expr instanceof AggregateExpression) {
            int paramIdx = ParameterizationInfo.getNextParamIndex();
            ParameterValueExpression pve = new ParameterValueExpression(paramIdx, expr);
            // Disallow aggregation of parent columns in a subquery.
            // except the case HAVING AGG(T1.C1) IN (SELECT T2.C2 ...)
            List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(expr);
            assert(m_parentStmt != null);
            for (TupleValueExpression tve : tves) {
                int origId = tve.getOrigStmtId();
                if (m_stmtId != origId && m_parentStmt.m_stmtId != origId) {
                    throw new PlanningErrorException(
                            "Subqueries do not support aggregation of parent statement columns");
                }
            }
            m_parameterTveMap.put(paramIdx, expr);
            return pve;
        }

        if (expr.getLeft() != null) {
            expr.setLeft(replaceExpressionsWithPve(expr.getLeft()));
        }
        if (expr.getRight() != null) {
            expr.setRight(replaceExpressionsWithPve(expr.getRight()));
        }
        if (expr.getArgs() != null) {
            List<AbstractExpression> newArgs = new ArrayList<>();
            for (AbstractExpression argument : expr.getArgs()) {
                newArgs.add(replaceExpressionsWithPve(argument));
            }
            expr.setArgs(newArgs);
        }
        return expr;
    }

    /**
     *
     * @param paramsById
     * @param exprNode
     * @return
     */

    private AbstractExpression parseAggregationExpression(VoltXMLElement exprNode) {
        String type = exprNode.attributes.get("optype");
        String tempId = exprNode.attributes.get("user_aggregate_id");
        int userAggregateId = tempId == null ? -1 : Integer.parseInt(tempId);
        String functionName = exprNode.attributes.get("name");
        ExpressionType exprType = ExpressionType.get(type);

        if (exprType == ExpressionType.INVALID) {
            throw new PlanningErrorException("Unsupported aggregation type '" + type + "'");
        }

        // Allow expressions to read expression-specific data from exprNode.
        // The design fully abstracts other volt classes from the XML serialization.
        // So, this goes here instead of in derived Expression implementations.

        assert(exprNode.children.size() <= 1);

        // get the single required child node
        VoltXMLElement childExprNode = exprNode.children.get(0);
        assert(childExprNode != null);

        // recursively parse the child subtree -- could (in theory) be an operator or
        // a constant, column, or param value operand or null in the specific case of "COUNT(*)".
        AbstractExpression childExpr = parseExpressionNode(childExprNode);
        if (childExpr == null) {
            assert(exprType == ExpressionType.AGGREGATE_COUNT);
            exprType = ExpressionType.AGGREGATE_COUNT_STAR;
        }

        AggregateExpression expr = new AggregateExpression(exprType, userAggregateId, functionName);
        expr.setLeft(childExpr);

        String node;
        if ((node = exprNode.attributes.get("distinct")) != null && Boolean.parseBoolean(node)) {
            expr.setDistinct();
        }
        if (m_aggregationList != null) {
            ExpressionUtil.finalizeValueTypes(expr);
            m_aggregationList.add(expr);
        }
        return expr;
    }


    /**
     *
     * @param exprNode
     * @return a new Function Expression
     */
    private AbstractExpression parseFunctionExpression(VoltXMLElement exprNode) {
        String name = exprNode.attributes.get("name").toLowerCase();
        String disabled = exprNode.attributes.get("disabled");
        if (disabled != null) {
            throw new PlanningErrorException("Function '" + name + "' is not supported in VoltDB: " + disabled);
        }
        String value_type_name = exprNode.attributes.get("valuetype");
        VoltType value_type = VoltType.typeFromString(value_type_name);
        String function_id = exprNode.attributes.get("function_id");
        assert(function_id != null);
        int idArg = 0;
        try {
            idArg = Integer.parseInt(function_id);
        } catch (NumberFormatException ignored) {}
        assert(idArg > 0);
        String result_type_parameter_index = exprNode.attributes.get("result_type_parameter_index");
        String implied_argument = exprNode.attributes.get("implied_argument");
        String optional_argument = exprNode.attributes.get("optional_argument");

        ArrayList<AbstractExpression> args = new ArrayList<>();
        for (VoltXMLElement argNode : exprNode.children) {
            assert(argNode != null);
            // recursively parse each argument subtree (could be any kind of expression).
            AbstractExpression argExpr = parseExpressionNode(argNode);
            assert(argExpr != null);
            args.add(argExpr);
        }

        FunctionExpression expr = new FunctionExpression();
        expr.setAttributes(name, implied_argument, optional_argument, idArg);
        expr.setArgs(args);
        if (value_type != null) {
            expr.setValueType(value_type);
            if (value_type != VoltType.INVALID && value_type != VoltType.NUMERIC) {
                int size = value_type.getMaxLengthInBytes();
                expr.setValueSize(size);
            }
        }

        if (result_type_parameter_index != null) {
            int parameter_idx = -1;
            try {
                parameter_idx = Integer.parseInt(result_type_parameter_index);
            } catch (NumberFormatException ignored) {}
            assert(parameter_idx >= 0); // better be valid by now.
            assert(parameter_idx < args.size()); // must refer to a provided argument
            expr.setResultTypeParameterIndex(parameter_idx);
            expr.negotiateInitialValueTypes();
        }
        return expr;
    }

    /**
     * Build a WHERE expression for a single-table statement.
     */
    public AbstractExpression getSingleTableFilterExpression() {
        Preconditions.checkState(m_joinTree != null);
        return m_joinTree.getSimpleFilterExpression();
    }

    /**
     *
     * @param tableNode
     */
    private void parseTable(VoltXMLElement tableNode) {
        String tableName = tableNode.attributes.get("table");
        assert(tableName != null);

        String tableAlias = tableNode.attributes.get("tablealias");
        if (tableAlias == null) {
            tableAlias = tableName;
        }
        // Hsql rejects name conflicts in a single query
        m_tableAliasListAsJoinOrder.add(tableAlias);

        VoltXMLElement subqueryElement = null;

        // Possible sub-query
        for (VoltXMLElement childNode : tableNode.children) {
            if (childNode.name.equals("tablesubquery") && ! childNode.children.isEmpty()) {
                // sub-query FROM (SELECT ...)
                subqueryElement = childNode.children.get(0);
                break;
            }
        }

        // add table to the query cache before processing the JOIN/WHERE expressions
        // The order is important because processing sub-query expressions assumes that
        // the sub-query is already registered
        StmtTableScan tableScan;
        Table table;

        // In case of a subquery we need to preserve its filter expressions
        AbstractExpression simplifiedSubqueryFilter = null;

        // This might be a common table, persistent table or a derived
        // table, which we call a subquery.  We will know that it's the
        // latter case if subqueryElement is non-null.  But we can't really
        // differentiate between persistent tables and common tables.
        // So, first look for derived tables, then common tables and
        // finally persistent tables.
        //
        // First, look for a common table by its name.  This will
        // return a new object which we need to define, or else
        // null if we can't find it.
        if (subqueryElement != null) {
            // This has to be a derived table, which we call a subquery.
            AbstractParsedStmt subquery = parseFromSubQuery(subqueryElement);
            StmtSubqueryScan subqueryScan = addSubqueryToStmtCache(subquery, tableAlias);
            tableScan = subqueryScan;
            StmtTargetTableScan simpler = simplifierForSubquery(subquery);
            if (simpler != null) {
                tableScan = addSimplifiedSubqueryToStmtCache(subqueryScan, simpler);
                table = simpler.getTargetTable();
                // Extract subquery's filters
                assert(subquery.m_joinTree != null);
                // Adjust the table alias in all TVEs from the eliminated
                // subquery expressions. Example:
                // SELECT TA2.CA FROM (SELECT C CA FROM T TA1 WHERE C > 0) TA2
                // The table alias TA1 from the original TVE (T)TA1.C from the
                // subquery WHERE condition needs to be replaced with the alias
                // TA2. The new TVE will be (T)TA2.C.
                // The column alias does not require an adjustment.
                simplifiedSubqueryFilter = subquery.m_joinTree.getAllFilters();
                List<TupleValueExpression> tves =
                        ExpressionUtil.getTupleValueExpressions(simplifiedSubqueryFilter);
                for (TupleValueExpression tve : tves) {
                    tve.setTableAlias(tableScan.getTableAlias());
                    tve.setOrigStmtId(m_stmtId);
                }
            }
        } else {
            tableScan = resolveCommonTableByName(tableName, tableAlias);
            if (tableScan != null) {
                // Make the alias refer to the table scan we
                // just found.
                assert(tableScan instanceof StmtCommonTableScan);
                defineTableScanByAlias(tableAlias, tableScan);
            } else {
                // Well, this is not a common table, so look for a table in the catalog.
                table = getTableFromDB(tableName);
                if (table != null) {
                    tableScan = addTableToStmtCache(table, tableAlias);
                    m_tableList.add(table);
                }
            }
        }
        // It has to be one of the cases above.  Anything else is an
        // internal error.  Perhaps this should be an assert?
        assert(tableScan != null);
        AbstractExpression joinExpr = parseJoinCondition(tableNode);
        AbstractExpression whereExpr = parseWhereCondition(tableNode);
        if (simplifiedSubqueryFilter != null) {
            // Add subquery's expressions as JOIN filters to make sure they will
            // stay at the node level in case of an OUTER joins and won't affect
            // the join simplification process:
            // select * from T LEFT JOIN (select C FROM T1 WHERE C > 2) S ON T.C = S.C;
            joinExpr = (joinExpr != null) ?
                    ExpressionUtil.combine(joinExpr, simplifiedSubqueryFilter) :
                    simplifiedSubqueryFilter;
        }

        // The join type of the leaf node is always INNER
        // For a new tree its node's ids start with 0 and keep incrementing by 1
        int nodeId = (m_joinTree == null) ? 0 : m_joinTree.getId() + 1;

        JoinNode leafNode;
        leafNode = tableScan.makeLeafNode(nodeId, joinExpr, whereExpr);

        if (m_joinTree == null) {
            // this is the first table
            m_joinTree = leafNode;
        } else {
            // Build the tree by attaching the next table always to the right
            // The node's join type is determined by the type of its right node

            JoinType joinType = JoinType.get(tableNode.attributes.get("jointype"));
            assert(joinType != JoinType.INVALID);
            m_joinTree = new BranchNode(nodeId + 1, joinType, m_joinTree, leafNode);
       }
    }

    /**
     * Define a common table by alias.  This happens when a common
     * table is defined in a "from" clause.  For example,
     * in the SQL:  "select * from T as A" the table T is defined
     * as the alias A.  See the declaration of m_commonTableNameMap.
     *
     * @param tableAlias
     * @param tableScan
     */
    protected void defineTableScanByAlias(String tableAlias, StmtTableScan tableScan) {
        m_tableAliasMap.put(tableAlias, tableScan);
    }


    /**
     * Look for a common table by name, possibly in parent scopes.
     * This is different from resolveStmtTableByAlias in that it
     * looks for common tables and only by name, not by alias.  Of
     * course, a name and an alias are both strings, so this is kind
     * of a stylized distinction.
     *
     * @param tableName
     * @return
     */
    private StmtCommonTableScan resolveCommonTableByName(String tableName, String tableAlias) {
        StmtCommonTableScan answer = null;
        StmtCommonTableScanShared scan = null;
        for (AbstractParsedStmt scope = this; scope != null && scan == null; scope = scope.getParentStmt()) {
            scan = scope.getCommonTableByName(tableName);
        }
        if (scan != null) {
            answer = new StmtCommonTableScan(tableName, tableAlias, scan);
        }
        return answer;
    }

    private Map<String, StmtCommonTableScanShared> m_commonTableSharedMap = new HashMap<>();

    private StmtCommonTableScanShared getCommonTableByName(String tableName) {
        return m_commonTableSharedMap.get(tableName);
    }
    /**
     * Lookup or define the shared part of a common table by name.  This happens when the
     * common table name is first encountered.  For example,
     * in the SQL: "with name as ( select * from ttt ) select name as a, name as b"
     * the name "name" is a common table name.  It's not an
     * alias.  This is comparable to defining a table name in
     * the catalog, but it does not persist past the current
     * statement.  So it does not make any sense to make it a
     * catalog entry.
     *
     * @param tableName The table name, not the table alias.
     */
    protected StmtCommonTableScanShared defineCommonTableScanShared(String tableName, int stmtId) {
        assert (m_commonTableSharedMap.get(tableName) == null);
        StmtCommonTableScanShared answer = new StmtCommonTableScanShared(tableName, stmtId);
        m_commonTableSharedMap.put(tableName, answer);
        return answer;
    }

    private AbstractParsedStmt getParentStmt() {
        return m_parentStmt;
    }

    /**
     *
     * @param tablesNode
     */
    private void parseTables(VoltXMLElement tablesNode) {
        Set<String> visited = new HashSet<>();

        for (VoltXMLElement node : tablesNode.children) {
            if (node.name.equalsIgnoreCase("tablescan")) {

                String visitedTable = node.attributes.get("tablealias");
                if (visitedTable == null) {
                    visitedTable = node.attributes.get("table");
                }

                assert(visitedTable != null);

                if (visited.contains(visitedTable)) {
                    throw new PlanningErrorException("Not unique table/alias: " + visitedTable);
                }

                parseTable(node);
                visited.add(visitedTable);
            }
        }
    }

    /**
     * Populate the statement's paramList from the "parameters" element. Each
     * parameter has an id and an index, both of which are numeric. It also has
     * a type and an indication of whether it's a vector parameter. For each
     * parameter, we create a ParameterValueExpression, named pve, which holds
     * the type and vector parameter indication. We add the pve to two maps,
     * m_paramsById and m_paramsByIndex.
     *
     * A parameter's index attribute is its offset in the parameters array which
     * is used to determine the parameter's value in the EE at runtime.
     *
     * Some parameters are generated after we generate VoltXML but before we plan (constants may
     * become parameters in ad hoc queries so their plans may be cached).  In this case
     * the index of the parameter is already set.  Otherwise, the parameter's index will have been
     * set in HSQL.
     *
     * @param paramsNode
     */
    protected void parseParameters(VoltXMLElement root) {
        VoltXMLElement paramsNode = null;
        for (VoltXMLElement node : root.children) {
            if (node.name.equalsIgnoreCase("parameters")) {
                paramsNode = node;
                break;
            }
        }
        if (paramsNode == null) {
            return;
        }

        for (VoltXMLElement node : paramsNode.children) {
            if (node.name.equalsIgnoreCase("parameter")) {
                long id = Long.parseLong(node.attributes.get("id"));
                String typeName = node.attributes.get("valuetype");
                String isVectorParam = node.attributes.get("isvector");

                // Get the index for this parameter in the EE's parameter vector
                String indexAttr = node.attributes.get("index");
                assert(indexAttr != null);
                int index = Integer.parseInt(indexAttr);

                VoltType type = VoltType.typeFromString(typeName);
                ParameterValueExpression pve = new ParameterValueExpression();
                pve.setParameterIndex(index);
                pve.setValueType(type);
                if (isVectorParam != null && isVectorParam.equalsIgnoreCase("true")) {
                    pve.setParamIsVector();
                }
                m_paramsById.put(id, pve);
                getParamsByIndex().put(index, pve);
            }
        }
    }

    /** Get a list of the table subqueries used by this statement.  This method
     * may be overridden by subclasses, e.g., insert statements have a subquery
     * but does not use m_joinTree.
     **/
    public List<StmtEphemeralTableScan> getEphemeralTableScans() {
        List<StmtEphemeralTableScan> scans = new ArrayList<>();
        if (m_joinTree != null) {
            m_joinTree.extractEphemeralTableQueries(scans);
        }
        return scans;
    }

    // The parser currently attaches the summary parameter list
    // to each leaf (select) statement in a union, but not to the
    // union statement itself. It is always the same parameter list,
    // the one that applies globally to the entire set of leaf select
    // statements each of which may or may not use each parameter.
    // The list is required later at the top-level statement for
    // proper cataloging, so promote it here to each parent union.
    //
    // Similarly, as we build the parsed statement representation of a
    // a union, we need to promote "parameter TVEs" appearing in
    // leaf select statements.  Parameter TVEs are column references
    // in tables from an outer query.  These are represented as parameters
    // in the EE.
    protected void promoteUnionParametersFromChild(AbstractParsedStmt childStmt) {
        getParamsByIndex().putAll(childStmt.getParamsByIndex());
        m_parameterTveMap.putAll(childStmt.m_parameterTveMap);
    }

    /**
     * Collect value equivalence expressions across the entire SQL statement
     * @return a map of tuple value expressions to the other expressions,
     * TupleValueExpressions, ConstantValueExpressions, or ParameterValueExpressions,
     * that they are constrained to equal.
     */
    Map<AbstractExpression, Set<AbstractExpression>> analyzeValueEquivalence() {
        // collect individual where/join expressions
        m_joinTree.analyzeJoinExpressions(this);
        return m_joinTree.getAllEquivalenceFilters();
    }

    /**
     * Look up a table by name.  This table may be stored in the
     * local catalog or else the global catalog.
     * @param tableName
     * @return
     */
    protected Table getTableFromDB(String tableName) {
        return m_db.getTables().getExact(tableName);
    }

    @Override
    public String toString() {
        StringBuilder retval = new StringBuilder("SQL:\n\t" + m_sql + "\n");
        String sep;

        retval.append("PARAMETERS:\n\t");
        sep = "";
        for (Map.Entry<Integer, ParameterValueExpression> paramEntry : getParamsByIndex().entrySet()) {
            retval.append(sep).append(paramEntry.getValue().toString());
            sep = ", ";
        }

        retval.append("\nTABLE SOURCES:\n\t");
        sep = "";
        for (Table table : m_tableList) {
            retval.append(sep).append(table.getTypeName());
            sep = ", ";
        }
        // Find the common table sources.
        for (String commonTableName : m_tableAliasMap.keySet()) {
            StmtTableScan scan = m_tableAliasMap.get(commonTableName);
            if (scan instanceof StmtCommonTableScan) {
                retval.append(sep).append(commonTableName).append(" (CTE)");
                sep = ", ";
            }
        }
        retval.append("\nSCAN COLUMNS:\n");
        boolean hasAll = true;
        for (StmtTableScan tableScan : m_tableAliasMap.values()) {
            List<SchemaColumn> scanColumns = tableScan.getScanColumns();
            if (scanColumns.isEmpty()) {
                continue;
            }
            hasAll = false;
            retval.append("\tTable Alias: ").append(tableScan.getTableAlias()).append(":\n");
            for (SchemaColumn col : scanColumns) {
                retval.append("\t\tColumn: ").append(col.getColumnName()).append(": ");
                retval.append(col.getExpression().toString()).append("\n");
            }
        }
        if (hasAll) {
            retval.append("\tALL\n");
        }

        retval.append("\nJOIN TREE :\n");
        if (m_joinTree != null) {
            retval.append(m_joinTree.toString());
        }

        retval.append("NO TABLE SELECTION LIST:\n");
        int i = 0;
        for (AbstractExpression expr : m_noTableSelectionList) {
            retval.append("\t(").append(String.valueOf(i++)).append(") ").append(expr.toString()).append("\n");
        }
        return retval.toString();
    }

    protected AbstractParsedStmt parseFromSubQuery(VoltXMLElement queryNode) {
        AbstractParsedStmt subquery = AbstractParsedStmt.getParsedStmt(this, queryNode, m_paramValues, m_db);
        // Propagate parameters from the parent to the child
        subquery.m_paramsById.putAll(m_paramsById);
        subquery.setParamsByIndex(m_paramsByIndex);

        AbstractParsedStmt.parse(subquery, m_sql, queryNode, m_joinOrder);
        return subquery;
    }

    protected AbstractParsedStmt parseSubquery(VoltXMLElement suqueryElmt) {
        AbstractParsedStmt subQuery = AbstractParsedStmt.getParsedStmt(this, suqueryElmt, m_paramValues, m_db);
        // Propagate parameters from the parent to the child
        subQuery.m_paramsById.putAll(m_paramsById);

        AbstractParsedStmt.parse(subQuery, m_sql, suqueryElmt, m_joinOrder);
        updateContentDeterminismMessage(subQuery.calculateContentDeterminismMessage());
        return subQuery;
    }

    /** Parse a where or join clause. This behavior is common to all kinds of statements.
     */
    private AbstractExpression parseTableCondition(VoltXMLElement tableScan,
            String joinOrWhere) {
        AbstractExpression condExpr = null;
        for (VoltXMLElement childNode : tableScan.children) {
            if ( ! childNode.name.equalsIgnoreCase(joinOrWhere)) {
                continue;
            }
            assert(childNode.children.size() == 1);
            assert(condExpr == null);
            condExpr = parseConditionTree(childNode.children.get(0));
            assert(condExpr != null);
            ExpressionUtil.finalizeValueTypes(condExpr);
            condExpr = ExpressionUtil.evaluateExpression(condExpr);
            // If the condition is a trivial CVE(TRUE) (after the evaluation) simply drop it
            if (ConstantValueExpression.isBooleanTrue(condExpr)) {
                condExpr = null;
            }
        }
        return condExpr;
    }

    private AbstractExpression parseJoinCondition(VoltXMLElement tableScan) {
        return parseTableCondition(tableScan, "joincond");
    }

    private AbstractExpression parseWhereCondition(VoltXMLElement tableScan) {
        return parseTableCondition(tableScan, "wherecond");
    }

    public ParameterValueExpression[] getParameters() {
        // If a statement contains subqueries the parameters will be associated with
        // the parent statement
        if (m_parentStmt != null) {
            return m_parentStmt.getParameters();
        }

        return getParamsByIndex().values().toArray(new ParameterValueExpression[0]);
    }

    public void setParentAsUnionClause() {
        m_isChildOfUnion = true;
    }

    public boolean isParentUnionClause() {
        return m_isChildOfUnion;
    }

    public boolean hasLimitOrOffset() {
        // This dummy implementation for DML statements should never be called.
        // The interface is established on AbstractParsedStmt for support
        // in ParsedSelectStmt and ParsedUnionStmt.
        return false;
    }

    public boolean isOrderDeterministic() {
        // The interface is established on AbstractParsedStmt for support
        // in ParsedSelectStmt, ParsedUnionStmt, and ParsedDeleteStmt.
        throw new RuntimeException("isOrderDeterministic not supported by INSERT or UPDATE statements");
    }

    public boolean isOrderDeterministicInSpiteOfUnorderedSubqueries() {
        // This dummy implementation for DML statements should never be called.
        // The interface is established on AbstractParsedStmt for support
        // in ParsedSelectStmt and ParsedUnionStmt.
        throw new RuntimeException("isOrderDeterministicInSpiteOfUnorderedSubqueries not supported by DML statements");
    }

    protected AbstractExpression getParameterOrConstantAsExpression(long id, long value) {
        // The id was previously passed to parameterCountIndexById, so if not -1,
        // it has already been asserted to be a valid id for a parameter, and the
        // parameter's type has been refined to INTEGER.
        if (id != -1) {
            return m_paramsById.get(id);
        }

        // The limit/offset is a non-parameterized literal value that needs to be wrapped in a
        // BIGINT constant so it can be used in the addition expression for the pushed-down limit.
        ConstantValueExpression constant = new ConstantValueExpression();
        constant.setValue(Long.toString(value));
        constant.refineValueType(VoltType.BIGINT, VoltType.BIGINT.getLengthInBytesForFixedTypes());
        return constant;
    }

    /// This is for use with integer-valued row count parameters, namely LIMITs and OFFSETs.
    /// It should be called (at least) once for each LIMIT or OFFSET parameter to establish that
    /// the parameter is being used in a BIGINT context.
    /// There may be limitations elsewhere that restrict limits and offsets to 31-bit unsigned values,
    /// but enforcing that at parameter passing/checking time seems a little arbitrary, so we keep
    /// the parameters at maximum width -- a 63-bit unsigned BIGINT.
    protected int parameterCountIndexById(long paramId) {
        if (paramId == -1) {
            return -1;
        }

        assert(m_paramsById.containsKey(paramId));
        ParameterValueExpression pve = m_paramsById.get(paramId);
        // As a side effect, re-establish these parameters as integer-typed
        // -- this helps to catch type errors earlier in the invocation process
        // and prevents a more serious error in HSQLBackend statement reconstruction.
        // The HSQL parser originally had these correctly pegged as BIGINTs,
        // but the VoltDB code ( @see AbstractParsedStmt#parseParameters )
        // skeptically second-guesses that pending its own verification. This case is now verified.
        pve.refineValueType(VoltType.BIGINT, VoltType.BIGINT.getLengthInBytesForFixedTypes());
        return pve.getParameterIndex();
    }

    /**
     * Produce a LimitPlanNode from the given XML
     * @param limitXml    Volt XML for limit
     * @param offsetXml   Volt XML for offset
     * @return An instance of LimitPlanNode for the given XML
     */
    LimitPlanNode limitPlanNodeFromXml(VoltXMLElement limitXml, VoltXMLElement offsetXml) {

        if (limitXml == null && offsetXml == null) {
            return null;
        }

        String node;
        long limitParameterId = -1;
        long offsetParameterId = -1;
        long limit = -1;
        long offset = 0;
        if (limitXml != null) {
            // Parse limit
            if ((node = limitXml.attributes.get("limit_paramid")) != null) {
                limitParameterId = Long.parseLong(node);
            } else {
                assert(limitXml.children.size() == 1);
                VoltXMLElement valueNode = limitXml.children.get(0);
                String isParam = valueNode.attributes.get("isparam");
                if ((isParam != null) && (isParam.equalsIgnoreCase("true"))) {
                    limitParameterId = Long.parseLong(valueNode.attributes.get("id"));
                } else {
                    node = limitXml.attributes.get("limit");
                    assert(node != null);
                    limit = Long.parseLong(node);
                }
            }
        }
        if (offsetXml != null) {
            // Parse offset
            if ((node = offsetXml.attributes.get("offset_paramid")) != null) {
                offsetParameterId = Long.parseLong(node);
            } else {
                if (offsetXml.children.size() == 1) {
                    VoltXMLElement valueNode = offsetXml.children.get(0);
                    String isParam = valueNode.attributes.get("isparam");
                    if ((isParam != null) && (isParam.equalsIgnoreCase("true"))) {
                        offsetParameterId = Long.parseLong(valueNode.attributes.get("id"));
                    } else {
                        node = offsetXml.attributes.get("offset");
                        assert(node != null);
                        offset = Long.parseLong(node);
                    }
                }
            }
        }

        // limit and offset can't have both value and parameter
        assert limit == -1 || limitParameterId == -1 : "Parsed value and param. limit.";
        assert offset == 0 || offsetParameterId == -1 : "Parsed value and param. offset.";

        LimitPlanNode limitPlanNode = new LimitPlanNode();
        limitPlanNode.setLimit((int) limit);
        limitPlanNode.setOffset((int) offset);
        limitPlanNode.setLimitParameterIndex(parameterCountIndexById(limitParameterId));
        limitPlanNode.setOffsetParameterIndex(parameterCountIndexById(offsetParameterId));
        return limitPlanNode;
    }

    /**
     * Order by Columns or expressions has to operate on the display columns or expressions.
     * @return
     */
    protected boolean orderByColumnsCoverUniqueKeys() {
        // In theory, if EVERY table in the query has a uniqueness constraint
        // (primary key or other unique index) on columns that are all listed in the ORDER BY values,
        // the result is deterministic.
        // This holds regardless of whether the associated index is actually used in the selected plan,
        // so this check is plan-independent.
        //
        // baseTableAliases associates table aliases with the order by
        // expressions which reference them.  Presumably by using
        // table aliases we will map table scans to expressions rather
        // than tables to expressions, and not confuse ourselves with
        // different instances of the same table in self joins.
        HashMap<String, List<AbstractExpression> > baseTableAliases =
                new HashMap<>();
        for (ParsedColInfo col : orderByColumns()) {
            AbstractExpression expr = col.m_expression;
            //
            // Compute the set of tables mentioned in the expression.
            //   1. Search out all the TVEs.
            //   2. Throw the aliases of the tables of each of these into a HashSet.
            //      The table must have an alias.  It might not have a name.
            //   3. If the HashSet has size > 1 we can't use this expression.
            //
            List<TupleValueExpression> baseTVEExpressions =
                    expr.findAllTupleValueSubexpressions();
            Set<String> baseTableNames = new HashSet<>();
            for (TupleValueExpression tve : baseTVEExpressions) {
                String tableAlias = tve.getTableAlias();
                assert(tableAlias != null);
                baseTableNames.add(tableAlias);
            }
            if (baseTableNames.size() != 1) {
                // Table-spanning ORDER BYs -- like ORDER BY A.X + B.Y are not helpful.
                // Neither are (nonsense) constant (table-less) expressions.
                continue;
            }
            // Everything in the baseTVEExpressions table is a column
            // in the same table and has the same alias. So just grab the first one.
            // All we really want is the alias.
            TupleValueExpression baseTVE = baseTVEExpressions.get(0);
            String nextTableAlias = baseTVE.getTableAlias();
            // This was tested above.  But the assert above may prove to be over cautious
            // and disappear.
            assert(nextTableAlias != null);
            List<AbstractExpression> perTable = baseTableAliases.computeIfAbsent(nextTableAlias, k -> new ArrayList<>());
            perTable.add(expr);
        }

        if (m_tableAliasMap.size() > baseTableAliases.size()) {
            // FIXME: There are more table aliases in the select list than tables
            //        named in the order by clause.  So, some tables named in the
            //        select list are not explicitly listed in the order by
            //        clause.
            //
            //        This would be one of the tricky cases where the goal would be to prove that the
            //        row with no ORDER BY component came from the right side of a 1-to-1 or many-to-1 join.
            //        like Unique Index nested loop join, etc.
            return false;
        }
        boolean allScansAreDeterministic = true;
        for (Entry<String, List<AbstractExpression>> orderedAlias : baseTableAliases.entrySet()) {
            List<AbstractExpression> orderedAliasExprs = orderedAlias.getValue();
            StmtTableScan tableScan = getStmtTableScanByAlias(orderedAlias.getKey());
            if (tableScan == null) {
                assert(false);
                return false;
            } else if (tableScan instanceof StmtSubqueryScan) {
                return false; // don't yet handle FROM clause subquery, here.
            }

            Table table = ((StmtTargetTableScan)tableScan).getTargetTable();

            // This table's scans need to be proven deterministic.
            allScansAreDeterministic = false;
            // Search indexes for one that makes the order by deterministic
            for (Index index : table.getIndexes()) {
                // skip non-unique indexes
                if ( ! index.getUnique()) {
                    continue;
                }

                // get the list of expressions for the index
                List<AbstractExpression> indexExpressions = new ArrayList<>();

                String jsonExpr = index.getExpressionsjson();
                // if this is a pure-column index...
                if (jsonExpr.isEmpty()) {
                    for (ColumnRef cref : index.getColumns()) {
                        final Column col = cref.getColumn();
                        final TupleValueExpression tve = new TupleValueExpression(
                                table.getTypeName(), orderedAlias.getKey(),
                                col.getName(), col.getName(), col.getIndex());
                        indexExpressions.add(tve);
                    }
                } else { // if this is a fancy expression-based index...
                    try {
                        indexExpressions = AbstractExpression.fromJSONArrayString(jsonExpr, tableScan);
                    } catch (JSONException e) {
                        e.printStackTrace();
                        assert(false);
                        continue;
                    }
                }

                // If the sort covers the index, then it's a unique sort.
                // TODO: The statement's equivalence sets would be handy here to recognize cases like
                //    WHERE B.unique_id = A.b_id
                //    ORDER BY A.unique_id, A.b_id
                if (orderedAliasExprs.containsAll(indexExpressions)) {
                    allScansAreDeterministic = true;
                    break;
                }
            }
            // ALL tables' scans need to have proved deterministic
            if ( ! allScansAreDeterministic) {
                return false;
            }
        }
        return true;
    }

    /**
     * Given a set of order-by expressions and a select list, which is a list of
     * columns, each with an expression and an alias, expand the order-by list
     * with new expressions which could be on the order-by list without changing
     * the sort order and which are otherwise helpful.
     */
    protected void addHonoraryOrderByExpressions(
            HashSet<AbstractExpression> orderByExprs,
            List<ParsedColInfo> candidateColumns) {
        // If there is not exactly one table scan we will not proceed.
        // We don't really know how to make indices work with joins,
        // and there is nothing more to do with subqueries.  The processing
        // of joins is the content of ticket ENG-8677.
        if (m_tableAliasMap.size() != 1) {
            return;
        }

        Map<AbstractExpression, Set<AbstractExpression>> valueEquivalence =
                analyzeValueEquivalence();
        for (ParsedColInfo colInfo : candidateColumns) {
            AbstractExpression colExpr = colInfo.m_expression;
            if (colExpr instanceof TupleValueExpression) {
                Set<AbstractExpression> tveEquivs = valueEquivalence.get(colExpr);
                if (tveEquivs != null) {
                    for (AbstractExpression expr : tveEquivs) {
                        if (expr instanceof ParameterValueExpression
                                || expr instanceof ConstantValueExpression) {
                            orderByExprs.add(colExpr);
                        }
                    }
                }
            }
        }
        // We know there's exactly one.
        StmtTableScan scan = m_tableAliasMap.values().iterator().next();
        // Get the table.  There's only one.
        Table table = getTableFromDB(scan.getTableName());
        // Maybe this is a subquery?  If we can't find the table
        // there's no use to continue.
        if (table == null) {
            return;
        }
        // Now, look to see if there is a constraint which can help us.
        // If there is a unique constraint on a set of columns, and all
        // the constrained columns are in the order by list, then all
        // the columns in the table can be added to the order by list.
        //
        // The indices we care about have columns, but the order by list has expressions.
        // Extract the columns from the order by list.
        Set<Column> orderByColumns = new HashSet<>();
        for (AbstractExpression expr : orderByExprs) {
            if (expr instanceof TupleValueExpression) {
                TupleValueExpression tve = (TupleValueExpression) expr;
                Column col = table.getColumns().get(tve.getColumnName());
                orderByColumns.add(col);
            }
        }
        CatalogMap<Constraint> constraints = table.getConstraints();
        // If we have no constraints, there's nothing more to do here.
        if (constraints == null) {
            return;
        }

        Set<Index> indices = new HashSet<>();
        for (Constraint constraint : constraints) {
            Index index = constraint.getIndex();
            // Only use column indices for now.
            if (index != null && index.getUnique() && index.getExpressionsjson().isEmpty()) {
                indices.add(index);
            }
        }
        for (ParsedColInfo colInfo : candidateColumns) {
            AbstractExpression expr = colInfo.m_expression;
            if (expr instanceof TupleValueExpression) {
                TupleValueExpression tve = (TupleValueExpression) expr;
                // If one of the indices is completely covered
                // we will not have to process any other indices.
                // So, we remember this and early-out.
                for (Index index : indices) {
                    CatalogMap<ColumnRef> columns = index.getColumns();
                    // If all the columns in this index are in the current
                    // honorary order by list, then we can add all the
                    // columns in this table to the honorary order by list.
                    boolean addAllColumns = true;
                    for (ColumnRef cr : columns) {
                        Column col = cr.getColumn();
                        if (! orderByColumns.contains(col)) {
                            addAllColumns = false;
                            break;
                        }
                    }
                    if (addAllColumns) {
                        for (Column addCol : table.getColumns()) {
                            // We have to convert this to a TVE to add
                            // it to the orderByExprs.  We will use -1
                            // for the column index.  We don't have a column
                            // alias.
                            TupleValueExpression ntve = new TupleValueExpression(
                                    tve.getTableName(), tve.getTableAlias(), addCol.getName(),
                                    null, -1);
                            orderByExprs.add(ntve);
                        }
                        // Don't forget to remember to forget the other indices.  (E. Presley, 1955)
                        break;
                    }
                }
            }
        }
    }

    /** May be true for DELETE or SELECT */
    public boolean hasOrderByColumns() {
        return false;
    }

    /** Subclasses should override this method of they have order by columns */
    public List<ParsedColInfo> orderByColumns() {
        assert(false);
        return null;
    }

    /**
     * Return the number of window function expressions.  This
     * is non-zero for ParsedSelectStmt only.
     */
    public int getWindowFunctionExpressionCount() {
        return m_windowFunctionExpressions.size();
    }
    /*
     *  Extract all subexpressions of a given expression class from this statement
     */
    protected Set<AbstractExpression> findAllSubexpressionsOfClass(Class< ? extends AbstractExpression> aeClass) {
        HashSet<AbstractExpression> exprs = new HashSet<>();
        if (m_joinTree != null) {
            AbstractExpression treeExpr = m_joinTree.getAllFilters();
            if (treeExpr != null) {
                exprs.addAll(treeExpr.findAllSubexpressionsOfClass(aeClass));
            }
        }
        return exprs;
    }

    /**
     * Return true if a SQL statement contains a subquery of any kind
     * @return TRUE is this statement contains a subquery
     */
    public boolean hasSubquery() {
        // This method should be called only after the statement is parsed and join tree is built
        assert(m_joinTree != null);
        if (m_joinTree.hasSubqueryScans() ) {
            return true;
        } else {
            // Verify expression subqueries
            return !findSubquerySubexpressions().isEmpty();
        }
    }

    protected Set<AbstractExpression> findSubquerySubexpressions() {
        return findAllSubexpressionsOfClass(SelectSubqueryExpression.class);
    }

    public abstract boolean isDML();

    /**
     * Is the statement a subquery of a DML statement.
     * Currently, subquery statements initialized through parseFromSubQuery
     * (as opposed to parseSubquery) do NOT initialize m_parentStmt so they
     * are indistinguishable from top-level statements. There is a plan to
     * "fix" that for greater simplicity/consistency.
     * https://issues.voltdb.com/browse/ENG-9313
     * This code SHOULD work acceptably with or without that fix,
     * because we do not allow FROM clause subqueries in DML statements.
     * Regardless, the current caller has no requirement to return true
     * if THIS statement is DML.
     * @return
     */
    public boolean topmostParentStatementIsDML() {
        if (m_parentStmt == null) {
            return false; // Do not need to check if THIS statement is DML.
        } else if (m_parentStmt.isDML()) {
            // A parent DML statement is always the root parent because DML is not
            // allowed in subqueries.
            return true;
        } else {
            // For queries (potentially subqueries), keep searching upward.
            return m_parentStmt.topmostParentStatementIsDML();
        }
    }

    /**
     * Return an error message iff this statement is inherently content
     * deterministic. Some operations can cause non-determinism. Notably,
     * aggregate functions of floating point type can cause non-deterministic
     * round-off error. The default is to return null, which means the query is
     * inherently deterministic.
     *
     * Note that this has nothing to do with limit-order non-determinism.
     *
     * @return An error message if this statement is *not* inherently content
     *         deterministic. Otherwise we return null.
     */
    public abstract String calculateContentDeterminismMessage();

    /**
     * Just fetch the content determinism message. Don't do any calculations.
     */
    protected final String getContentDeterminismMessage() {
        return m_contentDeterminismMessage;
    }

    /**
     * Set the content determinism message, but only if it's currently non-null.
     *
     * @param msg
     */
    protected void updateContentDeterminismMessage(String msg) {
        if (m_contentDeterminismMessage == null) {
            m_contentDeterminismMessage = msg;
        }
    }

    public boolean isContentDetermistic() {
        return m_contentDeterminismMessage != null;
    }

    // Function evaluates whether the statement results in at most
    // one output row. This is implemented for single table by checking
    // value equivalence of predicates in where clause and checking
    // if all defined unique indexes are in value equivalence set.
    // Returns true if the statement results is at most one output
    // row else false
    protected boolean producesOneRowOutput () {
        if (m_tableAliasMap.size() != 1) {
            return false;
        }

        // Get the table.  There's only one.
        StmtTableScan scan = m_tableAliasMap.values().iterator().next();
        Table table = getTableFromDB(scan.getTableName());
        // May be sub-query? If can't find the table there's no use to continue.
        if (table == null) {
            return false;
        }

        // Get all the indexes defined on the table
        CatalogMap<Index> indexes = table.getIndexes();
        if (indexes == null || indexes.size() == 0) {
            // no indexes defined on the table
            return false;
        }

        // Collect value equivalence expression for the SQL statement
        Map<AbstractExpression, Set<AbstractExpression>> valueEquivalence = analyzeValueEquivalence();

        // If no value equivalence filter defined in SQL statement, there's no use to continue
        if (valueEquivalence.isEmpty()) {
            return false;
        }

        // Collect all tve expressions from value equivalence set which have equivalence
        // defined to parameterized or constant value expression.
        // Eg: T.A = ? or T.A = 1
        Set <AbstractExpression> parameterizedConstantKeys = new HashSet<>();
        Set<AbstractExpression> valueEquivalenceKeys = valueEquivalence.keySet();   // get all the keys
        for (AbstractExpression key : valueEquivalenceKeys) {
            if (key instanceof TupleValueExpression) {
                Set<AbstractExpression> values = valueEquivalence.get(key);
                for (AbstractExpression value : values) {
                    if (value instanceof ParameterValueExpression ||
                            value instanceof ConstantValueExpression) {
                        TupleValueExpression tve = (TupleValueExpression) key;
                        parameterizedConstantKeys.add(tve);
                    }
                }
            }
        }

        // Iterate over the unique indexes defined on the table to check if the unique
        // index defined on table appears in tve equivalence expression gathered above.
        for (Index index : indexes) {
            // Perform lookup only on pure column indices which are unique
            if (!index.getUnique() || !index.getExpressionsjson().isEmpty()) {
                continue;
            }

            Set<AbstractExpression> indexExpressions = new HashSet<>();
            CatalogMap<ColumnRef> indexColRefs = index.getColumns();
            for (ColumnRef indexColRef:indexColRefs) {
                final Column col = indexColRef.getColumn();
                final TupleValueExpression tve = new TupleValueExpression(
                        scan.getTableName(), scan.getTableAlias(), col.getName(),
                        col.getName(), col.getIndex());
                indexExpressions.add(tve);
            }

            if (parameterizedConstantKeys.containsAll(indexExpressions)) {
                return true;
            }
        }
        return false;
    }

    protected boolean m_parsingInDisplayColumns = false;

    /**
     * Whwn we parse a rank expression we want to know if we are in the display column.
     *
     * @return the parsingInDisplayColumns
     */
    public final boolean isParsingInDisplayColumns() {
        return m_parsingInDisplayColumns;
    }

    /**
     * Whwn we parse a rank expression we want to know if we are in the display column.
     *
     * @param parsingInDisplayColumns the parsingInDisplayColumns to set
     */
    public final void setParsingInDisplayColumns(boolean parsingInDisplayColumns) {
        m_parsingInDisplayColumns = parsingInDisplayColumns;
    }

    /**
     * Calculate the UDF dependees.  These are the UDFs called in an expression
     * in this procedure.
     *
     * @return The list of names of UDF dependees.  These are function names, and
     *         should all be in lower case.
     */
    public Collection<String> calculateUDFDependees() {
        List<String> answer = new ArrayList<>();
        Collection<AbstractExpression> fCalls = findAllSubexpressionsOfClass(FunctionExpression.class);
        Collection<AbstractExpression> aCalls = findAllSubexpressionsOfClass(AggregateExpression.class);
        for (AbstractExpression fCall : fCalls) {
            FunctionExpression fexpr = (FunctionExpression)fCall;
            if (fexpr.isUserDefined()) {
                answer.add(fexpr.getFunctionName());
            }
        }
        for (AbstractExpression aCall : aCalls) {
            AggregateExpression aexpr = (AggregateExpression)aCall;
            if (aexpr.isUserDefined()) {
                answer.add(aexpr.getFunctionName());
            }
        }
        return answer;
    }

    public Map<Integer, ParameterValueExpression> getParamsByIndex() {
        return m_paramsByIndex;
    }

    public void setParamsByIndex(Map<Integer, ParameterValueExpression> paramsByIndex) {
        m_paramsByIndex = paramsByIndex;
    }

    public Integer getStmtId() {
        return m_stmtId;
    }

    public void setStmtId(int id) {
        m_stmtId = id;
    }
}
