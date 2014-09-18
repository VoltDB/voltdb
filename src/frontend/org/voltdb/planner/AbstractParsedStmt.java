/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.SubqueryExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.VectorValueExpression;
import org.voltdb.planner.parseinfo.BranchNode;
import org.voltdb.planner.parseinfo.JoinNode;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.planner.parseinfo.SubqueryLeafNode;
import org.voltdb.planner.parseinfo.TableLeafNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;

public abstract class AbstractParsedStmt {

     // Internal statement counter
    public static int NEXT_STMT_ID = 0;
    // Internal parameter counter
    public static int NEXT_PARAMETER_ID = 0;

    // The unique id to identify the statement
    public int m_stmtId;

    public String m_sql;

    // The initial value is a safety net for the case of parameter-less statements.
    private ParameterValueExpression[] m_paramList = new ParameterValueExpression[0];

    protected HashMap<Long, ParameterValueExpression> m_paramsById = new HashMap<Long, ParameterValueExpression>();

    // The parameter expression from the correlated expressions. The key is the parameter index.
    // This map acts as an intermediate storage for the parameter TVE until they are
    // distributed to an appropriate subquery expression where they are originated
    public Map<Integer, AbstractExpression> m_parameterTveMap = new HashMap<Integer, AbstractExpression>();

    public ArrayList<Table> m_tableList = new ArrayList<Table>();

    private Table m_DDLIndexedTable = null;

    public ArrayList<AbstractExpression> m_noTableSelectionList = new ArrayList<AbstractExpression>();

    protected ArrayList<AbstractExpression> m_aggregationList = null;

    // Hierarchical join representation
    public JoinNode m_joinTree = null;

    // User specified join order, null if none is specified
    public String m_joinOrder = null;

    public HashMap<String, StmtTableScan> m_tableAliasMap = new HashMap<String, StmtTableScan>();

    // This list is used to identify the order of the table aliases returned by
    // the parser for possible use as a default join order.
    protected ArrayList<String> m_tableAliasList = new ArrayList<String>();

    protected final String[] m_paramValues;
    public final Database m_db;

    // Parent statement if any
    public AbstractParsedStmt m_parentStmt = null;

    static final String INSERT_NODE_NAME = "insert";
    static final String UPDATE_NODE_NAME = "update";
    static final String DELETE_NODE_NAME = "delete";
    static final String SELECT_NODE_NAME = "select";
    static final String UNION_NODE_NAME  = "union";

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    protected AbstractParsedStmt(String[] paramValues, Database db) {
        this.m_paramValues = paramValues;
        this.m_db = db;
    }

    public void setTable(Table tbl) {
        m_DDLIndexedTable = tbl;
        // Add this table to the cache
        assert(tbl.getTypeName() != null);
        addTableToStmtCache(tbl.getTypeName(), tbl.getTypeName(), null);
    }

    /**
    *
    * @param stmtTypeElement
    * @param paramValues
    * @param db
    */
   private static AbstractParsedStmt getParsedStmt(VoltXMLElement stmtTypeElement, String[] paramValues,
           Database db) {
       AbstractParsedStmt retval = null;

       if (stmtTypeElement == null) {
           System.err.println("Unexpected error parsing hsql parsed stmt xml");
           throw new RuntimeException("Unexpected error parsing hsql parsed stmt xml");
       }

       // create non-abstract instances
       if (stmtTypeElement.name.equalsIgnoreCase(INSERT_NODE_NAME)) {
           retval = new ParsedInsertStmt(paramValues, db);
       }
       else if (stmtTypeElement.name.equalsIgnoreCase(UPDATE_NODE_NAME)) {
           retval = new ParsedUpdateStmt(paramValues, db);
       }
       else if (stmtTypeElement.name.equalsIgnoreCase(DELETE_NODE_NAME)) {
           retval = new ParsedDeleteStmt(paramValues, db);
       }
       else if (stmtTypeElement.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
           retval = new ParsedSelectStmt(paramValues, db);
       }
       else if (stmtTypeElement.name.equalsIgnoreCase(UNION_NODE_NAME)) {
           retval = new ParsedUnionStmt(paramValues, db);
       }
       else {
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
     * @param db
     * @param joinOrder
     */
    private static void parse(AbstractParsedStmt parsedStmt, String sql,
            VoltXMLElement stmtTypeElement, Database db, String joinOrder) {
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
    public static AbstractParsedStmt parse(String sql, VoltXMLElement stmtTypeElement, String[] paramValues,
            Database db, String joinOrder) {

        // reset the statemet counteres
        NEXT_STMT_ID = 0;
        NEXT_PARAMETER_ID = 0;
        AbstractParsedStmt retval = getParsedStmt(stmtTypeElement, paramValues, db);

        parse(retval, sql, stmtTypeElement, db, joinOrder);
        return retval;
    }

    /**
     *
     * @param stmtElement
     * @param db
     */
    abstract void parse(VoltXMLElement stmtElement);

    void parseTargetColumns(VoltXMLElement columnsNode, Table table, HashMap<Column, AbstractExpression> columns)
    {
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
                expr.refineValueType(VoltType.get((byte)col.getType()), col.getSize());
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
        for (VoltXMLElement node : root.children) {
            if (node.name.equalsIgnoreCase("parameters")) {
                parseParameters(node);
                break;
            }
        }
        for (VoltXMLElement node : root.children) {
            if (node.name.equalsIgnoreCase("tablescan")) {
                parseTable(node);
            } else if (node.name.equalsIgnoreCase("tablescans")) {
                parseTables(node);
            }
        }
    }

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
    public AbstractExpression parseExpressionTree(VoltXMLElement root) {
        String elementName = root.name.toLowerCase();
        AbstractExpression retval = null;

        if (elementName.equals("value")) {
            retval = parseValueExpression(root);
        }
        else if (elementName.equals("vector")) {
            retval = parseVectorExpression(root);
        }
        else if (elementName.equals("columnref")) {
            retval = parseColumnRefExpression(root);
        }
        else if (elementName.equals("operation")) {
            retval = parseOperationExpression(root);
        }
        else if (elementName.equals("aggregation")) {
            retval = parseAggregationExpression(root);
            if (m_aggregationList != null) {
                ExpressionUtil.finalizeValueTypes(retval);
                m_aggregationList.add(retval);
            }
        }
        else if (elementName.equals("function")) {
            retval = parseFunctionExpression(root);
        }
        else if (elementName.equals("asterisk")) {
            return null;
        }
        else if (elementName.equals("tablesubquery")) {
            retval = parseSubqueryExpression(root);
        }
        else if (elementName.equals("row")) {
            retval = parseRowExpression(root);
        }
        else {
            throw new PlanningErrorException("Unsupported expression node '" + elementName + "'");
        }

        return retval;
    }

    /**
     * Parse a Vector value for SQL-IN
     */
    private AbstractExpression parseVectorExpression(VoltXMLElement exprNode) {
        ArrayList<AbstractExpression> args = new ArrayList<AbstractExpression>();
        for (VoltXMLElement argNode : exprNode.children) {
            assert(argNode != null);
            // recursively parse each argument subtree (could be any kind of expression).
            AbstractExpression argExpr = parseExpressionTree(argNode);
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
        String isPlannerGenerated = exprNode.attributes.get("isplannergenerated");

        // A ParameterValueExpression is needed to represent any user-provided or planner-injected parameter.
        boolean needParameter = (isParam != null) && (isParam.equalsIgnoreCase("true"));

        // A ConstantValueExpression is needed to represent a constant in the statement,
        // EVEN if that constant has been "parameterized" by the plan caching code.
        ConstantValueExpression cve = null;
        boolean needConstant = (needParameter == false) ||
            ((isPlannerGenerated != null) && (isPlannerGenerated.equalsIgnoreCase("true")));

        if (needConstant) {
            String type = exprNode.attributes.get("valuetype");
            VoltType vt = VoltType.typeFromString(type);
            int size = VoltType.MAX_VALUE_LENGTH;
            assert(vt != VoltType.VOLTTABLE);

            if ((vt != VoltType.STRING) && (vt != VoltType.VARBINARY)) {
                if (vt == VoltType.NULL) size = 0;
                else size = vt.getLengthInBytesForFixedTypes();
            }
            cve = new ConstantValueExpression();
            cve.setValueType(vt);
            cve.setValueSize(size);
            if ( ! needParameter && vt != VoltType.NULL) {
                String valueStr = exprNode.attributes.get("value");
                cve.setValue(valueStr);
            }
        }

        if (needParameter) {
            long id = Long.parseLong(exprNode.attributes.get("id"));
            ParameterValueExpression expr = m_paramsById.get(id);
            if (needConstant) {
                expr.setOriginalValue(cve);
                cve.setValue(m_paramValues[expr.getParameterIndex()]);
            }
            return expr;
        }
        return cve;
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
        StmtTableScan tableScan = getStmtTableScanByAlias(this, tableAlias);
        assert(tableScan != null);
        String columnName = exprNode.attributes.get("column");
        String columnAlias = exprNode.attributes.get("alias");
        TupleValueExpression expr = new TupleValueExpression(tableName, tableAlias, columnName, columnAlias);
        // Collect the unique columns used in the plan for a given scan.
        // Resolve the tve and add it to the scan's cache of referenced columns
        tableScan.resolveTVE(expr, columnName);
        // Get tableScan where this TVE is originated from. In case of the
        // correlated queries it may not be THIS statement but its parent
        StmtTableScan tableCache = getStmtTableScanByAlias(this, tableAlias);
        assert(tableCache != null);
        expr.setOrigStmtId(tableCache.getStatementId());

        if (m_stmtId == expr.getOrigStmtId()) {
            return expr;
        } else {
            // This a TVE from the correlated expression
            int paramIdx = AbstractParsedStmt.NEXT_PARAMETER_ID++;
            ParameterValueExpression pve = new ParameterValueExpression();
            pve.setParameterIndex(paramIdx);
            pve.setValueSize(expr.getValueSize());
            pve.setValueType(expr.getValueType());
            m_parameterTveMap.put(paramIdx, expr);
            return pve;
        }
    }

    /**
     * Parse a subquery for SQL-IN(SELECT...)
     */
    private SubqueryExpression parseSubqueryExpression(VoltXMLElement exprNode) {
        assert(exprNode.children.size() == 1);
        VoltXMLElement subqueryElmt = exprNode.children.get(0);
        AbstractParsedStmt subqueryStmt = parseSubquery(subqueryElmt);
        String tableName = "VOLT_TEMP_TABLE_" + subqueryStmt.m_stmtId;
        // add table to the query cache
        StmtTableScan tableCache = addTableToStmtCache(tableName, tableName, subqueryStmt);
        assert(tableCache instanceof StmtSubqueryScan);
        return new SubqueryExpression((StmtSubqueryScan)tableCache);
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
           return parseExpressionTree(exprNode.children.get(0));
       } else {
           // (COL1, COL2) IN (SELECT C1, C2 FROM...)
           return parseVectorExpression(exprNode);
       }
   }

    /**
     * Return StmtTableScan by table alias. In case of correlated queries, would need
     * to walk the statement tree up.
     *
     * @param stmt
     * @param tableAlias
     */
    private StmtTableScan getStmtTableScanByAlias(AbstractParsedStmt stmt, String tableAlias) {
        assert(stmt != null);
        StmtTableScan tableScan = stmt.m_tableAliasMap.get(tableAlias);
        if (tableScan != null) {
            return tableScan;
        } else if (stmt.m_parentStmt != null) {
            // This may be a correlated subquery
            return getStmtTableScanByAlias(stmt.m_parentStmt, tableAlias);
        } else {
            return null;
        }
    }

    /**
     * Add a table or a sub-query to the statement cache. If the subQuery is not NULL,
     * the table name and the alias specify the sub-query
     * @param tableName
     * @param tableAlias
     * @param subQuery
     * @return index into the cache array
     */
    protected StmtTableScan addTableToStmtCache(String tableName, String tableAlias, AbstractParsedStmt subquery) {
        // Create an index into the query Catalog cache
        StmtTableScan tableScan = m_tableAliasMap.get(tableAlias);
        if (tableScan == null) {
            if (subquery == null) {
                tableScan = new StmtTargetTableScan(getTableFromDB(tableName), tableAlias, m_stmtId);
            } else {
                tableScan = new StmtSubqueryScan(subquery, tableAlias, m_stmtId);
            }
            m_tableAliasMap.put(tableAlias, tableScan);
        }
        return tableScan;
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
        AbstractExpression expr = null;

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

        // get the first (left) node that is an element
        VoltXMLElement leftExprNode = exprNode.children.get(0);
        assert(leftExprNode != null);

        // recursively parse the left subtree (could be another operator or
        // a constant/tuple/param value operand).
        AbstractExpression leftExpr = parseExpressionTree(leftExprNode);
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
            AbstractExpression rightExpr = parseExpressionTree(rightExprNode);
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
                expr.setValueSize(voltType.getMaxLengthInBytes());
            }
        }
        if ((exprType == ExpressionType.COMPARE_IN && expr.getRight() instanceof SubqueryExpression) ||
                exprType == ExpressionType.OPERATOR_EXISTS) {
            // Compact the expression hierarchy by removing parent and setting
            // the subquery type (IN/EXISTS) to the child - SubqueryExpression
            AbstractExpression subqueryExpr = null;
            if (ExpressionType.COMPARE_IN == exprType) {
                subqueryExpr = expr.getRight();
                subqueryExpr.setLeft(expr.getLeft());
                subqueryExpr.setExpressionType(ExpressionType.IN_SUBQUERY);
            } else {
                subqueryExpr = expr.getLeft();
                subqueryExpr.setExpressionType(ExpressionType.EXISTS_SUBQUERY);
            }
            assert(subqueryExpr != null);

            // weed of IN (values) expression
            // Break up UNION/INTERSECT (ALL) set ops into individual selects connected by
            // AND/OR operator
            // col IN ( queryA UNION queryB ) - > col IN (queryA) OR col IN (queryB)
            // col IN ( queryA INTERSECTS queryB ) - > col IN (queryA) AND col IN (queryB)
            subqueryExpr = ParsedUnionStmt.breakUpSetOpSubquery(subqueryExpr);
            expr = optimizeSubqueryExpression(subqueryExpr);
        }
        return expr;
    }

    /**
     * Perform various optimizations for IN/EXISTS subqueries if possible
     *
     * @param expr to optimize
     * @return optimized expression
     */
    private AbstractExpression optimizeSubqueryExpression(AbstractExpression expr) {
        ExpressionType exprType = expr.getExpressionType();
        if (ExpressionType.CONJUNCTION_AND == exprType || ExpressionType.CONJUNCTION_OR == exprType) {
            AbstractExpression optimizedLeft = optimizeSubqueryExpression(expr.getLeft());
            expr.setLeft(optimizedLeft);
            AbstractExpression optimizedRight = optimizeSubqueryExpression(expr.getRight());
            expr.setRight(optimizedRight);
        }
        AbstractExpression retval = expr;
        if (ExpressionType.IN_SUBQUERY == retval.getExpressionType()) {
            retval = optimizeInExpression(retval);
            // Do not return here because the original IN expressions
            // is converted to EXISTS and can be optimized farther.
        }
        if (ExpressionType.EXISTS_SUBQUERY == retval.getExpressionType()) {
            retval = optimizeExistsExpression(retval);
        }
        return retval;
    }

    /**
     * Verify that an IN expression can be safely converted to an EXISTS one
     * IN (SELECT" forms e.g. "(A, B) IN (SELECT X, Y, FROM ...) =>
     * EXISTS (SELECT 42 FROM ... AND|WHERE|HAVING A=X AND|WHERE|HAVING B=Y)
     *
     * @param inExpr
     * @return existsExpr
     */
    private boolean canConvertInToExistsExpression(AbstractParsedStmt subquery) {
        // Must be a SELECT statement
        if (!(subquery instanceof ParsedSelectStmt)) {
            return false;
        }

        boolean canConvert = false;
        ParsedSelectStmt selectStmt = (ParsedSelectStmt) subquery;
        // Must not have OFFSET set
        // EXISTS (select * from T where T.X = parent.X order by T.Y offset 10 limit 5)
        //      seems to require 11 matches
        // parent.X IN (select T.X from T order by T.Y offset 10 limit 5)
        //      seems to require 1 match that has exactly 10-14 rows (matching or not) with lesser or equal values of Y.
        canConvert = !selectStmt.hasOffset();

        return canConvert;
    }

    /**
     * Optimize EXISTS expression:
     *  1. Replace the display columns with a single dummy column "1"
     *  2. Drop DISTINCT expression
     *  3. Add LIMIT 1
     *  4. Remove ORDER BY, GROUP BY expressions if HAVING expression is not present
     *
     * @param inExpr
     * @return existsExpr
     */
    private AbstractExpression optimizeExistsExpression(AbstractExpression existsExpr) {
        assert(ExpressionType.EXISTS_SUBQUERY == existsExpr.getExpressionType());
        assert(existsExpr instanceof  SubqueryExpression);

        SubqueryExpression subqueryExpr = (SubqueryExpression) existsExpr;
        AbstractParsedStmt subquery = subqueryExpr.getSubquery();
        if (subquery instanceof ParsedSelectStmt) {
            ParsedSelectStmt selectSubquery = (ParsedSelectStmt) subquery;

            selectSubquery.simplifyExistsExpression();
        }
        return existsExpr;
    }

    /**
     * Optimize IN expression
     *
     * @param inExpr
     * @return existsExpr
     */
    private AbstractExpression optimizeInExpression(AbstractExpression inExpr) {
        assert(ExpressionType.IN_SUBQUERY == inExpr.getExpressionType());
        assert(inExpr instanceof SubqueryExpression);
        assert(inExpr.getLeft() != null);

        AbstractExpression inColumns = inExpr.getLeft();
        SubqueryExpression subqueryExpr = (SubqueryExpression) inExpr;
        AbstractParsedStmt subquery = subqueryExpr.getSubquery();
        if (canConvertInToExistsExpression(subquery)) {
            ParsedSelectStmt selectStmt = (ParsedSelectStmt) subquery;
            ParsedSelectStmt.rewriteInSubqueryAsExists(selectStmt, inColumns);
            subqueryExpr.moveUpTVE();
            subqueryExpr.setExpressionType(ExpressionType.EXISTS_SUBQUERY);
            subqueryExpr.setLeft(null);
            return subqueryExpr;
        } else {
            // Need to replace TVE from the IN list with the corresponding PVE
            // to be passed as parameters to the subquery similar to the correlated TVE
            inColumns = ParsedSelectStmt.replaceExpressionsWithPve(subquery, inColumns, false);
            subqueryExpr.moveUpTVE();
            inExpr.setLeft(inColumns);
            return inExpr;
        }
    }

    /**
     *
     * @param paramsById
     * @param exprNode
     * @return
     */

    private AbstractExpression parseAggregationExpression(VoltXMLElement exprNode)
    {
        String type = exprNode.attributes.get("optype");
        ExpressionType exprType = ExpressionType.get(type);

        if (exprType == ExpressionType.INVALID) {
            throw new PlanningErrorException("Unsupported aggregation type '" + type + "'");
        }

        // Allow expressions to read expression-specific data from exprNode.
        // The design fully abstracts other volt classes from the XML serialization.
        // So, this goes here instead of in derived Expression implementations.

        assert (exprNode.children.size() == 1);

        // get the single required child node
        VoltXMLElement childExprNode = exprNode.children.get(0);
        assert(childExprNode != null);

        // recursively parse the child subtree -- could (in theory) be an operator or
        // a constant, column, or param value operand or null in the specific case of "COUNT(*)".
        AbstractExpression childExpr = parseExpressionTree(childExprNode);
        if (childExpr == null) {
            assert(exprType == ExpressionType.AGGREGATE_COUNT);
            exprType = ExpressionType.AGGREGATE_COUNT_STAR;
        }

        AggregateExpression expr = new AggregateExpression(exprType);
        expr.setLeft(childExpr);

        String node;
        if ((node = exprNode.attributes.get("distinct")) != null && Boolean.parseBoolean(node)) {
            expr.setDistinct();
        }
        return expr;
    }


    /**
     *
     * @param paramsById
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
        String id = exprNode.attributes.get("function_id");
        assert(id != null);
        int idArg = 0;
        try {
            idArg = Integer.parseInt(id);
        } catch (NumberFormatException nfe) {}
        assert(idArg > 0);
        String parameter = exprNode.attributes.get("parameter");
        String volt_alias = exprNode.attributes.get("volt_alias");
        if (volt_alias == null) {
            volt_alias = name; // volt shares the function name with HSQL
        }

        ArrayList<AbstractExpression> args = new ArrayList<AbstractExpression>();
        for (VoltXMLElement argNode : exprNode.children) {
            assert(argNode != null);
            // recursively parse each argument subtree (could be any kind of expression).
            AbstractExpression argExpr = parseExpressionTree(argNode);
            assert(argExpr != null);
            args.add(argExpr);
        }

        FunctionExpression expr = new FunctionExpression();
        expr.setAttributes(name, volt_alias, idArg);
        expr.setArgs(args);
        if (value_type != null) {
            expr.setValueType(value_type);
            expr.setValueSize(value_type.getMaxLengthInBytes());
        }

        if (parameter != null) {
            int parameter_idx = -1; // invalid argument index
            try {
                parameter_idx = Integer.parseInt(parameter);
            } catch (NumberFormatException nfe) {}
            assert(parameter_idx >= 0); // better be valid by now.
            assert(parameter_idx < args.size()); // must refer to a provided argument
            expr.setParameterArg(parameter_idx);
            expr.negotiateInitialValueTypes();
        }
        return expr;
    }

    /**
     * Build a WHERE expression for a single-table statement.
     */
    public AbstractExpression getSingleTableFilterExpression() {
        if (m_joinTree == null) { // Not possible.
            assert(m_joinTree != null);
            return null;
        }
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
        m_tableAliasList.add(tableAlias);

        AbstractParsedStmt subquery = null;
        // Possible sub-query
        for (VoltXMLElement childNode : tableNode.children) {
            if ( ! childNode.name.equals("tablesubquery")) {
                continue;
            }
            if (childNode.children.isEmpty()) {
                continue;
            }
            // sub-query FROM (SELECT ...)
            subquery = parseFromSubQuery(childNode.children.get(0));
            break;
        }

        // add table to the query cache before processing the JOIN/WHERE expressions
        // The order is important because processing sub-query expressions assumes that
        // the sub-query is already registered
        StmtTableScan tableScan = addTableToStmtCache(tableName, tableAlias, subquery);

        AbstractExpression joinExpr = parseJoinCondition(tableNode);
        AbstractExpression whereExpr = parseWhereCondition(tableNode);

        // The join type of the leaf node is always INNER
        // For a new tree its node's ids start with 0 and keep incrementing by 1
        int nodeId = (m_joinTree == null) ? 0 : m_joinTree.getId() + 1;

        JoinNode leafNode;
        if (tableScan instanceof StmtTargetTableScan) {
            Table table = ((StmtTargetTableScan)tableScan).getTargetTable();
            m_tableList.add(table);
            leafNode = new TableLeafNode(nodeId, joinExpr, whereExpr, (StmtTargetTableScan)tableScan);
        } else {
            leafNode = new SubqueryLeafNode(nodeId, joinExpr, whereExpr, (StmtSubqueryScan)tableScan);
        }

        if (m_joinTree == null) {
            // this is the first table
            m_joinTree = leafNode;
        } else {
            // Build the tree by attaching the next table always to the right
            // The node's join type is determined by the type of its right node

            JoinType joinType = JoinType.get(tableNode.attributes.get("jointype"));
            assert(joinType != JoinType.INVALID);
            if (joinType == JoinType.FULL) {
                throw new PlanningErrorException("VoltDB does not support full outer joins");
            }

            JoinNode joinNode = new BranchNode(nodeId + 1, joinType, m_joinTree, leafNode);
            m_joinTree = joinNode;
       }
    }

    /**
     *
     * @param tablesNode
     */
    private void parseTables(VoltXMLElement tablesNode) {
        Set<String> visited = new HashSet<String>();

        for (VoltXMLElement node : tablesNode.children) {
            if (node.name.equalsIgnoreCase("tablescan")) {

                String visitedTable = node.attributes.get("tablealias");
                if (visitedTable == null) {
                    visitedTable = node.attributes.get("table");
                }

                assert(visitedTable != null);

                if( visited.contains(visitedTable)) {
                    throw new PlanningErrorException("Not unique table/alias: " + visitedTable);
                }

                parseTable(node);
                visited.add(visitedTable);
            }
        }
    }

    /**
     * Populate the statement's paramList from the "parameters" element
     * @param paramsNode
     */
    private void parseParameters(VoltXMLElement paramsNode) {
        m_paramList = new ParameterValueExpression[paramsNode.children.size()];

        long max_parameter_id = -1;
        for (VoltXMLElement node : paramsNode.children) {
            if (node.name.equalsIgnoreCase("parameter")) {
                long id = Long.parseLong(node.attributes.get("id"));
                int index = Integer.parseInt(node.attributes.get("index"));
                if (index > max_parameter_id) {
                    max_parameter_id = index;
                }
                String typeName = node.attributes.get("valuetype");
                String isVectorParam = node.attributes.get("isvector");
                VoltType type = VoltType.typeFromString(typeName);
                ParameterValueExpression pve = new ParameterValueExpression();
                pve.setParameterIndex(index);
                pve.setValueType(type);
                if (isVectorParam != null && isVectorParam.equalsIgnoreCase("true")) {
                    pve.setParamIsVector();
                }
                m_paramsById.put(id, pve);
                m_paramList[index] = pve;
            }
        }
        if (max_parameter_id >= NEXT_PARAMETER_ID) {
            NEXT_PARAMETER_ID = (int)max_parameter_id + 1;
        }
    }

    /** Get a list of the subqueries used by this statement.  This method
     * may be overridden by subclasses, e.g., insert statements have a subquery
     * but does not use m_joinTree.
     **/
    public List<StmtSubqueryScan> getSubqueries() {
        List<StmtSubqueryScan> subqueries = new ArrayList<>();

        if (m_joinTree != null) {
          m_joinTree.extractSubQueries(subqueries);
        }

        return subqueries;
    }

    // The parser currently attaches the summary parameter list
    // to each leaf (select) statement in a union, but not to the
    // union statement itself. It is always the same parameter list,
    // the one that applies globally to the entire set of leaf select
    // statements each of which may or may not use each parameter.
    // The list is required later at the top-level statement for
    // proper cataloging, so promote it here to each parent union.
    protected void promoteUnionParametersFromChild(AbstractParsedStmt childStmt) {
        m_paramList = childStmt.m_paramList;
    }

    /**
     * Collect value equivalence expressions across the entire SQL statement
     * @return a map of tuple value expressions to the other expressions,
     * TupleValueExpressions, ConstantValueExpressions, or ParameterValueExpressions,
     * that they are constrained to equal.
     */
    HashMap<AbstractExpression, Set<AbstractExpression>> analyzeValueEquivalence() {
        // collect individual where/join expressions
        m_joinTree.analyzeJoinExpressions(m_noTableSelectionList);
        return m_joinTree.getAllEquivalenceFilters();
    }

    protected Table getTableFromDB(String tableName) {
        Table table = m_db.getTables().getExact(tableName);
        return table;
    }

    @Override
    public String toString() {
        String retval = "SQL:\n\t" + m_sql + "\n";

        retval += "PARAMETERS:\n\t";
        for (ParameterValueExpression param : m_paramList) {
            retval += param.toString() + " ";
        }

        retval += "\nTABLE SOURCES:\n\t";
        for (Table table : m_tableList) {
            retval += table.getTypeName() + " ";
        }

        retval += "\nSCAN COLUMNS:\n";
        boolean hasAll = true;
        for (StmtTableScan tableScan : m_tableAliasMap.values()) {
            if ( ! tableScan.getScanColumns().isEmpty()) {
                hasAll = false;
                retval += "\tTable Alias: " + tableScan.getTableAlias() + ":\n";
                for (SchemaColumn col : tableScan.getScanColumns()) {
                    retval += "\t\tColumn: " + col.getColumnName() + ": ";
                    retval += col.getExpression().toString() + "\n";
                }
            }
        }
        if (hasAll) {
            retval += "\tALL\n";
        }

        retval += "\nJOIN TREE :\n";
        if (m_joinTree != null ) {
            retval += m_joinTree.toString();
        }

        retval += "NO TABLE SELECTION LIST:\n";
        int i = 0;
        for (AbstractExpression expr : m_noTableSelectionList)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        return retval;
    }

    protected AbstractParsedStmt parseFromSubQuery(VoltXMLElement queryNode) {
        AbstractParsedStmt subquery = AbstractParsedStmt.getParsedStmt(queryNode, m_paramValues, m_db);
        // Propagate parameters from the parent to the child
        subquery.m_paramsById.putAll(m_paramsById);
        subquery.m_paramList = m_paramList;
        AbstractParsedStmt.parse(subquery, m_sql, queryNode, m_db, m_joinOrder);
        return subquery;
    }

    protected AbstractParsedStmt parseSubquery(VoltXMLElement suqueryElmt) {
        AbstractParsedStmt subQuery = AbstractParsedStmt.getParsedStmt(suqueryElmt, m_paramValues, m_db);
        // Propagate parameters from the parent to the child
        subQuery.m_parentStmt = this;
        subQuery.m_paramsById.putAll(m_paramsById);
        subQuery.m_paramList = m_paramList;

        AbstractParsedStmt.parse(subQuery, m_sql, suqueryElmt, m_db, m_joinOrder);
        return subQuery;
    }

    /** Parse a where or join clause. This behavior is common to all kinds of statements.
     */
    private AbstractExpression parseTableCondition(VoltXMLElement tableScan, String joinOrWhere)
    {
        AbstractExpression condExpr = null;
        for (VoltXMLElement childNode : tableScan.children) {
            if ( ! childNode.name.equalsIgnoreCase(joinOrWhere)) {
                continue;
            }
            assert(childNode.children.size() == 1);
            assert(condExpr == null);
            condExpr = parseExpressionTree(childNode.children.get(0));
            assert(condExpr != null);
            ExpressionUtil.finalizeValueTypes(condExpr);
        }
        return condExpr;
    }

    private AbstractExpression parseJoinCondition(VoltXMLElement tableScan)
    {
        return parseTableCondition(tableScan, "joincond");
    }

    private AbstractExpression parseWhereCondition(VoltXMLElement tableScan)
    {
        return parseTableCondition(tableScan, "wherecond");
    }

    public ParameterValueExpression[] getParameters() {
        // Is a statement contains subqueries the parameters will be associated with
        // the parent statement
        if (m_parentStmt != null) {
            return m_parentStmt.getParameters();
        } else {
            return m_paramList;
        }
    }

    public boolean hasLimitOrOffset()
    {
        // This dummy implementation for DML statements should never be called.
        // The interface is established on AbstractParsedStmt for support
        // in ParsedSelectStmt and ParsedUnionStmt.
        return false;
    }

    public boolean isOrderDeterministic()
    {
        // This dummy implementation for DML statements should never be called.
        // The interface is established on AbstractParsedStmt for support
        // in ParsedSelectStmt and ParsedUnionStmt.
        throw new RuntimeException("isOrderDeterministic not supported by DML statements");
    }

    public boolean isOrderDeterministicInSpiteOfUnorderedSubqueries() {
        // This dummy implementation for DML statements should never be called.
        // The interface is established on AbstractParsedStmt for support
        // in ParsedSelectStmt and ParsedUnionStmt.
        throw new RuntimeException("isOrderDeterministicInSpiteOfUnorderedSubqueries not supported by DML statements");
    }

    /*
     *  Extract FROM(SELECT...) sub-queries from this statement
     */
    public List<StmtSubqueryScan> findAllFromSubqueries() {
        List<StmtSubqueryScan> subqueries = new ArrayList<StmtSubqueryScan>();
        if (m_joinTree != null) {
            m_joinTree.extractSubQueries(subqueries);
        }
        return subqueries;
    }

    /*
     *  Extract all subexpressions of a given type from this statement
     */
    public List<AbstractExpression> findAllSubexpressionsOfType(ExpressionType exprType) {
        List<AbstractExpression> exprs = new ArrayList<AbstractExpression>();
        if (m_joinTree != null) {
            AbstractExpression treeExpr = m_joinTree.getAllFilters();
            if (treeExpr != null) {
                exprs.addAll(treeExpr.findAllSubexpressionsOfType(exprType));
            }
        }
        return exprs;
    }
}
