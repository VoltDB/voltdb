/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.RowSubqueryExpression;
import org.voltdb.expressions.SelectSubqueryExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.VectorValueExpression;
import org.voltdb.planner.parseinfo.BranchNode;
import org.voltdb.planner.parseinfo.JoinNode;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.planner.parseinfo.SubqueryLeafNode;
import org.voltdb.planner.parseinfo.TableLeafNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;
import org.voltdb.types.QuantifierType;

public abstract class AbstractParsedStmt {

     // Internal statement counter
    public static int NEXT_STMT_ID = 0;
    // Internal parameter counter
    public static int NEXT_PARAMETER_ID = 0;

    // The unique id to identify the statement
    public int m_stmtId;

    public String m_sql;

    // The initial value is a safety net for the case of parameter-less statements.
    private TreeMap<Integer, ParameterValueExpression> m_paramsByIndex = new TreeMap<Integer, ParameterValueExpression>();

    protected HashMap<Long, ParameterValueExpression> m_paramsById = new HashMap<Long, ParameterValueExpression>();

    // The parameter expression from the correlated expressions. The key is the parameter index.
    // This map acts as intermediate storage for the parameter TVEs found while planning a subquery
    // until they can be distributed to the parent's subquery expression where they originated.
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
    boolean m_isUpsert = false;

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
        addTableToStmtCache(tbl, tbl.getTypeName());
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
           if (stmtTypeElement.attributes.containsKey(QueryPlanner.UPSERT_TAG)) {
               retval.m_isUpsert = true;
           }
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

        // reset the statement counters
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
        AbstractExpression expr = parseExpressionTreeHelper(root);

        // If there were any subquery expressions appearing in a scalar context,
        // we must wrap them in ScalarValueExpressions to avoid wrong answers.
        // See ENG-8226.
        expr = ExpressionUtil.wrapScalarSubqueries(expr);
        return expr;
    }

    private AbstractExpression parseExpressionTreeHelper(VoltXMLElement root) {
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
        assert(retval != null);
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
            AbstractExpression argExpr = parseExpressionTreeHelper(argNode);
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
            assert(expr != null);
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
        String columnName = exprNode.attributes.get("column");
        String columnAlias = exprNode.attributes.get("alias");
        TupleValueExpression expr = new TupleValueExpression(tableName, tableAlias, columnName, columnAlias);
        // Collect the unique columns used in the plan for a given scan.
        // Resolve the tve and add it to the scan's cache of referenced columns
        // Get tableScan where this TVE is originated from. In case of the
        // correlated queries it may not be THIS statement but its parent
        StmtTableScan tableScan = getStmtTableScanByAlias(tableAlias);
        if (tableScan == null) {
            // This never used to happen.  HSQL should make sure all the
            // identifiers are defined.  But something has gone wrong.
            // The query is "create index bidx2 on books ( cash + ( select cash from books as child where child.title < books.title ) );"
            // from TestVoltCompler.testScalarSubqueriesExpectedFailures.
            throw new PlanningErrorException("Object not found: " + tableAlias);
        }
        tableScan.resolveTVE(expr);

        if (m_stmtId == tableScan.getStatementId()) {
            return expr;
        }

        // This a TVE from the correlated expression
        int paramIdx = NEXT_PARAMETER_ID++;
        ParameterValueExpression pve = new ParameterValueExpression(paramIdx, expr);
        m_parameterTveMap.put(paramIdx, expr);
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

    /**
    *
    * @param exprNode
    * @return
    */
   private AbstractExpression parseRowExpression(VoltXMLElement exprNode) {
       // Parse individual columnref expressions from the IN output schema
       // Short-circuit for COL IN (LIST) and COL IN (SELECT COL FROM ..)
       if (exprNode.children.size() == 1) {
           return parseExpressionTreeHelper(exprNode.children.get(0));
       } else {
           // (COL1, COL2) IN (SELECT C1, C2 FROM...)
           return parseRowExpression(exprNode.children);
       }
   }

   /**
   *
   * @param exprNode
   * @return
   */
  private AbstractExpression parseRowExpression(List<VoltXMLElement> exprNodes) {
      // Parse individual columnref expressions from the IN output schema
      List<AbstractExpression> exprs = new ArrayList<AbstractExpression>();
      for (VoltXMLElement exprNode : exprNodes) {
          AbstractExpression expr = this.parseExpressionTreeHelper(exprNode);
          exprs.add(expr);
      }
      return new RowSubqueryExpression(exprs);
  }

    /**
     * Return StmtTableScan by table alias. In case of correlated queries, would need
     * to walk the statement tree up.
     *
     * @param stmt
     * @param tableAlias
     */
    private StmtTableScan getStmtTableScanByAlias(String tableAlias) {
        StmtTableScan tableScan = m_tableAliasMap.get(tableAlias);
        if (tableScan != null) {
            return tableScan;
        }
        if (m_parentStmt != null) {
            // This may be a correlated subquery
            return m_parentStmt.getStmtTableScanByAlias(tableAlias);
        }
        return null;
    }

    /**
     * Add a table to the statement cache.
     * @param tableName
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
     * @param subQuery
     * @param tableAlias
     * @return the cache entry
     */
    protected StmtSubqueryScan addSubqueryToStmtCache(AbstractParsedStmt subquery, String tableAlias) {
        assert(subquery != null);
        // If there is no usable alias because the subquery is inside an expression,
        // generate a unique one for internal use.
        if (tableAlias == null) {
            tableAlias = "VOLT_TEMP_TABLE_" + subquery.m_stmtId;
        }
        StmtTableScan tableScan = m_tableAliasMap.get(tableAlias);
        assert(tableScan == null);
        StmtSubqueryScan subqueryScan = new StmtSubqueryScan(subquery, tableAlias, m_stmtId);
        m_tableAliasMap.put(tableAlias, subqueryScan);
        return subqueryScan;
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
        AbstractExpression leftExpr = parseExpressionTreeHelper(leftExprNode);
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
            AbstractExpression rightExpr = parseExpressionTreeHelper(rightExprNode);
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
        if ((exprType == ExpressionType.COMPARE_EQUAL && QuantifierType.ANY == ((ComparisonExpression) expr).getQuantifier()) ||
                exprType == ExpressionType.OPERATOR_EXISTS) {
            // Break up UNION/INTERSECT (ALL) set ops into individual selects connected by
            // AND/OR operator
            // col IN ( queryA UNION queryB ) - > col IN (queryA) OR col IN (queryB)
            // col IN ( queryA INTERSECTS queryB ) - > col IN (queryA) AND col IN (queryB)
            expr = ParsedUnionStmt.breakUpSetOpSubquery(expr);
            expr = optimizeSubqueryExpression(expr);
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
            return expr;
        }
        if (expr instanceof ComparisonExpression) {
            QuantifierType quantifer = ((ComparisonExpression)expr).getQuantifier();
            if (ExpressionType.COMPARE_EQUAL == expr.getExpressionType() && quantifer == QuantifierType.ANY) {
                expr = optimizeInExpression(expr);
                // Do not return here because the original IN expressions
                // is converted to EXISTS and can be optimized farther.
            }
        }
        if (ExpressionType.OPERATOR_EXISTS == expr.getExpressionType()) {
            optimizeExistsExpression(expr);
        }
        return expr;
    }

    /**
     * Verify that an IN expression can be safely converted to an EXISTS one
     * IN (SELECT" forms e.g. "(A, B) IN (SELECT X, Y, FROM ...) =>
     * EXISTS (SELECT 42 FROM ... AND|WHERE|HAVING A=X AND|WHERE|HAVING B=Y)
     *
     * @param inExpr
     * @return existsExpr
     */
    private boolean canConvertInToExistsExpression(AbstractExpression inExpr) {
        AbstractExpression leftExpr = inExpr.getLeft();
        if (leftExpr instanceof SelectSubqueryExpression) {
            // If the left child is a (SELECT ...) expression itself we can't convert it
            // to the EXISTS expression because the manadatory run time scalar check -
            // (expression must return a single row at most)
            return false;
        }
        AbstractExpression rightExpr = inExpr.getRight();
        if (!(rightExpr instanceof SelectSubqueryExpression)) {
            return false;
        }

        // Must be a SELECT statement
        SelectSubqueryExpression subqueryExpr = (SelectSubqueryExpression) inExpr.getRight();
        AbstractParsedStmt subquery = subqueryExpr.getSubqueryStmt();
        if (!(subquery instanceof ParsedSelectStmt)) {
            return false;
        }
        // Must not have OFFSET set
        // EXISTS (select * from T where T.X = parent.X order by T.Y offset 10 limit 5)
        //      seems to require 11 matches
        // parent.X IN (select T.X from T order by T.Y offset 10 limit 5)
        //      seems to require 1 match that has exactly 10-14 rows (matching or not) with lesser or equal values of Y.
        return ! ((ParsedSelectStmt) subquery).hasOffset();
    }

    /**
     * Optimize EXISTS expression:
     *  1. Replace the display columns with a single dummy column "1"
     *  2. Drop DISTINCT expression
     *  3. Add LIMIT 1
     *  4. Remove ORDER BY, GROUP BY expressions if HAVING expression is not present
     *
     * @param existsExpr
     */
    private void optimizeExistsExpression(AbstractExpression existsExpr) {
        assert(ExpressionType.OPERATOR_EXISTS == existsExpr.getExpressionType());
        assert(existsExpr.getLeft() != null);

        if (existsExpr.getLeft() instanceof SelectSubqueryExpression) {
            SelectSubqueryExpression subqueryExpr = (SelectSubqueryExpression) existsExpr.getLeft();
            AbstractParsedStmt subquery = subqueryExpr.getSubqueryStmt();
            if (subquery instanceof ParsedSelectStmt) {
                ParsedSelectStmt selectSubquery = (ParsedSelectStmt) subquery;
                selectSubquery.simplifyExistsSubqueryStmt();
            }
        }
    }

    /**
     * Optimize IN expression
     *
     * @param inExpr
     * @return existsExpr
     */
    private AbstractExpression optimizeInExpression(AbstractExpression inExpr) {
        assert(ExpressionType.COMPARE_EQUAL == inExpr.getExpressionType());
        if (canConvertInToExistsExpression(inExpr)) {
            AbstractExpression inColumns = inExpr.getLeft();
            assert(inColumns != null);
            assert(inExpr.getRight() instanceof SelectSubqueryExpression);
            SelectSubqueryExpression subqueryExpr = (SelectSubqueryExpression) inExpr.getRight();
            AbstractParsedStmt subquery = subqueryExpr.getSubqueryStmt();
            assert(subquery instanceof ParsedSelectStmt);
            ParsedSelectStmt selectStmt = (ParsedSelectStmt) subquery;
            ParsedSelectStmt.rewriteInSubqueryAsExists(selectStmt, inColumns);
            subqueryExpr.resolveCorrelations();
            AbstractExpression existsExpr = null;
            try {
                existsExpr = ExpressionType.OPERATOR_EXISTS.getExpressionClass().newInstance();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage(), e);
            }
            existsExpr.setExpressionType(ExpressionType.OPERATOR_EXISTS);
            existsExpr.setLeft(subqueryExpr);
            return existsExpr;
        } else {
            return inExpr;
        }
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
            int paramIdx = NEXT_PARAMETER_ID++;
            ParameterValueExpression pve = new ParameterValueExpression(paramIdx, expr);
            m_parameterTveMap.put(paramIdx, expr);
            return pve;
        }
        if (expr instanceof AggregateExpression) {
            int paramIdx = NEXT_PARAMETER_ID++;
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
            List<AbstractExpression> newArgs = new ArrayList<AbstractExpression>();
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

        assert(exprNode.children.size() == 2);

        // Get the implicit second aggregation argument.
        // It's expected to be a Boolean true constant value for the cases VoltDB supports.
        VoltXMLElement fixedExprNode = exprNode.children.get(1);
        if ( ! "BOOLEAN".equals(fixedExprNode.attributes.get("valuetype")) ||
                ! ("true".equals(fixedExprNode.attributes.get("value")) ||
                        "true".equals(fixedExprNode.attributes.get("isplannergenerated")))) {
            //TODO: it's only a little risky to give parameterized constants full credit
            //      as probably stemming from the familiar true value
            // -- if constant FALSE turns out to be a real use case,
            //    as opposed to a more complex expression that would NOT get parameterized,
            // we'll at least be throwing here in the stored proc case
            // -- at which point we will probably want to revisit all of this.
            //TODO: get a better description of this hsql "extension" feature.
            //TODO: long-term, consider supporting this hsql "extension" feature.
            throw new PlanningErrorException("Unsupported aggregation extension");
        }
        // get the single required child node
        VoltXMLElement childExprNode = exprNode.children.get(0);
        assert(childExprNode != null);

        // recursively parse the child subtree -- could (in theory) be an operator or
        // a constant, column, or param value operand or null in the specific case of "COUNT(*)".
        AbstractExpression childExpr = parseExpressionTreeHelper(childExprNode);
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
            AbstractExpression argExpr = parseExpressionTreeHelper(argNode);
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

        VoltXMLElement subqueryElement = null;
        // Possible sub-query
        for (VoltXMLElement childNode : tableNode.children) {
            if ( ! childNode.name.equals("tablesubquery")) {
                continue;
            }
            if (childNode.children.isEmpty()) {
                continue;
            }
            // sub-query FROM (SELECT ...)
            subqueryElement = childNode.children.get(0);
            break;
        }

        // add table to the query cache before processing the JOIN/WHERE expressions
        // The order is important because processing sub-query expressions assumes that
        // the sub-query is already registered
        StmtTableScan tableScan = null;
        Table table = null;
        if (subqueryElement == null) {
            table = getTableFromDB(tableName);
            m_tableList.add(table);
            assert(table != null);
            tableScan = addTableToStmtCache(table, tableAlias);
        } else {
            AbstractParsedStmt subquery = parseFromSubQuery(subqueryElement);
            tableScan = addSubqueryToStmtCache(subquery, tableAlias);
        }

        AbstractExpression joinExpr = parseJoinCondition(tableNode);
        AbstractExpression whereExpr = parseWhereCondition(tableNode);

        // The join type of the leaf node is always INNER
        // For a new tree its node's ids start with 0 and keep incrementing by 1
        int nodeId = (m_joinTree == null) ? 0 : m_joinTree.getId() + 1;

        JoinNode leafNode;
        if (table != null) {
            leafNode = new TableLeafNode(nodeId, joinExpr, whereExpr, (StmtTargetTableScan)tableScan);
        } else {
            assert(tableScan instanceof StmtSubqueryScan);
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
                m_paramsByIndex.put(index, pve);
            }
        }
        if (max_parameter_id >= NEXT_PARAMETER_ID) {
            NEXT_PARAMETER_ID = (int)max_parameter_id + 1;
        }
    }

    /** Get a list of the table subqueries used by this statement.  This method
     * may be overridden by subclasses, e.g., insert statements have a subquery
     * but does not use m_joinTree.
     **/
    public List<StmtSubqueryScan> getSubqueryScans() {
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
        m_paramsByIndex.putAll(childStmt.m_paramsByIndex);
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
        for (Map.Entry<Integer, ParameterValueExpression> paramEntry : m_paramsByIndex.entrySet()) {
            retval += paramEntry.getValue().toString() + " ";
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
        subquery.m_paramsByIndex = m_paramsByIndex;

        AbstractParsedStmt.parse(subquery, m_sql, queryNode, m_db, m_joinOrder);
        return subquery;
    }

    protected AbstractParsedStmt parseSubquery(VoltXMLElement suqueryElmt) {
        AbstractParsedStmt subQuery = AbstractParsedStmt.getParsedStmt(suqueryElmt, m_paramValues, m_db);
        // Propagate parameters from the parent to the child
        subQuery.m_parentStmt = this;
        subQuery.m_paramsById.putAll(m_paramsById);

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
            return m_paramsByIndex.values().toArray(new ParameterValueExpression[m_paramsByIndex.size()]);
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

        if (limitXml == null && offsetXml == null)
            return null;

        String node;
        long limitParameterId = -1;
        long offsetParameterId = -1;
        long limit = -1;
        long offset = 0;
        if (limitXml != null) {
            // Parse limit
            if ((node = limitXml.attributes.get("limit_paramid")) != null)
                limitParameterId = Long.parseLong(node);
            else {
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
            if ((node = offsetXml.attributes.get("offset_paramid")) != null)
                offsetParameterId = Long.parseLong(node);
            else {
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
        if (limit != -1) assert limitParameterId == -1 : "Parsed value and param. limit.";
        if (offset != 0) assert offsetParameterId == -1 : "Parsed value and param. offset.";

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
    protected boolean orderByColumnsCoverUniqueKeys()
    {
        // In theory, if EVERY table in the query has a uniqueness constraint
        // (primary key or other unique index) on columns that are all listed in the ORDER BY values,
        // the result is deterministic.
        // This holds regardless of whether the associated index is actually used in the selected plan,
        // so this check is plan-independent.
        HashMap<String, List<AbstractExpression> > baseTableAliases =
                new HashMap<String, List<AbstractExpression> >();
        for (ParsedColInfo col : orderByColumns()) {
            AbstractExpression expr = col.expression;
            List<AbstractExpression> baseTVEs = expr.findBaseTVEs();
            if (baseTVEs.size() != 1) {
                // Table-spanning ORDER BYs -- like ORDER BY A.X + B.Y are not helpful.
                // Neither are (nonsense) constant (table-less) expressions.
                continue;
            }
            // This loops exactly once.
            AbstractExpression baseTVE = baseTVEs.get(0);
            String nextTableAlias = ((TupleValueExpression)baseTVE).getTableAlias();
            assert(nextTableAlias != null);
            List<AbstractExpression> perTable = baseTableAliases.get(nextTableAlias);
            if (perTable == null) {
                perTable = new ArrayList<AbstractExpression>();
                baseTableAliases.put(nextTableAlias, perTable);
            }
            perTable.add(expr);
        }

        if (m_tableAliasMap.size() > baseTableAliases.size()) {
            // FIXME: This would be one of the tricky cases where the goal would be to prove that the
            // row with no ORDER BY component came from the right side of a 1-to-1 or many-to-1 join.
            // like Unique Index nested loop join, etc.
            return false;
        }
        boolean allScansAreDeterministic = true;
        for (Entry<String, List<AbstractExpression>> orderedAlias : baseTableAliases.entrySet()) {
            List<AbstractExpression> orderedAliasExprs = orderedAlias.getValue();
            StmtTableScan tableScan = m_tableAliasMap.get(orderedAlias.getKey());
            if (tableScan == null) {
                assert(false);
                return false;
            }

            if (tableScan instanceof StmtSubqueryScan) {
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
                List<AbstractExpression> indexExpressions = new ArrayList<AbstractExpression>();

                String jsonExpr = index.getExpressionsjson();
                // if this is a pure-column index...
                if (jsonExpr.isEmpty()) {
                    for (ColumnRef cref : index.getColumns()) {
                        Column col = cref.getColumn();
                        TupleValueExpression tve = new TupleValueExpression(table.getTypeName(),
                                                                            orderedAlias.getKey(),
                                                                            col.getName(),
                                                                            col.getName(),
                                                                            col.getIndex());
                        indexExpressions.add(tve);
                    }
                }
                // if this is a fancy expression-based index...
                else {
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

    /** May be true for DELETE or SELECT */
    public boolean hasOrderByColumns() {
        return false;
    }

    /** Subclasses should override this method of they have order by columns */
    public List<ParsedColInfo> orderByColumns() {
        assert(false);
        return null;
    }

    /*
     *  Extract all subexpressions of a given expression class from this statement
     */
    public List<AbstractExpression> findAllSubexpressionsOfClass(Class< ? extends AbstractExpression> aeClass) {
        List<AbstractExpression> exprs = new ArrayList<AbstractExpression>();
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
        // TO DO: If performance is an issue,
        // hard-code a tree walk that stops at the first subquery scan.
        if ( ! getSubqueryScans().isEmpty()) {
            return true;
        }
        // Verify expression subqueries
        List<AbstractExpression> subqueryExprs = findAllSubexpressionsOfClass(
                SelectSubqueryExpression.class);
        return !subqueryExprs.isEmpty();
    }

}
