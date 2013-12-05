/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.VectorValueExpression;
import org.voltdb.planner.parseinfo.BranchNode;
import org.voltdb.planner.parseinfo.JoinNode;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.planner.parseinfo.SubqueryLeafNode;
import org.voltdb.planner.parseinfo.TableLeafNode;
import org.voltdb.planner.parseinfo.TempTable;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;

public abstract class AbstractParsedStmt {

    public String sql;

    // The initial value is a safety net for the case of parameter-less statements.
    private ParameterValueExpression[] m_paramList = new ParameterValueExpression[0];

    protected HashMap<Long, ParameterValueExpression> m_paramsById = new HashMap<Long, ParameterValueExpression>();

    public ArrayList<Table> tableList = new ArrayList<Table>();
    private Table m_DDLIndexedTable = null;

    public void setTable(Table tbl) {
        m_DDLIndexedTable = tbl;
        // Add this table to the cache
        assert(tbl.getTypeName() != null);
        addTableToStmtCache(tbl.getTypeName(), tbl.getTypeName());
    }

    public ArrayList<AbstractExpression> noTableSelectionList = new ArrayList<AbstractExpression>();

    protected ArrayList<AbstractExpression> aggregationList = null;

    // Hierarchical join representation
    public JoinNode joinTree = null;

    //User specified join order, null if none is specified
    public String joinOrder = null;

    // Statement cache
    public List<StmtTableScan> stmtCache = new ArrayList<StmtTableScan>();
    public HashMap<String, Integer> tableAliasIndexMap = new HashMap<String, Integer>();

    protected final String[] m_paramValues;
    protected final Database m_db;

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
       return retval;
   }

   /**
   * @param parsedStmt
   * @param sql
   * @param xmlSQL
   * @param db
   * @param joinOrder
   */
  private static AbstractParsedStmt parse(AbstractParsedStmt parsedStmt, String sql,
          VoltXMLElement stmtTypeElement,  String[] paramValues, Database db, String joinOrder) {
      // parse tables and parameters
      parsedStmt.parseTablesAndParams(stmtTypeElement);

      // parse specifics
      parsedStmt.parse(stmtTypeElement);

      // post parse action
      parsedStmt.postParse(sql, joinOrder);

      return parsedStmt;

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

        AbstractParsedStmt retval = getParsedStmt(stmtTypeElement, paramValues, db);
        return parse(retval, sql, stmtTypeElement, paramValues, db, joinOrder);
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
            Column col = table.getColumns().getIgnoreCase(name.trim());

            assert(child.children.size() == 1);
            VoltXMLElement subChild = child.children.get(0);
            AbstractExpression expr = parseExpressionTree(subChild);
            assert(expr != null);
            expr.refineValueType(VoltType.get((byte)col.getType()), col.getSize());
            ExpressionUtil.finalizeValueTypes(expr);
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
        this.sql = sql;
        this.joinOrder = joinOrder;
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
            if (aggregationList != null) {
                ExpressionUtil.finalizeValueTypes(retval);
                aggregationList.add(retval);
            }
        }
        else if (elementName.equals("function")) {
            retval = parseFunctionExpression(root);
        }
        else if (elementName.equals("asterisk")) {
            return null;
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
    private TupleValueExpression parseColumnRefExpression(VoltXMLElement exprNode) {

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
        addScanColumn(expr);
        return expr;
    }

    /**
     * Collect unique columns used in the plan for a given table.
     *
     * @param tveColumn - scan column to add
     */
    private void addScanColumn(TupleValueExpression tveColumn)
    {
        // add table to the query cache if it's not there yet
        int tableCacheIdx = StmtTableScan.NULL_ALIAS_INDEX;
        // The sub-query temp table should be already registered
        // during the parseTable and/or setTable calls
        String tableAlias = tveColumn.getTableAlias();
        assert(tableAliasIndexMap.containsKey(tableAlias) == true);
        tableCacheIdx = tableAliasIndexMap.get(tableAlias);
        assert(tableCacheIdx != StmtTableScan.NULL_ALIAS_INDEX);

        StmtTableScan tableCache = stmtCache.get(tableCacheIdx);
        // For the subqueries replace table name with its alias.
        if (tableCache.getScanType() == StmtTableScan.TABLE_SCAN_TYPE.TEMP_TABLE_SCAN) {
            tveColumn.setTableName(tableAlias);
        }
        // Resolve the tve before adding it to the cache
        tableCache.resolveTVEForDB(m_db, tveColumn);
        SchemaColumn col = new SchemaColumn(tveColumn.getTableName(),
                tveColumn.getTableAlias(),
                tveColumn.getColumnName(),
                tveColumn.getColumnAlias(),
                tveColumn);
        tableCache.getScanColumns().add(col);
    }

    /**
     * Add a table or a sub-query to the statement cache. If the subQuery is not NULL,
     * the table name and the alias specify the sub-query
     * @param tableName
     * @param tableAlias
     * @param subQuery
     * @return index into the cache array
     */
    private int addTableToStmtCache(String tableName, String tableAlias, AbstractParsedStmt subQuery) {
        // Create an index into the query Catalog cache
        if (!tableAliasIndexMap.containsKey(tableAlias)) {
            int nextIndex = stmtCache.size();
            tableAliasIndexMap.put(tableAlias, nextIndex);
            StmtTableScan tableCache = null;
            if (subQuery == null) {
                tableCache = new StmtTargetTableScan(getTableFromDB(tableName), tableAlias);
            } else {
                // Temp table always have name SYSTEM_SUBQUERY.
                // Need to use its alias to uniquely identify the sub-query
                tableCache = new StmtSubqueryScan(new TempTable(tableAlias, tableAlias, subQuery), tableAlias);
            }
            stmtCache.add(tableCache);
            tableAliasIndexMap.put(tableAlias, nextIndex);
        }
        return tableAliasIndexMap.get(tableAlias);
    }

    /**
     * Add a table to the statement cache.
     * @param tableName
     * @param tableAlias
     * @return index into the cache array
     */
    private int addTableToStmtCache(String tableName, String tableAlias) {
        return addTableToStmtCache(tableName, tableAlias, null);
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
        }

        expr.resolveForStmt(m_db, this);
        return expr;
    }

    /**
     * Build a WHERE expression for a single-table statement.
     */
    public AbstractExpression getSingleTableFilterExpression() {
        if (joinTree == null) { // Not possible.
            assert(joinTree != null);
            return null;
        }
        return joinTree.getSimpleFilterExpression();
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
        // Possible sub-query
        AbstractParsedStmt subQuery = parseSubQuery(tableNode);

        // add table to the query cache before processing the JOIN/WHERE expressions
        // The order is important because processing sub-query expressions assumes that
        // the sub-query is already registered
        int aliasIdx = addTableToStmtCache(tableName, tableAlias, subQuery);

        AbstractExpression joinExpr = parseJoinCondition(tableNode);
        AbstractExpression whereExpr = parseWhereCondition(tableNode);

        StmtTableScan tableCache = stmtCache.get(aliasIdx);
        if (tableCache.getScanType() == StmtTableScan.TABLE_SCAN_TYPE.TARGET_TABLE_SCAN) {
            Table table = tableCache.getTargetTable();
            tableList.add(table);
        }

        // The join type of the leaf node is always INNER
        // For a new tree its node's ids start with 0 and keep incrementing by 1
        int nodeId = (joinTree == null) ? 0 : joinTree.getId() + 1;
        JoinNode leafNode = (subQuery == null) ?
                new TableLeafNode(nodeId, aliasIdx, joinExpr, whereExpr) :
                new SubqueryLeafNode(nodeId, aliasIdx, joinExpr, whereExpr);

        if (joinTree == null) {
            // this is the first table
            joinTree = leafNode;
        } else {
            // Build the tree by attaching the next table always to the right
            // The node's join type is determined by the type of its right node

            JoinType joinType = JoinType.get(tableNode.attributes.get("jointype"));
            assert(joinType != JoinType.INVALID);
            if (joinType == JoinType.FULL) {
                throw new PlanningErrorException("VoltDB does not support full outer joins");
            }

            JoinNode joinNode = new BranchNode(nodeId + 1, joinType, joinTree, leafNode);
            joinTree = joinNode;
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

        for (VoltXMLElement node : paramsNode.children) {
            if (node.name.equalsIgnoreCase("parameter")) {
                long id = Long.parseLong(node.attributes.get("id"));
                int index = Integer.parseInt(node.attributes.get("index"));
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
    }

    /**
     * Collect value equivalence expressions across the entire SQL statement
     * @return a map of tuple value expressions to the other expressions,
     * TupleValueExpressions, ConstantValueExpressions, or ParameterValueExpressions,
     * that they are constrained to equal.
     */
    HashMap<AbstractExpression, Set<AbstractExpression>> analyzeValueEquivalence() {
        // collect individual where/join expressions
        analyzeJoinExpressions(joinTree);
        return joinTree.getAllEquivalenceFilters();
    }

    /**
     * Analyze join expressions
     */
    void analyzeJoinExpressions(JoinNode joinNode) {
        //assert (joinNode != null);
        if (joinNode == null) {
            return;
        }
        if (joinNode.getNodeType() == JoinNode.NodeType.LEAF ||
            joinNode.getNodeType() == JoinNode.NodeType.SUBQUERY) {
            // Leaf node. Simply un-combine expressions and move them to the inner lists
            // The expressions will be classified later at the join node level.
            // If this is a single table select then classification is not required.
            assert(joinNode.getLeftNode() == null && joinNode.getRightNode() == null);
            joinNode.m_joinInnerList.addAll(ExpressionUtil.uncombineAny(joinNode.getJoinExpression()));
            joinNode.m_whereInnerList.addAll(ExpressionUtil.uncombineAny(joinNode.getWhereExpression()));
            return;
        }

        assert(joinNode.getNodeType() == JoinNode.NodeType.JOIN);
        analyzeJoinExpressions(joinNode.getLeftNode());
        analyzeJoinExpressions(joinNode.getRightNode());

        // At this moment all RIGHT joins are already converted to the LEFT ones
        assert (joinNode.getJoinType() == JoinType.LEFT || joinNode.getJoinType() == JoinType.INNER);

        ArrayList<AbstractExpression> joinList = new ArrayList<AbstractExpression>();
        ArrayList<AbstractExpression> whereList = new ArrayList<AbstractExpression>();

        // Collect node's own join and where expressions
        joinList.addAll(ExpressionUtil.uncombineAny(joinNode.getJoinExpression()));
        whereList.addAll(ExpressionUtil.uncombineAny(joinNode.getWhereExpression()));

        // Collect children expressions only if a child is a leaf. They are not classified yet
        JoinNode leftChild = joinNode.getLeftNode();
        if (leftChild.getNodeType() == JoinNode.NodeType.LEAF ||
                leftChild.getNodeType() == JoinNode.NodeType.SUBQUERY) {
            joinList.addAll(leftChild.m_joinInnerList);
            leftChild.m_joinInnerList.clear();
            whereList.addAll(leftChild.m_whereInnerList);
            leftChild.m_whereInnerList.clear();
        }
        JoinNode rightChild = joinNode.getRightNode();
        if (rightChild.getNodeType() == JoinNode.NodeType.LEAF ||
                rightChild.getNodeType() == JoinNode.NodeType.SUBQUERY) {
            joinList.addAll(rightChild.m_joinInnerList);
            rightChild.m_joinInnerList.clear();
            whereList.addAll(rightChild.m_whereInnerList);
            rightChild.m_whereInnerList.clear();
        }

        Collection<Integer> outerTables = leftChild.generateTableJoinOrder();
        Collection<Integer> innerTables = rightChild.generateTableJoinOrder();

        // Classify join expressions into the following categories:
        // 1. The OUTER-only join conditions. If any are false for a given outer tuple,
        // then NO inner tuples should match it (and it can automatically get null-padded by the join
        // without even considering the inner table). Testing the outer-only conditions
        // COULD be considered as an optimal first step to processing each outer tuple
        // 2. The INNER-only join conditions apply to the inner tuples (even prior to considering any outer tuple).
        // if true for a given inner tuple, the condition has no effect, if false,
        // it prevents the inner tuple from matching ANY outer tuple,
        // In case of multi-tables join, they could be pushed down to a child node if this node is a join itself
        // 3. The two-sided expressions that get evaluated on each combination of outer and inner tuple
        // and either accept or reject that particular combination.
        // 4. The TVE expressions where neither inner nor outer tables are involved. This is not possible
        // for the currently supported two table joins but could change if number of tables > 2
        classifyJoinExpressions(joinList, outerTables, innerTables,  joinNode.m_joinOuterList,
                joinNode.m_joinInnerList, joinNode.m_joinInnerOuterList);

        // Apply implied transitive constant filter to join expressions
        // outer.partkey = ? and outer.partkey = inner.partkey is equivalent to
        // outer.partkey = ? and inner.partkey = ?
        applyTransitiveEquivalence(joinNode.m_joinOuterList, joinNode.m_joinInnerList, joinNode.m_joinInnerOuterList);

        // Classify where expressions into the following categories:
        // 1. The OUTER-only filter conditions. If any are false for a given outer tuple,
        // nothing in the join processing of that outer tuple will get it past this filter,
        // so it makes sense to "push this filter down" to pre-qualify the outer tuples before they enter the join.
        // 2. The INNER-only join conditions. If these conditions reject NULL inner tuple it make sense to
        // move them "up" to the join conditions, otherwise they must remain post-join conditions
        // to preserve outer join semantic
        // 3. The two-sided expressions. Same as the inner only conditions.
        // 4. The TVE expressions where neither inner nor outer tables are involved. Same as for the join expressions
        classifyJoinExpressions(whereList, outerTables, innerTables,  joinNode.m_whereOuterList,
                joinNode.m_whereInnerList, joinNode.m_whereInnerOuterList);

        // Apply implied transitive constant filter to where expressions
        applyTransitiveEquivalence(joinNode.m_whereOuterList, joinNode.m_whereInnerList, joinNode.m_whereInnerOuterList);

        // In case of multi-table joins certain expressions could be pushed down to the children
        // to improve join performance.
        pushDownExpressions(joinNode);
    }

    /**
     * Split the input expression list into the three categories
     * 1. TVE expressions with outer tables only
     * 2. TVE expressions with inner tables only
     * 3. TVE expressions with inner and outer tables
     * The outer tables are the tables reachable from the outer node of the join
     * The inner tables are the tables reachable from the inner node of the join
     * @param exprList expression list to split
     * @param outerTables outer table
     * @param innerTable outer table
     * @param outerList expressions with outer table only
     * @param innerList expressions with inner table only
     * @param innerOuterList with inner and outer tables
     */
    void classifyJoinExpressions(Collection<AbstractExpression> exprList,
            Collection<Integer> outerTables, Collection<Integer> innerTables,
            List<AbstractExpression> outerList, List<AbstractExpression> innerList,
            List<AbstractExpression> innerOuterList) {
        HashSet<Integer> tableAliasSet = new HashSet<Integer>();
        HashSet<Integer> outerSet = new HashSet<Integer>(outerTables);
        HashSet<Integer> innerSet = new HashSet<Integer>(innerTables);
        for (AbstractExpression expr : exprList) {
            tableAliasSet.clear();
            getTablesForExpression(expr, tableAliasSet);
            Integer tableAliasIdxs[] = tableAliasSet.toArray(new Integer[0]);
            if (tableAliasSet.isEmpty()) {
                noTableSelectionList.add(expr);
            } else {
                boolean outer = false;
                boolean inner = false;
                for (Integer aliasIdx : tableAliasIdxs) {
                    outer = outer || outerSet.contains(aliasIdx);
                    inner = inner || innerSet.contains(aliasIdx);
                }
                if (outer && inner) {
                    innerOuterList.add(expr);
                } else if (outer) {
                    outerList.add(expr);
                } else if (inner) {
                    innerList.add(expr);
                } else {
                    // can not be, right?
                    assert(false);
                }
            }
        }
    }

    /**
     * Apply implied transitive constant filter to join expressions
     * outer.partkey = ? and outer.partkey = inner.partkey is equivalent to
     * outer.partkey = ? and inner.partkey = ?
     * @param innerTableExprs inner table expressions
     * @param outerTableExprs outer table expressions
     * @param innerOuterTableExprs inner-outer tables expressions
     */
    private static void applyTransitiveEquivalence(List<AbstractExpression> outerTableExprs,
            List<AbstractExpression> innerTableExprs,
            List<AbstractExpression> innerOuterTableExprs)
    {
        List<AbstractExpression> simplifiedOuterExprs = applyTransitiveEquivalence(innerTableExprs, innerOuterTableExprs);
        List<AbstractExpression> simplifiedInnerExprs = applyTransitiveEquivalence(outerTableExprs, innerOuterTableExprs);
        outerTableExprs.addAll(simplifiedOuterExprs);
        innerTableExprs.addAll(simplifiedInnerExprs);
    }

    private static List<AbstractExpression>
    applyTransitiveEquivalence(List<AbstractExpression> singleTableExprs,
                               List<AbstractExpression> twoTableExprs)
    {
        ArrayList<AbstractExpression> simplifiedExprs = new ArrayList<AbstractExpression>();
        HashMap<AbstractExpression, Set<AbstractExpression> > eqMap1 =
                new HashMap<AbstractExpression, Set<AbstractExpression> >();
        ExpressionUtil.collectPartitioningFilters(singleTableExprs, eqMap1);

        for (AbstractExpression expr : twoTableExprs) {
            if (! ExpressionUtil.isColumnEquivalenceFilter(expr)) {
                continue;
            }
            AbstractExpression leftExpr = expr.getLeft();
            AbstractExpression rightExpr = expr.getRight();
            assert(leftExpr instanceof TupleValueExpression && rightExpr instanceof TupleValueExpression);
            Set<AbstractExpression> eqSet1 = eqMap1.get(leftExpr);
            AbstractExpression singleExpr = leftExpr;
            if (eqSet1 == null) {
                eqSet1 = eqMap1.get(rightExpr);
                if (eqSet1 == null) {
                    continue;
                }
                singleExpr = rightExpr;
            }

            for (AbstractExpression eqExpr : eqSet1) {
                if (eqExpr instanceof ConstantValueExpression) {
                    if (singleExpr == leftExpr) {
                        expr.setLeft(eqExpr);
                    } else {
                        expr.setRight(eqExpr);
                    }
                    simplifiedExprs.add(expr);
                    // Having more than one const value for a single column doesn't make
                    // much sense, right?
                    break;
                }
            }

        }

         twoTableExprs.removeAll(simplifiedExprs);
         return simplifiedExprs;
    }

    /**
     * Push down each WHERE expression on a given join node to the most specific child join
     * or table the expression applies to.
     *  1. The OUTER WHERE expressions can be pushed down to the outer (left) child for all joins
     *    (INNER and LEFT).
     *  2. The INNER WHERE expressions can be pushed down to the inner (right) child for the INNER joins.
     * @param joinNode JoinNode
     */
    private void pushDownExpressions(JoinNode joinNode) {
        assert (joinNode != null && joinNode.getNodeType() == JoinNode.NodeType.JOIN);
        JoinNode outerNode = joinNode.getLeftNode();
        if (outerNode.getNodeType() == JoinNode.NodeType.JOIN) {
            pushDownExpressionsRecursively(outerNode, joinNode.m_whereOuterList);
        }
        JoinNode innerNode = joinNode.getRightNode();
        if (innerNode.getNodeType() == JoinNode.NodeType.JOIN && joinNode.getJoinType() == JoinType.INNER) {
            pushDownExpressionsRecursively(innerNode, joinNode.m_whereInnerList);
        }
    }

    private void pushDownExpressionsRecursively(JoinNode joinNode, List<AbstractExpression> pushDownExprList) {
        assert(joinNode.getNodeType() == JoinNode.NodeType.JOIN);
        // It is a join node. Classify pushed down expressions as inner, outer, or inner-outer
        // WHERE expressions.
        Collection<Integer> outerTables = joinNode.getLeftNode().generateTableJoinOrder();
        Collection<Integer> innerTables = joinNode.getRightNode().generateTableJoinOrder();
        classifyJoinExpressions(pushDownExprList, outerTables, innerTables,
                joinNode.m_whereOuterList, joinNode.m_whereInnerList, joinNode.m_whereInnerOuterList);
        // Remove them from the original list
        pushDownExprList.clear();
        // Descend to the inner child
        pushDownExpressions(joinNode);
    }

    /**
     *
     * @param expr
     * @param tables
     */
    void getTablesForExpression(AbstractExpression expr, HashSet<Integer> tables) {
        List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(expr);
        for (TupleValueExpression tupleExpr : tves) {
            String tableAlias = tupleExpr.getTableAlias();
            assert(tableAliasIndexMap.containsKey(tableAlias));
            tables.add(tableAliasIndexMap.get(tableAlias));
        }
    }

    protected Table getTableFromDB(String tableName) {
        Table table = m_db.getTables().getIgnoreCase(tableName);
        return table;
    }

    @Override
    public String toString() {
        String retval = "SQL:\n\t" + sql + "\n";

        retval += "PARAMETERS:\n\t";
        for (ParameterValueExpression param : m_paramList) {
            retval += param.toString() + " ";
        }

        retval += "\nTABLE SOURCES:\n\t";
        for (Table table : tableList) {
            retval += table.getTypeName() + " ";
        }

        retval += "\nSCAN COLUMNS:\n";
        boolean hasAll = true;
        for (StmtTableScan tableCache : stmtCache) {
            if (tableCache.getScanColumns().isEmpty() == false) {
                hasAll = false;
                retval += "\tTable Alias: " + tableCache.getTableAlias() + ":\n";
                for (SchemaColumn col : tableCache.getScanColumns())
                {
                    retval += "\t\tColumn: " + col.getColumnName() + ": ";
                    retval += col.getExpression().toString() + "\n";
                }
            }
        }
        if (hasAll == true) {
            retval += "\tALL\n";
        }

        retval += "\nJOIN TREE :\n";
        if (joinTree != null ) {
            retval += joinTree.toString();
        }

        retval += "NO TABLE SELECTION LIST:\n";
        int i = 0;
        for (AbstractExpression expr : noTableSelectionList)
            retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

        return retval;
    }

    private AbstractParsedStmt parseSubQuery(VoltXMLElement tableScan) {
        AbstractParsedStmt subQuery = null;
        for (VoltXMLElement childNode : tableScan.children) {
            if (childNode.name.equals("tablesubquery")) {
                if (!childNode.children.isEmpty()) {
                    subQuery = AbstractParsedStmt.getParsedStmt(childNode.children.get(0), m_paramValues, m_db);
                    // Propagate parameters from the parent to the child
                    subQuery.m_paramsById.putAll(m_paramsById);
                    subQuery.m_paramList = m_paramList;
                    subQuery = AbstractParsedStmt.parse(subQuery, sql, childNode.children.get(0), m_paramValues, m_db, joinOrder);
                    break;
                }
            }
        }
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
        return m_paramList;
    }

}
