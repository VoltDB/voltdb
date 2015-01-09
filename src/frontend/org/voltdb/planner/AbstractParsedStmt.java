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
import java.util.Set;
import java.util.Map.Entry;

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
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.ParameterValueExpression;
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

public abstract class AbstractParsedStmt {

    public String m_sql;

    // The initial value is a safety net for the case of parameter-less statements.
    private ParameterValueExpression[] m_paramList = new ParameterValueExpression[0];

    protected HashMap<Long, ParameterValueExpression> m_paramsById = new HashMap<Long, ParameterValueExpression>();

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
            VoltXMLElement stmtTypeElement,  String[] paramValues, Database db, String joinOrder) {
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

        AbstractParsedStmt retval = getParsedStmt(stmtTypeElement, paramValues, db);
        parse(retval, sql, stmtTypeElement, paramValues, db, joinOrder);
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
        else if (elementName.equals("row")) {
            throw new PlanningErrorException("Unsupported subquery syntax within an expression.");
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
        StmtTableScan tableScan = m_tableAliasMap.get(tableAlias);
        assert(tableScan != null);
        String columnName = exprNode.attributes.get("column");
        String columnAlias = exprNode.attributes.get("alias");
        TupleValueExpression expr = new TupleValueExpression(tableName, tableAlias, columnName, columnAlias);
        // Collect the unique columns used in the plan for a given scan.
        // Resolve the tve and add it to the scan's cache of referenced columns
        tableScan.resolveTVE(expr, columnName);
        return expr;
    }

    /**
     * Add a table or a sub-query to the statement cache. If the subQuery is not NULL,
     * the table name and the alias specify the sub-query
     * @param tableName
     * @param tableAlias
     * @param subQuery
     * @return index into the cache array
     */
    private StmtTableScan addTableToStmtCache(String tableName, String tableAlias, AbstractParsedStmt subquery) {
        // Create an index into the query Catalog cache
        StmtTableScan tableScan = m_tableAliasMap.get(tableAlias);
        if (tableScan == null) {
            if (subquery == null) {
                tableScan = new StmtTargetTableScan(getTableFromDB(tableName), tableAlias);
            } else {
                tableScan = new StmtSubqueryScan(subquery, tableAlias);
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
            subquery = parseSubquery(childNode.children.get(0));
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

    protected AbstractParsedStmt parseSubquery(VoltXMLElement queryNode) {
        AbstractParsedStmt subquery = AbstractParsedStmt.getParsedStmt(queryNode, m_paramValues, m_db);
        // Propagate parameters from the parent to the child
        subquery.m_paramsById.putAll(m_paramsById);
        subquery.m_paramList = m_paramList;
        AbstractParsedStmt.parse(subquery, m_sql, queryNode, m_paramValues, m_db, m_joinOrder);
        return subquery;
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

    /**
     * Return true if a SQL statement contains a subquery of any kind
     * @return TRUE is this statement contains a subquery
     */
    public boolean hasSubquery() {
        // This method should be called only after the statement is parsed and join tree is built
        return !getSubqueries().isEmpty();
    }
}
