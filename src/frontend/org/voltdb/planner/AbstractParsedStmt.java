/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
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
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;

public abstract class AbstractParsedStmt {

    public static class TablePair {
        public Table t1;
        public Table t2;

        @Override
        public boolean equals(Object obj) {
            if ((obj instanceof TablePair) == false)
                return false;
            TablePair tp = (TablePair)obj;

            return (((t1 == tp.t1) && (t2 == tp.t2)) ||
                    ((t1 == tp.t2) && (t2 == tp.t1)));
        }

        @Override
        public int hashCode() {
            assert((t1.hashCode() ^ t2.hashCode()) == (t2.hashCode() ^ t1.hashCode()));

            return t1.hashCode() ^ t2.hashCode();
        }
    }


    public String sql;

    public ArrayList<ParameterInfo> paramList = new ArrayList<ParameterInfo>();

    public HashMap<Long, ParameterInfo> paramsById = new HashMap<Long, ParameterInfo>();

    public ArrayList<Table> tableList = new ArrayList<Table>();

    public AbstractExpression where = null;

    public ArrayList<AbstractExpression> whereSelectionList = new ArrayList<AbstractExpression>();

    public ArrayList<AbstractExpression> noTableSelectionList = new ArrayList<AbstractExpression>();

    public ArrayList<AbstractExpression> multiTableSelectionList = new ArrayList<AbstractExpression>();

    public HashMap<Table, ArrayList<AbstractExpression>> tableFilterList = new HashMap<Table, ArrayList<AbstractExpression>>();

    public HashMap<TablePair, ArrayList<AbstractExpression>> joinSelectionList = new HashMap<TablePair, ArrayList<AbstractExpression>>();

    public HashMap<AbstractExpression, Set<AbstractExpression> > valueEquivalence = new HashMap<AbstractExpression, Set<AbstractExpression>>();

    //User specified join order, null if none is specified
    public String joinOrder = null;

    // Store a table-hashed list of the columns actually used by this statement.
    // XXX An unfortunately counter-intuitive (but hopefully temporary) meaning here:
    // if this is null, that means ALL the columns get used.
    public HashMap<String, ArrayList<SchemaColumn>> scanColumns = null;

    /**
     *
     * @param sql
     * @param xmlSQL
     * @param db
     */
    public static AbstractParsedStmt parse(String sql, VoltXMLElement xmlSQL, Database db, String joinOrder) {
        final String INSERT_NODE_NAME = "insert";
        final String UPDATE_NODE_NAME = "update";
        final String DELETE_NODE_NAME = "delete";
        final String SELECT_NODE_NAME = "select";

        AbstractParsedStmt retval = null;

        if (xmlSQL == null) {
            System.err.println("Unexpected error parsing hsql parsed stmt xml");
            throw new RuntimeException("Unexpected error parsing hsql parsed stmt xml");
        }

        assert(xmlSQL.name.equals("statement"));

        VoltXMLElement stmtTypeElement = xmlSQL.children.get(0);

        // create non-abstract instances
        if (stmtTypeElement.name.equalsIgnoreCase(INSERT_NODE_NAME)) {
            retval = new ParsedInsertStmt();
        }
        else if (stmtTypeElement.name.equalsIgnoreCase(UPDATE_NODE_NAME)) {
            retval = new ParsedUpdateStmt();
        }
        else if (stmtTypeElement.name.equalsIgnoreCase(DELETE_NODE_NAME)) {
            retval = new ParsedDeleteStmt();
        }
        else if (stmtTypeElement.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
            retval = new ParsedSelectStmt();
        }
        else {
            throw new RuntimeException("Unexpected Element: " + stmtTypeElement.name);
        }

        // parse tables and parameters
        for (VoltXMLElement node : stmtTypeElement.children) {
            if (node.name.equalsIgnoreCase("parameters")) {
                retval.parseParameters(node, db);
            }
            if (node.name.equalsIgnoreCase("tablescans")) {
                retval.parseTables(node, db);
            }
            if (node.name.equalsIgnoreCase("scan_columns"))
            {
                retval.parseScanColumns(node, db);
            }
        }

        // parse specifics
        retval.parse(stmtTypeElement, db);

        // split up the where expression into categories
        retval.analyzeWhereExpression(db);
        // these just shouldn't happen right?
        assert(retval.multiTableSelectionList.size() == 0);
        assert(retval.noTableSelectionList.size() == 0);

        retval.sql = sql;
        retval.joinOrder = joinOrder;

        return retval;
    }

    /**
     *
     * @param stmtElement
     * @param db
     */
    abstract void parse(VoltXMLElement stmtElement, Database db);

    /**
     * Convert a HSQL VoltXML expression to an AbstractExpression tree.
     * @param root
     * @param db
     * @return configured AbstractExpression
     */
    AbstractExpression parseExpressionTree(VoltXMLElement root, Database db) {
        String elementName = root.name.toLowerCase();
        AbstractExpression retval = null;

        if (elementName.equals("value")) {
            retval = parseValueExpression(root);
        }
        else if (elementName.equals("columnref")) {
            retval = parseColumnRefExpression(root, db);
        }
        else if (elementName.equals("bool")) {
            retval = parseBooleanExpresion(root);
        }
        else if (elementName.equals("operation")) {
            retval = parseOperationExpression(root, db);
        }
        else if (elementName.equals("function")) {
            retval = parseFunctionExpression(root, db);
        }
        else if (elementName.equals("asterisk")) {
            return null;
        }
        else
            throw new PlanningErrorException("Unsupported expression node '" + elementName + "'");

        return retval;
    }

    /**
     *
     * @param exprNode
     * @param attrs
     * @return
     */
    AbstractExpression parseValueExpression(VoltXMLElement exprNode) {
        String type = exprNode.attributes.get("type");
        String isParam = exprNode.attributes.get("isparam");

        VoltType vt = VoltType.typeFromString(type);
        int size = VoltType.MAX_VALUE_LENGTH;
        assert(vt != VoltType.VOLTTABLE);

        if ((vt != VoltType.STRING) && (vt != VoltType.VARBINARY)) {
            if (vt == VoltType.NULL) size = 0;
            else size = vt.getLengthInBytesForFixedTypes();
        }
        if ((isParam != null) && (isParam.equalsIgnoreCase("true"))) {
            ParameterValueExpression expr = new ParameterValueExpression();
            long id = Long.parseLong(exprNode.attributes.get("id"));
            int paramIndex = paramIndexById(id);

            expr.setValueType(vt);
            expr.setValueSize(size);
            expr.setParameterIndex(paramIndex);

            return expr;
        }
        else {
            ConstantValueExpression expr = new ConstantValueExpression();
            expr.setValueType(vt);
            expr.setValueSize(size);
            if (vt == VoltType.NULL)
                expr.setValue(null);
            else
                expr.setValue(exprNode.attributes.get("value"));
            return expr;
        }
    }

    /**
     *
     * @param exprNode
     * @param attrs
     * @param db
     * @return
     */
    AbstractExpression parseColumnRefExpression(VoltXMLElement exprNode, Database db) {
        TupleValueExpression expr = new TupleValueExpression();

        String alias = exprNode.attributes.get("alias");
        String tableName = exprNode.attributes.get("table");
        String columnName = exprNode.attributes.get("column");

        Table table = db.getTables().getIgnoreCase(tableName);
        assert(table != null);
        Column column = table.getColumns().getIgnoreCase(columnName);
        assert(column != null);

        expr.setColumnAlias(alias);
        expr.setColumnName(columnName);
        expr.setColumnIndex(column.getIndex());
        expr.setTableName(tableName);
        expr.setValueType(VoltType.get((byte)column.getType()));
        expr.setValueSize(column.getSize());

        return expr;
    }

    /**
     *
     * @param exprNode
     * @param attrs
     * @return
     */
    AbstractExpression parseBooleanExpresion(VoltXMLElement exprNode) {
        ConstantValueExpression expr = new ConstantValueExpression();

        expr.setValueType(VoltType.BIGINT);
        expr.setValueSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
        if (exprNode.attributes.get("attrs").equalsIgnoreCase("true"))
            expr.setValue("1");
        else
            expr.setValue("0");
        return expr;
    }

    /**
     *
     * @param exprNode
     * @param attrs
     * @param db
     * @return
     */
    AbstractExpression parseOperationExpression(VoltXMLElement exprNode, Database db) {
        String type = exprNode.attributes.get("type");
        ExpressionType exprType = ExpressionType.get(type);
        AbstractExpression expr = null;

        if (exprType == ExpressionType.INVALID) {
            throw new PlanningErrorException("Unsupported operation type '" + type + "'");
        }
        try {
            expr = exprType.getExpressionClass().newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }
        expr.setExpressionType(exprType);

        // If the operation type was 'simplecolumn' then it's going to turn
        // into a TVE and we need to bail out before we try parsing the
        // left and right subtrees
        if (expr instanceof TupleValueExpression)
        {
            return expr;
        }

        // Allow expressions to read expression-specific data from exprNode.
        // Looks like the design fully abstracts other volt classes from
        // the XML serialization?  Putting this here instead of in derived
        // Expression implementations.

        if (expr instanceof AggregateExpression) {
            String node;
            if ((node = exprNode.attributes.get("distinct")) != null) {
                AggregateExpression ae = (AggregateExpression)expr;
                ae.m_distinct = Boolean.parseBoolean(node);
            }
        }

        // get the first (left) node that is an element
        VoltXMLElement leftExprNode = exprNode.children.get(0);
        assert(leftExprNode != null);

        // get the second (right) node that is an element (might be null)
        VoltXMLElement rightExprNode = null;
        if (exprNode.children.size() > 1)
            rightExprNode = exprNode.children.get(1);

        // recursively parse the left subtree (could be another operator or
        // a constant/tuple/param value operand).
        AbstractExpression leftExpr = parseExpressionTree(leftExprNode, db);
        assert((leftExpr != null) || (exprType == ExpressionType.AGGREGATE_COUNT));
        expr.setLeft(leftExpr);

        if (expr.needsRightExpression()) {
            assert(rightExprNode != null);

            // recursively parse the right subtree
            AbstractExpression rightExpr = parseExpressionTree(rightExprNode, db);
            assert(rightExpr != null);
            expr.setRight(rightExpr);
        }

        return expr;
    }


    /**
     *
     * @param exprNode
     * @param db
     * @return a new Function Expression
     */
    AbstractExpression parseFunctionExpression(VoltXMLElement exprNode, Database db) {
        String name = exprNode.attributes.get("name").toLowerCase();
        // Parameterized argument type of function. One parameter type is apparently all that SQL ever needs.
        String value_type_name = exprNode.attributes.get("type");
        VoltType value_type = VoltType.typeFromString(value_type_name);
        AbstractExpression expr = null;

        ArrayList<AbstractExpression> args = new ArrayList<AbstractExpression>();
        // This needs to be conditional on an expected/allowed number/type of arguments.
        for (VoltXMLElement argNode : exprNode.children) {
            assert(argNode != null);
            // recursively parse each argument subtree (could be any kind of expression).
            AbstractExpression argExpr = parseExpressionTree(argNode, db);
            assert(argExpr != null);
            args.add(argExpr);
        }

        List<SQLFunction> overloads = SQLFunction.functionsByNameAndArgumentCount(name, args.size());
        if (overloads == null) {
            throw new PlanningErrorException("Function '" + name + "' with " + args.size() + " arguments is not supported");
        }

        // Validate/select specific named function overload against supported argument type(s?).
        // This amounts to a not-yet-implemented performance feature that allows the planner to direct the
        // executor to argument-type-specific functions instead of relying on its current tendency to
        // type-check every row/value at runtime.
        // Until that day, overloads can be a singleton list.
        SQLFunction resolved = null;
        for (SQLFunction supportedFunction : overloads) {
            resolved = supportedFunction;
            if ( ! supportedFunction.hasParameter()) {
                break;
            }
            VoltType paramType = supportedFunction.paramType();
            if (paramType.equals(value_type)) {
                break;
            }
            if (paramType.equals(VoltType.NUMERIC) && (value_type.isExactNumeric() ||  value_type.equals(VoltType.FLOAT))) {
                break;
            }
            // type was not acceptable or not supported
            resolved = null;
        }

        if (resolved == null) {
            throw new PlanningErrorException("Function '" + name + "' does not support argument type '" + value_type_name + "'");
        }

        ExpressionType exprType = resolved.getExpressionType();

        try {
            expr = exprType.getExpressionClass().newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }
        expr.setExpressionType(exprType);

        VoltType vt = resolved.getValueType();
        // Null return type is a place-holder for the parser-provided parameter type.
        if (vt != null) {
            expr.setValueType(vt);
            expr.setValueSize(vt.getMaxLengthInBytes());
        }

        expr.setArgs(args);
        return expr;
    }

    /**
     * Parse the scan_columns element out of the HSQL-generated XML.
     * Fills scanColumns with a list of the columns used in the plan, hashed by
     * table name.
     *
     * @param columnsNode
     * @param db
     */
    void parseScanColumns(VoltXMLElement columnsNode, Database db)
    {
        scanColumns = new HashMap<String, ArrayList<SchemaColumn>>();

        for (VoltXMLElement child : columnsNode.children) {
            assert(child.name.equals("columnref"));
            AbstractExpression col_exp = parseExpressionTree(child, db);
            ExpressionUtil.assignLiteralConstantTypesRecursively(col_exp);
            ExpressionUtil.assignOutputValueTypesRecursively(col_exp);
            assert(col_exp != null);
            assert(col_exp instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)col_exp;
            SchemaColumn col = new SchemaColumn(tve.getTableName(),
                                                tve.getColumnName(),
                                                tve.getColumnAlias(),
                                                col_exp);
            ArrayList<SchemaColumn> table_cols = null;
            if (!scanColumns.containsKey(col.getTableName()))
            {
                table_cols = new ArrayList<SchemaColumn>();
                scanColumns.put(col.getTableName(), table_cols);
            }
            table_cols = scanColumns.get(col.getTableName());
            table_cols.add(col);
        }
    }

    /**
     *
     * @param tablesNode
     * @param db
     */
    private void parseTables(VoltXMLElement tablesNode, Database db) {
        for (VoltXMLElement node : tablesNode.children) {
            if (node.name.equalsIgnoreCase("tablescan")) {
                String tableName = node.attributes.get("table");
                Table table = db.getTables().getIgnoreCase(tableName);
                assert(table != null);
                tableList.add(table);
            }
        }
    }

    private void parseParameters(VoltXMLElement paramsNode, Database db) {
        for (VoltXMLElement node : paramsNode.children) {
            if (node.name.equalsIgnoreCase("parameter")) {
                ParameterInfo param = new ParameterInfo();

                long id = Long.parseLong(node.attributes.get("id"));
                param.index = Integer.parseInt(node.attributes.get("index"));
                String typeName = node.attributes.get("type");
                param.type = VoltType.typeFromString(typeName);
                paramsById.put(id, param);
                paramList.add(param);
            }
        }
    }

    /**
     *
     * @param db
     */
    void analyzeWhereExpression(Database db) {

        // nothing to do if there's no where expression
        if (where == null) return;

        // this first chunk of code breaks the code into a list of expression that
        // all have to be true for the where clause to be true

        ArrayDeque<AbstractExpression> in = new ArrayDeque<AbstractExpression>();
        ArrayDeque<AbstractExpression> out = new ArrayDeque<AbstractExpression>();
        in.add(where);

        AbstractExpression inExpr = null;
        while ((inExpr = in.poll()) != null) {
            if (inExpr.getExpressionType() == ExpressionType.CONJUNCTION_AND) {
                in.add(inExpr.getLeft());
                in.add(inExpr.getRight());
            }
            else {
                out.add(inExpr);
            }
        }

        // the where selection list contains all the clauses
        whereSelectionList.addAll(out);

        // This next bit of code identifies which tables get classified how
        HashSet<Table> tableSet = new HashSet<Table>();
        for (AbstractExpression expr : whereSelectionList) {
            tableSet.clear();
            getTablesForExpression(db, expr, tableSet);
            if (tableSet.size() == 0) {
                noTableSelectionList.add(expr);
            }
            else if (tableSet.size() == 1) {
                Table table = (Table) tableSet.toArray()[0];

                ArrayList<AbstractExpression> exprs;
                if (tableFilterList.containsKey(table)) {
                    exprs = tableFilterList.get(table);
                }
                else {
                    exprs = new ArrayList<AbstractExpression>();
                    tableFilterList.put(table, exprs);
                }
                expr.m_isJoiningClause = false;
                addExprToEquivalenceSets(expr);
                exprs.add(expr);
            }
            else if (tableSet.size() == 2) {
                TablePair pair = new TablePair();
                pair.t1 = (Table) tableSet.toArray()[0];
                pair.t2 = (Table) tableSet.toArray()[1];

                ArrayList<AbstractExpression> exprs;
                if (joinSelectionList.containsKey(pair)) {
                    exprs = joinSelectionList.get(pair);
                }
                else {
                    exprs = new ArrayList<AbstractExpression>();
                    joinSelectionList.put(pair, exprs);
                }
                expr.m_isJoiningClause = true;
                addExprToEquivalenceSets(expr);
                exprs.add(expr);
            }
            else if (tableSet.size() > 2) {
                multiTableSelectionList.add(expr);
            }
        }
    }

    /**
     *
     * @param db
     * @param expr
     * @param tables
     */
    void getTablesForExpression(Database db, AbstractExpression expr, HashSet<Table> tables) {
        List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(expr);
        for (TupleValueExpression tupleExpr : tves) {
            String tableName = tupleExpr.getTableName();
            Table table = db.getTables().getIgnoreCase(tableName);
            tables.add(table);
        }
    }

    @Override
    public String toString() {
        String retval = "SQL:\n\t" + sql + "\n";

        retval += "PARAMETERS:\n\t";
        for (ParameterInfo param : paramList) {
            retval += param.toString() + " ";
        }

        retval += "\nTABLE SOURCES:\n\t";
        for (Table table : tableList) {
            retval += table.getTypeName() + " ";
        }

        retval += "\nSCAN COLUMNS:\n";
        if (scanColumns != null)
        {
            for (String table : scanColumns.keySet())
            {
                retval += "\tTable: " + table + ":\n";
                for (SchemaColumn col : scanColumns.get(table))
                {
                    retval += "\t\tColumn: " + col.getColumnName() + ": ";
                    retval += col.getExpression().toString() + "\n";
                }
            }
        }
        else
        {
            retval += "\tALL\n";
        }

        if (where != null) {
            retval += "\nWHERE:\n";
            retval += "\t" + where.toString() + "\n";

            retval += "WHERE SELECTION LIST:\n";
            int i = 0;
            for (AbstractExpression expr : whereSelectionList)
                retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

            retval += "NO TABLE SELECTION LIST:\n";
            i = 0;
            for (AbstractExpression expr : noTableSelectionList)
                retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

            retval += "TABLE FILTER LIST:\n";
            for (Entry<Table, ArrayList<AbstractExpression>> pair : tableFilterList.entrySet()) {
                i = 0;
                retval += "\tTABLE: " + pair.getKey().getTypeName() + "\n";
                for (AbstractExpression expr : pair.getValue())
                    retval += "\t\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";
            }

            retval += "JOIN CLAUSE LIST:\n";
            for (Entry<TablePair, ArrayList<AbstractExpression>> pair : joinSelectionList.entrySet()) {
                i = 0;
                retval += "\tTABLES: " + pair.getKey().t1.getTypeName() + " and " + pair.getKey().t2.getTypeName() + "\n";
                for (AbstractExpression expr : pair.getValue())
                    retval += "\t\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";
            }
        }
        return retval;
    }

    public int paramIndexById(long paramId) {
        if (paramId == -1) {
            return -1;
        }
        ParameterInfo param = paramsById.get(paramId);
        assert(param != null);
        return param.index;
    }

    private void addExprToEquivalenceSets(AbstractExpression expr) {
        // Ignore expressions that are not of COMPARE_EQUAL type
        if (expr.getExpressionType() != ExpressionType.COMPARE_EQUAL) {
            return;
        }

        AbstractExpression leftExpr = expr.getLeft();
        AbstractExpression rightExpr = expr.getRight();
        // Can't use an expression based on a column value that is not just a simple column value.
        if ( ( ! (leftExpr instanceof TupleValueExpression)) && leftExpr.hasAnySubexpressionOfClass(TupleValueExpression.class) ) {
            return;
        }
        if ( ( ! (rightExpr instanceof TupleValueExpression)) && rightExpr.hasAnySubexpressionOfClass(TupleValueExpression.class) ) {
            return;
        }

        // Any two asserted-equal expressions need to map to the same equivalence set,
        // which must contain them and must be the only such set that contains them.
        Set<AbstractExpression> eqSet1 = null;
        if (valueEquivalence.containsKey(leftExpr)) {
            eqSet1 = valueEquivalence.get(leftExpr);
        }
        if (valueEquivalence.containsKey(rightExpr)) {
            Set<AbstractExpression> eqSet2 = valueEquivalence.get(rightExpr);
            if (eqSet1 == null) {
                // Add new leftExpr into existing rightExpr's eqSet.
                valueEquivalence.put(leftExpr, eqSet2);
                eqSet2.add(leftExpr);
            } else {
                // Merge eqSets, re-mapping all the rightExpr's equivalents into leftExpr's eqset.
                for (AbstractExpression eqMember : eqSet2) {
                    eqSet1.add(eqMember);
                    valueEquivalence.put(eqMember, eqSet1);
                }
            }
        } else {
            if (eqSet1 == null) {
                // Both leftExpr and rightExpr are new -- add leftExpr to the new eqSet first.
                eqSet1 = new HashSet<AbstractExpression>();
                valueEquivalence.put(leftExpr, eqSet1);
                eqSet1.add(leftExpr);
            }
            // Add new rightExpr into leftExpr's eqSet.
            valueEquivalence.put(rightExpr, eqSet1);
            eqSet1.add(rightExpr);
        }
    }

}
