/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.InputMismatchException;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

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
import org.voltdb.utils.StringInputStream;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public abstract class AbstractParsedStmt {

    static class HSQLXMLErrorHandler implements ErrorHandler {
        @Override
        public void error(SAXParseException exception) throws SAXException {
            throw exception;
        }

        @Override
        public void fatalError(SAXParseException exception) throws SAXException {
            throw exception;
        }

        @Override
        public void warning(SAXParseException exception) throws SAXException {
            throw exception;
        }
    }

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
    public static AbstractParsedStmt parse(String sql, String xmlSQL, Database db, String joinOrder) {
        final String INSERT_NODE_NAME = "insert";
        final String UPDATE_NODE_NAME = "update";
        final String DELETE_NODE_NAME = "delete";
        final String SELECT_NODE_NAME = "select";

        AbstractParsedStmt retval = null;
        StringInputStream input = new StringInputStream(xmlSQL);
        HSQLXMLErrorHandler errHandler = new HSQLXMLErrorHandler();
        Document doc = null;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(errHandler);
            doc = builder.parse(input);
        } catch (SAXParseException sxe) {
            System.err.println(sxe.getMessage() + ": " + String.valueOf(sxe.getLineNumber()));
            throw new InputMismatchException("XML Parsing failure during planning");
        } catch (SAXException sxe) {
            System.err.println(sxe.getMessage());
            throw new InputMismatchException("XML Parsing failure during planning");
        } catch (ParserConfigurationException e) {
            System.err.println(e.getMessage());
            throw new InputMismatchException("XML Parsing failure during planning");
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw new InputMismatchException("XML Parsing failure during planning");
        }

        if (doc == null) {
            System.err.println("Unexpected error parsing hsql parsed stmt xml");
            System.exit(-1);
        }

        Node docElement = doc.getDocumentElement();
        assert(docElement.getNodeName().equalsIgnoreCase("statement"));

        Node stmtTypeElement = docElement.getFirstChild();
        while (stmtTypeElement.getNodeType() != Node.ELEMENT_NODE)
            stmtTypeElement = stmtTypeElement.getNextSibling();

        // create non-abstract instances
        if (stmtTypeElement.getNodeName().equalsIgnoreCase(INSERT_NODE_NAME)) {
            retval = new ParsedInsertStmt();
        }
        else if (stmtTypeElement.getNodeName().equalsIgnoreCase(UPDATE_NODE_NAME)) {
            retval = new ParsedUpdateStmt();
        }
        else if (stmtTypeElement.getNodeName().equalsIgnoreCase(DELETE_NODE_NAME)) {
            retval = new ParsedDeleteStmt();
        }
        else if (stmtTypeElement.getNodeName().equalsIgnoreCase(SELECT_NODE_NAME)) {
            retval = new ParsedSelectStmt();
        }
        else {
            throw new RuntimeException("Unexpected Element: " + stmtTypeElement.getNodeName());
        }

        // parse tables and parameters
        NodeList children = stmtTypeElement.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeName().equalsIgnoreCase("parameters")) {
                retval.parseParameters(node, db);
            }
            if (node.getNodeName().equalsIgnoreCase("tablescans")) {
                retval.parseTables(node, db);
            }
            if (node.getNodeName().equalsIgnoreCase("scan_columns"))
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
    abstract void parse(Node stmtElement, Database db);

    /**
     * Convert a HSQL VoltXML expression to an AbstractExpression tree.
     * @param root
     * @param db
     * @return configured AbstractExpression
     */
    AbstractExpression parseExpressionTree(Node root, Database db) {
        String elementName = root.getNodeName().toLowerCase();
        NamedNodeMap attrs = root.getAttributes();
        AbstractExpression retval = null;

        if (elementName.equals("value")) {
            retval = parseValueExpression(root, attrs);
        }
        if (elementName.equals("columnref")) {
            retval = parseColumnRefExpression(root, attrs, db);
        }
        if (elementName.equals("bool")) {
            retval = parseBooleanExpresion(root, attrs);
        }
        if (elementName.equals("operation")) {
            retval = parseOperationExpression(root, attrs, db);
        }
        if (elementName.equals("asterisk")) {
            return null;
        }

        return retval;
    }

    /**
     *
     * @param exprNode
     * @param attrs
     * @return
     */
    AbstractExpression parseValueExpression(Node exprNode, NamedNodeMap attrs) {
        String type = attrs.getNamedItem("type").getNodeValue();
        Node isParam = attrs.getNamedItem("isparam");

        VoltType vt = VoltType.typeFromString(type);
        int size = VoltType.MAX_VALUE_LENGTH;
        assert(vt != VoltType.VOLTTABLE);

        if ((vt != VoltType.STRING) && (vt != VoltType.VARBINARY)) {
            if (vt == VoltType.NULL) size = 0;
            else size = vt.getLengthInBytesForFixedTypes();
        }
        if ((isParam != null) && (isParam.getNodeValue().equalsIgnoreCase("true"))) {
            ParameterValueExpression expr = new ParameterValueExpression();
            long id = Long.parseLong(attrs.getNamedItem("id").getNodeValue());
            ParameterInfo param = paramsById.get(id);

            expr.setValueType(vt);
            expr.setValueSize(size);
            expr.setParameterId(param.index);

            return expr;
        }
        else {
            ConstantValueExpression expr = new ConstantValueExpression();
            expr.setValueType(vt);
            expr.setValueSize(size);
            if (vt == VoltType.NULL)
                expr.setValue(null);
            else
                expr.setValue(attrs.getNamedItem("value").getNodeValue());
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
    AbstractExpression parseColumnRefExpression(Node exprNode, NamedNodeMap attrs, Database db) {
        TupleValueExpression expr = new TupleValueExpression();

        String alias = attrs.getNamedItem("alias").getNodeValue();
        String tableName = attrs.getNamedItem("table").getNodeValue();
        String columnName = attrs.getNamedItem("column").getNodeValue();

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
    AbstractExpression parseBooleanExpresion(Node exprNode, NamedNodeMap attrs) {
        ConstantValueExpression expr = new ConstantValueExpression();

        expr.setValueType(VoltType.BIGINT);
        expr.setValueSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
        if (attrs.getNamedItem("attrs").getNodeValue().equalsIgnoreCase("true"))
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
    AbstractExpression parseOperationExpression(Node exprNode, NamedNodeMap attrs, Database db) {
        String type = attrs.getNamedItem("type").getNodeValue();
        ExpressionType exprType = ExpressionType.get(type);
        AbstractExpression expr = null;

        if (exprType == ExpressionType.INVALID) {
            throw new PlanningErrorException("Unsupported operation type '" + type + "'");
        }
        try {
            expr = exprType.getExpressionClass().newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
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
            Node node;
            if ((node = attrs.getNamedItem("distinct")) != null) {
                AggregateExpression ae = (AggregateExpression)expr;
                ae.m_distinct = Boolean.parseBoolean(node.getNodeValue());
            }
        }

        // setup for children access
        NodeList children = exprNode.getChildNodes();
        int i = 0;

        // get the first (left) node that is an element
        Node leftExprNode = children.item(i++);
        while ((leftExprNode != null) && (leftExprNode.getNodeType() != Node.ELEMENT_NODE))
            leftExprNode = children.item(i++);
        assert(leftExprNode != null);

        // get the second (right) node that is an element (might be null)
        Node rightExprNode = children.item(i++);
        while ((rightExprNode != null) && (rightExprNode.getNodeType() != Node.ELEMENT_NODE))
            rightExprNode = children.item(i++);

        // recursively parse the left subtree (could be another operator or
        // a constant/tuple/param value operand).
        AbstractExpression leftExpr = parseExpressionTree(leftExprNode, db);
        assert((leftExpr != null) || (exprType == ExpressionType.AGGREGATE_COUNT));
        expr.setLeft(leftExpr);

        if (ExpressionUtil.needsRightExpression(expr)) {
            assert(rightExprNode != null);

            // recursively parse the right subtree
            AbstractExpression rightExpr = parseExpressionTree(rightExprNode, db);
            assert(rightExpr != null);
            expr.setRight(rightExpr);
        }

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
    void parseScanColumns(Node columnsNode, Database db)
    {
        scanColumns = new HashMap<String, ArrayList<SchemaColumn>>();
        for (Node child = columnsNode.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child.getNodeType() != Node.ELEMENT_NODE)
                continue;
            final String nodeName = child.getNodeName();
            assert(nodeName.equals("columnref"));
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
    private void parseTables(Node tablesNode, Database db) {
        NodeList children = tablesNode.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeName().equalsIgnoreCase("tablescan")) {
                String tableName = node.getAttributes().getNamedItem("table").getNodeValue();
                Table table = db.getTables().getIgnoreCase(tableName);
                assert(table != null);
                tableList.add(table);
            }
        }
    }

    private void parseParameters(Node paramsNode, Database db) {
        NodeList children = paramsNode.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeName().equalsIgnoreCase("parameter")) {
                ParameterInfo param = new ParameterInfo();

                NamedNodeMap attrs = node.getAttributes();
                long id = Long.parseLong(attrs.getNamedItem("id").getNodeValue());
                param.index = Integer.parseInt(attrs.getNamedItem("index").getNodeValue());
                String typeName = attrs.getNamedItem("type").getNodeValue();
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

        int loopOps;
        do {
            loopOps = 0;

            AbstractExpression inExpr = null;
            while ((inExpr = in.poll()) != null) {
                if (inExpr.getExpressionType() == ExpressionType.CONJUNCTION_AND) {
                    out.add(inExpr.getLeft());
                    out.add(inExpr.getRight());
                    loopOps++;
                }
                else {
                    out.add(inExpr);
                }
            }

            // swap the input/output
            ArrayDeque<AbstractExpression> temp = in;
            in = out;
            out = temp;
        // continue until a loop occurs that finds no ands
        } while (loopOps > 0);

        // the where selection list contains all the clauses
        whereSelectionList.addAll(in);

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
        if (expr.getLeft() != null)
            getTablesForExpression(db, expr.getLeft(), tables);
        if (expr.getRight() != null)
            getTablesForExpression(db, expr.getRight(), tables);
        if (expr.getExpressionType() == ExpressionType.VALUE_TUPLE) {
            TupleValueExpression tupleExpr = (TupleValueExpression)expr;
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

}
