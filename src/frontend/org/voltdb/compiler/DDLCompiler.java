/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.compiler;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import javax.xml.parsers.*;
import org.hsqldb.HSQLInterface;
import org.hsqldb.HSQLInterface.HSQLParseException;
import org.voltdb.VoltType;
import org.voltdb.catalog.*;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.ParsedSelectStmt;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.StringInputStream;
import org.w3c.dom.*;
import org.xml.sax.*;

/**
 * Compiles schema (SQL DDL) text files and stores the results in a given catalog.
 *
 */
public class DDLCompiler {

    static final int MAX_COLUMNS = 1024;

    HSQLInterface m_hsql;
    VoltCompiler m_compiler;
    String m_fullDDL = "";

    HashMap<String, Column> columnMap = new HashMap<String, Column>();
    HashMap<String, Index> indexMap = new HashMap<String, Index>();
    HashMap<Table, String> matViewMap = new HashMap<Table, String>();

    private class DDLStatement {
        String statement;
        int lineNo;
    }

    public DDLCompiler(VoltCompiler compiler, HSQLInterface hsql) {
        assert(hsql != null);
        this.m_hsql = hsql;
        this.m_compiler = compiler;
    }

    /**
     * Compile a DDL schema from a file on disk
     * @param catalog
     * @param db
     * @param path
     * @throws VoltCompiler.VoltCompilerException
     */
    public void loadSchema(String path)
    throws VoltCompiler.VoltCompilerException {
        File inputFile = new File(path);
        FileReader fr = null;
        LineNumberReader reader = null;
        try {
            fr = new FileReader(inputFile);
            reader = new LineNumberReader(fr);
        } catch (FileNotFoundException e) {
            throw m_compiler.new VoltCompilerException("Unable to open schema file for reading");
        }

        this.loadSchema(path, reader);
    }

    /**
     * Compile a file from an open input stream
     * @param catalog
     * @param db
     * @param path
     * @param reader
     * @throws VoltCompiler.VoltCompilerException
     */
    public void loadSchema(String path, LineNumberReader reader)
    throws VoltCompiler.VoltCompilerException {
        DDLStatement stmt = getNextStatement(reader, m_compiler);
        while (stmt != null) {
            try {
                m_fullDDL += stmt.statement + " ";
                m_hsql.runDDLCommand(stmt.statement);
                stmt = getNextStatement(reader, m_compiler);
            } catch (HSQLParseException e) {
                String msg = "DDL Error: " + e.getMessage();
                throw m_compiler.new VoltCompilerException(msg, stmt.lineNo);
            }
        }

        try {
            reader.close();
        } catch (IOException e) {
            throw m_compiler.new VoltCompilerException("Error closing schema file");
        }
    }

    public void compileToCatalog(Catalog catalog, Database db) throws VoltCompilerException {
        String hexDDL = Encoder.hexEncode(m_fullDDL);
        catalog.execute("set " + db.getPath() + " schema \"" + hexDDL + "\"");

        String xmlCatalog;
        try
        {
            xmlCatalog = m_hsql.getXMLFromCatalog();
        }
        catch (HSQLParseException e)
        {
            String msg = "DDL Error: " + e.getMessage();
            throw m_compiler.new VoltCompilerException(msg);
        }

        // output the xml catalog to disk
        PrintStream ddlXmlOutput = BuildDirectoryUtils.getDebugOutputPrintStream(
                "schema-xml", "hsql-catalog-output.xml");
        ddlXmlOutput.println(xmlCatalog);
        ddlXmlOutput.close();

        // build the local catalog from the xml catalog
        fillCatalogFromXML(catalog, db, xmlCatalog);
    }

    DDLStatement getNextStatement(LineNumberReader reader, VoltCompiler compiler)
    throws VoltCompiler.VoltCompilerException {
        DDLStatement retval = new DDLStatement();

        try {
            String stmt = "";

            // skip over any empty lines to read first real line
            while (stmt.equals("") || stmt.startsWith("--")) {
                stmt = reader.readLine();
                if (stmt == null) return null;
                stmt = stmt.trim();
            }
            // record the line number
            retval.lineNo = reader.getLineNumber();

            // add all lines until one ends with a semicolon
            while((stmt.endsWith(";") == false) && (stmt.endsWith(";\n") == false)) {
                String newline = reader.readLine();
                if (newline == null) {
                    String msg = "Schema file ended mid statment (no semicolon found)";
                    throw compiler.new VoltCompilerException(msg, retval.lineNo);
                }
                newline = newline.trim();
                if (newline.equals(""))
                    continue;
                if (newline.startsWith("--"))
                    continue;
                stmt += newline + "\n";
            }

            retval.statement = stmt;

        } catch (IOException e) {
            throw compiler.new VoltCompilerException("Unable to read from file");
        }

        return retval;
    }

    public void fillCatalogFromXML(Catalog catalog, Database db, String xml)
    throws VoltCompiler.VoltCompilerException {
        StringInputStream xmlStream = new StringInputStream(xml);

        Document doc = null;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(true);

        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            doc = builder.parse(xmlStream);
        } catch (SAXParseException sxe) {
            m_compiler.addErr(sxe.getMessage(), sxe.getLineNumber());
        } catch (SAXException sxe) {
            m_compiler.addErr(sxe.getMessage());
        } catch (ParserConfigurationException e) {
            m_compiler.addErr(e.getMessage());
        } catch (IOException e) {
            m_compiler.addErr(e.getMessage());
        }

        if ((doc == null) || m_compiler.hasErrors())
            throw m_compiler.new VoltCompilerException("Unable to parse catalog xml file from hsqldb");

        Node root = doc.getDocumentElement();
        assert root.getNodeName().equals("databaseschema");

        NodeList tableNodes = root.getChildNodes();
        for (int i = 0; i < tableNodes.getLength(); i++) {
            Node node = tableNodes.item(i);
            if (node.getNodeName().equals("table"))
                addTableToCatalog(catalog, db, node);
        }

        processMaterializedViews(db);
    }

    void addTableToCatalog(Catalog catalog, Database db, Node node) throws VoltCompilerException {
        assert node.getNodeName().equals("table");

        // clear these maps, as they're table specific
        columnMap.clear();
        indexMap.clear();

        NamedNodeMap attrs = node.getAttributes();
        String name = attrs.getNamedItem("name").getNodeValue();

        Table table = db.getTables().add(name);

        // handle the case where this is a materialized view
        Node queryAttr = attrs.getNamedItem("query");
        if (queryAttr != null) {
            String query = queryAttr.getNodeValue();
            assert(query.length() > 0);
            matViewMap.put(table, query);
        }

        // all tables start replicated
        // if a partition is found in the project file later,
        //  then this is reversed
        table.setIsreplicated(true);

        NodeList childNodes = node.getChildNodes();

        for (int i = 0; i < childNodes.getLength(); i++) {
            Node subNode = childNodes.item(i);

            if (subNode.getNodeName().equals("columns")) {
                NodeList columnNodes = subNode.getChildNodes();
                int colIndex = 0;
                for (int j = 0; j < columnNodes.getLength(); j++) {
                    Node columnNode = columnNodes.item(j);
                    if (columnNode.getNodeName().equals("column"))
                        addColumnToCatalog(table, columnNode, colIndex++);
                }
                // limit the total number of columns in a table
                if (colIndex > MAX_COLUMNS) {
                    String msg = "Table " + name + " has " +
                        colIndex + " columns (max is " + MAX_COLUMNS + ")";
                    throw m_compiler.new VoltCompilerException(msg);
                }
            }

            if (subNode.getNodeName().equals("indexes")) {
                NodeList indexNodes = subNode.getChildNodes();
                for (int j = 0; j < indexNodes.getLength(); j++) {
                    Node indexNode = indexNodes.item(j);
                    if (indexNode.getNodeName().equals("index"))
                        addIndexToCatalog(table, indexNode);
                }
            }

            if (subNode.getNodeName().equals("constraints")) {
                NodeList constraintNodes = subNode.getChildNodes();
                for (int j = 0; j < constraintNodes.getLength(); j++) {
                    Node constraintNode = constraintNodes.item(j);
                    if (constraintNode.getNodeName().equals("constraint"))
                        addConstraintToCatalog(table, constraintNode);
                }
            }
        }

    }

    void addColumnToCatalog(Table table, Node node, int index) throws VoltCompilerException {
        assert node.getNodeName().equals("column");

        NamedNodeMap attrs = node.getAttributes();

        String name = attrs.getNamedItem("name").getNodeValue();
        String typename = attrs.getNamedItem("type").getNodeValue();
        String nullable = attrs.getNamedItem("nullable").getNodeValue();
        String sizeString = attrs.getNamedItem("size").getNodeValue();
        String defaultvalue = null;
        String defaulttype = null;

        // throws an exception if string isn't an int (i think)
        Integer.parseInt(sizeString);

        // Default Value
        NodeList children = node.getChildNodes();
        for (int i = 0, cnt = children.getLength(); i < cnt; i++) {
            Node child = children.item(i);
            if (child.getNodeName().equals("default")) {
                NodeList inner_children = child.getChildNodes();
                for (int j = 0; j < inner_children.getLength(); j++) {
                    Node inner_child = inner_children.item(j);
                    attrs = inner_child.getAttributes();

                    // Value
                    if (inner_child.getNodeName().equals("value")) {
                        defaultvalue = attrs.getNamedItem("value").getNodeValue();
                        defaulttype = attrs.getNamedItem("type").getNodeValue();
                    }
                    // Function
                    /*else if (inner_child.getNodeName().equals("function")) {
                        defaultvalue = attrs.getNamedItem("name").getNodeValue();
                        defaulttype = VoltType.VOLTFUNCTION.name();
                    }*/
                    if (defaultvalue != null) break;
                }
            }
        }
        if (defaultvalue != null && defaultvalue.equals("NULL"))
            defaultvalue = null;
        if (defaulttype != null)
            defaulttype = Integer.toString(VoltType.typeFromString(defaulttype).getValue());

        Column column = table.getColumns().add(name);
        // need to set other column data here (default, nullable, etc)
        column.setName(name);
        column.setIndex(index);
        VoltType type = VoltType.typeFromString(typename);
        column.setType(type.getValue());
        column.setNullable(nullable.toLowerCase().startsWith("t") ? true : false);
        column.setSize(Integer.parseInt(sizeString));

        column.setDefaultvalue(defaultvalue);
        if (defaulttype != null)
            column.setDefaulttype(Integer.parseInt(defaulttype));

        columnMap.put(name, column);
    }

    void addIndexToCatalog(Table table, Node node) throws VoltCompilerException {
        assert node.getNodeName().equals("index");

        NamedNodeMap attrs = node.getAttributes();

        String name = attrs.getNamedItem("name").getNodeValue();
        // this won't work for multi-column indices
        String colList = attrs.getNamedItem("columns").getNodeValue();
        String[] colNames = colList.split(",");
        Column[] columns = new Column[colNames.length];

        for (int i = 0; i < colNames.length; i++) {
            columns[i] = columnMap.get(colNames[i]);
            if (columns[i] == null) {
                //String msg = "Index " + name + " references column " + colNames[i] +
                //  " which doesn't exist";
                //throw compiler.new VoltCompilerException(msg);
                return;
            }
        }

        Index index = table.getIndexes().add(name);
        // all indexes default to hash tables
        // if they are used in a non-equality lookup, the planner
        //  will change this to a binary tree

        // set the type of the index based on it's name (giant hack)
        String indexNameNoCase = name.toLowerCase();
        if (indexNameNoCase.contains("tree"))
            index.setType(IndexType.BALANCED_TREE.getValue());
        else if (indexNameNoCase.contains("array"))
            index.setType(IndexType.ARRAY.getValue());
        else
            index.setType(IndexType.HASH_TABLE.getValue());

        // need to set other index data here (column, etc)
        for (int i = 0; i < columns.length; i++) {
            ColumnRef cref = index.getColumns().add(columns[i].getTypeName());
            cref.setColumn(columns[i]);
            cref.setIndex(i);
        }

        indexMap.put(name, index);
    }

    /**
     * Add a constraint on a given table to the catalog
     * @param table
     * @param node
     * @throws VoltCompilerException
     */
    void addConstraintToCatalog(Table table, Node node) throws VoltCompilerException {
        assert node.getNodeName().equals("constraint");

        NamedNodeMap attrs = node.getAttributes();

        String name = attrs.getNamedItem("name").getNodeValue();
        String typeName = attrs.getNamedItem("type").getNodeValue();
        ConstraintType type = ConstraintType.valueOf(typeName);
        if (type == null) {
            throw this.m_compiler.new VoltCompilerException("Invalid constraint type '" + typeName + "'");
        }

        // The constraint is backed by an index, therefore we need to create it
        // TODO: We need to be able to use indexes for foreign keys. I am purposely
        //       leaving those out right now because HSQLDB just makes too many of them.
        Constraint catalog_const = null;
        if (attrs.getNamedItem("index") != null) {
            String indexName = attrs.getNamedItem("index") .getNodeValue();
            Index catalog_index = indexMap.get(indexName);

            // if the constraint name contains index type hints, exercise them (giant hack)
            if (catalog_index != null) {
                String constraintNameNoCase = name.toLowerCase();
                if (constraintNameNoCase.contains("tree"))
                    catalog_index.setType(IndexType.BALANCED_TREE.getValue());
                if (constraintNameNoCase.contains("array"))
                    catalog_index.setType(IndexType.ARRAY.getValue());
            }

            catalog_const = table.getConstraints().add(name);
            if (catalog_index != null) {
                catalog_const.setIndex(catalog_index);
                catalog_index.setUnique(type == ConstraintType.UNIQUE || type == ConstraintType.PRIMARY_KEY);
            }
        } else {
            catalog_const = table.getConstraints().add(name);
        }
        catalog_const.setType(type.getValue());

        // Foreign Keys
        if (type == ConstraintType.FOREIGN_KEY) {
            String fkey_table_name = attrs.getNamedItem("foreignkeytable").getNodeValue();
            Table catalog_fkey_tbl = ((Database)table.getParent()).getTables().getIgnoreCase(fkey_table_name);
            if (catalog_fkey_tbl == null) {
                throw this.m_compiler.new VoltCompilerException("Invalid foreign key table '" + fkey_table_name + "'");
            }
            catalog_const.setForeignkeytable(catalog_fkey_tbl);

            // Column mappings
            NodeList children = node.getChildNodes();
            for (int i = 0, cnt = children.getLength(); i < cnt; i++) {
                Node child = children.item(i);
                if (child.getNodeName().equals("reference")) {
                    attrs = child.getAttributes();
                    String from_colname = attrs.getNamedItem("from").getNodeValue();
                    Column from_col = table.getColumns().get(from_colname);

                    String to_colname = attrs.getNamedItem("to").getNodeValue();
                    Column to_col = catalog_fkey_tbl.getColumns().get(to_colname);

                    // Make a reference in the fromcolumn to their column in the constraint
                    // We store the name of from_olumn as the name of the reference in the catalog
                    ColumnRef cref = catalog_const.getForeignkeycols().add(from_col.getTypeName());
                    cref.setColumn(to_col);

                    // Add a ConstraintRef for the from_column
                    ConstraintRef const_ref = from_col.getConstraints().add(catalog_const.getTypeName());
                    const_ref.setConstraint(catalog_const);
                }
            }
        // All other constraints
        } else {
            // Nothing for now...
        }
        return;
    }

    /**
     * Add materialized view info to the catalog for the tables that are
     * materialized views.
     */
    void processMaterializedViews(Database db) throws VoltCompiler.VoltCompilerException {
        for (Entry<Table, String> entry : matViewMap.entrySet()) {
            Table destTable = entry.getKey();
            String query = entry.getValue();

            // get the xml for the query
            String xmlquery = null;
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
                stmt = (ParsedSelectStmt) AbstractParsedStmt.parse(query, xmlquery, db);
            }
            catch (Exception e) {
                throw m_compiler.new VoltCompilerException(e.getMessage());
            }
            assert(stmt != null);

            // throw an error if the view isn't withing voltdb's limited worldview
            checkViewMeetsSpec(destTable.getTypeName(), stmt);

            // create the materializedviewinfo catalog node for the source table
            Table srcTable = stmt.tableList.get(0);
            MaterializedViewInfo matviewinfo = srcTable.getViews().add(destTable.getTypeName());
            matviewinfo.setDest(destTable);
            if (stmt.where == null)
                matviewinfo.setPredicate("");
            else {
                String hex = Encoder.hexEncode(stmt.where.toJSONString());
                matviewinfo.setPredicate(hex);
            }
            destTable.setMaterializer(srcTable);

            List<Column> srcColumnArray = CatalogUtil.getSortedCatalogItems(srcTable.getColumns(), "index");
            List<Column> destColumnArray = CatalogUtil.getSortedCatalogItems(destTable.getColumns(), "index");

            // add the group by columns from the src table
            for (int i = 0; i < stmt.groupByColumns.size(); i++) {
                ParsedSelectStmt.ParsedColInfo gbcol = stmt.groupByColumns.get(i);
                Column srcCol = srcColumnArray.get(gbcol.index);

                ColumnRef cref = matviewinfo.getGroupbycols().add(srcCol.getTypeName());
                cref.setColumn(srcCol);
            }

            ParsedSelectStmt.ParsedColInfo countCol = stmt.displayColumns.get(stmt.groupByColumns.size());
            assert(countCol.expression.getExpressionType() == ExpressionType.AGGREGATE_COUNT);
            assert(countCol.expression.getLeft() == null);
            processMaterializedViewColumn(matviewinfo, srcTable, destTable, destColumnArray.get(stmt.groupByColumns.size()),
                    ExpressionType.AGGREGATE_COUNT, null);

            // create an index and constraint for the table
            Index pkIndex = destTable.getIndexes().add("MATVIEW_PK_INDEX");
            pkIndex.setType(IndexType.BALANCED_TREE.getValue());
            pkIndex.setUnique(true);
            // add the group by columns from the src table
            // assume index 1 throuh #grpByCols + 1 are the cols
            for (int i = 0; i < stmt.groupByColumns.size(); i++) {
                ColumnRef c = pkIndex.getColumns().add(String.valueOf(i));
                c.setColumn(destColumnArray.get(i));
                c.setIndex(i);
            }
            Constraint pkConstraint = destTable.getConstraints().add("MATVIEW_PK_CONSTRAINT");
            pkConstraint.setType(ConstraintType.PRIMARY_KEY.getValue());
            pkConstraint.setIndex(pkIndex);

            // parse out the group by columns into the dest table
            for (int i = 0; i < stmt.groupByColumns.size(); i++) {
                ParsedSelectStmt.ParsedColInfo col = stmt.displayColumns.get(i);
                Column destColumn = destColumnArray.get(i);

                processMaterializedViewColumn(matviewinfo, srcTable, destTable, destColumn,
                        ExpressionType.VALUE_TUPLE, (TupleValueExpression)col.expression);
            }

            // parse out the aggregation columns into the dest table
            for (int i = stmt.groupByColumns.size() + 1; i < stmt.displayColumns.size(); i++) {
                ParsedSelectStmt.ParsedColInfo col = stmt.displayColumns.get(i);
                Column destColumn = destColumnArray.get(i);

                AbstractExpression colExpr = col.expression.getLeft();
                assert(colExpr.getExpressionType() == ExpressionType.VALUE_TUPLE);
                processMaterializedViewColumn(matviewinfo, srcTable, destTable, destColumn,
                        col.expression.getExpressionType(), (TupleValueExpression)colExpr);
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
        int groupColCount = stmt.groupByColumns.size();
        int displayColCount = stmt.displayColumns.size();
        String msg = "Materialized view \"" + viewName + "\" ";

        if (stmt.tableList.size() != 1) {
            msg += "has " + String.valueOf(stmt.tableList.size()) + " when only 1 is allowed.";
            throw m_compiler.new VoltCompilerException(msg);
        }

        if (displayColCount <= groupColCount) {
            msg += "has too few columns.";
            throw m_compiler.new VoltCompilerException(msg);
        }

        int i;
        for (i = 0; i < groupColCount; i++) {
            ParsedSelectStmt.ParsedColInfo gbcol = stmt.groupByColumns.get(i);
            ParsedSelectStmt.ParsedColInfo outcol = stmt.displayColumns.get(i);

            if (outcol.expression.getExpressionType() != ExpressionType.VALUE_TUPLE) {
                msg += "must have column at index " + String.valueOf(i) + " be " + gbcol.alias;
                throw m_compiler.new VoltCompilerException(msg);
            }

            TupleValueExpression expr = (TupleValueExpression) outcol.expression;
            if (expr.getColumnIndex() != gbcol.index) {
                msg += "must have column at index " + String.valueOf(i) + " be " + gbcol.alias;
                throw m_compiler.new VoltCompilerException(msg);
            }
        }

        AbstractExpression coli = stmt.displayColumns.get(i).expression;
        if ((coli.getExpressionType() != ExpressionType.AGGREGATE_COUNT) ||
                (coli.getLeft() != null) ||
                (coli.getRight() != null)) {
            msg += "is missing count(*) as the column after the group by columns, a materialized view requirement.";
            throw m_compiler.new VoltCompilerException(msg);
        }

        for (i++; i < displayColCount; i++) {
            ParsedSelectStmt.ParsedColInfo outcol = stmt.displayColumns.get(i);
            if ((outcol.expression.getExpressionType() != ExpressionType.AGGREGATE_COUNT) &&
                    (outcol.expression.getExpressionType() != ExpressionType.AGGREGATE_SUM)) {
                msg += "must have non-group by columns aggregated by sum or count.";
                throw m_compiler.new VoltCompilerException(msg);
            }
            if (outcol.expression.getLeft().getExpressionType() != ExpressionType.VALUE_TUPLE) {
                msg += "must have non-group by columns use only one level of aggregation.";
                throw m_compiler.new VoltCompilerException(msg);
            }
        }
    }

    void processMaterializedViewColumn(MaterializedViewInfo info, Table srcTable, Table destTable,
            Column destColumn, ExpressionType type, TupleValueExpression colExpr)
            throws VoltCompiler.VoltCompilerException {

        if (colExpr != null) {
            assert(colExpr.getTableName().equalsIgnoreCase(srcTable.getTypeName()));
            String srcColName = colExpr.getColumnName();
            Column srcColumn = srcTable.getColumns().getIgnoreCase(srcColName);
            destColumn.setMatviewsource(srcColumn);
        }
        destColumn.setMatview(info);
        destColumn.setAggregatetype(type.getValue());
    }
}
