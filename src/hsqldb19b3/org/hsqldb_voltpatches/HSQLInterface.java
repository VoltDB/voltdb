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

package org.hsqldb_voltpatches;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import org.hsqldb_voltpatches.VoltXMLElement.VoltXMLDiff;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.persist.HsqlProperties;
import org.hsqldb_voltpatches.result.Result;

/**
 * This class is built to create a single in-memory database
 * which can then be easily manipulated by VoltDB code.
 * <p>
 * Primary interaction with HSQLDB in the following ways:
 * <ul>
 * <li>Initialize an In-Memory SQL Store</li>
 * <li>Execute DDL Statements</li>
 * <li>Dump Serialized Catalog as XML</li>
 * <li>Compile SQL DML Statements to XML</li>
 * </ul>
 */
public class HSQLInterface {

    static public String XML_SCHEMA_NAME = "databaseschema";
    /**
     * Naming conventions for unnamed indexes and constraints
     */
    static public String AUTO_GEN_MATVIEW = "MATVIEW_PK_";
    static public String AUTO_GEN_MATVIEW_IDX = AUTO_GEN_MATVIEW + "INDEX";
    static public String AUTO_GEN_MATVIEW_CONST = AUTO_GEN_MATVIEW + "CONSTRAINT";
    static public String AUTO_GEN_PREFIX = "VOLTDB_AUTOGEN_";
    static public String AUTO_GEN_IDX_PREFIX = AUTO_GEN_PREFIX + "IDX_";
    static public String AUTO_GEN_CONSTRAINT_PREFIX = AUTO_GEN_IDX_PREFIX + "CT_";
    static public String AUTO_GEN_PRIMARY_KEY_PREFIX = AUTO_GEN_IDX_PREFIX + "PK_";
    static public String AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX = AUTO_GEN_PREFIX + "CONSTRAINT_IDX_";

    /**
     * The spacer to use for nested XML elements
     */
    public static final String XML_INDENT = "    ";

    /**
     * Exception subclass that is thrown from <code>getXMLCompiledStatement</code>
     * and <code>runDDLCommand</code> when a SQL parse error is encountered.
     *
     * @see getXMLCompiledStatement
     * @see runDDLCommand
     */
    public static class HSQLParseException extends Exception {
        private static final long serialVersionUID = -7341323582748684001L;

        private String message = null;
        private Integer lineNo = null;

        HSQLParseException(String msg) {
            message = msg;
        }

        HSQLParseException(String msg, int lineNo) {
            message = msg;
            this.lineNo = lineNo;
        }

        public Integer getLineNumber() {
            return lineNo;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

    Session sessionProxy;
    // Keep track of the previous XML for each table in the schema
    Map<String, VoltXMLElement> lastSchema = new TreeMap<>();
    // empty schema for cloning and for null diffs
    final VoltXMLElement emptySchema = new VoltXMLElement(XML_SCHEMA_NAME);
    static int instanceId = 0;

    private HSQLInterface(Session sessionProxy) {
        emptySchema.attributes.put("name", XML_SCHEMA_NAME);
        this.sessionProxy = sessionProxy;
    }

    @Override
    public void finalize() {
        final Database db = sessionProxy.getDatabase();
        sessionProxy.close();
        db.close(Database.CLOSEMODE_IMMEDIATELY);
        sessionProxy = null;
    }

    /**
     * Load up an HSQLDB in-memory instance.
     *
     * @return A newly initialized in-memory HSQLDB instance accessible
     * through the returned instance of HSQLInterface
     */
    public static HSQLInterface loadHsqldb() {
        Session sessionProxy = null;
        String name = "hsqldbinstance-" + String.valueOf(instanceId) + "-" + String.valueOf(System.currentTimeMillis());
        instanceId++;

        HsqlProperties props = new HsqlProperties();
        try {
            sessionProxy = DatabaseManager.newSession(DatabaseURL.S_MEM, name, "SA", "", props, 0);
        } catch (HsqlException e) {
            e.printStackTrace();
        }

        // Specifically set the timezone to UTC to avoid the default usage local timezone in HSQL.
        // This ensures that all VoltDB data paths use the same timezone for representing time.
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+0"));

        // make HSQL case insensitive
        sessionProxy.executeDirectStatement("SET IGNORECASE TRUE;");

        return new HSQLInterface(sessionProxy);
    }

    /**
     * Modify the current schema with a SQL DDL command and get the
     * diff which represents the changes.
     *
     * Note that you have to be consistent WRT case for the expected names.
     *
     * @param expectedTableAffected The name of the table affected by this DDL
     * or null if unknown
     * @param expectedIndexAffected The name of the index affected by this DDL
     * or null if table is known instead
     * @param ddl The SQL DDL statement to be run.
     * @return the "diff" of the before and after trees for the affected table
     * @throws HSQLParseException Throws exception if SQL parse error is
     * encountered.
     */
    public VoltXMLDiff runDDLCommandAndDiff(HSQLDDLInfo stmtInfo,
                                            String ddl)
                                            throws HSQLParseException
    {
        // name of the table we're going to have to diff (if any)
        String expectedTableAffected = null;

        // If we fail to pre-process a statement, then we want to fail, but we're
        // still going to run the statement through HSQL to get its error message.
        // This variable helps us make sure we don't fail to preprocess and then
        // succeed at runnign the statement through HSQL.
        boolean expectFailure = false;

        // If cascade, we're going to need to look for any views that might have
        // gotten deleted. So get a list of all tables and views that existed before
        // we run the ddl, then we'll do a comparison later.
        Set<String> existingTableNames = null;

        if (stmtInfo != null) {
            if (stmtInfo.cascade) {
                existingTableNames = getTableNames();
            }

            // we either have an index name or a table/view name, but not both
            if (stmtInfo.noun == HSQLDDLInfo.Noun.INDEX) {
                if (stmtInfo.verb == HSQLDDLInfo.Verb.CREATE) {
                    expectedTableAffected = stmtInfo.secondName;
                }
                else {
                    expectedTableAffected = tableNameForIndexName(stmtInfo.name);
                }
            }
            else {
                expectedTableAffected = stmtInfo.name;
            }

            // Note that we're assuming ifexists can't happen with "create"
            expectFailure = (expectedTableAffected == null) && !stmtInfo.ifexists;
        }
        else {
            expectFailure = true;
        }

        runDDLCommand(ddl);

        // If we expect to fail, but the statement above didn't bail...
        // (Shouldn't get here ever I think)
        if (expectFailure) {
            throw new HSQLParseException("Unable to plan statement due to VoltDB DDL pre-processing error");
        }
        // sanity checks for non-failure
        assert(stmtInfo != null);

        // get old and new XML representations for the affected table
        VoltXMLElement tableXMLNew = null, tableXMLOld = null;
        if (expectedTableAffected != null) {
            tableXMLNew = getXMLForTable(expectedTableAffected);
            tableXMLOld = lastSchema.get(expectedTableAffected);
        }

        // valid reasons for tableXMLNew to be null are DROP IF EXISTS and not much else
        if (tableXMLNew == null) {
            tableXMLNew = emptySchema;
        }

        // the old table can be null for CREATE TABLE of for IF EXISTS stuff
        if (tableXMLOld == null) {
            tableXMLOld = emptySchema;
        }

        VoltXMLDiff diff = VoltXMLElement.computeDiff(tableXMLOld, tableXMLNew);

        // now find any views that might be missing and make sure the diff reflects that
        // they're gone
        if (stmtInfo.cascade) {
            Set<String> finalTableNames = getTableNames();
            for (String tableName : existingTableNames) {
                if (!finalTableNames.contains(tableName)) {
                    tableName = tableName.toLowerCase();
                    tableXMLOld = lastSchema.get(tableName).children.get(0);
                    lastSchema.remove(tableName);
                    if (tableName.equals(expectedTableAffected)) {
                        continue;
                    }
                    diff.m_removedElements.add(tableXMLOld);
                }
            }
        }

        // this is a hack to allow the diff-apply-er to accept a diff that has no order
        diff.m_elementOrder.clear();

        // remember the current schema
        if (expectedTableAffected != null) {
            lastSchema.put(expectedTableAffected, tableXMLNew.duplicate());
        }
        return diff;
    }

    /**
     * Modify the current schema with a SQL DDL command.
     *
     * @param ddl The SQL DDL statement to be run.
     * @throws HSQLParseException Throws exception if SQL parse error is
     * encountered.
     */
    public void runDDLCommand(String ddl) throws HSQLParseException {
        Result result = sessionProxy.executeDirectStatement(ddl);
        if (result.hasError()) {
            throw new HSQLParseException(result.getMainString());
        }
    }

    /**
     * Load a text file full or DDL and run <code>runDDLCommand</code>
     * on every DDL statment in the file.
     *
     * @param path Path to a text file containing semi-colon
     * delimeted SQL DDL statements.
     * @throws HSQLParseException throws an exeption if there
     * is a problem reading, parsing, or running the file.
     */
    public void runDDLFile(String path) throws HSQLParseException {
        HSQLFileParser.Statement[] stmts;
        stmts = HSQLFileParser.getStatements(path);
        for (HSQLFileParser.Statement stmt : stmts) {
            try {
                runDDLCommand(stmt.statement);
            } catch (HSQLParseException e) {
                e.lineNo = stmt.lineNo;
                throw e;
            }
        }
    }

    /**
     * Compile a SQL statement with parameters into an XML representation.<p>
     * Any question-marks (?) in the statement will be considered parameters.
     *
     * @param sql SQL statement to be compiled against the current schema.
     * @return Pseudo XML representation of the compiled statement.
     * @throws HSQLParseException Throws exception if SQL parse error is
     * encountered.
     */
    public VoltXMLElement getXMLCompiledStatement(String sql) throws HSQLParseException
    {
        Statement cs = null;
        // clear the expression node id set for determinism
        sessionProxy.resetVoltNodeIds();

        try {
            cs = sessionProxy.compileStatement(sql);
        } catch( HsqlException hex) {
            // a switch in case we want to give more error details on additional error codes
            switch( hex.getErrorCode()) {
            case -ErrorCode.X_42581:
                throw new HSQLParseException("SQL Syntax error in \"" + sql + "\" " + hex.getMessage());
            default:
                throw new HSQLParseException(hex.getMessage());
            }
        } catch (Throwable t) {
            throw new HSQLParseException(t.getMessage());
        }

        //Result result = Result.newPrepareResponse(cs.id, cs.type, rmd, pmd);
        Result result = Result.newPrepareResponse(cs);
        if (result.hasError()) {
            throw new HSQLParseException(result.getMainString());
        }

        VoltXMLElement xml = null;
        xml = cs.voltGetStatementXML(sessionProxy);

        // this releases some small memory hsql uses that builds up over time if not
        // cleared
        // if it's not called for every call of getXMLCompiledStatement, that's ok;
        // it'll get called next time
        sessionProxy.sessionData.persistentStoreCollection.clearAllTables();

        // clean up sql-in expressions
        fixupInStatementExpressions(xml);

        assert(xml != null);

        return xml;
    }

    /**
     * Recursively find all in-lists found in the XML and munge them into the
     * simpler thing we want to pass to the AbstractParsedStmt.
     */
    private void fixupInStatementExpressions(VoltXMLElement expr) {
        if (doesExpressionReallyMeanIn(expr)) {
            inFixup(expr);
            // can return because in can't be nested
            return;
        }

        // recursive hunt
        for (VoltXMLElement child : expr.children) {
            fixupInStatementExpressions(child);
        }
    }

    /**
     * Find in-expressions in fresh-off-the-hsql-boat Volt XML. Is this fake XML
     * representing an in-list in the weird table/row way that HSQL generates
     * in-list expressions. Used by {@link this#fixupInStatementExpressions(VoltXMLElement)}.
     */
    private boolean doesExpressionReallyMeanIn(VoltXMLElement expr) {
        if (!expr.name.equals("operation")) {
            return false;
        }
        if (!expr.attributes.containsKey("optype") ||
            !expr.attributes.get("optype").equals("equal")) {
            return false;
        }

        // see if the children are "row" and "table".
        int rowCount = 0;
        int tableCount = 0;
        int valueCount = 0;
        for (VoltXMLElement child : expr.children) {
            if (child.name.equals("row")) {
                rowCount++;
            }
            else if (child.name.equals("table")) {
                tableCount++;
            }
            else if (child.name.equals("value")) {
                valueCount++;
            }
        }
        if ((tableCount + rowCount > 0) && (tableCount + valueCount > 0)) {
            assert rowCount == 1;
            assert tableCount + valueCount == 1;
            return true;
        }

        return false;
    }

    /**
     * Take an equality-test expression that represents in-list
     * and munge it into the simpler thing we want to output
     * to the ParsedExrpession classes.
     */
    private void inFixup(VoltXMLElement inElement) {
        // make this an in expression
        inElement.name = "operation";
        inElement.attributes.put("optype", "in");

        VoltXMLElement rowElem = null;
        VoltXMLElement tableElem = null;
        VoltXMLElement valueElem = null;
        for (VoltXMLElement child : inElement.children) {
            if (child.name.equals("row")) {
                rowElem = child;
            }
            else if (child.name.equals("table")) {
                tableElem = child;
            }
            else if (child.name.equals("value")) {
                valueElem = child;
            }
        }
        assert(rowElem.children.size() == 1);

        VoltXMLElement inlist;
        if (tableElem != null) {
            // make the table expression an in-list
            inlist = new VoltXMLElement("vector");
            for (VoltXMLElement child : tableElem.children) {
                assert(child.name.equals("row"));
                assert(child.children.size() == 1);
                inlist.children.addAll(child.children);
            }
        }
        else {
            assert valueElem != null;
            inlist = valueElem;
        }

        inElement.children.clear();
        // short out the row expression
        inElement.children.addAll(rowElem.children);
        // add the inlist
        inElement.children.add(inlist);
    }

    /**
     * Debug-only method that prints out the names of all
     * tables in the current schema.
     */
    @SuppressWarnings("unused")
    private void printTables() {
        try {
            String schemaName = sessionProxy.getSchemaName(null);
            System.out.println("*** Tables For Schema: " + schemaName + " ***");
        } catch (HsqlException e) {
            e.printStackTrace();
        }

        // load all the tables
        HashMappedList hsqlTables = getHSQLTables();
        for (int i = 0; i < hsqlTables.size(); i++) {
            Table table = (Table) hsqlTables.get(i);
            System.out.println(table.getName().name);
        }
    }

    /**
     * @return The set of all table/view names in the schema.
     */
    private Set<String> getTableNames() {
        Set<String> names = new HashSet<>();

        // load all the tables
        HashMappedList hsqlTables = getHSQLTables();
        for (int i = 0; i < hsqlTables.size(); i++) {
            Table table = (Table) hsqlTables.get(i);
            names.add(table.getName().name);
        }

        return names;
    }

    /**
     * Find the table that owns a particular index by name (or null if no match).
     * Case insensitive with whatever performance cost that implies.
     */
    String tableNameForIndexName(String indexName) {
        // the schema manager has a map of indexes by name
        // if this shows up on profiles, you can try to use that, but beware
        // the case insensitivity going on here
        HashMappedList hsqlTables = getHSQLTables();
        for (int i = 0; i < hsqlTables.size(); i++) {
            Table table = (Table) hsqlTables.get(i);
            for (Index index : table.getIndexList()) {
                if (index.getName().name.equalsIgnoreCase(indexName)) {
                    return table.getName().name.toLowerCase();
                }
            }
        }
        return null;
    }

    /**
     * Get an serialized XML representation of the current schema/catalog.
     *
     * @return The XML representing the catalog.
     * @throws HSQLParseException
     */
    public VoltXMLElement getXMLFromCatalog() throws HSQLParseException {
        VoltXMLElement xml = emptySchema.duplicate();

        // load all the tables
        HashMappedList hsqlTables = getHSQLTables();
        for (int i = 0; i < hsqlTables.size(); i++) {
            Table table = (Table) hsqlTables.get(i);
            VoltXMLElement vxmle = table.voltGetTableXML(sessionProxy);
            assert(vxmle != null);
            xml.children.add(vxmle);
        }

        return xml;
    }

    /**
     * Get an serialized XML representation of the a particular table.
     */
    public VoltXMLElement getXMLForTable(String tableName) throws HSQLParseException {
        VoltXMLElement xml = emptySchema.duplicate();

        // search all the tables XXX probably could do this non-linearly,
        //  but i don't know about case-insensitivity yet
        HashMappedList hsqlTables = getHSQLTables();
        for (int i = 0; i < hsqlTables.size(); i++) {
            Table table = (Table) hsqlTables.get(i);
            String candidateTableName = table.getName().name;

            // found the table of interest
            if (candidateTableName.equalsIgnoreCase(tableName)) {
                VoltXMLElement vxmle = table.voltGetTableXML(sessionProxy);
                assert(vxmle != null);
                xml.children.add(vxmle);
                return xml;
            }
        }
        return null;
    }

    /**
     * This code was repeated a lot so I factored it out.
     */
    private HashMappedList getHSQLTables() {
        String schemaName = null;
        try {
            schemaName = sessionProxy.getSchemaName(null);
        } catch (HsqlException e) {
            e.printStackTrace();
        }
        SchemaManager schemaManager = sessionProxy.getDatabase().schemaManager;

        // search all the tables XXX probably could do this non-linearly,
        //  but i don't know about case-insensitivity yet
        return schemaManager.getTables(schemaName);
    }
}
