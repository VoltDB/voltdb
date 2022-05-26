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
import org.voltcore.logging.VoltLogger;

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
    private static final VoltLogger m_logger = new VoltLogger("HSQLDB_COMPILER");

    public static final String XML_SCHEMA_NAME = "databaseschema";
    /**
     * Naming conventions for unnamed indexes and constraints
     */
    public static final String AUTO_GEN_PREFIX = "VOLTDB_AUTOGEN_";

    // Prefixes for system-generated indexes that enforce constraints
    public static final String AUTO_GEN_IDX_PREFIX = AUTO_GEN_PREFIX + "IDX_";
    public static final String AUTO_GEN_PRIMARY_KEY_PREFIX = AUTO_GEN_IDX_PREFIX + "PK_";
    public static final String AUTO_GEN_UNIQUE_IDX_PREFIX = AUTO_GEN_IDX_PREFIX + "CT_";
    public static final String AUTO_GEN_NAMED_CONSTRAINT_IDX = AUTO_GEN_PREFIX + "CONSTRAINT_IDX_";

    // Prefixes for indexes on materialized views
    public static final String AUTO_GEN_MATVIEW = "MATVIEW_PK_";
    public static final String AUTO_GEN_MATVIEW_IDX = AUTO_GEN_MATVIEW + "INDEX";

    // Prefixes for constraints
    public static final String AUTO_GEN_CONSTRAINT_PREFIX = AUTO_GEN_PREFIX + "CT_";
    public static final String AUTO_GEN_MATVIEW_CONST = AUTO_GEN_MATVIEW + "CONSTRAINT";

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

        private Integer lineNo = null;

        HSQLParseException(String msg, Throwable caught) {
            super(msg, caught);
        }

        HSQLParseException(String msg) {
            super(msg);
        }

        HSQLParseException(String msg, int lineNo) {
            super(msg);
            this.lineNo = lineNo;
        }

        public Integer getLineNumber() {
            return lineNo;
        }

    }

    private Session sessionProxy;
    // Keep track of the previous XML for each table in the schema
    Map<String, VoltXMLElement> lastSchema = new TreeMap<>();
    // empty schema for cloning and for null diffs
    private static final VoltXMLElement emptySchema = new VoltXMLElement(XML_SCHEMA_NAME);
    static {
        emptySchema.attributes.put("name", XML_SCHEMA_NAME);
    }
    static int instanceId = 0;

    private HSQLInterface(Session sessionProxy) {
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
     * This class lets HSQL inform VoltDB of the number of parameters
     * in the current statement, without directly referencing any VoltDB
     * classes.
     *
     * Any additional parameters introduced after HSQL parsing (such as
     * for subqueries with outer references or paramterization of ad hoc
     * queries) will need to come after parameters created by HSQL.
     */
    public static abstract interface ParameterStateManager {
        public int getNextParamIndex();
        public void resetCurrentParamIndex();
    }

    /**
     * Load up an HSQLDB in-memory instance.
     *
     * @return A newly initialized in-memory HSQLDB instance accessible
     * through the returned instance of HSQLInterface
     */
    public static HSQLInterface loadHsqldb(ParameterStateManager psMgr) {
        // Specifically set the timezone to UTC to avoid the default usage local timezone in HSQL.
        // This ensures that all VoltDB data paths use the same timezone for representing time.
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+0"));

        String name = "hsqldbinstance-" + String.valueOf(instanceId) + "-" + String.valueOf(System.currentTimeMillis());
        instanceId++;

        HsqlProperties props = new HsqlProperties();
        try {
            Session sessionProxy = DatabaseManager.newSession(DatabaseURL.S_MEM, name, "SA", "", props, 0);
            // make HSQL case insensitive
            sessionProxy.executeDirectStatement("SET IGNORECASE TRUE;");
            sessionProxy.setParameterStateManager(psMgr);
            return new HSQLInterface(sessionProxy);
        }
        catch (HsqlException caught) {
            m_logger.warn("Unexpected error initializing the SQL parser",
                    caught);
            caught.printStackTrace();
            throw caught;
        }
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

        // the old table can be null for CREATE TABLE or for IF EXISTS stuff
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

    public VoltXMLElement getLastSchema(String expectedTableAffected) {
        return lastSchema.get(expectedTableAffected);
    }

    /**
     * Modify the current schema with a SQL DDL command.
     *
     * @param ddl The SQL DDL statement to be run.
     * @throws HSQLParseException Throws exception if SQL parse error is
     * encountered.
     */
    public void runDDLCommand(String ddl) throws HSQLParseException {
        sessionProxy.clearLocalTables();
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
            }
            catch (HSQLParseException e) {
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
        sessionProxy.clearLocalTables();
        // clear the expression node id set for determinism
        sessionProxy.resetVoltNodeIds();

        try {
            cs = sessionProxy.compileStatement(sql);
        } catch (HsqlException caught) {
            // a switch in case we want to give more error details on additional error codes
            switch(caught.getErrorCode()) {
            case -ErrorCode.X_42581:
                throw new HSQLParseException(
                        "SQL Syntax error in \"" + sql + "\" " + caught.getMessage(),
                        caught);
            default:
                throw new HSQLParseException("Error in \"" + sql + "\" - " + caught.getMessage(), caught);
            }
        } catch (StackOverflowError caught) {
            // Handle this consistently in high level callers
            // regardless of where it is thrown.
            // It should be presumed to be a user error where the user is
            // exceeding a soft limit on the supportable complexity of a
            // SQL statement causing unreasonable levels of recursion.
            throw caught;
        } catch (Throwable caught) {
            // Expectable user errors should have been thrown as HSQLException.
            // So, this throwable should be an unexpected system error.
            // The details of these arbitrary Throwables are not typically
            // useful to an end user reading an error message.
            // They should be logged.
            m_logger.error("Unexpected error in the SQL parser for statement \"" + sql + "\" ",
                    caught);
            // The important thing for the end user is that
            // they be notified that there is a system error involved,
            // suggesting that it is not their fault -- though they MAY be
            // able to work around the issue with the help of VoltDB support
            // (especially if they can provide the log traces).
            throw new HSQLParseException(
                    "An unexpected system error was logged by the SQL parser for statement \"" + sql + "\" ",
                    caught);
        }

        //Result result = Result.newPrepareResponse(cs.id, cs.type, rmd, pmd);
        Result result = Result.newPrepareResponse(cs);
        if (result.hasError()) {
            throw new HSQLParseException(result.getMainString());
        }

        VoltXMLElement xml = null;
        xml = cs.voltGetStatementXML(sessionProxy);
        if (m_logger.isDebugEnabled()) {
            try {
                /*
                 * Sometimes exceptions happen.
                 */
                m_logger.debug(String.format("SQL: %s\n", sql));;
                m_logger.debug(String.format("HSQLDB:\n%s", (cs == null) ? "<NULL>" : cs.describe(sessionProxy)));
                m_logger.debug(String.format("VOLTDB:\n%s", (xml == null) ? "<NULL>" : xml.toXML()));
            }
            catch (Exception caught) {
                m_logger.warn("Unexpected error in the SQL parser",
                        caught);
                caught.printStackTrace(System.out);
            }
        }

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
     * Recursively find all in-lists, subquery, row comparisons found in the XML and munge them into the
     * simpler thing we want to pass to the AbstractParsedStmt.
     * @throws HSQLParseException
     */
    private void fixupInStatementExpressions(VoltXMLElement expr) throws HSQLParseException {
        if (doesExpressionReallyMeanIn(expr)) {
            inFixup(expr);
            // can't return because in with subquery can be nested
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
     * @throws HSQLParseException
     */
    private boolean doesExpressionReallyMeanIn(VoltXMLElement expr) throws HSQLParseException {
        if (!expr.name.equals("operation")) {
            return false;
        }
        if (!expr.attributes.containsKey("optype") ||
            !expr.attributes.get("optype").equals("equal")) {
            return false;
        }

        // see if the children are "row" and "table" or "tablesubquery".
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
        //  T.C     IN (SELECT ...) => row       equal                  tablesubquery => IN
        //  T.C     =  (SELECT ...) => columnref equal                  tablesubquery
        //  (C1,C2)  IN (SELECT ...) => row       equal/anyqunatified    tablesubquery
        //  (C1, C2) =  (SELECT ...) => row       equal                  tablesubquery
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
     * to the AbstractParsedStmt for its AbstractExpression classes.
     */
    private void inFixup(VoltXMLElement inElement) {
        // make this an in expression
        inElement.name = "operation";
        inElement.attributes.put("optype", "in");

        VoltXMLElement rowElem = null;
        VoltXMLElement tableElem = null;
        VoltXMLElement subqueryElem = null;
        VoltXMLElement valueElem = null;
        for (VoltXMLElement child : inElement.children) {
            if (child.name.equals("row")) {
                rowElem = child;
            }
            else if (child.name.equals("table")) {
                tableElem = child;
            }
            else if (child.name.equals("tablesubquery")) {
                subqueryElem = child;
            }
            else if (child.name.equals("value")) {
                valueElem = child;
            }
        }

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
        else if (subqueryElem != null) {
            inlist = subqueryElem;
        }
        else {
            assert valueElem != null;
            inlist = valueElem;
        }

        assert(rowElem != null);
        assert(inlist != null);
        inElement.children.clear();
        // add the row
        inElement.children.add(rowElem);
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
        }
        catch (HsqlException caught) {
            caught.printStackTrace();
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
     * Get a serialized XML representation of a particular table.
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

    private HashMappedList getHSQLTables() {
        try {
            String schemaName = null;
            schemaName = sessionProxy.getSchemaName(null);
            // search all the tables XXX probably could do this non-linearly,
            //  but i don't know about case-insensitivity yet
            SchemaManager schemaManager = sessionProxy.getDatabase().schemaManager;
            return schemaManager.getTables(schemaName);
        }
        catch (HsqlException caught) {
            m_logger.warn("Unexpected error in the SQL parser",
                    caught);
            return new HashMappedList();
        }

    }
}
