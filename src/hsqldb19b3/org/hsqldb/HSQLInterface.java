/* This file is part of VoltDB.
 * Copyright (C) 2008 Vertica Systems Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package org.hsqldb;

import org.hsqldb.lib.HashMappedList;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultMetaData;

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
    static int instanceId = 0;

    private HSQLInterface(Session sessionProxy) {
        this.sessionProxy = sessionProxy;
    }

    public void close() {
        sessionProxy.close();
        DatabaseManager.closeDatabases(Database.CLOSEMODE_IMMEDIATELY);
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

        // make HSQL case insensitive
        sessionProxy.executeDirectStatement("SET IGNORECASE TRUE;");

        return new HSQLInterface(sessionProxy);
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
        if (result.mode == ResultConstants.ERROR)
            throw new HSQLParseException(result.getMainString());
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
     * @return XML representation of the compiled statement.
     * @throws HSQLParseException Throws exception if SQL parse error is
     * encountered.
     */
    public String getXMLCompiledStatement(String sql) throws HSQLParseException
    {
        Statement cs = null;

        try {
            cs = sessionProxy.compileStatement(sql);
        } catch (Throwable t) {
            //t.printStackTrace();
            throw new HSQLParseException(t.getMessage());
        }

        //ResultMetaData rmd = cs.getResultMetaData();
        //ResultMetaData pmd = cs.getParametersMetaData();

        //Result result = Result.newPrepareResponse(cs.id, cs.type, rmd, pmd);
        Result result = Result.newPrepareResponse(cs);
        if (result.mode == ResultConstants.ERROR)
            throw new HSQLParseException(result.getMainString());

        String xml = null;
        xml = cs.voltGetXML(sessionProxy, HSQLInterface.XML_INDENT);

        String retval = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
        retval += DTDSource.getCompiledStatementDTD();
        retval += "<statement>\n" + xml + "\n</statement>\n";

        return retval;
    }

    /**
     * Debug-only method that prints out the names of all
     * tables in the current schema.
     */
    @SuppressWarnings("unused")
    private void printTables() {
        String schemaName = null;
        try {
            schemaName = sessionProxy.getSchemaName(null);
        } catch (HsqlException e) {
            e.printStackTrace();
        }
        SchemaManager schemaManager = sessionProxy.getDatabase().schemaManager;

        System.out.println("*** Tables For Schema: " + schemaName + " ***");

        // load all the tables
        HashMappedList hsqlTables = schemaManager.getTables(schemaName);
        for (int i = 0; i < hsqlTables.size(); i++) {
            Table table = (Table) hsqlTables.get(i);
            System.out.println(table.getName().name);
        }
    }

    /**
     * Get an serialized XML representation of the current schema/catalog.
     * The output will include the DTD
     *
     * @return The XML representing a catalog
     * @throws HSQLParseException
     */
    public String getXMLFromCatalog() throws HSQLParseException {
        return (getXMLFromCatalog(true));
    }

    /**
     * Get an serialized XML representation of the current schema/catalog.
     *
     * @param include_dtd if true, the output will include the XML's DTD embedded
     * @return The XML representing the catalog.
     * @throws HSQLParseException
     */
    public String getXMLFromCatalog(boolean include_dtd) throws HSQLParseException {
        StringBuilder sb = new StringBuilder();

        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        if (include_dtd) {
            sb.append(DTDSource.getCatalogDTD());
        }

        sb.append("<databaseschema>\n");

        String schemaName = null;
        try {
            schemaName = sessionProxy.getSchemaName(null);
        } catch (HsqlException e) {
            e.printStackTrace();
        }
        SchemaManager schemaManager = sessionProxy.getDatabase().schemaManager;

        // load all the tables
        HashMappedList hsqlTables = schemaManager.getTables(schemaName);
        for (int i = 0; i < hsqlTables.size(); i++) {
            Table table = (Table) hsqlTables.get(i);
            sb.append(table.voltGetXML(sessionProxy, "  "));
        }

        sb.append("</databaseschema>\n");

        return sb.toString();
    }
}
