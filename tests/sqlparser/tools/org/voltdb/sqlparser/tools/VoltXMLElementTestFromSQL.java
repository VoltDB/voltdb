/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.voltdb.sqlparser.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.sqlparser.syntax.SQLKind;
import org.voltdb.sqlparser.tools.model.Test;
import org.voltdb.sqlparser.tools.model.Testpoint;
import org.voltdb.sqlparser.tools.model.VoltXMLElementTestSuite;

public class VoltXMLElementTestFromSQL {
    /*
     * These probably needs to be encapsulated in a
     * single class.  Too much work.
     */
    List<String> m_sqlStrings = new ArrayList<String>();
    List<String> m_testNames = new ArrayList<String>();
    private List<SQLKind> m_testTypes = new ArrayList<SQLKind>();
    private List<String> m_testComments = new ArrayList<String>();

    String m_sqlSourceFolder = "~/src/voltdb/tests/sqlparser";
    String m_fullyQualifiedClassName = null;
    String m_className = null;
    List<String> m_ddl = new ArrayList<String>();
    private PrintStream m_outputStream = null;
    String m_packageName = null;
    int m_errors = 0;
    String m_xmlFile = null;


    public static void main(String[] args) throws JAXBException {
        VoltXMLElementTestFromSQL elem = new VoltXMLElementTestFromSQL(args);
        elem.processAll();
        int errstat = 0;
        if (elem.getErrors() > 0) {
            System.err.printf("%d errors\n", elem.getErrors());
            errstat = 100;
        }
        System.exit(errstat);
    }

    private final int getErrors() {
        return m_errors;
    }

    private void VoltXMLElementTest() {
        JAXBContext jc;
        try {
            jc = JAXBContext.newInstance( "org.voltdb.sqlparser.tools.model" );
            Unmarshaller u = jc.createUnmarshaller();
            Object o = u.unmarshal(new File(m_xmlFile));
            assert(o instanceof VoltXMLElementTestSuite);
            processTestSuite((VoltXMLElementTestSuite)o);
        } catch (JAXBException e) {
            System.err.printf("JAXB error: %s", e.getMessage());
            e.printStackTrace();
            m_errors += 1;
        }
    }

    private void processTestSuite(VoltXMLElementTestSuite aSuite) {
        for (Test t : aSuite.getTests().getTest()) {
            initializeState();
            processOneTest(t);
        }
    }

    private void initializeState() {
        m_sqlSourceFolder = null;
        m_className = null;
        m_fullyQualifiedClassName = null;
        m_outputStream = null;
        m_packageName = null;
        m_testTypes.clear();
        m_testNames.clear();
        m_sqlStrings.clear();
        m_testComments.clear();
        m_ddl.clear();
    }

    private void processOneTest(Test t) {
        m_sqlSourceFolder = t.getSourcefolder();
        m_fullyQualifiedClassName = t.getClassname();
        m_ddl.addAll(t.getSchema().getDdl());
        for (Testpoint tp : t.getTestpoint()) {
            m_testComments.add(tp.getComment());
            m_testNames.add(tp.getTestName());
            m_sqlStrings.add(tp.getTestSQL());
            String testKind = tp.getTestKind();
            if ("DQL".equals(tp.getTestKind().toUpperCase())) {
                m_testTypes.add(SQLKind.DQL);
            } else if ("DDL".equals(tp.getTestKind().toUpperCase())) {
                m_testTypes.add(SQLKind.DDL);
            } else if ("DML".equals(tp.getTestKind().toUpperCase())) {
                m_testTypes.add(SQLKind.DML);
            } else {
                throw new IllegalArgumentException(String.format("Unknown test type: %s", tp.getTestKind().toUpperCase()));
            }
        }
        process();
    }

    public VoltXMLElementTestFromSQL(String[] args) throws JAXBException {
        int idx;
        String sqlComment;
        /*
         * This is actually goofy.  We should just traffic in
         * the model elements.
         */
        for (idx = 0; idx < args.length; idx += 1) {
            sqlComment = null;
            if ("--source-folder".equals(args[idx]) || "-o".equals(args[idx])) {
                m_sqlSourceFolder = args[++idx];
            } else if ("--ddl".equals(args[idx])) {
                addToSchema(args[++idx]);
            } else if ("--ddl-file".equals(args[idx])) {
                addToSchema(fileContents(args[++idx]));
            } else if ("--class".equals(args[idx]) || "-C".equals(args[idx])) {
                m_fullyQualifiedClassName = args[++idx];
            } else if ("--dql-test".equals(args[idx])) {
                idx = addSQLTest(args, idx, SQLKind.DQL, sqlComment);
            } else if ("--ddl-test".equals(args[idx])) {
                idx = addSQLTest(args, idx, SQLKind.DDL, sqlComment);
            } else if ("--dml-test".equals(args[idx])) {
                idx = addSQLTest(args, idx, SQLKind.DML, sqlComment);
            } else if ("--comment".equals(args[idx])) {
                sqlComment = args[++idx];
            } else if ("--xml-file".equals(args[idx])) {
                m_xmlFile = args[++idx];
            } else {
                System.err.printf("Unknown comand line parameter \"%s\"\n", args[idx]);
                usage(args[0]);
                System.exit(100);
            }
        }
    }

    private int addSQLTest(String args[], int aIDX, SQLKind aKind, String aSQLComment) {
        if (args.length <= aIDX + 2) {
            System.err.printf("Not enough arguments for %s test\n", aKind);
            System.exit(100);
        }
        m_testNames.add(args[aIDX + 1]);
        m_sqlStrings.add(args[aIDX + 2]);
        m_testTypes.add(aKind);
        m_testComments.add(aSQLComment);
        return aIDX + 2;
    }

    private String fileContents(String aFileName) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(aFileName);
            byte[] answer = new byte[fis.available()];
            fis.read(answer);
            return answer.toString();
        } catch (IOException e) {
            System.err.printf("Can't read ddl file: %s\n", e.getMessage());
            m_errors += 1;
            return "";
        } finally {
            try {
               fis.close();
            } catch (Exception e){
                ;
            }
        }

    }

    private void processAll() {
        if (m_fullyQualifiedClassName != null) {
            process();
        }
        if (m_xmlFile != null) {
            VoltXMLElementTest();
        }
    }



    private void process() {
        if (m_fullyQualifiedClassName == null) {
            System.err.printf("No class name specified\n");
            m_errors += 1;
            return;
        }
        int tnLoc = m_fullyQualifiedClassName.lastIndexOf(".");
        m_packageName = m_fullyQualifiedClassName.substring(0, tnLoc);
        m_className = m_fullyQualifiedClassName.substring(tnLoc + 1);
        try {
            String fileName = makeFileName();
            m_outputStream = new PrintStream(new FileOutputStream(fileName));
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            return;
        }
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        if (haveSchema()) {
            try {
                for (String ddl : m_ddl) {
                    hif.runDDLCommand(ddl);
                }
            } catch (HSQLParseException ex) {
                System.err.printf("DDL Did not compile: %s\n", ex.getMessage());
                m_errors += 1;
            }
        }
        VoltXMLElement elem = null;
        writePrefix();
        for (int testIdx = 0; testIdx < m_sqlStrings.size(); testIdx += 1) {
            // Each test point gets a new set of test ids.  They
            // are just for debugging test failures, so having them
            // be unique in the entire suite is not helpful.
            resetTestId();
            String sql = m_sqlStrings.get(testIdx);
            String testName = m_testNames.get(testIdx);
            SQLKind testKind = m_testTypes.get(testIdx);
            String comment = m_testComments.get(testIdx);
            try {
                switch (testKind) {
                case DML:
                    elem = hif.getXMLCompiledStatement(sql);
                    writeDMLTestBody(sql, testName, elem, comment);
                    break;
                case DDL:
                    writeDDLTestBody(sql, testName, comment);
                    break;
                case DQL:
                    elem = hif.getXMLCompiledStatement(sql);
                    writeDQLTestBody(sql, testName, elem, comment);
                    break;
                }
            } catch (HSQLParseException ex) {
                System.err.printf("Can't parse sql:\n   \"%s\":\nError:  %s\n",
                                      sql,
                                      ex.getMessage());
                m_errors += 1;
            }
        }
        writePostfix();
        try {
            m_outputStream.close();
        } catch (IOError ex) {
            ;
        }
    }

    private String makeFileName() {
        StringBuffer sb = new StringBuffer(m_sqlSourceFolder);
        sb.append(File.separator);
        sb.append(m_fullyQualifiedClassName.replace(".", File.separator))
          .append(".java");
        return sb.toString();
    }

    private void usage(String aProgramName) {
        String usageMessage = "%s: Generate VoltXMLElement unit tests.\n"
                                              + "Usage:\n"
                                              + "%s [Options...] -- testName sqlString...\n"
                                              + "Options:\n"
                                              + "  --source-folder | -o  SF       The source folder of output file.\n"
                                              + "                                 The output file will be:\n"
                                              + "                                  SF/pkgPath/ClassName.java\n"
                                              + "                                 where pkgPath is the usual filesystem\n"
                                              + "                                 path for a package.\n"
                                              + "                                 The default is $HOME/src/voltdb/test/sqlparser\n"
                                              + "  --class-name | -C className    The test class name.  There is no default.\n"
                                              + "                                 This parameter is mandatory.\n"
                                              + "  --ddl \"string\"               Use the given string as a schema.\n"
                                              + "  --ddl-file fileName            Use the ddl in the given named file.  This ddl\n"
                                              + "                                 will not be checked, but the checked sql can\n"
                                              + "                                 depend on it being processed.\n"
                                              + "  --                             This marks the end of the options.\n"
                                              + "The options are followed by a sequence of pairs.  The first of the pairs\n"
                                              + "Is a test name, and the second is a sql string.  For example,\n"
                                              + "  %s -C org.voltdb.queries.TestQueries -- test1 \"select id from alpha\" test2 \"select beta from alpha;\""
                                              + "will write into the file ~/src/voltdb/test/voltsqlparser/org/voltdb/queries/testQueries.java\n"
                                              + "a java test which verifies that the VoltXMLElement generated for the two given SQL\n"
                                              + "queries meet expectations.  The java file will be a junit test whose two tests are named\n"
                                              + "\"test1\" and \"test2\".\n"
                                              + "";
       System.err.printf(usageMessage, aProgramName, aProgramName, aProgramName);
    }

    private void writePrefix() {
        String prefixFmt =
            ""
            + "/* This file is part of VoltDB.\n"
            + " * Copyright (C) 2008-2015 VoltDB Inc.\n"
            + " *\n"
            + " * This file contains original code and/or modifications of original code.\n"
            + " * Any modifications made by VoltDB Inc. are licensed under the following\n"
            + " * terms and conditions:\n"
            + " *\n"
            + " * Permission is hereby granted, free of charge, to any person obtaining\n"
            + " * a copy of this software and associated documentation files (the\n"
            + " * \"Software\"), to deal in the Software without restriction, including\n"
            + " * without limitation the rights to use, copy, modify, merge, publish,\n"
            + " * distribute, sublicense, and/or sell copies of the Software, and to\n"
            + " * permit persons to whom the Software is furnished to do so, subject to\n"
            + " * the following conditions:\n"
            + " *\n"
            + " * The above copyright notice and this permission notice shall be\n"
            + " * included in all copies or substantial portions of the Software.\n"
            + " *\n"
            + " * THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND,\n"
            + " * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF\n"
            + " * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.\n"
            + " * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR\n"
            + " * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,\n"
            + " * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR\n"
            + " * OTHER DEALINGS IN THE SOFTWARE.\n"
            + " */\n"
            + "/*\n"
            + " * This file has been created by elves.  If you make changes to it,\n"
            + " * the elves will become annoyed, will overwrite your changes with\n"
            + " * whatever odd notions they have of what should\n"
            + " * be here, and ignore your plaintive bleatings.  So, don't edit this file,\n"
            + " * Unless you want your work to disappear.\n"
            + " */\n"
            + "package %s;\n"
            + "\n"
            + "import org.junit.Test;\n"
            + "\n"
            + "import org.hsqldb_voltpatches.VoltXMLElement;\n"
            + "import org.hsqldb_voltpatches.HSQLInterface;\n"
            + "import org.voltdb.sqlparser.syntax.SQLKind;\n"
            + "\n"
            + "import org.voltdb.sqlparser.assertions.semantics.VoltXMLElementAssert.IDTable;\n"
            + "import static org.voltdb.sqlparser.assertions.semantics.VoltXMLElementAssert.*;\n"
            + "\n"
            + "public class %s {\n"
            + ""
            + "    HSQLInterface m_HSQLInterface = null;\n"
            + "    String        m_schema = null;\n"
            + "    public %s() {\n"
            + "        m_HSQLInterface = HSQLInterface.loadHsqldb();\n"
            + (!haveSchema() ? "" : "        String m_schema = \"%s\";\n")
            + (!haveSchema() ? "" : "        try {\n")
            + (!haveSchema() ? "" : "            m_HSQLInterface.processDDLStatementsUsingVoltSQLParser(m_schema, null);\n")
            + (!haveSchema() ? "" : "        } catch (Exception ex) {\n")
            + (!haveSchema() ? "" : "            System.err.printf(\"Error parsing ddl: %%s\\n\", ex.getMessage());\n")
            + (!haveSchema() ? "" : "        }\n")
            + "    }\n";
        m_outputStream.printf(prefixFmt, m_packageName, m_className, m_className, getSchema());
    }

    private void writeDDLTestBody(String aSql, String aTestName, String aComment) {
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        try {
            hif.runDDLCommand(aSql);
            VoltXMLElement elem = hif.getXMLFromCatalog();
            StringBuffer sb = new StringBuffer();
            if (aComment != null) {
                addComment(sb, aComment);
            }
            List<String> initialContext = new ArrayList<String>();
            initialContext.add(elem.name);
            sb.append("\n");
            sb.append("    @SuppressWarnings(\"unchecked\")\n");
            sb.append("    @Test\n");
            sb.append(String.format("    public void %s() throws Exception {\n", aTestName));
            sb.append(String.format("        String ddl    = \"%s\";\n", aSql));
            sb.append("        IDTable idTable = new IDTable();\n");
            sb.append("        HSQLInterface hif = HSQLInterface.loadHsqldb();\n");
            sb.append("        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);\n");
            sb.append("        VoltXMLElement element = hif.getVoltCatalogXML(null, null);\n");
            sb.append("        assertThat(element)\n");
            sb.append(String.format("            .hasName(%d, \"%s\")\n", getTestId(), elem.name));
            sb.append("            .hasAllOf(");
            describeVoltXML(elem, initialContext, sb, 16, "");
            sb.append(");\n");
            sb.append("    }\n");
            m_outputStream.print(sb.toString());
        } catch (HSQLParseException e) {
            System.err.printf("Test %s: DDL \"%s\" does not compile\n", aTestName, aSql);
            m_errors += 1;
        }


    }

    private void writeDMLTestBody(String aSQL, String aTestName, VoltXMLElement aElem, String comment) {
        writeSQLTestBody(aSQL, aTestName, aElem, SQLKind.DML, comment);
    }

    private void writeDQLTestBody(String aSql, String aTestName, VoltXMLElement aElem, String comment) {
        writeSQLTestBody(aSql, aTestName, aElem, SQLKind.DQL, comment);
    }

    private void writeSQLTestBody(String aSql, String aTestName, VoltXMLElement aElem, SQLKind aKind, String aComment) {
        StringBuffer sb = new StringBuffer();
        sb.append("\n");
        if (aComment != null) {
            addComment(sb, aComment);
        }
        List<String> initialContext = new ArrayList<String>();
        initialContext.add(aElem.name);
        sb.append("    @SuppressWarnings(\"unchecked\")\n");
        sb.append("    @Test\n");
        sb.append(String.format("    public void %s() throws Exception {\n", aTestName));
        sb.append(String.format("        String sql    = \"%s\";\n", aSql));
        sb.append("        IDTable idTable = new IDTable();\n");
        sb.append(String.format("        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.%s);\n", aKind));
        sb.append("        assertThat(element)\n");
        sb.append(String.format("            .hasName(%d, \"%s\")\n", getTestId(), aElem.name));
        sb.append("            .hasAllOf(");
        describeVoltXML(aElem, initialContext, sb, 16, "");
        sb.append(");\n");
        sb.append("    }\n");
        m_outputStream.print(sb.toString());
    }

    private void addComment(StringBuffer aSB, String aComment) {
        aSB.append("    /**\n");
        aSB.append(String.format("     * %s\n", aComment));
        aSB.append("     *\n");
        aSB.append("     * Throws: Exception\n");
        aSB.append("     */\n");

    }
    private StringBuffer indentStr(StringBuffer aSb,
                                   int aIndent,
                                   boolean aStartNL,
                                   boolean aEndNL) {
        if (aStartNL) {
            aSb.append("\n");
        }
        for (int idx = 0; idx < aIndent; idx += 1) {
            aSb.append(" ");
        }
        if (aEndNL) {
            aSb.append("\n");
        }
        return aSb;
    }

    private void describeVoltXML(VoltXMLElement aHSQLThinksItShouldBe,
                                 List<String>   aContext,
                                 StringBuffer   aSb,
                                 int            aIndent,
                                 String         aEOL) {
        for (String key : aHSQLThinksItShouldBe.attributes.keySet()) {
            String value = aHSQLThinksItShouldBe.attributes.get(key);
            aSb.append(aEOL);
            aEOL = ",";
            String attrTest;
            if ("id".equals(key)) {
                attrTest = String.format("withIdAttribute(%d, idTable)", getTestId());
            } else {
                attrTest = String.format("withAttribute(%d, \"%s\", \"%s\")",
                                         getTestId(), key, value);
            }
            indentStr(aSb, aIndent, true, false)
              .append(attrTest);
        }
        for (VoltXMLElement child : aHSQLThinksItShouldBe.children) {
            // Sometimes we will have an element with multiple
            // children, all with the same name.  In this case
            // we need to disambiguate by looking at the attributes.
            // The particular attribute we care about depends on
            // the context.
            List<String> attributes = new ArrayList<String>();
            // We ignore some subtrees entirely.
            boolean skipit = false;
            //
            // Add the child name, so that the match can see it.
            // We'll use this as well when recursing below.
            //
            aContext.add(child.name);
            if (contextMatch(aContext, "databaseschema.table.columns.column")) {
                attributes.add("name");
            } else if (contextMatch(aContext, "databaseschema.table.indexes.index")
                         || contextMatch(aContext, "databaseschema.table.constraints.constraint")) {
                // Some constraints are auto generated.  We want to ignore these.
                // But some constraints are user generated.  We want to include
                // these.
                String childName = child.attributes.get("name");
                if (isAutoGeneratedName(childName)) {
                    skipit = true;
                } else {
                    attributes.add("name");
                }
            } else if (contextMatch(aContext, "columnref")) {
                attributes.add("alias");
                attributes.add("column");
                attributes.add("table");
                attributes.add("tablealias");
            } else if (contextMatch(aContext, "operation")) {
                attributes.add("optype");
            }
            if (skipit == false) {
                aSb.append(aEOL);
                aEOL = ",";
                indentStr(aSb, aIndent, true, false)
                  .append(String.format("withChildNamed(%d, \"%s\"", getTestId(), child.name));
                if (attributes.size() > 0) {
                    for (String attribute : attributes) {
                        String value = child.attributes.get(attribute);
                        if (value != null) {
                            aSb.append(",");
                            indentStr(aSb, aIndent + 15, true, false)
                              .append(String.format("\"%s\", \"%s\"",
                                                    attribute,
                                                    value));
                        }
                    }
                }
                describeVoltXML(child, aContext, aSb, aIndent + 4, ",");
                //
                // We don't need the child name in the context anymore.
                //
                aSb.append(")");
            }
            aContext.remove(aContext.size()-1);
        }
    }

    private boolean isAutoGeneratedName(String childName) {
        return (childName == null
                || childName.startsWith("VOLTDB_AUTOGEN")
                || childName.startsWith("SYS_CT"));
    }

    /**
     * Return true iff the aMatch string describes the current
     * context.
     *
     * That is to say, if we are given a context, which is a list of names, and
     * a string which is a sequence of names separated by dots,
     * return true iff the sequence of names matches the trailing
     * sequence of the context.
     *
     * @param aContext
     * @param aMatch
     * @return
     */
    private boolean contextMatch(List<String> aContext, String aMatch) {
       String names[] = aMatch.split("[.]");
       int nidx = names.length - 1;
       int cidx = aContext.size() - 1;
       for (;0 <= nidx && 0 <= cidx;
            nidx -= 1, cidx -= 1) {
           String nname = names[nidx];
           String cname = aContext.get(cidx);
           if (nname == null || nname.equals(cname) == false) {
               return false;
           }
       }
       return true;
    }

    private void writePostfix() {
        String str = "}\n";
        m_outputStream.printf("%s", str);
    }

    private boolean haveSchema() {
        return m_ddl.size() > 0;
    }

    private void addToSchema(String aSchemaStatement) {
        List<String> stmts = Arrays.asList(aSchemaStatement.split(";"));
        m_ddl.addAll(stmts);
    }

    private String getSchema() {
        StringBuffer sb = new StringBuffer();
        String sep = "";
        for (String ddlStmt : m_ddl) {
            sb.append(sep)
              .append(ddlStmt);
            sep = ";";
        }
        return sb.toString();
    }

    private static int testIdCount = 1;

    private static int getTestId() {
        return testIdCount++;
    }

    public static void resetTestId() {
        testIdCount = 1;
    }
}
