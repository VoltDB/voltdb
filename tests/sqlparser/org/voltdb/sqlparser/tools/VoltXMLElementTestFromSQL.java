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
import java.util.List;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;

public class VoltXMLElementTestFromSQL {
    List<String> m_sqlString = new ArrayList<String>();
    List<String> m_testNames = new ArrayList<String>();
    String m_sqlSourceFolder = "~/src/voltdb/tests/sqlparser";
    String m_fullyQualifiedClassName = null;
    String m_className = null;
    StringBuffer m_ddl = null;
    private PrintStream m_outputStream = null;
    String m_packageName = null;


    public static void main(String[] args) {
        VoltXMLElementTestFromSQL elem = new VoltXMLElementTestFromSQL(args);
        elem.process();
    }

    public VoltXMLElementTestFromSQL(String[] args) {
        int idx;
        for (idx = 0; idx < args.length; idx += 1) {
            if ("--source-folder".equals(args[idx]) || "-o".equals(args[idx])) {
                m_sqlSourceFolder = args[++idx];
            } else if ("--ddl".equals(args[idx])) {
                addToSchema(args[++idx]);
            } else if ("--ddl-file".equals(args[idx])) {
                addToSchema(fileContents(args[++idx]));
            } else if ("--class".equals(args[idx]) || "-C".equals(args[idx])) {
                m_fullyQualifiedClassName = args[++idx];
            } else if ("--".equals(args[idx])) {
                break;
            } else {
                System.err.printf("Unknown comand line parameter \"%s\"\n", args[idx]);
                usage(args[0]);
                System.exit(100);
            }
        }
        for (idx += 1;idx < args.length; idx += 2) {
            if (idx + 1 == args.length) {
                System.err.printf("Unmatched test name (%s) and sql statements.\n", args[idx]);
                usage(args[0]);
                System.exit(100);
            }
            m_testNames.add(args[idx]);
            m_sqlString.add(args[idx+1]);
        }
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
            System.exit(100);
            return "";
        } finally {
            try {
               fis.close();
            } catch (Exception e){
                ;
            }
        }

    }

    private void process() {
        if (m_fullyQualifiedClassName == null) {
            System.err.printf("No class name specified\n");
            System.exit(100);
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
                hif.processDDLStatementsUsingVoltSQLParser(getSchema(), null);
            } catch (HSQLParseException ex) {
                System.err.printf("DDL Did not compile: %s\n", ex.getMessage());
                System.exit(100);
            }
        }
        VoltXMLElement elem = null;
        writePrefix(m_packageName, m_className, getSchema());
        for (int testIdx = 0; testIdx < m_sqlString.size(); testIdx += 1) {
            String sql = m_sqlString.get(testIdx);
            String testName = m_testNames.get(testIdx);
            try {
                elem = hif.getVoltXMLFromDQLUsingVoltSQLParser(sql, null);
                writeMeat(sql, testName, elem);
            } catch (HSQLParseException ex) {
                System.err.printf("Can't parse sql:\n   \"%s\":\nError:  %s\n",
                                      sql,
                                      ex.getMessage());
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
        System.out.printf(usageMessage, aProgramName, aProgramName, aProgramName);
    }

    private void writePrefix(String aPackageName, String aClassName, String aSchema) {
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
	    + "/**\n"
	    + " * Licensed to the Apache Software Foundation (ASF) under one or more\n"
	    + " * contributor license agreements. See the NOTICE file distributed with this\n"
	    + " * work for additional information regarding copyright ownership. The ASF\n"
	    + " * licenses this file to you under the Apache License, Version 2.0 (the\n"
	    + " * \"License\"); you may not use this file except in compliance with the License.\n"
	    + " * You may obtain a copy of the License at\n"
	    + " *\n"
	    + " * http://www.apache.org/licenses/LICENSE-2.0\n"
	    + " *\n"
	    + " * Unless required by applicable law or agreed to in writing, software\n"
	    + " * distributed under the License is distributed on an \"AS IS\" BASIS, WITHOUT\n"
	    + " * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the\n"
	    + " * License for the specific language governing permissions and limitations under\n"
	    + " * the License.\n"
	    + " */\n"
	    + "package %s;\n"
	    + "\n"
	    + "import org.hsqldb_voltpatches.VoltXMLElement;\n"
	    + "import org.hsqldb_voltpatches.HSQLInterface;\n"
	    + "import org.junit.Before;\n"
	    + "import org.junit.Test;\n"
	    + "\n"
	    + "import static org.voltdb.sqlparser.semantics.VoltXMLElementAssert.*;\n"
	    + "\n"
	    + "public class %s {\n"
	    + ""
	    + "    HSQLInterface m_HSQLInterface = null;\n"
	    + "    String        m_schema = null;\n"
	    + "    public %s() {\n"
	    + "        m_HSQLInterface = HSQLInterface.loadHsqldb();\n"
	    + (haveSchema() ? "" : "        String m_schema = \"%s\";\n")
	    + (haveSchema() ? "" : "        try {\n")
	    + (haveSchema() ? "" : "            m_HSQLInterface.processDDLStatementsUsingVoltSQLParser(m_schema, null);\n")
	    + (haveSchema() ? "" : "        } catch (Exception ex) {\n")
	    + (haveSchema() ? "" : "            System.err.printf(\"Error parsing ddl: %%s\\n\", ex.getMessage());\n")
	    + (haveSchema() ? "" : "        }\n")
	    + "    }\n";
        m_outputStream.printf(prefixFmt, aPackageName, aClassName, aClassName, aSchema);
    }

    private void writeMeat(String aSql, String aTestName, VoltXMLElement aElem) {
        StringBuffer sb = new StringBuffer();
        sb.append("\n");
        sb.append("    @SuppressWarnings(\"unchecked\")\n");
        sb.append("    @Test\n");
        sb.append(String.format("    public void %s() throws Exception {\n", aTestName));
        sb.append(String.format("        String sql    = \"%s\";\n", aSql));
        sb.append("        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromDQLUsingVoltSQLParser(sql, null);\n");
        sb.append("        assertThat(element)\n");
        sb.append(String.format("            .hasName(\"%s\")\n", aElem.name));
        sb.append("            .hasAllOf(");
        describeVoltXML(aElem, sb, 16, "");
        sb.append(");\n");
        sb.append("    }\n");
        m_outputStream.print(sb.toString());
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
                                 StringBuffer   aSb,
                                 int            aIndent,
                                 String         aEOL) {
        // Describe the attributes.
        for (String key : aHSQLThinksItShouldBe.attributes.keySet()) {
            String value = aHSQLThinksItShouldBe.attributes.get(key);
            aSb.append(aEOL);
            aEOL = ",";
            indentStr(aSb, aIndent, true, false)
              .append(String.format("withAttribute(\"%s\", \"%s\")", key, value));
        }
        for (VoltXMLElement child : aHSQLThinksItShouldBe.children) {
            aSb.append(aEOL);
            aEOL = ",";
            indentStr(aSb, aIndent, true, false)
              .append(String.format("withChildNamed(\"%s\"", child.name));
            describeVoltXML(child, aSb, aIndent + 4, ",");
            aSb.append(")");
        }
    }

    private void writePostfix() {
        String str = "}\n";
        m_outputStream.printf("%s", str);
    }

    private boolean haveSchema() {
        return m_ddl != null;
    }

    private String getSchema() {
        if (haveSchema()) {
            return m_ddl.toString();
        } else {
            return "";
        }
    }

    private void addToSchema(String aSchemaStatement) {
        if (m_ddl == null) {
            m_ddl = new StringBuffer();
        }
        m_ddl.append(";").append(aSchemaStatement);
    }


}
