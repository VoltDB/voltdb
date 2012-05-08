/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
 * NB: This file, plus the test implementations (*Test.java) should be the only Java code that
 * knows about specific tests. Please keep it that way. We need to know about specific tests here
 * in order to map the XML configuration to Java classes.
 */
package adhocbenchmark;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author scooper
 *
 */
public class BenchmarkConfiguration {

    /**
     * Table configuration data from config.xml
     */
    private static class ConfigurationTable {
        public final String name;
        public final String columnPrefix;
        public final int nColumns;
        public final int nVariations;

        public ConfigurationTable(final String name, final String columnPrefix, int nColumns, int nVariations) {
            this.name = name;
            this.columnPrefix = columnPrefix;
            this.nColumns = nColumns;
            this.nVariations = nVariations;
        }

        public static ConfigurationTable fromElement(Element elem) throws ConfigurationException {
            if (!elem.hasAttribute("name")) {
                throw new ConfigurationException("<table> element is missing the 'name' attribute");
            }
            if (!elem.hasAttribute("prefix")) {
                throw new ConfigurationException("<table> element is missing the 'prefix' attribute");
            }
            if (!elem.hasAttribute("columns")) {
                throw new ConfigurationException("<table> element is missing the 'columns' attribute");
            }
            String name = elem.getAttribute("name");
            String columnPrefix = elem.getAttribute("prefix");
            int nColumns = Integer.parseInt(elem.getAttribute("columns"));
            int nVariations = (elem.hasAttribute("variations")
                                    ? Integer.parseInt(elem.getAttribute("variations"))
                                    : 1);
            return new ConfigurationTable(name, columnPrefix, nColumns, nVariations);
        }
    }

    /**
     * Test configuration data from config.xml
     */
    private static class ConfigurationTest {
        public final String type;
        public final String table;
        public final int nLevels;

        public ConfigurationTest(final String type, final String table, int nLevels) {
            this.type = type;
            this.table = table;
            this.nLevels = nLevels;
        }

        public static ConfigurationTest fromElement(Element elem) throws ConfigurationException {
            if (!elem.hasAttribute("type")) {
                throw new ConfigurationException("<test> element is missing the 'type' attribute");
            }
            if (!elem.hasAttribute("table")) {
                throw new ConfigurationException("<test> element is missing the 'table' attribute");
            }
            String type = elem.getAttribute("type");
            String table = elem.getAttribute("table");
            int nLevels = (elem.hasAttribute("levels") ? Integer.parseInt(elem.getAttribute("levels")) : 0);
            return new ConfigurationTest(type, table, nLevels);
        }
    }

    /**
     * Provide the list of known test names.
     * @return  test name string array
     */
    public static String[] getTestNames() {
        return new String[]{"join", "projection"};
    }

    /**
     * Provide the default test name.
     * @return  default test name
     */
    public static String getDefaultTestName() {
        return "join";
    }

    /**
     * Parse config.xml and produce test configuration.
     *
     * @param path  path to configuration file
     * @return  test list
     * @throws ConfigurationException
     */
    public static List<QueryTestBase> configureTests(final String path, final String testName) throws ConfigurationException {
        List<QueryTestBase> tests = new ArrayList<QueryTestBase>();

        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse(new File(path));
            doc.getDocumentElement().normalize();

            // Read schema tables (provides variation count for tests, etc.)
            Map<String, ConfigurationTable> tables = new HashMap<String, ConfigurationTable>();
            Element schemaElement = (Element)doc.getElementsByTagName("schema").item(0);
            NodeList tableNodes = schemaElement.getElementsByTagName("table");
            for (int iTable = 0; iTable < tableNodes.getLength(); iTable++) {
                ConfigurationTable table = ConfigurationTable.fromElement((Element)tableNodes.item(iTable));
                tables.put(table.name, table);
            }

            // Read tests and build test lists mixing in data from schema read above.
            Element testElement = (Element)doc.getElementsByTagName("tests").item(0);
            NodeList testNodes = testElement.getElementsByTagName("test");
            for (int iTest = 0; iTest < testNodes.getLength(); iTest++) {
                ConfigurationTest test = ConfigurationTest.fromElement((Element)testNodes.item(iTest));
                if (tables.containsKey(test.table)) {
                    ConfigurationTable table = tables.get(test.table);
                    if (test.type.equalsIgnoreCase("join")) {
                        if (testName.equalsIgnoreCase("join")) {
                            tests.add(new JoinTest(test.table, table.nVariations, table.columnPrefix,
                                                   table.nColumns, test.nLevels));
                        }
                    }
                    else if (test.type.equalsIgnoreCase("projection")) {
                        if (testName.equalsIgnoreCase("projection")) {
                            tests.add(new ProjectionTest(test.table, table.columnPrefix, table.nColumns));
                        }
                    } else {
                        throw new ConfigurationException(
                                String.format("Configuration has unknown test type '%s'", test.type));
                    }
                } else {
                    throw new ConfigurationException(
                            String.format("Configuration has test for unknown table '%s'", test.table));
                }
            }
        } catch (SAXException e) {
            throw new ConfigurationException("XML parser SAX exception", e);
        } catch (ParserConfigurationException e) {
            throw new ConfigurationException("XML parser configuration exception", e);
        } catch (IOException e) {
            throw new ConfigurationException("XML parser I/O exception", e);
        }

        return tests;
    }
}
