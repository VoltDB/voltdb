/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

package org.voltdb.compiler.statements;

import java.util.regex.Matcher;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONTokener;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Property;
import org.voltdb.catalog.Topic;
import org.voltdb.common.Constants;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

public class CreateTopic extends StatementProcessor {

    public CreateTopic(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        String statement = ddlStatement.statement;

        Matcher statementMatcher = SQLParser.matchCreateTopic(statement);
        if (!statementMatcher.matches()) {
            return false;
        }

        String topicName = statementMatcher.group("topicName");
        CatalogMap<Topic> topics = db.getTopics();

        Topic curTopic = topics.get(topicName);
        if (curTopic != null) {
            throw m_compiler.new VoltCompilerException(
                    String.format("Invalid CREATE TOPIC statement: topic %s already exists", curTopic.getTypeName()));
        }

        Topic topic = topics.add(topicName);
        processOpaque(statementMatcher, topic);

        boolean usingStream = statementMatcher.group("usingStream") != null;
        if (usingStream) {
            topic.setStreamname(topicName);
            checkTable(topicName);
        }
        String procedure = statementMatcher.group("procedureName");
        if (procedure != null) {
            checkIdentifierStart(procedure, statement);
            topic.setProcedurename(procedure);
        }
        processAllow(statementMatcher, topic);
        processProfile(statementMatcher, topic);
        processProperties(statementMatcher, topic);
        return true;
    }

    private void processProfile(Matcher statementMatcher, Topic topic) throws VoltCompilerException {
        String profile = statementMatcher.group("profile");
        if (profile != null) {
            topic.setProfile(profile);
        }
    }

    private void processOpaque(Matcher statementMatcher, Topic topic) throws VoltCompilerException {
        if (statementMatcher.group("opaque") == null) {
            topic.setIsopaque(false);
            if (statementMatcher.group("partitioned") != null) {
                throw m_compiler.new VoltCompilerException(
                        "Invalid CREATE TOPIC statement: PARTITIONED is only allowed with OPAQUE");
            }
        } else {
            topic.setIsopaque(true);
            if (statementMatcher.group("usingStream") != null) {
                throw m_compiler.new VoltCompilerException(
                        "Invalid CREATE TOPIC statement: USING STREAM is not allowed with OPAQUE");
            }
            if (statementMatcher.group("properties") != null) {
                throw m_compiler.new VoltCompilerException(
                        "Invalid CREATE TOPIC statement: PROPERTIES is not allowed with OPAQUE");
            }
            topic.setIssingle(statementMatcher.group("partitioned") == null);
        }
    }

    // Remove the stream from the default connector - validation is deferred to {@link TopicsValidator}
    private void checkTable(String topicName) throws VoltCompilerException {
        VoltXMLElement tableXML = m_schema.findChild("table", topicName.toUpperCase());
        if (tableXML == null) {
            return;
        }

        if (Boolean.parseBoolean(tableXML.getStringAttribute("stream", "false"))) {
            String exportTarget = tableXML.getStringAttribute("export", Constants.CONNECTORLESS_STREAM_TARGET_NAME);
            if (Constants.CONNECTORLESS_STREAM_TARGET_NAME.equals(exportTarget)) {
                tableXML.attributes.put("topicName", topicName);
                tableXML.attributes.remove("export");
            }
        }
    }

    private void processAllow(Matcher statementMatcher, Topic topic) {
        String allow = statementMatcher.group("allow");
        if (allow != null) {
            topic.setRoles(allow);
        }
    }

    private void processProperties(Matcher statementMatcher, Topic topic) throws VoltCompilerException {
        String properties = statementMatcher.group("properties");
        if (properties == null) {
            return;
        }

        new PropertyParser(m_compiler, properties, topic.getProperties()).parse();
    }

    private static class PropertyParser {
        private final VoltCompiler m_compiler;
        private final String m_propertiesString;
        private final CatalogMap<Property> m_properties;

        PropertyParser(VoltCompiler ddlCompiler, String propertiesString, CatalogMap<Property> properties) {
            m_compiler = ddlCompiler;
            m_propertiesString = propertiesString;
            m_properties = properties;
        }

        void parse() throws VoltCompilerException {
            // Reusing a JSON parser since the property map looks a lot like JSON
            JSONTokener tokener = new JSONTokener(m_propertiesString);
            try {
                while (true) {
                    if (!m_properties.isEmpty()) {
                        if (expectChar(tokener, ',', true)) {
                            return;
                        }
                    }
                    String key = getKeyOrValue(tokener);
                    expectChar(tokener, '=', false);
                    m_properties.add(key).setValue(getKeyOrValue(tokener));
                }
            } catch (JSONException e) {
                throw m_compiler.new VoltCompilerException("Unable to parse properties: " + e.getMessage());
            }
        }

        private String getKeyOrValue(JSONTokener tokener) throws VoltCompilerException, JSONException {
            char c = tokener.nextClean();
            switch (c) {
                case ',':
                case '=':
                    throw m_compiler.new VoltCompilerException("Unexpected token in properties: " + c + ": " + tokener);
                case 0:
                    throw m_compiler.new VoltCompilerException("Unexpected end of properties: " + tokener);
                default:
                    tokener.back();
                    return tokener.nextValue().toString();
            }
        }

        private boolean expectChar(JSONTokener tokener, char expected, boolean endOk)
                throws JSONException, VoltCompilerException {
            char c = tokener.nextClean();
            if (c != expected) {
                if (c == 0) {
                    if (endOk) {
                        return true;
                    }
                    throw m_compiler.new VoltCompilerException(
                            "Expected token '" + expected + "' in properties but encountered end of string: " + tokener);
                }
                throw m_compiler.new VoltCompilerException(
                        "Expected token '" + expected + "' in properties but encountered: '" + c + "': " + tokener);
            }
            return false;
        }
    }
}
