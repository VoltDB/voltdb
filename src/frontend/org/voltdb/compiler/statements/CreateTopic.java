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

import java.util.EnumSet;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.FormatParameter;
import org.voltdb.catalog.Topic;
import org.voltdb.common.Constants;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;
import org.voltdb.serdes.EncodeFormat;

import com.google_voltpatches.common.base.Splitter;


public class CreateTopic extends CreateOpaqueTopic {

    private static final String INVALID_DEF = "Invalid CREATE TOPIC statement. Format definitions are conflicted";

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

        String topicName = getTopicName(statementMatcher, db, statement);
        CatalogMap<Topic> topics = db.getTopics();
        Topic curTopic = topics.getIgnoreCase(topicName);
        if (curTopic != null) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid CREATE TOPIC statement: topic %s already exists", curTopic.getTypeName()));
        }

        statement = statement.replaceAll("( +)"," ");
        String st = statement.toUpperCase();
        boolean usingStream = st.contains("USING STREAM");
        if (usingStream) {
            checkTable(topicName);
        }

        Topic topic = topics.add(topicName);
        topic.setIsopaque(false);
        topic.setUsekey(st.contains("WITH KEYS"));

        String columnKeys = statementMatcher.group("columnKeys");
        if (usingStream) {
            topic.setStreamname(topicName);
            if (columnKeys != null) {
                topic.setKeycolumnnames(columnKeys.toUpperCase());
            }
        } else if(columnKeys != null) {
            throw m_compiler.new VoltCompilerException(
                    "Invalid CREATE TOPIC statement: KEYS are not allowed if not using stream");
        }
        String procedure = statementMatcher.group("procedureName");
        if (procedure != null) {
            checkIdentifierStart(procedure, statement);
            topic.setProcedurename(procedure);
        }
        processProfile(statementMatcher, statement, topic);
        if (processFormat(statementMatcher, statement, topic, 1)) {
            processFormat(statementMatcher, statement, topic, 2);
        }

        if (processPermissions(statementMatcher, statement, topic, 1)) {
            processPermissions(statementMatcher, statement, topic, 2);
        }
        return true;
    }


    // Check that the table is a stream exporting to no targets, set the topic name attribute and remove
    // the table from the default connector
    private void checkTable(String topicName) throws VoltCompilerException {
        VoltXMLElement tableXML = m_schema.findChild("table", topicName.toUpperCase());
        if (tableXML == null) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid CREATE TOPIC USING STREAM statement: stream %s does not exist", topicName));
        }

        if (Boolean.parseBoolean(tableXML.getStringAttribute("stream", "false"))) {
            String exportTarget = tableXML.getStringAttribute("export", Constants.CONNECTORLESS_STREAM_TARGET_NAME);
            if (Constants.CONNECTORLESS_STREAM_TARGET_NAME.equals(exportTarget)) {
                tableXML.attributes.put("topicName", topicName);
                tableXML.attributes.remove("export");
            } else {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Invalid CREATE TOPIC USING STREAM statement: stream %s must not have an export clause", topicName));
            }
        } else {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid CREATE TOPIC USING STREAM statement: object %s is not a stream", topicName));
        }
    }

    //[FORMAT [KEY | VALUE] {avro | csv | json} [PROPERTIES (key1=value1, ...)]]
    private boolean processFormat(Matcher statementMatcher, String statement, Topic topic, int index) throws VoltCompilerException {
        String formatName = statementMatcher.group("formatName" + index);
        if (formatName != null) {
            try {
                // Parse the format in an unchecked fashion and handle exceptions
                EncodeFormat.valueOf(formatName.toUpperCase());
            }
            catch (Exception ex) {
                throw m_compiler.new VoltCompilerException(
                        String.format("%s is not a valid topic format in TOPIC %s. Acceptable values are: %s",
                                formatName, topic.getTypeName(), EncodeFormat.valueSet()));
            }
        }
        String formatTarget = statementMatcher.group("formatTarget" + index);
        String formatProps = statementMatcher.group("formatProp" + index);
        if (index == 2 && formatName == null) {
            return true;
        }
        if (formatTarget == null) {
            if (index == 2) {
               throw m_compiler.new VoltCompilerException(INVALID_DEF);
            }
            // Unspecified format only sets the value format name
            if (formatName != null) {
                validateValueFormat(formatName);
                topic.setValueformatname(formatName);
                setFormatProperties(formatProps, topic.getValueformatproperties());
            }
        } else if ("key".equalsIgnoreCase(formatTarget)) {
            if (!StringUtils.isBlank(topic.getKeyformatname())) {
                throw m_compiler.new VoltCompilerException(INVALID_DEF);
            }
            validateKeyFormat(formatName);
            topic.setKeyformatname(formatName);
            setFormatProperties(formatProps, topic.getKeyformatproperties());
        } else {
            if (!StringUtils.isBlank(topic.getValueformatname())) {
                throw m_compiler.new VoltCompilerException(INVALID_DEF);
            }
            validateValueFormat(formatName);
            topic.setValueformatname(formatName);
            setFormatProperties(formatProps, topic.getValueformatproperties());
        }
        return true;
    }

    private void validateKeyFormat(String formatName) throws VoltCompilerException {
        EncodeFormat keyFormat = EncodeFormat.checkedValueOf(formatName.toUpperCase());
        if (!keyFormat.isSimple()) {
            throw m_compiler.new VoltCompilerException(
                    String.format("Invalid CREATE TOPIC statement. key format %s is invalid. Allowed key format: %s",
                            formatName, EncodeFormat.simpleValueSet().toString()));
        }
    }

    private void validateValueFormat(String formatName) throws VoltCompilerException {
        EncodeFormat keyFormat = EncodeFormat.checkedValueOf(formatName.toUpperCase());
        EnumSet<EncodeFormat> keySet = EncodeFormat.valueSet();
        if (!keySet.contains(keyFormat)) {
            throw m_compiler.new VoltCompilerException(
                    String.format("Invalid CREATE TOPIC statement. key format %s is invalid. Allowed key format: %s", formatName, keySet.toString()));
        }
    }

    private void setFormatProperties(String formatProps, CatalogMap<FormatParameter> properties) {
        if (!StringUtils.isBlank(formatProps)) {
            formatProps = formatProps.substring(1, formatProps.length()-1);
            Map<String, String> props = Splitter.on(',').withKeyValueSeparator(Splitter.on("=").trimResults()).split(formatProps);
            for (Map.Entry<String, String> e : props.entrySet()) {
                FormatParameter fp = properties.add(e.getKey());
                fp.setName(e.getKey());
                fp.setValue(e.getValue());
            }
        }
    }
}
