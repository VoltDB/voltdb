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

import org.apache.commons.lang3.StringUtils;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Topic;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;


public class CreateOpaqueTopic extends StatementProcessor {

    private static final String INVALID_ALLOW = "Invalid CREATE TOPIC statement: ALLOW [PRODUCER | CONSUMER] already defined.";

    public CreateOpaqueTopic(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        String statement = ddlStatement.statement;
        Matcher statementMatcher = SQLParser.matchCreateOpaqueTopic(statement);
        if (!statementMatcher.matches()) {
            return false;
        }

        String topicName = getTopicName(statementMatcher, db, statement);

        CatalogMap<Topic> topics = db.getTopics();
        Topic curTopic = topics.getIgnoreCase(topicName);
        if (curTopic != null) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid CREATE OPAQUE TOPIC statement: topic name %s conflicts with existing topic %s", curTopic.getTypeName()));
        }
        Topic topic = topics.add(topicName);
        topic.setIsopaque(true);
        String partitioned = statementMatcher.group("partitioned");
        topic.setIssingle(partitioned == null);
        processProfile(statementMatcher, statement, topic);
        if(processPermissions(statementMatcher, statement, topic, 1)) {
            processPermissions(statementMatcher, statement, topic, 2);
        }
        return true;
    }

    protected String getTopicName( Matcher statementMatcher, Database db, String statement) throws VoltCompilerException {
        String topicName = statementMatcher.group("topicName");
        if (topicName == null) {
            throw m_compiler.new VoltCompilerException("Topic name is missing.");
        }
        topicName = checkIdentifierStart(topicName, statement);
        return topicName;
    }

    protected void processProfile(Matcher statementMatcher, String statement, Topic topic) throws VoltCompilerException {
        String profile = statementMatcher.group("profile");
        if (profile != null) {
            profile = checkIdentifierStart(profile, statement);
            if (!StringUtils.isBlank(profile)) {
                topic.setProfile(profile);
            }
        }
    }

    protected boolean  processPermissions(Matcher statementMatcher, String statement, Topic topic, int index) throws VoltCompilerException {
        String roles = statementMatcher.group("allowedRole" + index);
        if (roles == null) {
            return false;
        }
        roles = checkIdentifierStart(roles, statement);
        if (!StringUtils.isBlank(roles)) {
            String producerConsumer = statementMatcher.group("producerConsumer" + index);
            if (producerConsumer == null) {
                if (index == 2) {
                    throw m_compiler.new VoltCompilerException(INVALID_ALLOW);
                }
                topic.setProducerroles(roles);
                topic.setConsumerroles(roles);
            } else if ("producer".equalsIgnoreCase(producerConsumer)) {
                if (!StringUtils.isBlank(topic.getProducerroles())) {
                    throw m_compiler.new VoltCompilerException(INVALID_ALLOW);
                }
                topic.setProducerroles(roles);
            } else {
                if (!StringUtils.isBlank(topic.getConsumerroles())) {
                    throw m_compiler.new VoltCompilerException(INVALID_ALLOW);
                }
                topic.setConsumerroles(roles);
            }
        }
        return true;
    }
}
