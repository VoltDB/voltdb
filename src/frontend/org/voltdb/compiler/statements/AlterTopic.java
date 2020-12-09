/* This file is part of VoltDB.
 * Copyright (C) 2020 VoltDB Inc.
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

import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.voltcore.logging.VoltLogger;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Property;
import org.voltdb.catalog.Topic;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * ALTER TOPIC topic ADD PROPERTIES(property=value, ...) - will fail if property added already defined</br>
 * ALTER TOPIC topic ALTER PROPERTIES(property=value, ...) - will fail if altered property cannot be found</br>
 * ALTER TOPIC topic DROP PROPERTIES(property, ...) - will fail if dropped property cannot be found</br>
 */
public class AlterTopic extends StatementProcessor {
    private static VoltLogger topicsLog = new VoltLogger("TOPICS");

    public AlterTopic(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        Matcher matcher = SQLParser.matchAlterTopic(ddlStatement.statement);
        if (!matcher.matches()) {
            return false;
        }

        String name = matcher.group("name");
        Topic topic = db.getTopics().get(name);
        if (topic == null) {
            throw m_compiler.new VoltCompilerException(
                    String.format("Topic name \"%s\" in ALTER TOPIC statement does not exist.", name));
        }
        CatalogMap<Property> properties = topic.getProperties();
        dumpProperties(properties, "Initial");

        String added_properties = matcher.group("added");
        if (added_properties != null) {
            new TopicPropertyParser(m_compiler, added_properties, properties, TopicPropertyParser.Mode.ADD).parse();
        }

        String altered_properties = matcher.group("altered");
        if (altered_properties != null) {
            new TopicPropertyParser(m_compiler, altered_properties, properties, TopicPropertyParser.Mode.ALTER).parse();
        }

        String dropped_properties = matcher.group("dropped");
        if (dropped_properties != null) {
            new TopicPropertyParser(m_compiler, dropped_properties, properties, TopicPropertyParser.Mode.DROP).parse();
        }
        dumpProperties(properties, "Altered");
        return true;
    }

    private void dumpProperties(CatalogMap<Property> properties, String what) {
        if (!topicsLog.isDebugEnabled()) {
            return;
        }
        Map<String, String> strProps = StreamSupport.stream(properties.spliterator(), false)
                .collect(Collectors.toMap(Property::getTypeName, Property::getValue, (k, v) -> {
                    throw new IllegalStateException("Duplicate key " + k);
                }, () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER)));
        topicsLog.debug(what + " properties: " + strProps);
    }
}
