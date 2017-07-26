/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.importclient.kafka10;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.FormatterBuilder;

/**
 * Holds configuration information required to connect a consumer to a topic.
 */
public class Kafka10StreamImporterConfig implements ImporterConfig
{
    private URI m_uri;
    private String m_brokers;
    private String m_topics;
    private String m_groupId;
    private String m_procedure;
    private FormatterBuilder m_formatterBuilder;

    public Kafka10StreamImporterConfig(Properties properties, FormatterBuilder formatterBuilder) {

        m_brokers = properties.getProperty("brokers", "").trim();
        m_topics = properties.getProperty("topics");
        m_groupId = properties.getProperty("groupid");
        m_procedure = properties.getProperty("procedure");
        m_formatterBuilder = formatterBuilder;

        try {
            m_uri = new URI("fake://uri/for/kafka/consumer/group/" + m_groupId);
        }
        catch (URISyntaxException e) {
        }

    }

    public URI getURI() {
        return m_uri;
    }

    public String getBrokers() {
        return m_brokers;
    }

    public String getProcedure() {
        return m_procedure;
    }

    public String getTopics() {
        return m_topics;
    }

    @Override
    public URI getResourceID() {
        return m_uri;
    }

    @Override
    public FormatterBuilder getFormatterBuilder() {
        return m_formatterBuilder;
    }
}