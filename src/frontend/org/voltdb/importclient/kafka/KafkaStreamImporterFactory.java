/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.importclient.kafka;

import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.AbstractImporterFactory;
import org.voltdb.importer.ImporterConfig;

/**
 * Importer factory implementation for kafka stream importers.
 */
public class KafkaStreamImporterFactory extends AbstractImporterFactory
{

    @Override
    public String getTypeName()
    {
        return "KafkaStreamImporter";
    }

    @Override
    public Map<URI, ImporterConfig> createImporterConfigurations(Properties props)
    {
        return KafkaStreamImporterConfig.createConfigEntries(props);
    }

    @Override
    public AbstractImporter create(ImporterConfig config)
    {
        return new KafkaTopicPartitionImporter((KafkaStreamImporterConfig) config);
    }

    @Override
    public boolean isImporterRunEveryWhere()
    {
        return false;
    }
}
