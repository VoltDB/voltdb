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
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.AbstractImporterFactory;
import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.FormatterBuilder;

/**
 * Importer factory implementation for kafka stream importers.
 */
public class Kafka10StreamImporterFactory extends AbstractImporterFactory
{

    @Override
    public String getTypeName()  {
        return "Kafka10StreamImporter";
    }

    @Override
    public Map<URI, ImporterConfig> createImporterConfigurations(Properties props, FormatterBuilder formatterBuilder) {
        Kafka10StreamImporterConfig cfg = new Kafka10StreamImporterConfig(props, formatterBuilder);
        return Collections.singletonMap(cfg.getURI(), cfg);
    }

    @Override
    public AbstractImporter create(ImporterConfig config) {
        return new Kafka10StreamImporter((Kafka10StreamImporterConfig) config );
    }

    @Override
    public boolean isImporterRunEveryWhere() {
        return true;
    }
}