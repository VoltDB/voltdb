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

package org.voltdb.importclient.junit;

import com.google_voltpatches.common.base.Preconditions;
import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.FormatterBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by bshaw on 5/31/17.
 */
public class JUnitImporterConfig implements ImporterConfig {

    public static final String URI_SCHEME = "junitimporter";
    public static final String NUM_IMPORTERS_PROPERTY = "JUnitImporter.NUM_IMPORTERS";
    public static final String SLEEP_DURATION_PROPERTY = "JUnitImporter.SLEEP_DURATION_MS";

    private URI m_resourceID = null;
    private int m_sleepDurationMs = JUnitImporter.DEFAULT_IMPORTER_SLEEP_DURATION_MS;

    @Override
    public URI getResourceID() {
        return m_resourceID;
    }

    @Override
    public FormatterBuilder getFormatterBuilder() {
        return null;
    }

    private static URI generateURIForImporter(int index) {
        try {
            URI importerURI = new URI(URI_SCHEME, Integer.toString(index), null);
            return importerURI;
        } catch (URISyntaxException bug) {
            throw new RuntimeException(bug);
        }
    }

    public static Map<URI,ImporterConfig> createConfigEntries(Properties props, FormatterBuilder formatterBuilder) {
        assert formatterBuilder == null; // dummy importer doesn't need a formatter
        int numImporters;
        int importerSleepDurationMs;

        try {
            String numImportersAsString = props.getProperty(NUM_IMPORTERS_PROPERTY);
            Preconditions.checkNotNull(numImportersAsString, "number of importers was not specified");
            numImporters = Integer.parseInt(numImportersAsString);
            importerSleepDurationMs = Integer.parseInt(props.getProperty(SLEEP_DURATION_PROPERTY, Integer.toString(JUnitImporter.DEFAULT_IMPORTER_SLEEP_DURATION_MS)));

        } catch (NumberFormatException shouldNotHappen) {
            throw new RuntimeException(shouldNotHappen);
        }

        Map<URI, ImporterConfig> configMap = new HashMap<>();
        for (int i = 0; i < numImporters; i++) {
            URI importerURI = generateURIForImporter(i);
            JUnitImporterConfig config = new JUnitImporterConfig();
            config.m_resourceID = importerURI;
            config.m_sleepDurationMs = importerSleepDurationMs;
            configMap.put(importerURI, config);
        }
        return configMap;
    }

    public long getSleepDurationMs() {
        return m_sleepDurationMs;
    }
}
