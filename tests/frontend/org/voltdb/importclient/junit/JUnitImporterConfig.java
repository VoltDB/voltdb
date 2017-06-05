/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
    public static final String IMPORTER_ID_PROPERTY = "JUnitImporter.IMPORTER_ID";
    public static final String SLEEP_DURATION_PROPERTY = "JUnitImporter.SLEEP_DURATION_MS";

    private URI m_resourceID = null;
    private FormatterBuilder m_formatterBuilder = null;
    private int m_sleepDurationMs = JUnitImporter.DEFAULT_IMPORTER_SLEEP_DURATION_MS;

    @Override
    public URI getResourceID() {
        return m_resourceID;
    }

    @Override
    public FormatterBuilder getFormatterBuilder() {
        return m_formatterBuilder;
    }

    public static URI generateURIForImporter(String importerID) {
        try {
            URI importerURI = new URI(URI_SCHEME, importerID, null);
            return importerURI;
        } catch (URISyntaxException bug) {
            throw new RuntimeException(bug);
        }
    }

    public static Map<URI,ImporterConfig> createConfigEntries(Properties props, FormatterBuilder formatterBuilder) {
        int importerSleepDurationMs;
        try {
            importerSleepDurationMs = Integer.parseInt(props.getProperty(SLEEP_DURATION_PROPERTY, Integer.toString(JUnitImporter.DEFAULT_IMPORTER_SLEEP_DURATION_MS)));
        } catch (NumberFormatException shouldNotHappen) {
            throw new RuntimeException(shouldNotHappen);
        }

        String importerID = props.getProperty(IMPORTER_ID_PROPERTY);
        Preconditions.checkNotNull(importerID, "Every JUnitImporter must have a unique IMPORTER_ID");

        // NOTE: JUnitImporter only supports one 'real' importer per configured importer.
        // Kafka, by contrast, supports one per broker.
        Map<URI, ImporterConfig> configMap = new HashMap<>();
        URI importerURI = generateURIForImporter(importerID);
        JUnitImporterConfig config = new JUnitImporterConfig();
        config.m_resourceID = importerURI;
        config.m_formatterBuilder = formatterBuilder;
        config.m_sleepDurationMs = importerSleepDurationMs;
        ImporterConfig previousConfig = configMap.put(importerURI, config);
        Preconditions.checkState(previousConfig == null, "Importer ID " + importerID + " is not unique.");
        return configMap;
    }

    public long getSleepDurationMs() {
        return m_sleepDurationMs;
    }
}
