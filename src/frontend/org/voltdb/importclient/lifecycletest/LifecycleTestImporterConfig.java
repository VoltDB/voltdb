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

package org.voltdb.importclient.lifecycletest;

import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.FormatterBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class LifecycleTestImporterConfig implements ImporterConfig {

    private static final String URI_SCHEME = "lifecycletestimporter";

    private final String m_filepath;
    private final URI m_resourceID;
    private final FormatterBuilder m_formatterBuilder;

    public LifecycleTestImporterConfig(Properties props, FormatterBuilder formatterBuilder)
    {
        Properties propsCopy = (Properties) props.clone();
        m_filepath = propsCopy.getProperty("filepath");
        if (m_filepath == null) {
            throw new IllegalArgumentException()
        }

        try {
            m_resourceID = new URI(URI_SCHEME, m_filepath, null);
        } catch(URISyntaxException e) { // Will not happen
            throw new RuntimeException(e);
        }
        m_formatterBuilder = formatterBuilder;
    }

    @Override
    public URI getResourceID() {
        return m_resourceID;
    }

    @Override
    public FormatterBuilder getFormatterBuilder() {
        return m_formatterBuilder;
    }
}
