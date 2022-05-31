/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.utils;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.voltcore.logging.VoltLogger;

/**
 * This class allows strings to be loaded from
 * a properties file stored as a resource in the
 * VoltDB jar.
 *
 * The main use at present is white-label customization.
 * The odd name of the properties file is due to its
 * historical origins.
 */
public class CustomProperties {

    private static final String DEFAULT_RESOURCE = "org/voltdb/utils/voltdb_logstrings.properties";

    private Properties props = new Properties();

    public CustomProperties() {
        load(DEFAULT_RESOURCE);
    }

    public CustomProperties(String resource) {
        load(resource);
    }

    private void load(String path) {
        try {
            props.load(new InputStreamReader(ClassLoader.getSystemResourceAsStream(path), StandardCharsets.UTF_8));
        }
        catch (Exception ex) {
            VoltLogger log = new VoltLogger("HOST");
            log.warnFmt("Unable to load custom properties from resource '%s': %s", path, ex.getMessage());
        }
    }

    public String get(String prop) {
        return get(prop, null);
    }

    public String get(String prop, String deflt) {
        String val = props.getProperty(prop);
        if (val == null || val.isEmpty()) {
            val = deflt;
        }
        return val;
    }
}
