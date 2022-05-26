/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

import com.google_voltpatches.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

public class DatabaseConfiguration {

    // DR_MODE
    public static final String DR_MODE_NAME = "DR";

    // add more configurations above, don't forget to add them and their names below after that
    static {
        String[] names = new String[] { DR_MODE_NAME };

        allNames = Joiner.on(", ").join(names);
    }

    public static final String allNames;

    private interface ConfigurationValueFiller {
        String getValue(Database db);
    }

    private final String name;
    private final String description;
    private final ConfigurationValueFiller filler;

    private DatabaseConfiguration(String name, String description, ConfigurationValueFiller filler) {
        this.name = name;
        this.description = description;
        this.filler = filler;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getValue(Database db) {
        return filler.getValue(db);
    }
}
