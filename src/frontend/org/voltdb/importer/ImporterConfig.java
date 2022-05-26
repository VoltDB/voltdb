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

package org.voltdb.importer;

import java.net.URI;
import org.voltdb.importer.formatter.FormatterBuilder;


/**
 * Represents importer configurations created from properties specified in deployment file.
 * Implementations will contain importer specific details. Importer framework will create
 * ImporterConfig objects from importer properties in deployment file and use it to create
 * importer instances when it is time to run importers for the different resources.`
 */
public interface ImporterConfig
{
    /**
     * Unique resource id for which the configuration is specified.
     * This must be unique per importer type.
     *
     * @return the unique resource id
     */
    public URI getResourceID();

    public FormatterBuilder getFormatterBuilder();
}
