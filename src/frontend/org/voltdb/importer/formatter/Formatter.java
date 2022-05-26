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

package org.voltdb.importer.formatter;

import java.nio.ByteBuffer;

/**
 * Interface for the formatter, the transform method gets called after the formatter factory
 * creates the formatter.
 */
public interface Formatter {
    /**
     * Transforms data from one format to another.
     * @param sourceData - raw data from source
     * @return Array of data converted from sourceData
     * @throws FormatException
     */
    public Object[] transform(ByteBuffer sourceData) throws FormatException;
}
