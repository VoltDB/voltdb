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

package org.voltdb.utils;

import java.io.IOException;
import java.io.PrintStream;

import org.voltdb.VoltTable;

/**
 * Default output formatter for SQLCommand.
 */
class SQLCommandOutputFormatterDefault implements SQLCommandOutputFormatter
{
    @Override
    public void printTable(PrintStream stream, VoltTable t, boolean includeMetaData)
            throws IOException
    {
        // Use the VoltTable pretty printer to display formatted output.
        if (includeMetaData) {
            stream.println(t.toFormattedString(includeMetaData));
        }
        else {
            // don't insert line break at end when not including meta data
            stream.print(t.toFormattedString(includeMetaData));
        }
    }
}
