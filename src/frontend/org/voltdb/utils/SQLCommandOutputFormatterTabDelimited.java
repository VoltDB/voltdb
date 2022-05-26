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
import org.voltdb.VoltType;


/**
 * Tab-delimited output formatter for SQLCommand.
 */
class SQLCommandOutputFormatterTabDelimited implements SQLCommandOutputFormatter
{
    @Override
    public void printTable(PrintStream stream, VoltTable t, boolean addMetadata)
            throws IOException
    {
        int columnCount = t.getColumnCount();
        if (addMetadata) {
            for (int i = 0; i < t.getColumnCount(); i++) {
                if (i > 0) stream.print("\t");
                stream.print(t.getColumnName(i));
            }
            stream.print("\n");
            t.resetRowPosition();
        }
        while(t.advanceRow())
        {
            for (int i = 0; i < columnCount; i++)
            {
                if (i > 0) stream.print("\t");
                Object v = t.get(i, t.getColumnType(i));
                if (t.wasNull())
                    v = "NULL";
                else if (t.getColumnType(i) == VoltType.VARBINARY) {
                    v = Encoder.hexEncode((byte[])v);
                }
                else {
                    v = v.toString();
                }
                stream.print(v);
            }
            stream.print("\n");
        }
    }
}
