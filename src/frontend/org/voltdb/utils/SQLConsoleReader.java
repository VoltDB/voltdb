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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ListIterator;

/**
 * Customized JLine2 ConsoleReader.
 *
 * Inheritance allows behavior changes without modifying JLine2 source.
 * The case-insensitive search tweak assumes that all searches go through
 * searchBackwards(String searchTerm, int startIndex, boolean startsWith).
 * Be aware of possible breakage when implementing JLine2 updates.
 */
public class SQLConsoleReader extends jline.console.ConsoleReader implements SQLCommandLineReader
{
    /**
     * Constructor.
     *
     * @param in   Input stream
     * @param out  Output stream
     * @throws IOException
     */
    public SQLConsoleReader(InputStream in, OutputStream out) throws IOException {
        super(in, out);
        // This jLine method may have disappeared in later versions of jline.
        // If we decide to upgrade, take note.
        setExpandEvents(false);  // don't process shell ! and !!
    }

    @Override
    public String readBatchLine() throws IOException { return readLine("batch>"); }

    @Override
    public int getLineNumber() {
        return 0;
    }

    /* (non-Javadoc)
     * @see jline.console.ConsoleReader#searchBackwards(java.lang.String, int, boolean)
     * Overrides and replaces search logic to make it case-insensitive.
     */
    @Override
    public int searchBackwards(String searchTerm, int startIndex, boolean startsWith)
    {
        ListIterator<jline.console.history.History.Entry> it = this.getHistory().entries(startIndex);
        while (it.hasPrevious()) {
            jline.console.history.History.Entry e = it.previous();
            String itemNormalized = e.value().toString().toLowerCase();
            String termNormalized = searchTerm.toLowerCase();
            if (startsWith) {
                if (itemNormalized.startsWith(termNormalized)) {
                    return e.index();
                }
            }
            else {
                if (itemNormalized.contains(termNormalized)) {
                    return e.index();
                }
            }
        }
        return -1;
    }
}
