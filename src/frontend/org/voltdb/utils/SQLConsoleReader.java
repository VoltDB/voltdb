/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
public class SQLConsoleReader extends jline.console.ConsoleReader
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
