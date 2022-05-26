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

import java.util.List;

import jline.console.completer.Completer;

/**
 * SQLCommand JLine2 Completer implementation.
 */
class SQLCompleter implements Completer
{
    /// The list of valid command prefixes, e.g. the first one or two words of a command.
    static String[] m_commandPrefixes = null;
    /// Maximum command prefix length calculated when the prefixes are received.
    static int m_maxCommandPrefixLength = 0;

    /**
     * Constructor.
     * @param commandPrefixes  valid SQL command prefixes
     */
    public SQLCompleter(final String[] commandPrefixes) {
        super();

        // Grab the command prefixes and determine the maximum prefix length.
        m_commandPrefixes = commandPrefixes;
        for (final String command : m_commandPrefixes) {
            if (command.length() > m_maxCommandPrefixLength) {
                m_maxCommandPrefixLength = command.length();
            }
        }
    }

    /* (non-Javadoc)
     * @see jline.console.completer.Completer#complete(java.lang.String, int, java.util.List)
     */
    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates)
    {
        // For now only support tab completion at the end of the line.
        if (cursor == buffer.length()) {
            // Check for an initial token match?
            if (cursor <= m_maxCommandPrefixLength) {
                final String bufferu = buffer.toUpperCase();
                for (final String command : m_commandPrefixes) {
                    if (command.startsWith(bufferu)) {
                        candidates.add(command);
                    }
                }
                if (!candidates.isEmpty()) {
                    return 0;
                }
            }
        }
        return cursor;
    }

}
