/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.util.List;

import jline.console.completer.Completer;

class SQLCompleter implements Completer
{
    /* (non-Javadoc)
     * @see jline.console.completer.Completer#complete(java.lang.String, int, java.util.List)
     */
    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates)
    {
        // For now only support tab completion at the end of the line.
        if (cursor == buffer.length()) {
            // Check for an initial token match?
            if (cursor <= SQLCommand.m_maxCommandLength) {
                final String bufferu = buffer.toUpperCase();
                for (final String command : SQLCommand.m_commands) {
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