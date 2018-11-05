/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

/**
 * Command to make changes to a catalog.
 * @author Yiqun Zhang
 * @since 8.4
 */
public final class CatalogCommand {
    public final char cmd;
    public final String path;
    public final String arg1;
    public final String arg2;

    /**
     * Create a catalog command from a serialized string.
     * @param cmdStr serialized command string.
     */
    public CatalogCommand(String cmdStr) {
        int pos = 0;
        cmdStr = cmdStr.trim();
        cmd = cmdStr.charAt(pos++);
        while (cmdStr.charAt(pos++) != ' ');

        int pathStart = pos;
        while (cmdStr.charAt(pos++) != ' ');
        path = cmdStr.substring(pathStart, pos - 1);

        int argStart = pos;
        while (cmdStr.charAt(pos++) != ' ');
        arg1 = cmdStr.substring(argStart, pos - 1);
        arg2 = cmdStr.substring(pos);
    }
}
