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

public class CatalogCommand {
    public final char cmd;
    public final String path;
    public final String arg1;
    public final String arg2;

    public CatalogCommand(String cmdStr) {
        int pos = 0;
        cmdStr = cmdStr.trim();
        // Command comes before the first space (add or set)
        cmd = cmdStr.charAt(pos++);
        while (cmdStr.charAt(pos++) != ' ');

        // Path to a catalog node between first two spaces
        int pathStart = pos;
        while (cmdStr.charAt(pos++) != ' ');
        path = cmdStr.substring(pathStart, pos - 1);

        // spaces 2 & 3 separate the two arguments
        int argStart = pos;
        while (cmdStr.charAt(pos++) != ' ');
        arg1 = cmdStr.substring(argStart, pos - 1);
        arg2 = cmdStr.substring(pos);
    }

    boolean isProcedureRelatedCmd() {
        if (path.indexOf("procedures#") != -1) {
            return true;
        }
        if ("procedures".equals(arg1) && path.endsWith("#database")) {
            return true;
        }
        return false;
    }
}
