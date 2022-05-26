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

package org.hsqldb_voltpatches;

/**
 *  Information about DDL passed to HSQL that allows it to be better understood by VoltDB
 */
public class HSQLDDLInfo {

    /**
     * CREATE, ALTER or DROP
     */
    public static enum Verb {
        CREATE, ALTER, DROP;

        public static Verb get(String name) {
            if (name.equalsIgnoreCase("CREATE")) {
                return CREATE;
            }
            else if (name.equalsIgnoreCase("ALTER")) {
                return ALTER;
            }
            else if (name.equalsIgnoreCase("DROP")) {
                return DROP;
            }
            else {
                return null;
            }
        }
    }

    /**
     * TABLE, INDEX or VIEW
     */
    public static enum Noun {
        TABLE, INDEX, VIEW, STREAM;

        public static Noun get(String name) {
            if (name.equalsIgnoreCase("TABLE")) {
                return TABLE;
            }
            else if (name.equalsIgnoreCase("INDEX")) {
                return INDEX;
            }
            else if (name.equalsIgnoreCase("VIEW")) {
                return VIEW;
            }
            else if (name.equalsIgnoreCase("STREAM")) {
                return STREAM;
            }
            else {
                return null;
            }
        }
    }

    public final HSQLDDLInfo.Verb verb;
    public final HSQLDDLInfo.Noun noun;
    // the index/table/view that goes with the noun (subject of the verb)
    public final String name;
    // used today to hold the table that new indexes are created on
    // CREATE INDEX name ON TABLE secondName ...
    public final String secondName;
    public final boolean cascade;
    public final boolean ifexists;
    public final boolean creatStream;

    public HSQLDDLInfo(HSQLDDLInfo.Verb verb,
                       HSQLDDLInfo.Noun noun,
                       String name,
                       String secondName,
                       boolean cascade,
                       boolean ifexists,
                       boolean createStream)
    {
        this.verb = verb;
        this.noun = noun;
        this.name = name;
        this.secondName = secondName;
        this.cascade = cascade;
        this.ifexists = ifexists;
        this.creatStream = createStream;
    }
}
