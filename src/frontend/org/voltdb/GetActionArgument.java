/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb;

public enum GetActionArgument {

    DEPLOYMENT("deployment") {
        @Override
        public String getDefaultOutput() {
            return "deployment.xml";
        }
    },
    SCHEMA("schema") {
        @Override
        public String getDefaultOutput() {
            return "schema.sql";
        }
    },
    CLASSES("classes") {
        @Override
        public String getDefaultOutput() {
            return "procedures.jar";
        }
    };

    final String m_resource;

    public String getDefaultOutput() { return ""; }
    public String getVerb() { return m_resource; }

//    GetActionArgument() {
//        m_resource = name().toLowerCase();
//    }

    GetActionArgument(String verb) {
        m_resource = verb;
    }

    public static String supportedVerbs() {
        StringBuilder verbNames = new StringBuilder();
        for (GetActionArgument verb: GetActionArgument.values()) {
            verbNames.append(verb.name().toLowerCase() + " ");
        }
        return verbNames.toString();
    }
}
