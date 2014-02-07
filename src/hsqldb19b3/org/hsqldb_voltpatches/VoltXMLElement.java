/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Used to fake generate XML without actually generating the text and parsing it.
 * A performance optimization, and something of a simplicity win
 *
 */
public class VoltXMLElement {

    public String name;
    public final Map<String, String> attributes = new TreeMap<String, String>();
    public final List<VoltXMLElement> children = new ArrayList<VoltXMLElement>();

    public VoltXMLElement(String name) {
        this.name = name;
    }

    public VoltXMLElement withValue(String key, String value) {
        attributes.put(key, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        append(sb, "");
        return sb.toString();
    }

    private void append(StringBuilder sb, String indent) {
        sb.append(indent).append("ELEMENT: ").append(name).append("\n");
        for (Entry<String, String> e : attributes.entrySet()) {
            sb.append(indent).append(" ").append(e.getKey());
            sb.append(" = ").append(e.getValue()).append("\n");
        }
        if ( ! children.isEmpty()) {
            sb.append(indent).append("[").append("\n");
            for (VoltXMLElement e : children) {
                e.append(sb, indent + "   ");
            }
        }
    }

    public VoltXMLElement duplicate() {
        VoltXMLElement retval = new VoltXMLElement(name);
        for (Entry<String, String> e : attributes.entrySet()) {
            retval.attributes.put(e.getKey(), e.getValue());
        }
        for (VoltXMLElement child : children) {
            retval.children.add(child.duplicate());
        }
        return retval;
	}

    /**
     * Get a string representation that is designed to be as short as possible
     * with as much certainty of uniqueness as possible.
     * A SHA-1 hash would suffice, but here's hoping just dumping to a string is
     * faster. Will measure later.
     */
    public String toMinString() {
        StringBuilder sb = new StringBuilder();
        toMinString(sb);
        return sb.toString();
    }

    protected StringBuilder toMinString(StringBuilder sb) {
        sb.append("\tE").append(name).append('\t');
        for (Entry<String, String> e : attributes.entrySet()) {
            sb.append('\t').append(e.getKey());
            sb.append('\t').append(e.getValue());
        }
        sb.append("\t[");
        for (VoltXMLElement e : children) {
            e.toMinString(sb);
        }
        return sb;
    }
}
