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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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

    public VoltXMLElement findChild(String name)
    {
        for (VoltXMLElement vxe : children) {
            if (name.equals(vxe.name)) {
                return vxe;
            }
        }
        return null;
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

    static public class VoltXMLDiff
    {
        List<VoltXMLElement> m_addedNodes = new ArrayList<VoltXMLElement>();
        List<VoltXMLElement> m_removedNodes = new ArrayList<VoltXMLElement>();
        List<VoltXMLElement> m_changedNodes = new ArrayList<VoltXMLElement>();
        List<String> m_addedAttributes = new ArrayList<String>();
        List<String> m_removedAttributes = new ArrayList<String>();
        List<String> m_changedAttributes = new ArrayList<String>();

        public List<VoltXMLElement> getAddedNodes()
        {
            return m_addedNodes;
        }

        public List<VoltXMLElement> getRemovedNodes()
        {
            return m_removedNodes;
        }

        public List<VoltXMLElement> getChangedNodes()
        {
            return m_changedNodes;
        }

        public List<String> getAddedAttributes()
        {
            return m_addedAttributes;
        }

        public List<String> getRemovedAttributes()
        {
            return m_removedAttributes;
        }

        public List<String> getChangedAttributes()
        {
            return m_changedAttributes;
        }

        public boolean isEmpty()
        {
            return (m_addedNodes.isEmpty() &&
                    m_removedNodes.isEmpty() &&
                    m_changedNodes.isEmpty() &&
                    m_addedAttributes.isEmpty() &&
                    m_removedAttributes.isEmpty() &&
                    m_changedAttributes.isEmpty());
        }
    }

    static public VoltXMLDiff computeDiff(VoltXMLElement first, VoltXMLElement second)
    {
        VoltXMLDiff result = new VoltXMLDiff();
        // Top level call needs both names to match (I think this makes sense)
        // Just treat it as first removed and second added and don't descend
        if (!first.name.equals(second.name)) {
            result.m_removedNodes.add(first);
            result.m_addedNodes.add(second);
        }
        else {
            // first, check the attributes
            Set<String> firstKeys = first.attributes.keySet();
            Set<String> secondKeys = new HashSet<String>();
            secondKeys.addAll(second.attributes.keySet());
            // Do removed and changed attributes walking the first element's attributes
            for (String firstKey : firstKeys) {
                if (!secondKeys.contains(firstKey)) {
                    result.m_removedAttributes.add(firstKey);
                }
                else if (!(second.attributes.get(firstKey).equals(first.attributes.get(firstKey)))) {
                    result.m_changedAttributes.add(firstKey);
                }
                // remove the firstKey from secondKeys to track things added
                secondKeys.remove(firstKey);
            }
            // everything in secondKeys should be something added
            result.m_addedAttributes.addAll(secondKeys);

            // Now, need to check the children.  Each pair of children with the same names
            // need to be descended to look for changes
            // Probably more efficient ways to do this, but brute force it for now
            // Would be helpful if the underlying children objects were Maps rather than
            // Lists.
            Set<String> firstChildren = new HashSet<String>();
            for (VoltXMLElement child : first.children) {
                firstChildren.add(child.name);
            }
            Set<String> secondChildren = new HashSet<String>();
            for (VoltXMLElement child : second.children) {
                secondChildren.add(child.name);
            }
            Set<String> commonNames = new HashSet<String>();
            for (VoltXMLElement firstChild : first.children) {
                if (!secondChildren.contains(firstChild.name)) {
                    result.m_removedNodes.add(firstChild);
                }
                else {
                    commonNames.add(firstChild.name);
                }
            }
            for (VoltXMLElement secondChild : second.children) {
                if (!firstChildren.contains(secondChild.name)) {
                    result.m_addedNodes.add(secondChild);
                }
                else {
                    assert(commonNames.contains(secondChild.name));
                }
            }

            for (String name : commonNames) {
                VoltXMLDiff childDiff = computeDiff(first.findChild(name), second.findChild(name));
                if (!childDiff.isEmpty()) {
                    result.m_changedNodes.add(second.findChild(name));
                }
            }
        }

        return result;
    }
}
