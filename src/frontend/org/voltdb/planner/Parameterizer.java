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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.ParameterConverter;
import org.voltdb.VoltType;

class Parameterizer {

    final VoltXMLElement m_root;
    VoltXMLElement m_paramsNode = null;
    Map<Long, Integer> m_idToParamIndexMap;
    List<Object> m_paramValues;

    public Parameterizer(VoltXMLElement xmlSQL) {
        m_root = xmlSQL;
        assert(m_root != null);

        // find the parameters xml node
        for (VoltXMLElement child : m_root.children) {
            if (child.name.equals("parameters")) {
                m_paramsNode = child;
            }
        }
        assert(m_paramsNode != null);
    }

    public int countParams() {
        return m_paramsNode.children.size();
    }

    public Object[] parameterize() {
        // don't optimize plans with params yet
        if (m_paramsNode.children.size() > 0) {
            return null;
        }

        m_idToParamIndexMap = new HashMap<Long, Integer>();
        m_paramValues = new ArrayList<Object>();

        parameterizeRecursively(m_root);

        return m_paramValues.toArray();
    }

    void parameterizeRecursively(VoltXMLElement node) {
        if (node.name.equals("value")) {
            String idStr = node.attributes.get("id");
            assert(idStr != null);
            long id = Long.parseLong(idStr);

            String typeStr = node.attributes.get("type");
            VoltType type = VoltType.typeFromString(typeStr);

            Integer paramIndex = m_idToParamIndexMap.get(id);
            if (paramIndex == null) {
                paramIndex = m_paramValues.size();

                VoltXMLElement paramIndexNode = new VoltXMLElement("parameter");
                paramIndexNode.attributes.put("index", String.valueOf(paramIndex));
                paramIndexNode.attributes.put("type", typeStr);
                paramIndexNode.attributes.put("id", idStr);
                m_paramsNode.children.add(paramIndexNode);

                m_idToParamIndexMap.put(id, paramIndex);

                String value = node.attributes.get("value");
                if (type == VoltType.NULL) {
                    value = null;
                }

                m_paramValues.add(value);
            }

            node.attributes.put("isparam", "true");
            node.attributes.remove("value");
        }

        for (VoltXMLElement child : node.children) {
            parameterizeRecursively(child);
        }
    }

    public static Object valueForStringWithType(String value, VoltType type) throws Exception {
        if (type == VoltType.NULL) {
            return null;
        }

        return ParameterConverter.tryToMakeCompatible(false, false, type.classFromType(), null, value);


        /*switch (type) {
        case NULL:
            return null;
        case BIGINT:
            return Long.parseLong(value);
        case INTEGER:
            return Integer.parseInt(value);
        case SMALLINT:
            return Short.parseShort(value);
        case TINYINT:
            return Byte.parseByte(value);
        case FLOAT:
            return Double.parseDouble(value);
        case TIMESTAMP:
            return value;
        default:
            return value;
        }*/
    }
}
