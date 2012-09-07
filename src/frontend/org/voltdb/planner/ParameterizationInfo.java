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

/**
 * Given a SQL statements plan from HSQLDB, as our fake XML tree,
 * find all of the constant value expression and turn them into
 * parameters, mutating the fake XML tree in place.
 *
 * Returns the extracted params as strings because we don't 100%
 * HSQLDB's typing (ours is better). We do convert values typed as
 * null to Java null though.
 *
 */
class ParameterizationInfo {

    final VoltXMLElement originalXmlSQL;
    final VoltXMLElement parameterizedXmlSQL;
    final String[] paramLiteralValues;

    public ParameterizationInfo(VoltXMLElement originalXmlSQL,
                                VoltXMLElement parameterizedXmlSQL,
                                String[] paramLiteralValues)
    {
        assert(parameterizedXmlSQL != null);
        assert(originalXmlSQL != null);
        assert(paramLiteralValues != null);

        this.originalXmlSQL = originalXmlSQL;
        this.parameterizedXmlSQL = parameterizedXmlSQL;
        this.paramLiteralValues = paramLiteralValues;
    }

    public static ParameterizationInfo parameterize(VoltXMLElement xmlSQL) {
        assert(xmlSQL != null);

        VoltXMLElement parameterizedXmlSQL = xmlSQL.duplicate();

        // find the parameters xml node
        VoltXMLElement paramsNode = null;
        for (VoltXMLElement child : parameterizedXmlSQL.children) {
            if (child.name.equals("parameters")) {
                paramsNode = child;
            }
        }
        assert(paramsNode != null);

        // don't optimize plans with params yet
        if (paramsNode.children.size() > 0) {
            return null;
        }

        Map<Long, Integer> idToParamIndexMap = new HashMap<Long, Integer>();
        List<String> paramValues = new ArrayList<String>();

        parameterizeRecursively(parameterizedXmlSQL, paramsNode,
                idToParamIndexMap, paramValues);

        ParameterizationInfo info = new ParameterizationInfo(
                xmlSQL, parameterizedXmlSQL,
                paramValues.toArray(new String[paramValues.size()]));

        return info;
    }

    static void parameterizeRecursively(VoltXMLElement node,
                                        VoltXMLElement paramsNode,
                                        Map<Long, Integer> idToParamIndexMap,
                                        List<String> paramValues) {
        if (node.name.equals("value")) {
            String idStr = node.attributes.get("id");
            assert(idStr != null);
            long id = Long.parseLong(idStr);

            String typeStr = node.attributes.get("type");
            VoltType type = VoltType.typeFromString(typeStr);

            Integer paramIndex = idToParamIndexMap.get(id);
            if (paramIndex == null) {
                paramIndex = paramValues.size();

                VoltXMLElement paramIndexNode = new VoltXMLElement("parameter");
                paramIndexNode.attributes.put("index", String.valueOf(paramIndex));
                paramIndexNode.attributes.put("type", typeStr);
                paramIndexNode.attributes.put("id", idStr);
                paramsNode.children.add(paramIndexNode);

                idToParamIndexMap.put(id, paramIndex);

                String value = node.attributes.get("value");
                if (type == VoltType.NULL) {
                    value = null;
                }

                paramValues.add(value);
            }

            node.attributes.put("isparam", "true");
            node.attributes.remove("value");
        }

        for (VoltXMLElement child : node.children) {
            parameterizeRecursively(child, paramsNode, idToParamIndexMap, paramValues);
        }
    }

    public static Object valueForStringWithType(String value, VoltType type) throws Exception {
        if (type == VoltType.NULL) {
            return null;
        }

        // leverage existing (rather heavyweight) code to convert param types
        return ParameterConverter.tryToMakeCompatible(false, false, type.classFromType(), null, value);
    }
}
