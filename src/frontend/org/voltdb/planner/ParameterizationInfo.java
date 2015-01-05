/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.ParameterConverter;
import org.voltdb.ParameterSet;
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

        Map<String, Integer> idToParamIndexMap = new HashMap<String, Integer>();
        List<String> paramValues = new ArrayList<String>();

        parameterizeRecursively(parameterizedXmlSQL, idToParamIndexMap, paramValues);

        ParameterizationInfo info = null;
        if(idToParamIndexMap.size() > 0) {
            info = new ParameterizationInfo(
                xmlSQL, parameterizedXmlSQL,
                paramValues.toArray(new String[paramValues.size()]));
        }
        return info;
    }

    public static void parameterizeRecursively(VoltXMLElement parameterizedXmlSQL,
                                    Map<String, Integer> idToParamIndexMap,
                                    List<String> paramValues) {

        if (parameterizedXmlSQL.name.equals("union")) {
            // UNION has its parameters on the individual selects level
            for (VoltXMLElement xmlChildSQL : parameterizedXmlSQL.children) {
                parameterizeRecursively(xmlChildSQL, idToParamIndexMap, paramValues);
            }
        } else {
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
                return;
            }

            parameterizeRecursively(parameterizedXmlSQL, paramsNode,
                    idToParamIndexMap, paramValues);
        }
    }

    static void parameterizeRecursively(VoltXMLElement node,
                                        VoltXMLElement paramsNode,
                                        Map<String, Integer> idToParamIndexMap,
                                        List<String> paramValues) {
        if (node.name.equals("value")) {
            String idStr = node.attributes.get("id");
            assert(idStr != null);

            // A value id is currently a "string-formatted long", but there's no need to commit
            // to that format in this early processing -- here, the id just needs to be a unique
            // string for each parsed value. It allows hsql to replicate a parameter reference
            // within its parse trees without causing code like this to lose track of its identity.
            Integer paramIndex = idToParamIndexMap.get(idStr);
            if (paramIndex == null) {
                // Use the next param index for each new value with an unfamiliar id,
                // starting at 0.
                paramIndex = paramValues.size();
                // Later references to this value's id will re-use this same param index.
                idToParamIndexMap.put(idStr, paramIndex);

                VoltXMLElement paramIndexNode = new VoltXMLElement("parameter");
                paramIndexNode.attributes.put("index", String.valueOf(paramIndex));
                String typeStr = node.attributes.get("valuetype");
                paramIndexNode.attributes.put("valuetype", typeStr);
                paramIndexNode.attributes.put("id", idStr);
                paramsNode.children.add(paramIndexNode);

                String value = null;
                if (VoltType.typeFromString(typeStr) != VoltType.NULL) {
                    value = node.attributes.get("value");
                }
                paramValues.add(value);
            }

            // Assume that all values, whether or not their ids have been seen before, can
            // be considered planner-generated parameters (proxies for user-provided constants).
            // This is one simplification that leverages the fact that statements that came with
            // user-provided parameters were barred from being (further) parameterized.
            node.attributes.put("isparam", "true");
            node.attributes.put("isplannergenerated", "true");

            // Remove the "value" attribute -- this is the critical step to folding
            // different raw VoltXML trees into the same parameterized VoltXML tree.
            // The value differences are extracted into paramValues for future reference by:
            //     the execution engine which needs to substitute actual values for all parameters
            //         to run the query
            //     the index scan planner which may need to "bind" the parameters to their original
            //     values to apply indexes on expressions like "(colA + 2 * colB)" when used in a query
            //     like "... WHERE t2.colA + 2 * t2.colB > 3*t1.colC".
            node.attributes.remove("value");
        }

        for (VoltXMLElement child : node.children) {
            parameterizeRecursively(child, paramsNode, idToParamIndexMap, paramValues);
        }
    }

    public static Object valueForStringWithType(String value, VoltType type) {
        if (type == VoltType.NULL) {
            return null;
        }

        // leverage existing (rather heavyweight) code to convert param types
        Object retval = ParameterConverter.tryToMakeCompatible(type.classFromType(), value);
        // check the result type in an assert
        assert(ParameterConverter.verifyParameterConversion(retval, type.classFromType()));
        return retval;
    }

    public ParameterSet extractedParamValues(VoltType[] parameterTypes) {
        assert(paramLiteralValues.length == parameterTypes.length);
        Object[] params = new Object[paramLiteralValues.length];

        // the extracted params are all strings at first.
        // after the planner infers their types, fix them up
        // the only exception is that nulls are Java NULL, and not the string "null".
        for (int i = 0; i < paramLiteralValues.length; i++) {
            params[i] = valueForStringWithType(paramLiteralValues[i], parameterTypes[i]);
        }
        return ParameterSet.fromArrayNoCopy(params);
    }
}
