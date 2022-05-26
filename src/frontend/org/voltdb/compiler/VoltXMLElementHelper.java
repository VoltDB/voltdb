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

package org.voltdb.compiler;

import java.util.List;
import java.util.ArrayList;
import org.hsqldb_voltpatches.VoltXMLElement;

/**
 * Helper functions for assembling VoltXMLElement.
 * ENG-8641
 */
public class VoltXMLElementHelper {

    // Find the current maximum element id (recursively).
    public static int findMaxElementId(VoltXMLElement root) {
        int retval = 0;
        if (root != null) {
            String strId = root.attributes.get("id");
            if (strId != null) {
                retval = Integer.parseInt(strId);
            }
            for (VoltXMLElement child : root.children) {
                int childMaxId = findMaxElementId(child);
                if (retval < childMaxId) {
                    retval = childMaxId;
                }
            }
        }
        return retval;
    }

    public static VoltXMLElement getFirstChild(VoltXMLElement root, String name, boolean createIfNotExist) {
        if (root == null || name == null) { return null; }

        List<VoltXMLElement> elementList = root.findChildren(name);
        if (elementList.size() < 1) {
            if (createIfNotExist) {
                VoltXMLElement retval = new VoltXMLElement(name);
                root.children.add(retval);
                return retval;
            }
            else {
                return null;
            }
        }
        else {
            return elementList.get(0);
        }
    }

    public static VoltXMLElement getFirstChild(VoltXMLElement root, String name) {
        return getFirstChild( root, name, false );
    }

    // Merge two XML elements with a parent element of operation.
    // e.g. and, or, equal, greaterthan
    // If one of the elements is null, return the other one diectly.
    public static VoltXMLElement mergeTwoElementsUsingOperator(String opName,
                                                               String opElementId,
                                                               VoltXMLElement first,
                                                               VoltXMLElement second) {
        if (first == null || second == null) {
            return first == null ? second : first;
        }
        if (opName == null || opElementId == null) { return null; }

        VoltXMLElement retval = new VoltXMLElement("operation");
        retval.attributes.put( "id", opElementId );
        retval.attributes.put( "optype", opName );
        retval.children.add( first );
        retval.children.add( second );
        return retval;
    }

    public static VoltXMLElement buildValueElement(String elementId, boolean isParam, String value, String valueType) {
        if (elementId == null) { return null; }

        VoltXMLElement retval = new VoltXMLElement("value");
        retval.attributes.put("id", elementId);
        if (isParam) {
            retval.attributes.put("isparam", "true");
            return retval;
        }

        if (value == null || valueType == null) { return null; }
        retval.attributes.put("value", value);
        retval.attributes.put("valuetype", valueType);
        return retval;
    }

    public static VoltXMLElement buildValueElement(String elementId) {
        return buildValueElement(elementId, true, null, null);
    }

    // Build VoltXMLElement for expression like "LIMIT 1".
    public static List<VoltXMLElement> buildLimitElements(int limit, String limitValueElementId) {
        if (limitValueElementId == null) { return null; }

        List<VoltXMLElement> retval = new ArrayList<VoltXMLElement>();
        retval.add( new VoltXMLElement("offset") );

        VoltXMLElement limitElement = new VoltXMLElement("limit");
        String strLimit = String.valueOf( limit );
        limitElement.attributes.put( "limit", strLimit );
        limitElement.children.add( buildValueElement( limitValueElementId, false, strLimit, "BIGINT" ) );
        retval.add( limitElement );
        return retval;
    }

    // Build VoltXMLElement for expression like "column=?".
    public static VoltXMLElement buildColumnParamJoincondElement(String opName, VoltXMLElement leftElement, String valueParamElementId, String opElementId) {
        VoltXMLElement valueParamElement = buildValueElement(valueParamElementId);
        return mergeTwoElementsUsingOperator(opName, opElementId, leftElement, valueParamElement);
    }

    // Build an element to be inserted under the "parameters" tree.
    public static VoltXMLElement buildParamElement(String elementId, String index, String valueType) {
        VoltXMLElement retval = new VoltXMLElement("parameter");
        retval.attributes.put("id", elementId);
        retval.attributes.put("index", index);
        retval.attributes.put("valuetype", valueType);
        return retval;
    }

    public static VoltXMLElement buildOrderColumnsElement(VoltXMLElement orderbyColumn, boolean desc, String elementId) {
        VoltXMLElement retval = new VoltXMLElement("orderby");
        if (desc) {
            retval.attributes.put("desc", "true");
        }
        retval.attributes.put("id", elementId);
        retval.children.add(orderbyColumn);
        return retval;
    }

}
