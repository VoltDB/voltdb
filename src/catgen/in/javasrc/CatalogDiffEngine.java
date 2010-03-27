/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.catalog;

public class CatalogDiffEngine {

    public static String getCommandsToDiff(Catalog prevCat, Catalog newCat) {
        StringBuilder sb = new StringBuilder();

        getCommandsToDiff(prevCat, newCat, sb);

        String retval = sb.toString();

        // perform sanity checks on retval here...

        return retval;
    }

    // recursively do a preorder traversal
    static void getCommandsToDiff(CatalogType prevType, CatalogType newType, StringBuilder sb) {
        assert(prevType != null);
        assert(newType != null);

        // handle all of the local fields
        for (String field : prevType.getFields()) {
            if (field.equals("isUp"))
            {
                continue;
            }
            Object prevValue = prevType.getField(field);
            Object newValue = newType.getField(field);

            // check if the types are different
            // options are: both null => same
            //              one null and one not => different
            //              both not null => check Object.equals()
            if (((prevValue == null) != (newValue == null)) ||
                ((prevValue != null) && (prevValue.equals(newValue) == false))) {

                /*if (field.equals("fullplan")) {
                    String planHex = (String) prevValue;
                    String prevPlan = HexEncoder.hexDecodeToString(planHex);
                    planHex = (String) newValue;
                    String newPlan = HexEncoder.hexDecodeToString(planHex);

                    System.err.println("==== OLD ====");
                    System.err.println(prevPlan);
                    System.err.println("==== NEW ====");
                    System.err.println(newPlan);
                    System.err.println("==== END ====");
                }*/

                newType.writeCommandForField(sb, field);
            }
        }

        // handle all the children of this node
        for (String field : prevType.m_childCollections.keySet()) {
            CatalogMap<? extends CatalogType> prevMap = prevType.m_childCollections.get(field);
            assert(prevMap != null);
            CatalogMap<? extends CatalogType> newMap = newType.m_childCollections.get(field);
            assert(newMap != null);

            getCommandsToDiff(field, prevMap, newMap, sb);
        }
    }

    static void getCommandsToDiff(String mapName, CatalogMap<? extends CatalogType> prevMap, CatalogMap<? extends CatalogType> newMap,
            StringBuilder sb) {

        assert(prevMap != null);
        assert(newMap != null);

        // check if all the children in prevMap are present and identical in newMap
        for (CatalogType prevType : prevMap) {

            String name = prevType.getTypeName();
            CatalogType newType = newMap.get(name);
            if (newType == null) {
                // write the catalog delete command of the form:
                // delete parentname mapname childname
                sb.append("delete ").append(prevType.getParent().getPath()).append(" ");
                sb.append(mapName).append(" ").append(name).append("\n");
                continue;
            }

            getCommandsToDiff(prevType, newType, sb);
        }

        // check if anything is in newMap that isn't in prevMap
        for (CatalogType newType : newMap) {
            CatalogType prevType = prevMap.get(newType.getTypeName());
            if (prevType != null) continue;
            // need to add the child
            newType.writeCreationCommand(sb);
            newType.writeFieldCommands(sb);
            newType.writeChildCommands(sb);
        }
    }

}
