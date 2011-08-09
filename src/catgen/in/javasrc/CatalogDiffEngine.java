/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

    // contains the text of the difference
    private final StringBuilder m_sb;

    // true if the difference is allowed in a running system
    private boolean m_supported;

    // collection of reasons why a diff is not supported
    private final StringBuilder m_errors;

    /**
     * Instantiate a new diff. The resulting object can return the text
     * of the difference and report whether the difference is allowed in a
     * running system.
     * @param prev Tip of the old catalog.
     * @param next Tip of the new catalog.
     */
    public CatalogDiffEngine(final CatalogType prev, final CatalogType next) {
        m_sb = new StringBuilder();
        m_errors = new StringBuilder();
        m_supported = true;
        diffRecursively(prev, next);
    }

    public String commands() {
        return m_sb.toString();
    }

    public boolean supported() {
        return m_supported;
    }

    public String errors() {
        return m_errors.toString();
    }

    /**
     * @return true if the CatalogType can be dynamically added and removed
     * from a running system.
     */
    boolean checkAddDropWhitelist(CatalogType suspect) {
        // should generate this from spec.txt
        CatalogType orig = suspect;

        // Support add/drop of only the top level object.
        if (suspect instanceof Table)
            return true;

        // Support add/drop anywhere in these sub-trees
        do {
            if (suspect instanceof User)
                return true;
            if (suspect instanceof Group)
                return true;
            if (suspect instanceof Procedure)
                return true;
            if (suspect instanceof Connector)
                return true;
            if (suspect instanceof SnapshotSchedule)
                return true;
        } while ((suspect = suspect.m_parent) != null);

        m_errors.append("May not dynamically add/drop: " + orig + "\n");
        m_supported = false;
        return false;
    }

    /**
     * @return true if CatalogType can be dynamically modified
     * in a running system.
     */
    boolean checkModifyWhitelist(CatalogType suspect, String field) {
        // should generate this from spec.txt
        CatalogType orig = suspect;

        // Support modification of these specific fields
        if (suspect instanceof Database && field.equals("schema"))
            return true;
        if (suspect instanceof Cluster && field.equals("securityEnabled"))
            return true;

        // Support modification of these entire sub-trees
        do {
            if (suspect instanceof User)
                return true;
            if (suspect instanceof Group)
                return true;
            if (suspect instanceof Procedure)
                return true;
            if (suspect instanceof SnapshotSchedule)
                return true;
        } while ((suspect = suspect.m_parent) != null);

        m_errors.append("May not dynamically modify field " + field + " of " + orig + "\n");
        m_supported = false;
        return false;
    }

    /**
     * Add a modification
     */
    private void writeModification(CatalogType newType, String field)
    {
        checkModifyWhitelist(newType, field);
        newType.writeCommandForField(m_sb, field, true);
    }

    /**
     * Add a deletion
     */
    private void writeDeletion(CatalogType prevType, String mapName, String name)
    {
        checkAddDropWhitelist(prevType);
        m_sb.append("delete ").append(prevType.getParent().getPath()).append(" ");
        m_sb.append(mapName).append(" ").append(name).append("\n");
    }

    /**
     * Add an addition
     */
    private void writeAddition(CatalogType newType) {
        checkAddDropWhitelist(newType);
        newType.writeCreationCommand(m_sb);
        newType.writeFieldCommands(m_sb);
        newType.writeChildCommands(m_sb);
    }


    /**
     * Pre-order walk of catalog generating add, delete and set commands
     * that compose that full difference.
     * @param prevType
     * @param newType
     */
    void diffRecursively(CatalogType prevType, CatalogType newType)
    {
        assert(prevType != null) : "Null previous object found in catalog diff traversal.";
        assert(newType != null) : "Null new object found in catalog diff traversal";

        // diff local fields
        for (String field : prevType.getFields()) {
            if (field.equals("isUp"))
            {
                continue;
            }
            // check if the types are different
            // options are: both null => same
            //              one null and one not => different
            //              both not null => check Object.equals()
            Object prevValue = prevType.getField(field);
            Object newValue = newType.getField(field);
            if ((prevValue == null) != (newValue == null)) {
                writeModification(newType, field);
            }
            // if they're both not null (above/below ifs implies this)
            else if (prevValue != null) {
                // if comparing CatalogTypes (both must be same)
                if (prevValue instanceof CatalogType) {
                    assert(newValue instanceof CatalogType);
                    String prevPath = ((CatalogType) prevValue).getPath();
                    String newPath = ((CatalogType) newValue).getPath();
                    if (prevPath.compareTo(newPath) != 0) {
                        writeModification(newType, field);
                    }
                }
                // if scalar types
                else {
                    if (prevValue.equals(newValue) == false) {
                        writeModification(newType, field);
                    }
                }
            }
        }

        // recurse
        for (String field : prevType.m_childCollections.keySet()) {
            CatalogMap<? extends CatalogType> prevMap = prevType.m_childCollections.get(field);
            CatalogMap<? extends CatalogType> newMap = newType.m_childCollections.get(field);
            getCommandsToDiff(field, prevMap, newMap);
        }
    }


    /**
     * Check if all the children in prevMap are present and identical in newMap.
     * Then, check if anything is in newMap that isn't in prevMap.
     * @param mapName
     * @param prevMap
     * @param newMap
     */
    void getCommandsToDiff(String mapName,
                           CatalogMap<? extends CatalogType> prevMap,
                           CatalogMap<? extends CatalogType> newMap)
    {
        assert(prevMap != null);
        assert(newMap != null);

        // in previous, not in new
        for (CatalogType prevType : prevMap) {
            String name = prevType.getTypeName();
            CatalogType newType = newMap.get(name);
            if (newType == null) {
                writeDeletion(prevType, mapName, name);
                continue;
            }

            diffRecursively(prevType, newType);
        }

        // in new, not in previous
        for (CatalogType newType : newMap) {
            CatalogType prevType = prevMap.get(newType.getTypeName());
            if (prevType != null) continue;
            writeAddition(newType);
        }
    }
}
