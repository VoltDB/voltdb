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

package org.voltdb.catalog;

import java.util.HashMap;
import java.util.Map;

import org.voltdb.types.ConstraintType;

public class CatalogDiffEngine {

    // contains the text of the difference
    private final StringBuilder m_sb;

    // true if the difference is allowed in a running system
    private boolean m_supported;

    // collection of reasons why a diff is not supported
    private final StringBuilder m_errors;

    // original tables/indexes kept to check whether a new unique index is possible
    private final Map<String, CatalogMap<Index>> m_originalIndexesByTable = new HashMap<String, CatalogMap<Index>>();

    /**
     * Instantiate a new diff. The resulting object can return the text
     * of the difference and report whether the difference is allowed in a
     * running system.
     * @param prev Tip of the old catalog.
     * @param next Tip of the new catalog.
     */
    public CatalogDiffEngine(final Catalog prev, final Catalog next) {
        m_sb = new StringBuilder();
        m_errors = new StringBuilder();
        m_supported = true;

        // store the original tables so some extra checking can be done with
        // constraints and unique indexes
        CatalogMap<Table> tables = prev.getClusters().get("cluster").getDatabases().get("database").getTables();
        assert(tables != null);
        for (Table t : tables) {
            m_originalIndexesByTable.put(t.getTypeName(), t.getIndexes());
        }

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

    enum ChangeType {
        ADDITION, DELETION
    }

    /**
     * Check if a candidate unique index (for addition) covers an existing unique index.
     * If a unique index exists on a subset of the columns, then the less specific index
     * can be created without failing.
     */
    private boolean indexCovers(Index newIndex, Index existingIndex) {
        assert(newIndex.getParent().getTypeName().equals(existingIndex.getParent().getTypeName()));

        // non-unique indexes don't help with this check
        if (existingIndex.getUnique() == false) {
            return false;
        }

        // expression indexes only help if they are on exactly the same expressions in the same order.
        // OK -- that's obviously overspecifying the requirement, since expression order has nothing
        // to do with it, and uniqueness of just a subset of the new index expressions would do, but
        // that's hard to check for, so we punt on optimized dynamic update except for the critical
        // case of grand-fathering in a surviving pre-existing index.
        if (existingIndex.getExpressionsjson().length() > 0) {
            if (existingIndex.getExpressionsjson().equals(newIndex.getExpressionsjson())) {
                return true;
            } else {
                return false;
            }
        } else if (newIndex.getExpressionsjson().length() > 0) {
            // A column index does not generally provide coverage for an expression index,
            // though there are some special cases not being recognized, here,
            // like expression indexes that list a mix of non-column expressions and unique columns.
            return false;
        }

        // iterate over all of the existing columns
        for (ColumnRef existingColRef : existingIndex.getColumns()) {
            boolean foundMatch = false;
            // see if the current column is also in the candidate index
            // for now, assume the tables in question have the same schema
            for (ColumnRef colRef : newIndex.getColumns()) {
                int index1 = colRef.getColumn().getIndex();
                int index2 = existingColRef.getColumn().getIndex();
                if (index1 == index2) {
                    foundMatch = true;
                    break;
                }
            }
            // if this column isn't covered
            if (!foundMatch) {
                return false;
            }
        }

        // There exists a unique index that contains a subset of the columns in the new index
        return true;
    }

    /**
     * Check if there is a unique index that exists in the old catalog
     * that is covered by the new index. That would mean adding this index
     * can't fail with a duplicate key.
     *
     * @param newIndex The new index to check.
     * @return True if the index can be created without a chance of failing.
     */
    private boolean checkNewUniqueIndex(Index newIndex) {
        Table table = (Table) newIndex.getParent();
        CatalogMap<Index> existingIndexes = m_originalIndexesByTable.get(table.getTypeName());
        for (Index existingIndex : existingIndexes) {
            if (indexCovers(newIndex, existingIndex)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true if the CatalogType can be dynamically added or removed
     * from a running system.
     */
    private boolean checkAddDropWhitelist(CatalogType suspect, ChangeType changeType) {
        // should generate this from spec.txt
        CatalogType orig = suspect;

        // Support add/drop of only the top level object.
        if (suspect instanceof Table) {
            return true;
        }

        // allow addition/deletion of indexes except for the addition
        // of certain unique indexes that might fail if created
        if (suspect instanceof Index) {
            Index index = (Index) suspect;
            if (!index.m_unique) {
                return true;
            }

            // it's cool to remove unique indexes
            if (changeType == ChangeType.DELETION) {
                return true;
            }

            // if adding a unique index, check if the columns in the new
            // index cover an existing index
            if (checkNewUniqueIndex(index)) {
                return true;
            }

            m_errors.append("May not dynamically add unique indexes that don't cover existing unique indexes.\n");
            m_supported = false;
            return false;
        }

        // the only meaty constraints (for now) are UNIQUE, PKEY and NOT NULL.
        // others are basically no-ops and are cool
        if (suspect instanceof Constraint) {
            Constraint constraint = (Constraint) suspect;

            if (constraint.getType() == ConstraintType.NOT_NULL.getValue()) {
                // for the time being, you can't add NOT NULL constraints
                return changeType == ChangeType.ADDITION;
            }

            // all other constraints are either no-ops or will
            // pass or fail with the indexes that support them.
            return true;
        }

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

            // refs are safe to add drop if the thing they reference is
            if (suspect instanceof ConstraintRef)
                return true;
            if (suspect instanceof ColumnRef)
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
        if (suspect instanceof Constraint && field.equals("index"))
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
        checkAddDropWhitelist(prevType, ChangeType.DELETION);
        m_sb.append("delete ").append(prevType.getParent().getPath()).append(" ");
        m_sb.append(mapName).append(" ").append(name).append("\n");
    }

    /**
     * Add an addition
     */
    private void writeAddition(CatalogType newType) {
        checkAddDropWhitelist(newType, ChangeType.ADDITION);
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
