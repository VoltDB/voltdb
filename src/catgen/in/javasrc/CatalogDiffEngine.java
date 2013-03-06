/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.voltdb.VoltType;
import org.voltdb.types.ConstraintType;

public class CatalogDiffEngine {

    // contains the text of the difference
    private final StringBuilder m_sb = new StringBuilder();

    // true if the difference is allowed in a running system
    private boolean m_supported;

    // true if table changes require the catalog change runs
    // while no snapshot is running
    private boolean m_requiresSnapshotIsolation = false;

    /**
     * Given a type present in both catalogs, list the fields
     * that have changed between them. Together with the types
     * themselves, you can get the values of the fields before
     * and after.
     */
    private static class FieldChange {
        final CatalogType newType;
        final CatalogType prevType;
        final List<String> changedFields = new ArrayList<String>();
        FieldChange(CatalogType newType, CatalogType prevType) {
            this.newType = newType; this.prevType = prevType;
        }
    }

    /**
     * Enum used to break up the catalog tree into sub-roots based on CatalogType
     * class. This is purely used for printing human readable summaries.
     */
    private enum DiffClass {
        PROC (Procedure.class),
        TABLE (Table.class),
        OTHER (Catalog.class);

        final Class<?> clz;

        DiffClass(Class<?> clz) {
            this.clz = clz;
        }

        static DiffClass get(CatalogType type) {
            while (true) {
                if (type instanceof Catalog) {
                    return OTHER;
                }
                if (type instanceof Procedure) {
                    return PROC;
                }
                if (type instanceof Table) {
                    return TABLE;
                }
                type = type.getParent();
            }
        }
    }

    /**
     * Describes the set of changes to a subtree of the catalog. For example, a {@link ChangeGroup}
     * could describe all of the changes to procedures in a catalog. This is purely used for
     * printing human readable summaries.
     */
    class ChangeGroup {
        // the class of the base item in the subtree
        // for example, procedure
        final Class<?> clz;

        // nodes of type clz added
        List<CatalogType> additions = new ArrayList<CatalogType>();
        // nodes of type clz dropped
        List<CatalogType> deletions = new ArrayList<CatalogType>();
        // nodes added under a clz instance, mapped from their parent
        Map<CatalogType, List<CatalogType>> childAdditions = new TreeMap<CatalogType, List<CatalogType>>();
        // nodes dropped from under a clz instance, mapped from their parent
        Map<CatalogType, List<CatalogType>> childDeletions = new TreeMap<CatalogType, List<CatalogType>>();
        // all fields changed for a clz instance and any fields changed by children
        // map from base clz instance to node to the field changes to the FieldChange instance
        Map<CatalogType, Map<CatalogType, FieldChange>> childChanges = new TreeMap<CatalogType, Map<CatalogType, FieldChange>>();

        ChangeGroup(DiffClass diffClass) {
            clz = diffClass.clz;
        }

        void processAddition(CatalogType type) {
            if (type.getClass().equals(clz)) {
                additions.add(type);
                return;
            }
            CatalogType parent = type.getParent();
            while (parent.getClass().equals(clz) == false) {
                parent = parent.getParent();
            }

            List<CatalogType> localAdds = childAdditions.get(parent);
            if (localAdds == null) {
                localAdds = new ArrayList<CatalogType>();
                childAdditions.put(parent, localAdds);
            }
            localAdds.add(type);
        }

        void processDeletion(CatalogType type) {
            if (type.getClass().equals(clz)) {
                deletions.add(type);
                return;
            }
            CatalogType parent = type.getParent();
            while (parent.getClass().equals(clz) == false) {
                parent = parent.getParent();
            }

            List<CatalogType> localAdds = childDeletions.get(parent);
            if (localAdds == null) {
                localAdds = new ArrayList<CatalogType>();
                childDeletions.put(parent, localAdds);
            }
            localAdds.add(type);
        }

        void processChange(CatalogType newType, CatalogType prevType, String field) {
            CatalogType parent = newType;
            while (parent.getClass().equals(clz) == false) {
                parent = parent.getParent();
            }

            Map<CatalogType, FieldChange> changes = childChanges.get(parent);
            if (changes == null) {
                changes = new TreeMap<CatalogType, FieldChange>();
                childChanges.put(parent, changes);
            }
            FieldChange fc = changes.get(newType);
            if (fc == null) {
                fc = new FieldChange(newType, prevType);
                changes.put(newType, fc);
            }
            fc.changedFields.add(field);
        }
    }

    // track adds/drops/modifies in a secondary structure to make human readable descriptions
    private final Map<DiffClass, ChangeGroup> m_changes = new TreeMap<DiffClass, ChangeGroup>();

    // collection of reasons why a diff is not supported
    private final StringBuilder m_errors = new StringBuilder();

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
        m_supported = true;

        // make sure this map has an entry for each value
        for (DiffClass dc : DiffClass.values()) {
            m_changes.put(dc, new ChangeGroup(dc));
        }

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

    /**
     * @return true if table changes require the catalog change runs
     * while no snapshot is running.
     */
    public boolean requiresSnapshotIsolation() {
        return m_requiresSnapshotIsolation;
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
                String colName1 = colRef.getColumn().getName();
                String colName2 = existingColRef.getColumn().getName();
                if (colName1.equals(colName2)) {
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
     * @param oldType The old type of the column.
     * @param oldSize The old size of the column.
     * @param newType The new type of the column.
     * @param newSize The new size of the column.
     *
     * @return True if the change from one column type to another is possible
     * to do live without failing or truncating any data.
     */
    private boolean checkIfColumnTypeChangeIsSupported(VoltType oldType, int oldSize,
                                                       VoltType newType, int newSize)
    {
        // increases in size are cool; shrinks not so much
        if (oldType == newType) {
            // don't allow inline types to be made out-of-line types
            if ((oldType == VoltType.VARBINARY) || (oldType == VoltType.STRING)) {
                if (oldSize < 64 && newSize >= 64) {
                    return false;
                }
            }
            return oldSize <= newSize;
        }

        // allow people to convert timestamps to longs
        // (this is useful if they accidentally put millis instead of micros in there)
        if ((oldType == VoltType.TIMESTAMP) && (newType == VoltType.BIGINT)) {
            return true;
        }

        // allow integer size increased
        if (oldType == VoltType.INTEGER) {
            if (newType == VoltType.BIGINT) {
                return true;
            }
        }
        if (oldType == VoltType.SMALLINT) {
            if ((newType == VoltType.BIGINT) ||
                (newType == VoltType.INTEGER)) {
                return true;
            }
        }
        if (oldType == VoltType.TINYINT) {
            if ((newType == VoltType.BIGINT) ||
                (newType == VoltType.INTEGER) ||
                (newType == VoltType.SMALLINT)) {
                return true;
            }
        }

        // allow lossless conversion to double from ints < mantissa size
        if (newType == VoltType.FLOAT) {
            if ((newType == VoltType.INTEGER) ||
                (newType == VoltType.SMALLINT) ||
                (newType == VoltType.TINYINT)) {
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

        // Support add/drop anywhere in these sub-trees, except for a weird column case
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
            if (suspect instanceof Column) {
                Column col = (Column) suspect;
                if ((changeType == ChangeType.ADDITION) &&
                    (! col.getNullable()) &&
                    (col.getDefaultvalue() == null))
                {
                    m_errors.append("May not dynamically add non-nullable column.\n");
                    m_supported = false;
                    return false;
                }
                // adding/dropping a column requires isolation from snapshots
                m_requiresSnapshotIsolation = true;
                return true;
            }

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
    boolean checkModifyWhitelist(CatalogType suspect, CatalogType prevType, String field) {
        // should generate this from spec.txt
        CatalogType orig = suspect;

        // Support modification of these specific fields
        if (suspect instanceof Database && field.equals("schema"))
            return true;
        if (suspect instanceof Cluster && field.equals("securityEnabled"))
            return true;
        if (suspect instanceof Constraint && field.equals("index"))
            return true;
        if (suspect instanceof Table && field.equals("signature"))
            return true;

        // whitelist certain column changes
        if (suspect instanceof Column) {
            if (field.equals("index"))
                return true;
            if (field.equals("defaultvalue"))
                return true;
            if (field.equals("defaulttype"))
                return true;
            if (field.equals("nullable")) {
                Boolean nullable = (Boolean) suspect.getField(field);
                assert(nullable != null);
                if (nullable) return true;
            }
            if (field.equals("type") || field.equals("size")) {
                int oldTypeInt = (Integer) prevType.getField("type");
                int newTypeInt = (Integer) suspect.getField("type");
                int oldSize = (Integer) prevType.getField("size");
                int newSize = (Integer) suspect.getField("size");

                VoltType oldType = VoltType.get((byte) oldTypeInt);
                VoltType newType = VoltType.get((byte) newTypeInt);

                if (checkIfColumnTypeChangeIsSupported(oldType, oldSize, newType, newSize)) {
                    // changing the type of a column requires isolation from snapshots
                    m_requiresSnapshotIsolation = true;
                    return true;
                }
            }
        }

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
    private void writeModification(CatalogType newType, CatalogType prevType, String field)
    {
        // verify this is possible, write an error and mark return code false if so
        checkModifyWhitelist(newType, prevType, field);

        // write the commands to make it so
        // they will be ignored if the change is unsupported
        newType.writeCommandForField(m_sb, field, true);

        // record the field change for later generation of descriptive text
        ChangeGroup cgrp = m_changes.get(DiffClass.get(newType));
        cgrp.processChange(newType, prevType, field);
    }

    /**
     * Add a deletion
     */
    private void writeDeletion(CatalogType prevType, String mapName, String name)
    {
        // verify this is possible, write an error and mark return code false if so
        checkAddDropWhitelist(prevType, ChangeType.DELETION);

        // write the commands to make it so
        // they will be ignored if the change is unsupported
        m_sb.append("delete ").append(prevType.getParent().getPath()).append(" ");
        m_sb.append(mapName).append(" ").append(name).append("\n");

        // add it to the set of deletions to later compute descriptive text
        ChangeGroup cgrp = m_changes.get(DiffClass.get(prevType));
        cgrp.processDeletion(prevType);
    }

    /**
     * Add an addition
     */
    private void writeAddition(CatalogType newType) {
        // verify this is possible, write an error and mark return code false if so
        checkAddDropWhitelist(newType, ChangeType.ADDITION);

        // write the commands to make it so
        // they will be ignored if the change is unsupported
        newType.writeCreationCommand(m_sb);
        newType.writeFieldCommands(m_sb);
        newType.writeChildCommands(m_sb);

        // add it to the set of additions to later compute descriptive text
        ChangeGroup cgrp = m_changes.get(DiffClass.get(newType));
        cgrp.processAddition(newType);
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
            // this field is (or was) set at runtime, so ignore it for diff purposes
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
                writeModification(newType, prevType, field);
            }
            // if they're both not null (above/below ifs implies this)
            else if (prevValue != null) {
                // if comparing CatalogTypes (both must be same)
                if (prevValue instanceof CatalogType) {
                    assert(newValue instanceof CatalogType);
                    String prevPath = ((CatalogType) prevValue).getPath();
                    String newPath = ((CatalogType) newValue).getPath();
                    if (prevPath.compareTo(newPath) != 0) {
                        writeModification(newType, prevType, field);
                    }
                }
                // if scalar types
                else {
                    if (prevValue.equals(newValue) == false) {
                        writeModification(newType, prevType, field);
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

    private boolean isCRUDProc(Procedure proc) {
        if (proc.getTypeName().endsWith(".select")) return true;
        if (proc.getTypeName().endsWith(".insert")) return true;
        if (proc.getTypeName().endsWith(".delete")) return true;
        if (proc.getTypeName().endsWith(".update")) return true;
        return false;
    }

    /**
     * Get a human readable list of changes between two catalogs.
     *
     * This currently handles just the basics, but much of the plumbing is
     * in place to give a lot more detail, with a bit more work.
     */
    public String getDescriptionOfChanges() {
        StringBuilder sb = new StringBuilder();

        // DESCRIBE TABLE CHANGES
        ChangeGroup group = m_changes.get(DiffClass.TABLE);

        for (CatalogType type : group.deletions) {
            sb.append(String.format("Table %s dropped.\n", type.getTypeName()));
        }

        for (CatalogType type : group.additions) {
            sb.append(String.format("Table %s added.\n", type.getTypeName()));
        }

        TreeSet<CatalogType> changedTables = new TreeSet<CatalogType>();
        changedTables.addAll(group.childAdditions.keySet());
        changedTables.addAll(group.childDeletions.keySet());
        changedTables.addAll(group.childChanges.keySet());
        for (CatalogType type : changedTables) {
            sb.append(String.format("Table %s has been modified.\n", type.getTypeName()));
        }

        // DESCRIBE PROCEDURE CHANGES
        group = m_changes.get(DiffClass.PROC);

        for (CatalogType type : group.deletions) {
            if (isCRUDProc((Procedure) type)) continue;
            sb.append(String.format("Procedure %s dropped.\n", type.getTypeName()));
        }

        for (CatalogType type : group.additions) {
            if (isCRUDProc((Procedure) type)) continue;
            sb.append(String.format("Procedure %s added.\n", type.getTypeName()));
        }

        TreeSet<CatalogType> changedProcs = new TreeSet<CatalogType>();
        changedProcs.addAll(group.childAdditions.keySet());
        changedProcs.addAll(group.childDeletions.keySet());
        changedProcs.addAll(group.childChanges.keySet());
        for (CatalogType type : changedProcs) {
            if (isCRUDProc((Procedure) type)) continue;
            sb.append(String.format("Procedure %s has been modified.\n", type.getTypeName()));
        }

        // DESCRIBE OTHER CHANGES
        group = m_changes.get(DiffClass.OTHER);

        assert(group.additions.size() == 0);
        assert(group.deletions.size() == 0);

        for (List<CatalogType> types : group.childAdditions.values()) {
            for (CatalogType type : types) {
                sb.append(String.format("Catalog node %s of type %s has been added.\n",
                        type.getTypeName(), type.getClass().getSimpleName()));
            }
        }
        for (List<CatalogType> types : group.childDeletions.values()) {
            for (CatalogType type : types) {
                sb.append(String.format("Catalog node %s of type %s has been removed.\n",
                        type.getTypeName(), type.getClass().getSimpleName()));
            }
        }
        for (Map<CatalogType, FieldChange> changes : group.childChanges.values()) {
            for (FieldChange fc : changes.values()) {
                // skip the database node which has a schema field that changes, but is covered elsewhere
                if (fc.newType instanceof Database) {
                    continue;
                }
                sb.append(String.format("Catalog node %s of type %s has modified metadata.\n",
                        fc.newType.getTypeName(), fc.newType.getClass().getSimpleName()));
            }
        }

        return sb.toString();
    }
}
