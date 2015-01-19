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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogChangeGroup.FieldChange;
import org.voltdb.catalog.CatalogChangeGroup.TypeChanges;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.utils.CatalogSizing;
import org.voltdb.utils.CatalogUtil;

public class CatalogDiffEngine {

    //*  //IF-LINE-VS-BLOCK-STYLE-COMMENT
    /// A flag that controls output for debugging.
    private boolean m_triggeredVerbosity = false;
    /// A string that dynamically controls the verbose output flag, enabling it for the
    /// recursive descent into the branch referenced by any matching field name
    /// -- like "views" to get verbose output for materialized view comparisons.
    /// OR, when set to "final", enabling a final verbose report of errors and commands.
    private String m_triggerForVerbosity = "never ever"; //vs. "views"; vs. "final";
    /*/  //ELSE
    // set overrides for max verbiage.
    private boolean m_triggeredVerbosity = true;
    private String m_triggerForVerbosity = "always on";
    //*/ //ENDIF

    private boolean m_inStrictMatViewDiffMode = false;

    // contains the text of the difference
    private final StringBuilder m_sb = new StringBuilder();

    // true if the difference is allowed in a running system
    private boolean m_supported;

    // true if table changes require the catalog change runs
    // while no snapshot is running
    private boolean m_requiresSnapshotIsolation = false;

    private SortedMap<String,String> m_tablesThatMustBeEmpty = new TreeMap<>();

    //A very rough guess at whether only deployment changes are in the catalog update
    //Can be improved as more deployment things are going to be allowed to conflict
    //with Elasticity. Right now this just tracks whether a catalog update can
    //occur during a rebalance
    private boolean m_canOccurWithElasticRebalance = true;

    // collection of reasons why a diff is not supported
    private final StringBuilder m_errors = new StringBuilder();

    // original and new indexes kept to check whether a new/modified unique index is possible
    private final Map<String, CatalogMap<Index>> m_originalIndexesByTable = new HashMap<String, CatalogMap<Index>>();
    private final Map<String, CatalogMap<Index>> m_newIndexesByTable = new HashMap<String, CatalogMap<Index>>();

    /**
     * Instantiate a new diff. The resulting object can return the text
     * of the difference and report whether the difference is allowed in a
     * running system.
     * @param prev Tip of the old catalog.
     * @param next Tip of the new catalog.
     */
    public CatalogDiffEngine(final Catalog prev, final Catalog next) {
        m_supported = true;

        // store the complete set of old and new indexes so some extra checking can be done with
        // constraints and new/updated unique indexes

        CatalogMap<Table> tables = prev.getClusters().get("cluster").getDatabases().get("database").getTables();
        assert(tables != null);
        for (Table t : tables) {
            m_originalIndexesByTable.put(t.getTypeName(), t.getIndexes());
        }
        tables = next.getClusters().get("cluster").getDatabases().get("database").getTables();
        assert(tables != null);
        for (Table t : tables) {
            m_newIndexesByTable.put(t.getTypeName(), t.getIndexes());
        }

        // make sure this map has an entry for each value
        for (DiffClass dc : DiffClass.values()) {
            m_changes.put(dc, new CatalogChangeGroup(dc));
        }

        diffRecursively(prev, next);
        if (m_triggeredVerbosity || m_triggerForVerbosity.equals("final")) {
            System.out.println("DEBUG VERBOSE diffRecursively Errors:" +
                               ( m_supported ? " <none>" : "\n" + errors()));
            System.out.println("DEBUG VERBOSE diffRecursively Commands: " + commands());
        }
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

    public String[] tablesThatMustBeEmpty() {
        // this lines up with reasonsWhyTablesMustBeEmpty because SortedMap/TreeMap has order
        return m_tablesThatMustBeEmpty.keySet().toArray(new String[0]);
    }

    public String[] reasonsWhyTablesMustBeEmpty() {
        // this lines up with tablesThatMustBeEmpty because SortedMap/TreeMap has order
        return m_tablesThatMustBeEmpty.values().toArray(new String[0]);
    }

    public boolean worksWithElastic() {
        return m_canOccurWithElasticRebalance;
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
                                                       VoltType newType, int newSize,
                                                       boolean oldInBytes, boolean newInBytes)
    {
        // increases in size are cool; shrinks not so much
        if (oldType == newType) {
            if (oldType == VoltType.STRING && oldInBytes == false && newInBytes == true) {
                // varchar CHARACTER to varchar BYTES
                return oldSize * 4 <= newSize;
            }
            return oldSize <= newSize;
        }

        // allow people to convert timestamps to longs
        // (this is useful if they accidentally put millis instead of micros in there)
        if ((oldType == VoltType.TIMESTAMP) && (newType == VoltType.BIGINT)) {
            return true;
        }

        // allow integer size increase and allow promotion to DECIMAL
        if (oldType == VoltType.BIGINT) {
            if (newType == VoltType.DECIMAL) {
                return true;
            }
        }
        // also allow lossless conversion to double from ints < mantissa size
        else if (oldType == VoltType.INTEGER) {
            if ((newType == VoltType.DECIMAL) ||
                (newType == VoltType.FLOAT) ||
                newType == VoltType.BIGINT) {
                return true;
            }
        }
        else if (oldType == VoltType.SMALLINT) {
            if ((newType == VoltType.DECIMAL) ||
                (newType == VoltType.FLOAT) ||
                (newType == VoltType.BIGINT) ||
                (newType == VoltType.INTEGER)) {
                return true;
            }
        }
        else if (oldType == VoltType.TINYINT) {
            if ((newType == VoltType.DECIMAL) ||
                (newType == VoltType.FLOAT) ||
                (newType == VoltType.BIGINT) ||
                (newType == VoltType.INTEGER) ||
                (newType == VoltType.SMALLINT)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true if the parameter is an instance of Statement owned
     * by a table node.  This indicates that the Statement is the
     * DELETE statement in a
     *   LIMIT PARTITION ROWS <n> EXECUTE (DELETE ...)
     * constraint.
     */
    static private boolean isTableLimitDeleteStmt(final CatalogType catType) {
        if (catType instanceof Statement && catType.getParent() instanceof Table)
            return true;
        return false;
    }

    /**
     * @return null if the CatalogType can be dynamically added or removed
     * from a running system. Return an error string if it can't be changed on
     * a non-empty table. There will be a subsequent check for empty table
     * feasability.
     */
    private String checkAddDropWhitelist(final CatalogType suspect, final ChangeType changeType)
    {
        //Will catch several things that are actually just deployment changes, but don't care
        //to be more specific at this point
        m_canOccurWithElasticRebalance = false;

        // should generate this from spec.txt
        if (suspect instanceof User ||
            suspect instanceof Group ||
            suspect instanceof Procedure ||
            suspect instanceof Connector ||
            suspect instanceof SnapshotSchedule ||
            // refs are safe to add drop if the thing they reference is
            suspect instanceof ConstraintRef ||
            suspect instanceof GroupRef ||
            suspect instanceof UserRef ||
            // The only meaty constraints (for now) are UNIQUE, PKEY and NOT NULL.
            // The UNIQUE and PKEY constraints are supported as index definitions.
            // NOT NULL is supported as a field on columns.
            // So, in short, all of these constraints will pass or fail tests of other catalog differences
            // Even if they did show up as Constraints in the catalog (for no apparent functional reason),
            // flagging their changes here would be redundant.
            suspect instanceof Constraint ||
            // Support add/drop of the top level object.
            suspect instanceof Table)
        {
            return null;
        }

        else if (suspect instanceof ColumnRef) {
            if (suspect.getParent() instanceof Index) {
                Index parent = (Index) suspect.getParent();

                if (parent.getUnique() && (changeType == ChangeType.DELETION)) {
                    CatalogMap<Index> newIndexes= m_newIndexesByTable.get(parent.getParent().getTypeName());
                    Index newIndex = newIndexes.get(parent.getTypeName());

                    if (!checkNewUniqueIndex(newIndex)) {
                        return "May not dynamically remove columns from unique index: " +
                                parent.getTypeName();
                    }
                }
            }

            // ColumnRef is not part of an index, index is not unique OR unique index is safe to create
            return null;
        }

        else if (suspect instanceof Column) {
            // Note: "return false;" vs. fall through, in any of these branches
            // overrides the grandfathering-in of added/dropped Column-typed
            // sub-components of Procedure, Connector, etc. as checked in the loop, below.
            // Is this safe/correct?
            if (m_inStrictMatViewDiffMode) {
                return "May not dynamically add, drop, or rename materialized view columns.";
            }
            if (changeType == ChangeType.ADDITION) {
                Column col = (Column) suspect;
                if ((! col.getNullable()) && (col.getDefaultvalue() == null)) {
                    return "May not dynamically add non-nullable column without default value.";
                }
            }
            // adding/dropping a column requires isolation from snapshots
            m_requiresSnapshotIsolation = true;
            return null;
        }

        // allow addition/deletion of indexes except for the addition
        // of certain unique indexes that might fail if created
        else if (suspect instanceof Index) {
            Index index = (Index) suspect;
            if (!index.m_unique) {
                return null;
            }

            // it's cool to remove unique indexes
            if (changeType == ChangeType.DELETION) {
                return null;
            }

            // if adding a unique index, check if the columns in the new
            // index cover an existing index
            if (checkNewUniqueIndex(index)) {
                return null;
            }

            // Note: return error vs. fall through, here
            // overrides the grandfathering-in of (any? possible?) added/dropped Index-typed
            // sub-components of Procedure, Connector, etc. as checked in the loop, below.
            return "May not dynamically add unique indexes that don't cover existing unique indexes.\n";
        }

        else if (suspect instanceof MaterializedViewInfo && ! m_inStrictMatViewDiffMode) {
            return null;
        }

        else if (isTableLimitDeleteStmt(suspect)) {
            return null;
        }

        //TODO: This code is also pretty fishy
        // -- See the "salmon of doubt" comment in checkModifyWhitelist

        // Also allow add/drop of anything (that hasn't triggered an early return already)
        // if it is found anywhere in these sub-trees.
        for (CatalogType parent = suspect.getParent(); parent != null; parent = parent.getParent()) {
            if (parent instanceof Procedure ||
                parent instanceof Connector ||
                parent instanceof ConstraintRef ||
                parent instanceof Column) {
                if (m_triggeredVerbosity) {
                    System.out.println("DEBUG VERBOSE diffRecursively " +
                                       ((changeType == ChangeType.ADDITION) ? "addition" : "deletion") +
                                       " of schema object '" + suspect + "'" +
                                       " rescued by context '" + parent + "'");
                }
                return null;
            }
        }

        return "May not dynamically add/drop schema object: '" + suspect + "'\n";
    }

    /**
     * @return null if the change is not possible under any circumstances.
     * Return two strings if it is possible if the table is empty.
     * String 1 is name of a table if the change could be made if the table of that name had no tuples.
     * String 2 is the error message to show the user if that table isn't empty.
     */
    private String[] checkAddDropIfTableIsEmptyWhitelist(final CatalogType suspect, final ChangeType changeType) {
        String[] retval = new String[2];

        // handle adding an index - presumably unique
        if (suspect instanceof Index) {
            Index idx = (Index) suspect;
            assert(idx.getUnique());

            retval[0] = idx.getParent().getTypeName();
            retval[1] = String.format(
                    "Unable to add unique index %s because table %s is not empty.",
                    idx.getTypeName(), retval[0]);
            return retval;
        }

        CatalogType parent = suspect.getParent();

        // handle changes to columns in an index - presumably drops and presumably unique
        if ((suspect instanceof ColumnRef) && (parent instanceof Index)) {
            Index idx = (Index) parent;
            assert(idx.getUnique());
            assert(changeType == ChangeType.DELETION);
            Table table = (Table) idx.getParent();

            retval[0] = table.getTypeName();
            retval[1] = String.format(
                    "Unable to remove column %s from unique index %s because table %s is not empty.",
                    suspect.getTypeName(), idx.getTypeName(), retval[0]);
            return retval;
        }

        if ((suspect instanceof Column) && (parent instanceof Table) && (changeType == ChangeType.ADDITION)) {
            Column column = (Column)suspect;
            retval[0] = parent.getTypeName();
            retval[1] = String.format(
                    "Unable to add NOT NULL column %s because table %s is not empty and no default value was specified.",
                    suspect.getTypeName(), retval[0]);
            return retval;
        }

        return null;
    }

    private boolean areTableColumnsMutable(Table table) {
        //WARNING: There used to be a test here that the table's list of views was empty,
        // but what it actually appeared to be testing was whether the table HAD views prior
        // to any redefinition in the current catalog.
        // This means that dropping mat views and changing the underlying columns in one "live"
        // catalog change would not be an option -- they would have to be broken up into separate
        // steps.
        // Fortunately, for now, all the allowed "live column changes" seem to be supported without
        // disrupting materialized views.
        // In the future it MAY be required that column mutability gets re-checked after all of the
        // mat view definitions (drops and adds) have been processed, in case certain kinds of
        // underlying column change might cause special problems for certain specific cases of
        // materialized view definition.

        // no export tables
        Database db = (Database) table.getParent();
        for (Connector connector : db.getConnectors()) {
            for (ConnectorTableInfo tinfo : connector.getTableinfo()) {
                if (tinfo.getTable() == table) {
                    m_errors.append("May not change the columns of export table " +
                            table.getTypeName() + ".\n");
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * @return true if this change may be ignored
     */
    protected boolean checkModifyIgnoreList(final CatalogType suspect,
                                            final CatalogType prevType,
                                            final String field)
    {
        if (suspect instanceof Deployment) {
            // ignore host count differences as clusters may elastically expand,
            // and yet require catalog changes
            return "hostcount".equals(field);
        }
        return false;
    }

    /**
     * @return true if this addition may be ignored
     */
    protected boolean checkAddIgnoreList(final CatalogType suspect)
    {
        return false;
    }

    /**
     * @return true if this delete may be ignored
     */
    protected boolean checkDeleteIgnoreList(final CatalogType prevType,
                                            final CatalogType newlyChildlessParent,
                                            final String mapName,
                                            final String name)
    {
        return false;
    }

    /**
     * @return null if CatalogType can be dynamically modified
     * in a running system. Otherwise return an error message that
     * can be given if it turns out we really can't make the change.
     * Return "" if the error has already been handled.
     */
    private String checkModifyWhitelist(final CatalogType suspect,
                                        final CatalogType prevType,
                                        final String field)
    {
        // should generate this from spec.txt

        if (suspect instanceof Systemsettings &&
                (field.equals("elasticduration") || field.equals("elasticthroughput")
                        || field.equals("querytimeout"))) {
            return null;
        } else {
            m_canOccurWithElasticRebalance = false;
        }

        // Support any modification of these
        if (suspect instanceof User ||
            suspect instanceof Group ||
            suspect instanceof Procedure ||
            suspect instanceof SnapshotSchedule ||
            suspect instanceof UserRef ||
            suspect instanceof GroupRef ||
            suspect instanceof ColumnRef) {
            return null;
        }

        // Support modification of these specific fields
        if (suspect instanceof Database && field.equals("schema"))
            return null;
        if (suspect instanceof Database && "securityprovider".equals(field))
            return null;
        if (suspect instanceof Cluster && field.equals("securityEnabled"))
            return null;
        if (suspect instanceof Cluster && field.equals("adminstartup"))
            return null;
        if (suspect instanceof Cluster && field.equals("heartbeatTimeout"))
            return null;
        if (suspect instanceof Constraint && field.equals("index"))
            return null;
        if (suspect instanceof Table) {
            if (field.equals("signature") || field.equals("tuplelimit"))
                return null;
        }


        // Avoid over-generalization when describing limitations that are dependent on particular
        // cases of BEFORE and AFTER values by listing the offending values.
        String restrictionQualifier = "";

        // whitelist certain column changes
        if (suspect instanceof Column) {
            CatalogType parent = suspect.getParent();
            // can change statements
            if (parent instanceof Statement) {
                return null;
            }

            // all table column changes require snapshot isolation for now
            m_requiresSnapshotIsolation = true;

            // now assume parent is a Table
            Table parentTable = (Table) parent;
            if ( ! areTableColumnsMutable(parentTable)) {
                // Note: "return false;" vs. fall through, here
                // overrides the grandfathering-in of modified fields of
                // Column-typed sub-components of Procedure and ColumnRef.
                // Is this safe/correct?
                return ""; // error msg already appended
            }

            if (field.equals("index")) {
                return null;
            }
            if (field.equals("defaultvalue")) {
                return null;
            }
            if (field.equals("defaulttype")) {
                return null;
            }
            if (field.equals("nullable")) {
                Boolean nullable = (Boolean) suspect.getField(field);
                assert(nullable != null);
                if (nullable) return null;
                restrictionQualifier = " from nullable to non-nullable";
            }
            else if (field.equals("type") || field.equals("size") || field.equals("inbytes")) {
                int oldTypeInt = (Integer) prevType.getField("type");
                int newTypeInt = (Integer) suspect.getField("type");
                int oldSize = (Integer) prevType.getField("size");
                int newSize = (Integer) suspect.getField("size");

                VoltType oldType = VoltType.get((byte) oldTypeInt);
                VoltType newType = VoltType.get((byte) newTypeInt);

                boolean oldInBytes = false, newInBytes = false;
                if (oldType == VoltType.STRING) {
                    oldInBytes = (Boolean) prevType.getField("inbytes");
                }
                if (newType == VoltType.STRING) {
                    newInBytes = (Boolean) suspect.getField("inbytes");
                }

                if (checkIfColumnTypeChangeIsSupported(oldType, oldSize, newType, newSize,
                        oldInBytes, newInBytes)) {
                    return null;
                }
                if (oldTypeInt == newTypeInt) {
                    if (oldType == VoltType.STRING && oldInBytes == false && newInBytes == true) {
                        restrictionQualifier = "narrowing from " + oldSize + "CHARACTERS to "
                    + newSize * CatalogSizing.MAX_BYTES_PER_UTF8_CHARACTER + " BYTES";
                    } else {
                        restrictionQualifier = "narrowing from " + oldSize + " to " + newSize;
                    }
                }
                else {
                    restrictionQualifier = "from " + oldType.toSQLString() +
                                           " to " + newType.toSQLString();
                }
            }
        }

        else if (suspect instanceof MaterializedViewInfo) {
            if ( ! m_inStrictMatViewDiffMode) {
                // Ignore differences to json fields that only reflect other underlying
                // changes that are presumably checked and accepted/rejected separately.
                if (field.equals("groupbyExpressionsJson") ||
                        field.equals("aggregationExpressionsJson")) {
                    if (AbstractExpression.areOverloadedJSONExpressionLists((String)prevType.getField(field),
                            (String)suspect.getField(field))) {
                        return null;
                    }
                }
            }
        }

        else if (isTableLimitDeleteStmt(suspect)) {
            return null;
        }

        // Also allow any field changes (that haven't triggered an early return already)
        // if they are found anywhere in these sub-trees.

        //TODO: There's a "salmon of doubt" about all this upstream checking in the middle of a
        // downward recursion.
        // In effect, each sub-element of these certain parent object types has been forced to
        // successfully "run the gnutella" of qualifiers above.
        // Having survived, they are only now paternity tested
        //  -- which repeatedly revisits once per changed field, per (recursive) child,
        // each of the parents that were seen on the way down --
        // to possibly decide "nevermind, this change is grand-fathered in after all".
        // A better general approach would be for the parent object types,
        // as they are recursed into, to set one or more state mode flags on the CatalogDiffEngine.
        // These would be somewhat like m_inStrictMatViewDiffMode
        // -- but with a loosening rather than restricting effect on recursive tests.
        // This would provide flexibility in the future for the grand-fathered elements
        // to bypass as many or as few checks as desired.

        for (CatalogType parent = suspect.getParent(); parent != null; parent = parent.getParent()) {
            if (parent instanceof Procedure || parent instanceof ColumnRef) {
                if (m_triggeredVerbosity) {
                    System.out.println("DEBUG VERBOSE diffRecursively field change to " +
                                       "'" + field + "' of schema object '" + suspect + "'" +
                                       restrictionQualifier +
                                       " rescued by context '" + parent + "'");
                }
                return null;
            }

            if (isTableLimitDeleteStmt(parent)) {
                return null;
            }
        }

        return "May not dynamically modify field '" + field +
                        "' of schema object '" + suspect + "'" + restrictionQualifier;
    }

    /**
     * @return null if the change is not possible under any circumstances.
     * Return two strings if it is possible if the table is empty.
     * String 1 is name of a table if the change could be made if the table of that name had no tuples.
     * String 2 is the error message to show the user if that table isn't empty.
     */
    public String[] checkModifyIfTableIsEmptyWhitelist(final CatalogType suspect,
                                                     final CatalogType prevType,
                                                     final String field)
    {
        // first is table name, second is error message
        String[] retval = new String[2];

        if (prevType instanceof Table) {
            Table prevTable = (Table) prevType; // safe because of enclosing if-block
            Database db = (Database) prevType.getParent();

            // table name
            retval[0] = suspect.getTypeName();

            // for now, no changes to export tables
            if (CatalogUtil.isTableExportOnly(db, prevTable)) {
                return null;
            }

            // allowed changes to a table
            if (field.equalsIgnoreCase("isreplicated")) {
                // error message
                retval[1] = String.format(
                        "Unable to change whether table %s is replicated because it is not empty.",
                        retval[0]);
                return retval;
            }
            if (field.equalsIgnoreCase("partitioncolumn")) {
                // error message
                retval[1] = String.format(
                        "Unable to change the partition column of table %s because it is not empty.",
                        retval[0]);
                return retval;
            }
        }

        // handle narrowing columns and some modifications on empty tables
        if (prevType instanceof Column) {
            Table table = (Table) prevType.getParent();
            Column column = (Column)prevType;
            Database db = (Database) table.getParent();

            // for now, no changes to export tables
            if (CatalogUtil.isTableExportOnly(db, table)) {
                return null;
            }

            // capture the table name
            retval[0] = table.getTypeName();

            if (field.equalsIgnoreCase("type")) {
                // error message
                retval[1] = String.format(
                        "Unable to make a possibly-lossy type change to column %s in table %s because it is not empty.",
                        prevType.getTypeName(), retval[0]);
                return retval;
            }

            if (field.equalsIgnoreCase("size")) {
                // error message
                retval[1] = String.format(
                        "Unable to narrow the width of column %s in table %s because it is not empty.",
                        prevType.getTypeName(), retval[0]);
                return retval;
            }

            // Nullability changes are allowed on empty tables.
            if (field.equalsIgnoreCase("nullable")) {
                // Would be flipping the nullability, so invert the state for the message.
                String alteredNullness = column.getNullable() ? "NOT NULL" : "NULL";
                retval[1] = String.format(
                        "Unable to change column %s null constraint to %s in table %s because it is not empty.",
                        prevType.getTypeName(), alteredNullness, retval[0]);
                return retval;
            }
        }

        if (prevType instanceof Index) {
            Table table = (Table) prevType.getParent();
            Index index = (Index)prevType;

            // capture the table name
            retval[0] = table.getTypeName();
            if (field.equalsIgnoreCase("expressionsjson")) {
                // error message
                retval[1] = String.format(
                        "Unable to alter table %s with expression-based index %s becase table %s is not empty.",
                        retval[0], index.getTypeName(), retval[0]);
                return retval;
            }

        }

        return null;
    }

    /**
     * Add a modification
     */
    private void writeModification(CatalogType newType, CatalogType prevType, String field)
    {
        // Don't write modifications if the field can be ignored
        if (checkModifyIgnoreList(newType, prevType, field)) {
            return;
        }

        // verify this is possible, write an error and mark return code false if so
        String errorMessage = checkModifyWhitelist(newType, prevType, field);

        // if it's not possible with non-empty tables, check for possible with empty tables
        if (errorMessage != null) {
            String[] response = checkModifyIfTableIsEmptyWhitelist(newType, prevType, field);
            // handle all the error messages and state from the modify check
            processModifyResponses(errorMessage, response);
        }

        // write the commands to make it so
        // they will be ignored if the change is unsupported
        newType.writeCommandForField(m_sb, field, true);

        // record the field change for later generation of descriptive text
        // though skip the schema field of database because it changes all the time
        // and the diff will be caught elsewhere
        // need a better way to generalize this
        if ((newType instanceof Database) && field.equals("schema")) {
            return;
        }
        CatalogChangeGroup cgrp = m_changes.get(DiffClass.get(newType));
        cgrp.processChange(newType, prevType, field);
    }

    /**
     * After we decide we can't modify, add or delete something on a full table,
     * we do a check to see if we can do that on an empty table. The original error
     * and any response from the empty table check is processed here. This code
     * is basically in this method so it's not repeated 3 times for modify, add
     * and delete. See where it's called for context.
     */
    private void processModifyResponses(String errorMessage, String[] response) {
        assert(errorMessage != null);

        // if no tablename, then it's just not possible
        if (response == null) {
            m_supported = false;
            m_errors.append(errorMessage);
        }
        // otherwise, it's possible if a specific table is empty
        // collect the error message(s) and decide if it can be done inside @UAC
        else {
            assert(response.length == 2);
            String tableName = response[0]; assert(tableName != null);
            String nonEmptyErrorMessage = response[1]; assert(nonEmptyErrorMessage != null);

            String existingErrorMessagesForNonEmptyTable = m_tablesThatMustBeEmpty.get(tableName);
            if (nonEmptyErrorMessage.length() == 0) {
                // the empty string presumes there is already an error for this table
                assert(existingErrorMessagesForNonEmptyTable != null);
            }
            else {
                if (existingErrorMessagesForNonEmptyTable != null) {
                    nonEmptyErrorMessage = nonEmptyErrorMessage + "\n" + existingErrorMessagesForNonEmptyTable;
                }
                // add indentation here so the formatting comes out right for the user #gianthack
                m_tablesThatMustBeEmpty.put(tableName, "  " + nonEmptyErrorMessage);
            }
        }
    }

    /**
     * Add a deletion
     */
    private void writeDeletion(CatalogType prevType, CatalogType newlyChildlessParent, String mapName, String name)
    {
        // Don't write deletions if the field can be ignored
        if (checkDeleteIgnoreList(prevType, newlyChildlessParent, mapName, name)) {
            return;
        }

        // verify this is possible, write an error and mark return code false if so
        String errorMessage = checkAddDropWhitelist(prevType, ChangeType.DELETION);

        // if it's not possible with non-empty tables, check for possible with empty tables
        if (errorMessage != null) {
            String[] response = checkAddDropIfTableIsEmptyWhitelist(prevType, ChangeType.DELETION);
            // handle all the error messages and state from the modify check
            processModifyResponses(errorMessage, response);
        }

        // write the commands to make it so
        // they will be ignored if the change is unsupported
        m_sb.append("delete ").append(prevType.getParent().getCatalogPath()).append(" ");
        m_sb.append(mapName).append(" ").append(name).append("\n");

        // add it to the set of deletions to later compute descriptive text
        CatalogChangeGroup cgrp = m_changes.get(DiffClass.get(prevType));
        cgrp.processDeletion(prevType, newlyChildlessParent);
    }

    /**
     * Add an addition
     */
    private void writeAddition(CatalogType newType) {
        // Don't write additions if the field can be ignored
        if (checkAddIgnoreList(newType)) {
            return;
        }
        // verify this is possible, write an error and mark return code false if so
        String errorMessage = checkAddDropWhitelist(newType, ChangeType.ADDITION);

        // if it's not possible with non-empty tables, check for possible with empty tables
        if (errorMessage != null) {
            String[] response = checkAddDropIfTableIsEmptyWhitelist(newType, ChangeType.ADDITION);
            // handle all the error messages and state from the modify check
            processModifyResponses(errorMessage, response);
        }

        // write the commands to make it so
        // they will be ignored if the change is unsupported
        newType.writeCreationCommand(m_sb);
        newType.writeFieldCommands(m_sb);
        newType.writeChildCommands(m_sb);

        // add it to the set of additions to later compute descriptive text
        CatalogChangeGroup cgrp = m_changes.get(DiffClass.get(newType));
        cgrp.processAddition(newType);
    }


    /**
     * Pre-order walk of catalog generating add, delete and set commands
     * that compose that full difference.
     * @param prevType
     * @param newType
     */
    private void diffRecursively(CatalogType prevType, CatalogType newType)
    {
        assert(prevType != null) : "Null previous object found in catalog diff traversal.";
        assert(newType != null) : "Null new object found in catalog diff traversal";

        Object materializerValue = null;
        // Consider shifting into the strict more required within materialized view definitions.
        if (prevType instanceof Table) {
            // Under normal circumstances, it's highly unpossible that another (nested?) table will
            // appear in the details of a materialized view table. So, when it does (!?), be sure to
            // complain -- and don't let it throw off the accounting of the strict diff mode.
            // That is, don't set the local "materializerValue".
            if (m_inStrictMatViewDiffMode) {
                // Maybe this should log or append to m_errors?
                System.out.println("ERROR: unexpected nesting of a Table in CatalogDiffEngine.");
            } else {
                materializerValue = prevType.getField("materializer");
                if (materializerValue != null) {
                    // This table is a materialized view, so the changes to it and its children are
                    // strictly limited, e.g. no adding/dropping columns.
                    // In a future development, such changes may be allowed, but they may be implemented
                    // differently (get different catalog commands), such as through a wholesale drop/add
                    // of the entire view and materialized table definitions.
                    // The non-null local "materializerValue" is a reminder to pop out of this mode
                    // before returning from this level of the recursion.
                    m_inStrictMatViewDiffMode = true;
                    if (m_triggeredVerbosity) {
                        System.out.println("DEBUG VERBOSE diffRecursively entering strict mat view mode");
                    }
                }
            }
        }

        // diff local fields
        for (String field : prevType.getFields()) {
            // this field is (or was) set at runtime, so ignore it for diff purposes
            if (field.equals("isUp"))
            {
                continue;
            }

            boolean verbosityTriggeredHere = false;
            if (( ! m_triggeredVerbosity) && field.equals(m_triggerForVerbosity)) {
                System.out.println("DEBUG VERBOSE diffRecursively verbosity (triggered by field '" + field + "' is ON");
                verbosityTriggeredHere = true;
                m_triggeredVerbosity = true;
            }
            // check if the types are different
            // options are: both null => same
            //              one null and one not => different
            //              both not null => check Object.equals()
            Object prevValue = prevType.getField(field);
            Object newValue = newType.getField(field);
            if ((prevValue == null) != (newValue == null)) {
                if (m_triggeredVerbosity) {
                    if (prevValue == null) {
                        System.out.println("DEBUG VERBOSE diffRecursively found new '" + field + "' only.");
                    } else {
                        System.out.println("DEBUG VERBOSE diffRecursively found prev '" + field + "' only.");
                    }
                }
                writeModification(newType, prevType, field);
            }
            // if they're both not null (above/below ifs implies this)
            else if (prevValue != null) {
                // if comparing CatalogTypes (both must be same)
                if (prevValue instanceof CatalogType) {
                    assert(newValue instanceof CatalogType);
                    String prevPath = ((CatalogType) prevValue).getCatalogPath();
                    String newPath = ((CatalogType) newValue).getCatalogPath();
                    if (prevPath.compareTo(newPath) != 0) {
                        if (m_triggeredVerbosity) {
                            int padWidth = StringUtils.indexOfDifference(prevPath, newPath);
                            String pad = StringUtils.repeat(" ", padWidth);
                            System.out.println("DEBUG VERBOSE diffRecursively found a path change to '" + field + "':");
                            System.out.println("DEBUG VERBOSE prevPath=" + prevPath);
                            System.out.println("DEBUG VERBOSE diff at->" + pad + "^ position:" + padWidth);
                            System.out.println("DEBUG VERBOSE  newPath=" + newPath);
                        }
                        writeModification(newType, prevType, field);
                    }
                }
                // if scalar types
                else {
                    if (prevValue.equals(newValue) == false) {
                        if (m_triggeredVerbosity) {
                            System.out.println("DEBUG VERBOSE diffRecursively found a scalar change to '" + field + "':");
                            System.out.println("DEBUG VERBOSE diffRecursively prev:" + prevValue);
                            System.out.println("DEBUG VERBOSE diffRecursively new :" + newValue);
                        }
                        writeModification(newType, prevType, field);
                    }
                }
            }
            if (verbosityTriggeredHere) {
                System.out.println("DEBUG VERBOSE diffRecursively verbosity is OFF");
                m_triggeredVerbosity = false;
            }
        }

        // recurse
        for (String field : prevType.getChildCollections()) {
            boolean verbosityTriggeredHere = false;
            if (field.equals(m_triggerForVerbosity)) {
                System.out.println("DEBUG VERBOSE diffRecursively verbosity ON");
                m_triggeredVerbosity = true;
                verbosityTriggeredHere = true;
            }
            CatalogMap<? extends CatalogType> prevMap = prevType.getCollection(field);
            CatalogMap<? extends CatalogType> newMap = newType.getCollection(field);
            getCommandsToDiff(field, prevMap, newMap);
            if (verbosityTriggeredHere) {
                System.out.println("DEBUG VERBOSE diffRecursively verbosity OFF");
                m_triggeredVerbosity = false;
            }
        }

        if (materializerValue != null) {
            // Just getting back from recursing into a materialized view table,
            // so drop the strictness required only in that context.
            // It's safe to assume that the prior mode to which this must pop back is the non-strict
            // mode because nesting of table definitions is unpossible AND we guarded against its
            // potential side effects, above, anyway.
            m_inStrictMatViewDiffMode = false;
        }

    }


    /**
     * Check if all the children in prevMap are present and identical in newMap.
     * Then, check if anything is in newMap that isn't in prevMap.
     * @param mapName
     * @param prevMap
     * @param newMap
     */
    private void getCommandsToDiff(String mapName,
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
                writeDeletion(prevType, newMap.m_parent, mapName, name);
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

    ///////////////////////////////////////////////////////////////////
    //
    // Code below this point helps generate human-readable diffs, but
    // should have no functional impact on anything else.
    //
    ///////////////////////////////////////////////////////////////////

    /**
     * Enum used to break up the catalog tree into sub-roots based on CatalogType
     * class. This is purely used for printing human readable summaries.
     */
    enum DiffClass {
        PROC (Procedure.class),
        TABLE (Table.class),
        USER (User.class),
        GROUP (Group.class),
        //CONNECTOR (Connector.class),
        //SCHEDULE (SnapshotSchedule.class),
        //CLUSTER (Cluster.class),
        OTHER (Catalog.class); // catch all for even the commented stuff above

        final Class<?> clz;

        DiffClass(Class<?> clz) {
            this.clz = clz;
        }

        static DiffClass get(CatalogType type) {
            // this exits because eventually OTHER will catch everything
            while (true) {
                for (DiffClass dc : DiffClass.values()) {
                    if (type.getClass() == dc.clz) {
                        return dc;
                    }
                }
                type = type.getParent();
            }
        }
    }

    interface Filter {
        public boolean include(CatalogType type);
    }

    interface Namer {
        public String getName(CatalogType type);
    }

    private boolean basicMetaChangeDesc(StringBuilder sb, String heading, DiffClass dc, Filter filter, Namer namer) {
        CatalogChangeGroup group = m_changes.get(dc);

        // exit if nothing has changed
        if ((group.groupChanges.size() == 0) && (group.groupAdditions.size() == 0) && (group.groupDeletions.size() == 0)) {
            return false;
        }

        // default namer uses simplename
        if (namer == null) {
            namer = new Namer() {
                @Override
                public String getName(CatalogType type) {
                    return type.getClass().getSimpleName() + " " + type.getTypeName();
                }
            };
        }

        sb.append(heading).append("\n");

        for (CatalogType type : group.groupDeletions) {
            if ((filter != null) && !filter.include(type)) continue;
            sb.append(String.format("  %s dropped.\n",
                    namer.getName(type)));
        }
        for (CatalogType type : group.groupAdditions) {
            if ((filter != null) && !filter.include(type)) continue;
            sb.append(String.format("  %s added.\n",
                    namer.getName(type)));
        }
        for (Entry<CatalogType, TypeChanges> entry : group.groupChanges.entrySet()) {
            if ((filter != null) && !filter.include(entry.getKey())) continue;
            sb.append(String.format("  %s has been modified.\n",
                    namer.getName(entry.getKey())));
        }

        sb.append("\n");
        return true;
    }

    // track adds/drops/modifies in a secondary structure to make human readable descriptions
    private final Map<DiffClass, CatalogChangeGroup> m_changes = new TreeMap<DiffClass, CatalogChangeGroup>();

    /**
     * Get a human readable list of changes between two catalogs.
     *
     * This currently handles just the basics, but much of the plumbing is
     * in place to give a lot more detail, with a bit more work.
     */
    public String getDescriptionOfChanges() {
        StringBuilder sb = new StringBuilder();

        sb.append("Catalog Difference Report\n");
        sb.append("=========================\n");
        if (supported()) {
            sb.append("  This change can occur while the database is running.\n");
            if (requiresSnapshotIsolation()) {
                sb.append("  This change must occur when no snapshot is running.\n");
                sb.append("  If a snapshot is in progress, the system will wait \n" +
                          "  until the snapshot is complete to make the changes.\n");
            }
        }
        else {
            sb.append("  Making this change requires stopping and restarting the database.\n");
        }
        sb.append("\n");

        boolean wroteChanges = false;

        // DESCRIBE TABLE CHANGES
        Namer tableNamer = new Namer() {
            @Override
            public String getName(CatalogType type) {
                Table table = (Table) type;
                // check if view
                // note, this has to be pretty raw to avoid some smarts that wont work
                // in this context. this may return an unresolved link which points nowhere,
                // but that's good enough to know it's a view
                if (table.getField("materializer") != null) {
                    return "View " + type.getTypeName();
                }

                // check if export table
                // this probably doesn't work due to the same kinds of problesm we have
                // when identifying views. Tables just need a field that says if they
                // are export tables or not... ugh. FIXME
                for (Connector c : ((Database) table.getParent()).getConnectors()) {
                    for (ConnectorTableInfo cti : c.getTableinfo()) {
                        if (cti.getTable() == table) {
                            return "Export Table " + type.getTypeName();
                        }
                    }
                }

                // just a regular table
                return "Table " + type.getTypeName();
            }
        };
        wroteChanges |= basicMetaChangeDesc(sb, "TABLE CHANGES:", DiffClass.TABLE, null, tableNamer);

        // DESCRIBE PROCEDURE CHANGES
        Filter crudProcFilter = new Filter() {
            @Override
            public boolean include(CatalogType type) {
                if (type.getTypeName().endsWith(".select")) return false;
                if (type.getTypeName().endsWith(".insert")) return false;
                if (type.getTypeName().endsWith(".delete")) return false;
                if (type.getTypeName().endsWith(".update")) return false;
                return true;
            }
        };
        wroteChanges |= basicMetaChangeDesc(sb, "PROCEDURE CHANGES:", DiffClass.PROC, crudProcFilter, null);

        // DESCRIBE GROUP CHANGES
        wroteChanges |= basicMetaChangeDesc(sb, "GROUP CHANGES:", DiffClass.GROUP, null, null);

        // DESCRIBE OTHER CHANGES
        CatalogChangeGroup group = m_changes.get(DiffClass.OTHER);
        if (group.groupChanges.size() > 0) {
            wroteChanges = true;

            sb.append("OTHER CHANGES:\n");

            assert(group.groupAdditions.size() == 0);
            assert(group.groupDeletions.size() == 0);

            for (TypeChanges metaChanges : group.groupChanges.values()) {
                for (CatalogType type : metaChanges.typeAdditions) {
                    sb.append(String.format("  Catalog node %s of type %s has been added.\n",
                            type.getTypeName(), type.getClass().getSimpleName()));
                }
                for (CatalogType type : metaChanges.typeDeletions) {
                    sb.append(String.format("  Catalog node %s of type %s has been removed.\n",
                            type.getTypeName(), type.getClass().getSimpleName()));
                }
                for (FieldChange fc : metaChanges.childChanges.values()) {
                    sb.append(String.format("  Catalog node %s of type %s has modified metadata.\n",
                            fc.newType.getTypeName(), fc.newType.getClass().getSimpleName()));
                }
            }
        }

        if (!wroteChanges) {
            sb.append("  No changes detected.\n");
        }

        // trim the last newline
        sb.setLength(sb.length() - 1);

        return sb.toString();
    }
}
