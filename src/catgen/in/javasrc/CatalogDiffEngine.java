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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.TableType;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogChangeGroup.FieldChange;
import org.voltdb.catalog.CatalogChangeGroup.TypeChanges;
import org.voltdb.compiler.MaterializedViewProcessor;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.utils.CatalogSizing;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.Encoder;

public class CatalogDiffEngine {

    /**
     * If all of the tables in the m_tableNames list are
     * populated, this represents an error with the given
     * error message.  If one of the tables is empty, this
     * object represents a non-error.
     */
    public class TablePopulationRequirements {
        /**
         * This is the most common case.  We have one table which
         * needs to be empty, and one error message if the table
         * is not empty.
         */
        public TablePopulationRequirements(String objectName, String tableName, String errMessage) {
            m_objectName = objectName;
            m_tableNames.add(tableName);
            m_errorMessage = errMessage;
        }
        /**
         * This is a more nuanced case.  Nothing happens, and the
         * user must add all table names and just one error message.
         * But we still know the name of the object we want to
         * add.
         */
        public TablePopulationRequirements(String objectName) {
            m_objectName = objectName;
        }
        public final List<String> getTableNames() {
            return m_tableNames;
        }
        public final void addTableName(String name) {
            m_tableNames.add(name);
        }
        public final String getErrorMessage() {
            return m_errorMessage;
        }
        public final void setErrorMessage(String errorMessage) {
            // The final error message wants a space at the beginning.
            // Don't ask why.
            m_errorMessage = " " + errorMessage;
        }
        public final String getObjectName() {
            return m_objectName;
        }
        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append("{")
              .append(m_objectName != null ? m_objectName : "<<NULL>>")
              .append(", Names: \"" + String.join(", ", m_tableNames) + "\"")
              .append(", Msg: \"" + (m_errorMessage != null ? m_errorMessage : "<<NULL>>") + "\"")
              .append("}");
            return sb.toString();
        }
        private String       m_objectName     = null;
        private List<String> m_tableNames     = new ArrayList<>();
        private String       m_errorMessage   = null;
    }

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
    private final CatalogSerializer m_serializer = new CatalogSerializer();

    // true if the difference is allowed in a running system
    private boolean m_supported;

    private boolean m_requiresCatalogDiffCmdsApplyToEE = false;

    // true if table changes require the catalog change runs
    // while no snapshot is running
    private boolean m_requiresSnapshotIsolation = false;
    // true if export needs new generation.
    private boolean m_requiresNewExportGeneration = false;

    private final SortedMap<String, TablePopulationRequirements> m_tablesThatMustBeEmpty = new TreeMap<>();

    // Track all new tables.  We use this to know which
    // tables do not need to be checked for emptiness.
    private final SortedSet<String> m_newTables = new TreeSet<>();

    //A very rough guess at whether only deployment changes are in the catalog update
    //Can be improved as more deployment things are going to be allowed to conflict
    //with Elasticity. Right now this just tracks whether a catalog update can
    //occur during a rebalance
    private boolean m_canOccurWithElasticRebalance = true;

    // collection of reasons why a diff is not supported
    private final StringBuilder m_errors = new StringBuilder();

    // original and new indexes kept to check whether a new/modified unique index is possible
    private final Map<String, CatalogMap<Index>> m_originalIndexesByTable = new HashMap<>();
    private final Map<String, CatalogMap<Index>> m_newIndexesByTable = new HashMap<>();

    /**
     * Instantiate a new diff. The resulting object can return the text
     * of the difference and report whether the difference is allowed in a
     * running system.
     * @param prev Tip of the old catalog.
     * @param next Tip of the new catalog.
     */
    public CatalogDiffEngine(Catalog prev, Catalog next, boolean forceVerbose) {
        this(prev, next, forceVerbose, true);
    }

    protected CatalogDiffEngine(Catalog prev, Catalog next, boolean forceVerbose, boolean runDiff) {
        m_supported = true;
        if (forceVerbose) {
            m_triggeredVerbosity = true;
            m_triggerForVerbosity = "always on";
        }

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

        if (runDiff) {
            runDiff(prev, next);
        }
    }

    public CatalogDiffEngine(Catalog prev, Catalog next) {
        this(prev, next, false);
    }

    public String commands() {
        return m_serializer.getResult();
    }

    public boolean supported() {
        return m_supported;
    }

    public boolean requiresCatalogDiffCmdsApplyToEE() {
        return m_requiresCatalogDiffCmdsApplyToEE;
    }

    /**
     * @return true if table changes require the catalog change runs
     * while no snapshot is running.
     */
    public boolean requiresSnapshotIsolation() {
        return m_requiresSnapshotIsolation;
    }

    /**
     * @return true if changes require export generation to be updated.
     */
    public boolean requiresNewExportGeneration() {
        return m_requiresNewExportGeneration;
    }

    public boolean hasSecurityUserChanges() {
        CatalogChangeGroup ccg = m_changes.get(DiffClass.USER);
        if (!ccg.groupAdditions.isEmpty()) {
            return true;
        }

        if (!ccg.groupDeletions.isEmpty()) {
            return true;
        }

        if (ccg.groupChanges.isEmpty()) {
            return false;
        }

        Map<CatalogType, TypeChanges> groupModifications = new TreeMap<CatalogType, TypeChanges>();
        groupModifications.putAll(ccg.groupChanges);
        //ignore these fields for changes since these fields are dynamically encrypted.
        //user object updates are tracked with passwords, not the SHAed ones.
        //In the future, any new fields which are not part of the user updates, should be
        //added to the list.
        List<String> ignoredFields = Arrays.asList("shadowPassword", "sha256ShadowPassword");
        for (Map.Entry<CatalogType, TypeChanges> entry : ccg.groupChanges.entrySet()) {
            Set<String> fields = new HashSet<>();
            fields.addAll(entry.getValue().typeChanges.changedFields);
            fields.removeAll(ignoredFields);
            if (fields.isEmpty()) {
                groupModifications.remove(entry.getKey());
            }
        }

        return !groupModifications.isEmpty();
    }

    public String[][] tablesThatMustBeEmpty() {
        ArrayList<String> tableSetNames = new ArrayList<>();
        ArrayList<String> errorMessages = new ArrayList<>();
        for (Map.Entry<String, TablePopulationRequirements> entry : m_tablesThatMustBeEmpty.entrySet()) {
            List<String> tableNames = entry.getValue().getTableNames();
            if (tableNames.size() > 0) {
                // It's unfortunate that we can't use String.join here.
                StringBuffer sb = new StringBuffer();
                String sep = "";
                for (String name : tableNames) {
                    if (! m_newTables.contains(name.toUpperCase())) {
                        sb.append(sep)
                          .append(name);
                        sep = "+";
                    }
                }
                if (sb.length() > 0) {
                    tableSetNames.add(sb.toString());
                    errorMessages.add(entry.getValue().getErrorMessage());
                }
            }
        }
        String answer[][] = new String[2][];
        answer[0] = tableSetNames.toArray(new String[0]);
        answer[1] = errorMessages.toArray(new String[0]);

        return answer;
    }

    public boolean worksWithElastic() {
        return m_canOccurWithElasticRebalance;
    }

    public String errors() {
        return m_errors.toString();
    }

    protected void runDiff(Catalog prev, Catalog next) {
        diffRecursively(prev, next);
        if (m_triggeredVerbosity || m_triggerForVerbosity.equals("final")) {
            System.out.println("DEBUG VERBOSE diffRecursively Errors:" +
                               ( m_supported ? " <none>" : "\n" + errors()));
            System.out.println("DEBUG VERBOSE diffRecursively Commands: " + commands());
        }
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

        // partial indexes must have identical predicates
        if (existingIndex.getPredicatejson().length() > 0) {
            if (existingIndex.getPredicatejson().equals(newIndex.getPredicatejson())) {
                return true;
            } else {
                return false;
            }
        } else if (newIndex.getPredicatejson().length() > 0) {
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
     * Check if an addition or deletion can be safely completed
     * in any database state.
     *
     * @return Return null if the CatalogType can be dynamically added or removed
     *         from any running system. Return an error message string if it can't be changed
     *         in an arbitrary running system.  The change might still be possible,
     *         and we check subsequently for database states in which the change is
     *         allowed.  Typically these states require a particular
     *         table, or one of a set of tables be empty.
     */
    protected String checkAddDropWhitelist(final CatalogType suspect, final ChangeType changeType)
    {
        //Will catch several things that are actually just deployment changes, but don't care
        //to be more specific at this point
        m_canOccurWithElasticRebalance = false;

        // should generate this from spec.txt
        if (suspect instanceof User ||
            suspect instanceof Group ||
            suspect instanceof Procedure ||
            suspect instanceof Function ||
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
            suspect instanceof Task)
        {
            return null;
        }
        else if (suspect instanceof Topic) {
            m_requiresNewExportGeneration = true;
            return null;
        }
        else if (suspect instanceof TimeToLive) {
            Column column = ((TimeToLive) suspect).getTtlcolumn();
            Table table = (Table) column.getParent();
            // view table can not have ttl columns
            if (m_inStrictMatViewDiffMode) {
                return "May not dynamically add TTl on materialized view's columns.";
            }
            // stream table can not have ttl columns
            if (CatalogUtil.isStream((Database)table.getParent(), table) ) {
                return "May not dynamically add TTL on stream table's columns.";
            }
            return null;
        }

        else if (suspect instanceof Table) {
            Table tbl = (Table)suspect;
            if (TableType.isStream(tbl.getTabletype()) || TableType.needsShadowStream(tbl.getTabletype())) {
                m_requiresNewExportGeneration = true;
            }
            // No special guard against dropping a table or view
            // (although some procedures may fail to plan)
            if (ChangeType.DELETION == changeType) {
                return null;
            }
            String tableName = tbl.getTypeName();

            // Remember the name of the new table.
            m_newTables.add(tableName.toUpperCase());

            String viewName = null;
            String sourceTableName = null;
            // If this is a materialized view, and it's not safe for non-empty
            // tables, then we need to note this.  In this conditional, we set
            // viewName to nonNull if the view has unsafe operations.  Otherwise
            // we leave it as null.
            if (tbl.getMvhandlerinfo().size() > 0) {
                MaterializedViewHandlerInfo mvhInfo = tbl.getMvhandlerinfo().get("mvhandlerinfo");
                if ( mvhInfo != null ) {
                    if ( ! mvhInfo.getIssafewithnonemptysources()) {
                        // Set viewName, but don't set sourceTableName
                        // because this is a multi-table view.
                        viewName = tbl.getTypeName();
                    }
                }
            } else if (tbl.getMaterializer() != null) {
                MaterializedViewInfo mvInfo = MaterializedViewProcessor.getMaterializedViewInfo(tbl);
                if (mvInfo != null && ( ! mvInfo.getIssafewithnonemptysources() ) ) {
                    // Set both names, as this is a single table view.
                    // We know getMaterializer() will return non-null.
                    viewName = tbl.getTypeName();
                    sourceTableName = tbl.getMaterializer().getTypeName();
                }
            }
            // Skip guard for view on stream, given the fact that stream table is always empty
            if (viewName != null && !TableType.isStream(tbl.getMaterializer().getTabletype())) {
                return createViewDisallowedMessage(viewName, sourceTableName);
            }
            // Otherwise, support add/drop of the top level object.
            return null;
        }

        else if (suspect instanceof Connector
                || suspect instanceof ThreadPool
                || suspect instanceof ConnectorTableInfo
                || suspect instanceof ConnectorProperty
                || suspect instanceof Topic) {
            m_requiresNewExportGeneration = true;
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
            Column column = (Column) suspect;
            Table table = (Table) column.getParent();
            if (m_inStrictMatViewDiffMode) {
                return "May not dynamically add, drop, or rename materialized view columns.";
            }
            boolean isStreamOrStreamView = CatalogUtil.isStream((Database)table.getParent(), table)
                    || TableType.needsShadowStream(table.getTabletype());
            if (isStreamOrStreamView) {
                m_requiresNewExportGeneration = true;
            }
            if (changeType == ChangeType.ADDITION) {
                Column col = (Column) suspect;
                // Skip guard for view on stream, given the fact that stream table is always empty
                if ((! col.getNullable()) && (col.getDefaultvalue() == null) && !isStreamOrStreamView) {
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

            // it's cool to remove indexes
            if (changeType == ChangeType.DELETION) {
                return null;
            }

            if (! index.getIssafewithnonemptysources()) {
                return "Unable to create index " + index.getTypeName() +
                       " while the table contains data." +
                       " The index definition uses operations that cannot be applied " +
                       "if table " + index.getParent().getTypeName() + " is not empty.";
            }

            if (! index.getUnique()) {
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

            if (parent instanceof Topic) {
                m_requiresNewExportGeneration = true;
                return null;
            }
        }
        return "May not dynamically add/drop schema object: '" + suspect + "', parent: " + suspect.getParent() + "\n";
    }

    /**
     * Check if an addition or deletion can be made depending on the
     * database state.
     *
     * @return Return null if the change is not possible under any circumstances.
     *         Otherwise, return a TablePopulationRequirements which encodes
     *         the required database state.  For example, when creating a
     *         materialized view, if the view's query uses unsafe operations,
     *         one of the source tables must be empty.  So, the TablePopulationRequirements
     *         object which we return would have a set of table names.   The returned
     *         TablePopulationRequirements object will have an error message.
     */
    protected TablePopulationRequirements checkAddDropIfTableIsEmptyWhitelist(final CatalogType suspect,
                                                                               final ChangeType changeType) {
        TablePopulationRequirements retval = null;

        // handle adding an index - presumably unique
        if (suspect instanceof Index) {
            Index idx = (Index) suspect;
            String indexName = idx.getTypeName();
            retval = new TablePopulationRequirements(indexName);
            String tableName = idx.getParent().getTypeName();
            retval.addTableName(tableName);

            if (! idx.getIssafewithnonemptysources()) {
                retval.setErrorMessage("Unable to create index " + indexName +
                                       " while the table contains data." +
                                       " The index definition uses operations that cannot be applied " +
                                       "if table " + tableName + " is not empty.");
            }
            else if (idx.getUnique()) {
                retval.setErrorMessage(
                        String.format(
                                "Unable to add unique index %s because table %s is not empty.",
                                indexName,
                                tableName));
            }
            return retval;
        }

        CatalogType parent = suspect.getParent();

        // handle changes to columns in an index - presumably drops and presumably unique
        if ((suspect instanceof ColumnRef) && (parent instanceof Index)) {
            Index idx = (Index) parent;
            assert(idx.getUnique());
            assert(changeType == ChangeType.DELETION);
            Table table = (Table) idx.getParent();

            String indexName = idx.getTypeName();
            String tableName = table.getTypeName();
            String errorMessage =
                    String.format(
                            "Unable to remove column %s from unique index %s because table %s is not empty.",
                            suspect.getTypeName(),
                            indexName,
                            tableName);
            retval = new TablePopulationRequirements(indexName, tableName, errorMessage);
            retval.addTableName(tableName);
            return retval;
        }

        if ((suspect instanceof Column) && (parent instanceof Table) && (changeType == ChangeType.ADDITION)) {
            String tableName = parent.getTypeName();
            retval = new TablePopulationRequirements(tableName);
            retval.addTableName(tableName);
            retval.setErrorMessage(
                    String.format(
                            "Unable to add NOT NULL column %s because table %s is not empty and no default value was specified.",
                            suspect.getTypeName(), tableName));
            return retval;
        }

        // Check to see if a table is a materialized view.  If
        // so, we want to check if the table is safe for non-empty
        // source tables, and leave the correct error message if so.
        if (suspect instanceof Table) {
            Table tbl = (Table)suspect;
            if (tbl.getMvhandlerinfo().size() > 0) {
                MaterializedViewHandlerInfo mvhInfo = tbl.getMvhandlerinfo().get("mvhandlerinfo");
                if ( mvhInfo != null && ( ! mvhInfo.getIssafewithnonemptysources()) ) {
                    retval = getMVHandlerInfoMessage(mvhInfo);
                    if (retval != null) {
                        return retval;
                    }
                }
            } else {
                MaterializedViewInfo mvInfo = MaterializedViewProcessor.getMaterializedViewInfo(tbl);
                if (mvInfo != null && ( ! mvInfo.getIssafewithnonemptysources())) {
                    retval = getMVInfoMessage(tbl, mvInfo);
                    if (retval != null) {
                        return retval;
                    }
                }
            }
            if (TableType.needsShadowStream(tbl.getTabletype())) {
                m_requiresNewExportGeneration = true;
            }
        }
        return null;
    }

    /**
     * Return an error message asserting that we cannot create a view
     * with a given name.
     *
     * @param viewName The name of the view we are refusing to create.
     * @param singleTableName The name of the source table if there is
     *                        one source table.  If there are multiple
     *                        tables this should be null.  This only
     *                        affects the wording of the error message.
     * @return
     */
    private String createViewDisallowedMessage(String viewName, String singleTableName) {
        boolean singleTable = (singleTableName != null);
        return String.format(
                "Unable to create %sview %s %sbecause the view definition uses operations that cannot always be applied if %s.",
                (singleTable
                        ? "single table "
                        : "multi-table "),
                viewName,
                (singleTable
                        ? String.format("on table %s ", singleTableName)
                        : ""),
                (singleTable
                        ? "the table already contains data"
                        : "none of the source tables are empty"));
    }

    /**
     * Check a MaterializedViewHandlerInfo object for safety.  Return
     * an object with table population requirements on the table for it to be
     * allowed.  The return object, if it is non-null, will have a set of names
     * of tables one of which must be empty if the view can be created.  It will
     * also have an error message.
     *
     * @param mvh A MaterializedViewHandlerInfo object describing the view part
     *            of a table.
     * @return A TablePopulationRequirements object describing a set of tables
     *         and an error message.
     */
    private TablePopulationRequirements getMVHandlerInfoMessage(MaterializedViewHandlerInfo mvh) {
        if ( ! mvh.getIssafewithnonemptysources()) {
            TablePopulationRequirements retval;
            String viewName = mvh.getDesttable().getTypeName();
            String errorMessage = createViewDisallowedMessage(viewName, null);
            retval = new TablePopulationRequirements(viewName);
            retval.setErrorMessage(errorMessage);
            for (TableRef tref : mvh.getSourcetables()) {
                String tableName = tref.getTable().getTypeName();
                retval.addTableName(tableName);
            }
            return retval;
        }
        return null;
    }

    private TablePopulationRequirements getMVInfoMessage(Table table, MaterializedViewInfo mv) {
        if (! mv.getIssafewithnonemptysources()) {
            TablePopulationRequirements retval;
            String viewName = mv.getTypeName();
            String sourceName = mv.getParent().getTypeName();
            String errorMessage = createViewDisallowedMessage(viewName, sourceName);
            retval = new TablePopulationRequirements(viewName);
            retval.setErrorMessage(errorMessage);
            retval.addTableName(sourceName);
            return retval;
        }
        return null;
    }

    /**
     * @return true if this change may be ignored
     */
    protected boolean checkModifyIgnoreList(final CatalogType suspect,
                                            final CatalogType prevType,
                                            final String field)
    {
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
     * Check to see if a CatalogType can be dynamically
     * modified in any running system, regardless of the
     * system's state.
     *
     * @return Return null if the change can be made.  Otherwise return
     *         an error message.  The change may be possible in
     *         particular database states, but this routine just
     *         decides of the modification is possible in any state.
     */
    protected String checkModifyWhitelist(final CatalogType suspect,
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
        // I added Statement and PlanFragment for the need of materialized view recalculation plan updates.
        // ENG-8641, yzhang.
        if (suspect instanceof User ||
            suspect instanceof Group ||
            suspect instanceof Procedure ||
            suspect instanceof SnapshotSchedule ||
            suspect instanceof UserRef ||
            suspect instanceof GroupRef ||
            suspect instanceof ColumnRef ||
            suspect instanceof Statement ||
            suspect instanceof PlanFragment ||
            suspect instanceof TimeToLive) {
            return null;
        }

        // Allow functions to add dependers.
        if (suspect instanceof Function && field.equals("stmtDependers")) {
            return null;
        }
        // Support modification of these specific fields
        if (suspect instanceof Database && field.equals("schema")) {
            return null;
        }
        if (suspect instanceof Database && "securityprovider".equals(field)) {
            return null;
        }
        if (suspect instanceof Cluster && field.equals("securityEnabled")) {
            return null;
        }
        if (suspect instanceof Cluster && field.equals("adminstartup")) {
            return null;
        }
        if (suspect instanceof Cluster && field.equals("heartbeatTimeout")) {
            return null;
        }
        if (suspect instanceof Cluster && field.equals("drProducerEnabled")) {
            return null;
        }
        if (suspect instanceof Cluster && field.equals("drConsumerEnabled")) {
            return null;
        }
        if (suspect instanceof Cluster && field.equals("preferredSource")) {
            return null;
        }
        if (suspect instanceof Connector && ("enabled".equals(field) || "loaderclass".equals(field) || "threadpoolname".equals(field))) {
            m_requiresNewExportGeneration = true;
            return null;
        }
        if (suspect instanceof ThreadPool) {
            m_requiresNewExportGeneration = true;
            return null;
        }
        // ENG-6511 Allow materialized views to change the index they use dynamically.
        if (suspect instanceof IndexRef && field.equals("name")) {
            return null;
        }
        if (suspect instanceof Deployment && field.equals("schemaRegistryUrl")) {
            m_requiresNewExportGeneration = true;
            return null;
        }
        if (suspect instanceof Topic) {
            m_requiresNewExportGeneration = true;
            return null;
        }

        // Avoid over-generalization when describing limitations that are dependent on particular
        // cases of BEFORE and AFTER values by listing the offending values.
        String restrictionQualifier = "";

        if (suspect instanceof Cluster) {
            if (field.equals("drFlushInterval")) {
                return null;
            } else if (field.equals("drProducerPort")) {
                // Don't allow changes to ClusterId or ProducerPort while not transitioning to or from Disabled
                if ((Boolean)prevType.getField("drProducerEnabled") && (Boolean)suspect.getField("drProducerEnabled")) {
                    restrictionQualifier = " while DR is enabled";
                }
                else {
                    return null;
                }
            } else if (field.equals("drMasterHost")) {
                String source = (String)suspect.getField("drMasterHost");
                if (source.isEmpty() && (Boolean)suspect.getField("drConsumerEnabled")) {
                    restrictionQualifier = " while DR is enabled";
                }
                else {
                    return null;
                }
            } else if (field.equals("drRole")) {
                final String prevRole = (String) prevType.getField("drRole");
                final String newRole = (String) suspect.getField("drRole");
                // Promote from replica to master
                if (prevRole.equals(DrRoleType.REPLICA.value()) && newRole.equals(DrRoleType.MASTER.value())) {
                    return null;
                }
                // Everything else is illegal
                else {
                    restrictionQualifier = " from " + prevRole + " to " + newRole;
                }
            } else if (field.equals("drConsumerSslPropertyFile")) {
                return null;
            }
        }

        if (suspect instanceof Constraint && field.equals("index"))
            return null;
        if (suspect instanceof Table) {
            if (field.equals("signature")) {
                return null;
            }
            if (field.equals("topicName") && prevType != null) {
                if (!(((Table)suspect).getTopicname().equals(((Table)prevType).getTopicname()))) {
                    m_requiresNewExportGeneration = true;
                }
                return null;
            }

            if (field.equals("tableType") && prevType != null) {
                if (((Table)suspect).getTabletype() != ((Table)prevType).getTabletype()) {
                    m_requiresNewExportGeneration = true;
                    return null;
                }
            }

            if (field.equals("migrationTarget")) {
                if (prevType != null && ((Table) suspect).getMigrationtarget() != ((Table) prevType).getMigrationtarget()) {
                    m_requiresNewExportGeneration = true;
                }
                return null;
            }
            // Always allow disabling DR on table
            if (field.equalsIgnoreCase("isdred")) {
                Boolean isDRed = (Boolean) suspect.getField(field);
                assert isDRed != null;
                if (!isDRed) return null;
            }
        }

        if (suspect instanceof Task && (field.equals("enabled") || field.equals("onError"))) {
            return null;
        }

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
            Table table = (Table) parent;
            if (TableType.needsExportDataSource(table.getTabletype())) {
                m_requiresNewExportGeneration = true;
                return null;
            } else if (TableType.isConnectorLessStream(table.getTabletype())) {
                return null;
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
                        restrictionQualifier = " narrowing from " + oldSize + "CHARACTERS to "
                    + newSize * CatalogSizing.MAX_BYTES_PER_UTF8_CHARACTER + " BYTES";
                    } else {
                        restrictionQualifier = " narrowing from " + oldSize + " to " + newSize;
                    }
                }
                else {
                    // ENG-13094 If the data type already changed, we do not throw more errors about the size.
                    if ( ! field.equals("type")) {
                        return null;
                    }
                    restrictionQualifier = " from " + oldType.toSQLString() +
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
            // allow export connector property changes
            if (parent instanceof Connector && suspect instanceof ConnectorProperty) {
                m_requiresNewExportGeneration = true;
                return null;
            }

            // allow topic property changes
            if (parent instanceof Topic && suspect instanceof Property) {
                m_requiresNewExportGeneration = true;
                return null;
            }
        }

        return "May not dynamically modify field '" + field +
                        "' of schema object '" + suspect + "'" + restrictionQualifier + ", parent:" + suspect.getParent();
    }

    /**
     * Return an indication of whether a catalog change may be when the
     * legality of the change depends on the state of the database.  Generally
     * this means some set of tables must be empty, or else one of a set of
     * tables must be empty.  For example, when changing the isActiveDRed
     * state, all DR'd tables must be empty.  See checkAddDropIfTableIsEmptyWhitelist
     * for a more complex example, adding materialized views.
     *
     * @return Null or a list of TablePopulationRequirement objects describe the required
     *         database state.  The list may be empty.
     */
    public List<TablePopulationRequirements> checkModifyIfTableIsEmptyWhitelist(final CatalogType suspect,
                                                                                final CatalogType prevType,
                                                                                final String field)
    {
        if (prevType instanceof Database) {
            return null;
        }

        if (prevType instanceof Table) {
            String objectName = suspect.getTypeName();
            TablePopulationRequirements entry = new TablePopulationRequirements(objectName);

            Table prevTable = (Table) prevType; // safe because of enclosing if-block
            Database db = (Database) prevType.getParent();

            // table name
            entry.addTableName(suspect.getTypeName());

            // allowed changes to a table
            if (field.equalsIgnoreCase("isreplicated")) {
                // error message
                entry.setErrorMessage(String.format(
                            "Unable to change whether table %s is replicated because it is not empty.",
                            objectName));
                return Collections.singletonList(entry);
            }
            if (field.equalsIgnoreCase("partitioncolumn")) {
                // error message
                entry.setErrorMessage(String.format(
                                            "Unable to change the partition column of table %s because it is not empty.",
                                            objectName));
                return Collections.singletonList(entry);
            }
            if (field.equalsIgnoreCase("isdred")) {
                // error message
                entry.setErrorMessage(String.format(
                                        "Unable to enable DR on table %s because it is not empty.",
                                        objectName));
                return Collections.singletonList(entry);
            }
        }

        // handle narrowing columns and some modifications on empty tables
        if (prevType instanceof Column) {
            Table table = (Table) prevType.getParent();
            Database db = (Database) table.getParent();

            String tableName = table.getTypeName();
            Column column = (Column)prevType;
            String columnName = column.getTypeName();

            // This is just used as a key in a map which helps us keep
            // track of error messages.
            String objectName = table.getTypeName() + "." + column.getName();
            TablePopulationRequirements entry = new TablePopulationRequirements(objectName);

            // capture the table name
            entry.addTableName(tableName);

            if (field.equalsIgnoreCase("type")) {
                // error message
                entry.setErrorMessage(String.format(
                        "Unable to make a possibly-lossy type change to column %s in table %s because it is not empty.",
                        columnName, tableName));
                return Collections.singletonList(entry);
            }

            if (field.equalsIgnoreCase("size")) {
                // error message
                entry.setErrorMessage(String.format(
                        "Unable to narrow the width of column %s in table %s because it is not empty.",
                        columnName, tableName));
                return Collections.singletonList(entry);
            }

            // Nullability changes are allowed on empty tables.
            if (field.equalsIgnoreCase("nullable")) {
                // Would be flipping the nullability, so invert the state for the message.
                String alteredNullness = column.getNullable() ? "NOT NULL" : "NULL";
                entry.setErrorMessage(String.format(
                                        "Unable to change column %s null constraint to %s in table %s because it is not empty.",
                                        columnName, alteredNullness, tableName));
                return Collections.singletonList(entry);
            }
        }

        if (prevType instanceof Index) {
            Table table = (Table) prevType.getParent();
            String tableName = table.getTypeName();
            Index index = (Index)prevType;
            String indexName = index.getTypeName();

            // capture the table name
            TablePopulationRequirements entry = new TablePopulationRequirements(indexName);
            entry.addTableName(tableName);
            if (field.equalsIgnoreCase("expressionsjson")) {
                // error message
                entry.setErrorMessage(String.format(
                        "Unable to alter table %s with expression-based index %s becase table %s is not empty.",
                        tableName, indexName, tableName));
                return Collections.singletonList(entry);
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
            List<TablePopulationRequirements> responseList = checkModifyIfTableIsEmptyWhitelist(newType, prevType, field);
            // handle all the error messages and state from the modify check
            processModifyResponses(errorMessage, responseList);
        }

        if (! m_requiresCatalogDiffCmdsApplyToEE && checkCatalogDiffShouldApplyToEE(newType)) {
            m_requiresCatalogDiffCmdsApplyToEE = true;
        }

        // write the commands to make it so
        // they will be ignored if the change is unsupported
        m_serializer.writeCommandForField(newType, field, true);

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
     * Our EE has a list of Catalog items that are in use, but Java catalog contains much more.
     * Some of the catalog diff commands will only be useful to Java. So this function will
     * decide whether the @param suspect catalog item will be used in EE or not.
     * @param suspect
     * @param prevType
     * @param field
     * @return true if the suspect catalog will be updated in EE, false otherwise.
     */
    protected static boolean checkCatalogDiffShouldApplyToEE(final CatalogType suspect)
    {
        // Warning:
        // This check list should be consistent with catalog items defined in EE
        // Once a new catalog type is added in EE, we should add it here.

        if (suspect instanceof Cluster || suspect instanceof Database) {
            return true;
        }

        // Information about user-defined functions need to be applied to EE.
        // Because the EE needs to know about the parameter types and the return type to do
        // many type casting operations.
        if (suspect instanceof Function) {
            return true;
        }

        if (suspect instanceof Table || suspect instanceof TableRef ||
                suspect instanceof Column || suspect instanceof ColumnRef ||
                suspect instanceof Index || suspect instanceof IndexRef ||
                suspect instanceof Constraint || suspect instanceof ConstraintRef ||
                suspect instanceof MaterializedViewInfo || suspect instanceof MaterializedViewHandlerInfo) {
            return true;
        }

        // Statement can be children of Table or MaterilizedViewInfo, which should apply to EE
        // But if they are under Procedure, we can skip them.
        if (suspect instanceof Statement && (suspect.getParent() instanceof Procedure == false)) {
            return true;
        }

        // PlanFragment is a similar case like Statement
        if (suspect instanceof PlanFragment && suspect.getParent() instanceof Statement &&
                (suspect.getParent().getParent() instanceof Procedure == false)) {
            return true;
        }

        if (suspect instanceof Connector ||
                suspect instanceof ConnectorProperty ||
                suspect instanceof ConnectorTableInfo) {
            // export table related change, should not skip EE
            return true;
        }

        // Note: only topics encoded inline need an EE update but this flag is coarse
        if (suspect instanceof Topic || suspect.getParent() instanceof Topic) {
            return true;
        }

        // The other changes in the catalog will not be applied to EE,
        // including User, Group, Procedures, etc
        return false;
    }

    /**
     * After we decide we can't modify, add or delete something on a full table,
     * we do a check to see if we can do that on an empty table. The original error
     * and any response from the empty table check is processed here. This code
     * is basically in this method so it's not repeated 3 times for modify, add
     * and delete. See where it's called for context.
     * If the responseList equals null, it is not possible to modify, otherwise we
     * do the check described above for every element in the responseList, if there
     * is no element in the responseList, it means no tables must be empty, which is
     * totally fine.
     */
    private void processModifyResponses(String errorMessage, List<TablePopulationRequirements> responseList) {
        assert(errorMessage != null);

        // if no requirements, then it's just not possible
        if (responseList == null) {
            m_supported = false;
            m_errors.append(errorMessage + "\n");
            return;
        }
        // otherwise, it's possible if a specific table is empty
        // collect the error message(s) and decide if it can be done inside @UAC
        for (TablePopulationRequirements response : responseList) {
            String objectName = response.getObjectName();
            String nonEmptyErrorMessage = response.getErrorMessage();
            assert (nonEmptyErrorMessage != null);

            TablePopulationRequirements popreq = m_tablesThatMustBeEmpty.get(objectName);
            if (popreq == null) {
                popreq = response;
                m_tablesThatMustBeEmpty.put(objectName, popreq);
            } else {
                String newErrorMessage = popreq.getErrorMessage() + "\n " + response.getErrorMessage();
                popreq.setErrorMessage(newErrorMessage);
            }
        }
    }

    /**
     * Add a deletion
     */
    private void writeDeletion(CatalogType prevType, CatalogType newlyChildlessParent, String mapName)
    {
        // Don't write deletions if the field can be ignored
        if (checkDeleteIgnoreList(prevType, newlyChildlessParent, mapName, prevType.getTypeName())) {
            return;
        }

        // verify this is possible, write an error and mark return code false if so
        String errorMessage = checkAddDropWhitelist(prevType, ChangeType.DELETION);

        // if it's not possible with non-empty tables, check for possible with empty tables
        if (errorMessage != null) {
            TablePopulationRequirements response = checkAddDropIfTableIsEmptyWhitelist(prevType, ChangeType.DELETION);
            List<TablePopulationRequirements> responseList = null;
            if (response != null) {
                responseList = Collections.singletonList(response);
            }
            processModifyResponses(errorMessage, responseList);
        }

        if (! m_requiresCatalogDiffCmdsApplyToEE && checkCatalogDiffShouldApplyToEE(prevType)) {
            m_requiresCatalogDiffCmdsApplyToEE = true;
        }

        // write the commands to make it so
        // they will be ignored if the change is unsupported
        m_serializer.writeDeleteDiffStatement(prevType, mapName);

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
            TablePopulationRequirements response = checkAddDropIfTableIsEmptyWhitelist(newType, ChangeType.ADDITION);
            // handle all the error messages and state from the modify check
            List<TablePopulationRequirements> responseList = null;
            if (response != null) {
                responseList = Collections.singletonList(response);
            }
            processModifyResponses(errorMessage, responseList);
        }

        if (! m_requiresCatalogDiffCmdsApplyToEE && checkCatalogDiffShouldApplyToEE(newType)) {
            m_requiresCatalogDiffCmdsApplyToEE = true;
        }

        // write the commands to make it so
        // they will be ignored if the change is unsupported
        newType.accept(m_serializer);

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
            // Under normal circumstances, it's highly unlikely that another (nested?) table will
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
                            System.out.println("DEBUG VERBOSE diffRecursively prev: " + prevValue);
                            System.out.println("DEBUG VERBOSE diffRecursively new : " + newValue);
                            if (field.equals("plannodetree")) {
                                try {
                                    System.out.println("DEBUG VERBOSE where prev plannodetree expands to: " +
                                            new String(CompressionService.decodeBase64AndDecompressToBytes((String)prevValue), "UTF-8"));
                                }
                                catch (Exception e) {
                                    try {
                                        System.out.println("DEBUG VERBOSE where prev plannodetree expands to: " +
                                                new String(Encoder.decodeBase64AndDecompressToBytes((String)prevValue), "UTF-8"));
                                    }
                                    catch (UnsupportedEncodingException e2) {}
                                }
                                try {
                                    System.out.println("DEBUG VERBOSE where new plannodetree expands to: " +
                                            new String(CompressionService.decodeBase64AndDecompressToBytes((String)newValue), "UTF-8"));
                                }
                                catch (Exception e) {
                                    try {
                                        System.out.println("DEBUG VERBOSE where new plannodetree expands to: " +
                                                new String(Encoder.decodeBase64AndDecompressToBytes((String)newValue), "UTF-8"));
                                    }
                                    catch (UnsupportedEncodingException e2) {}
                                }
                            }
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
                writeDeletion(prevType, newMap.m_parent, mapName);
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
        FUNC (Function.class),
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
    private final Map<DiffClass, CatalogChangeGroup> m_changes = new TreeMap<>();

    /**
     * Get a human readable list of changes between two catalogs.
     *
     * This currently handles just the basics, but much of the plumbing is
     * in place to give a lot more detail, with a bit more work.
     */
    public String getDescriptionOfChanges(boolean updatedClass) {
        StringBuilder sb = new StringBuilder();

        sb.append("Catalog Difference Report\n");
        sb.append("=========================\n");
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
                // this probably doesn't work due to the same kinds of problems we have
                // when identifying views. Tables just need a field that says if they
                // are export tables or not... ugh. FIXME
                for (Connector c : ((Database) table.getParent()).getConnectors()) {
                    for (ConnectorTableInfo cti : c.getTableinfo()) {
                        if (cti.getTable() == table) {
                            return "Stream Table " + type.getTypeName();
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

        // DESCRIBE FUNCTION CHANGES
        wroteChanges |= basicMetaChangeDesc(sb, "FUNCTION CHANGES:", DiffClass.FUNC, null, null);

        // DESCRIBE GROUP CHANGES
        wroteChanges |= basicMetaChangeDesc(sb, "GROUP CHANGES:", DiffClass.GROUP, null, null);

        // DESCRIBE USER CHANGES
        wroteChanges |= basicMetaChangeDesc(sb, "USER CHANGES:", DiffClass.USER, null, null);

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
            if (updatedClass) {
                sb.append("  Changes have been made to user code (procedures, supporting classes, etc).\n");
            } else {
                sb.append("  No changes detected.\n");
            }
        }

        // trim the last newline
        sb.setLength(sb.length() - 1);

        return sb.toString();
    }
}
