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

package org.voltdb.plannodes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.OperationMode;
import org.voltdb.VoltDB;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.MaterializedViewHandlerInfo;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.PlanNodeType;

public class SwapTablesPlanNode extends AbstractOperationPlanNode {
    private static class Members {
        static final String OTHER_TARGET_TABLE_NAME = "OTHER_TARGET_TABLE_NAME";
        static final String INDEXES = "INDEXES";
        static final String OTHER_INDEXES = "OTHER_INDEXES";
    }

    private String m_otherTargetTableName;
    private List<String> m_theIndexes = new ArrayList<>();
    private List<String> m_otherIndexes = new ArrayList<>();

    private static class FailureMessage {
        private final List<String> m_failureReasons = new ArrayList<>();
        private final String m_theTable;
        private final String m_otherTable;

        public FailureMessage(String theTable, String otherTable) {
            m_theTable = theTable;
            m_otherTable = otherTable;
        }

        public int numFailures() {
            return m_failureReasons.size();
        }

        public void addReason(String reason) {
            m_failureReasons.add(reason);
        }

        public String getMessage() {
            if (numFailures() == 0) {
                return "";
            }

            StringBuilder sb = new StringBuilder();

            sb.append("Swapping tables ")
                    .append(m_theTable)
                    .append(" and ")
                    .append(m_otherTable)
                    .append(" failed for the following reason(s):");

            for (String reason : m_failureReasons) {
                sb.append("\n  - ").append(reason);
            }

            return sb.toString();
        }
    }

    public SwapTablesPlanNode() {
        super();
    }

    @Override
    public SwapTablesPlanNode clone() throws CloneNotSupportedException {
        SwapTablesPlanNode other = (SwapTablesPlanNode) super.clone();
        other.m_theIndexes = new ArrayList<>(m_theIndexes);
        other.m_otherIndexes = new ArrayList<>(m_otherIndexes);
        return other;
    }

    @Override
    public PlanNodeType getPlanNodeType() { return PlanNodeType.SWAPTABLES; };

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.OTHER_TARGET_TABLE_NAME,
                m_otherTargetTableName);
        toJSONStringArrayString(stringer, Members.INDEXES, m_theIndexes);
        toJSONStringArrayString(stringer, Members.OTHER_INDEXES, m_otherIndexes);
    }

    @Override
    public void loadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        m_otherTargetTableName = jobj.getString(Members.OTHER_TARGET_TABLE_NAME);
        m_theIndexes = loadStringListMemberFromJSON(jobj, "INDEXES");
        m_otherIndexes = loadStringListMemberFromJSON(jobj, "OTHER_INDEXES");
    }

    @Override
    protected String explainPlanForNode(String indent) {
        StringBuilder sb = new StringBuilder("SWAP TABLE ");
        sb.append(getTargetTableName())
        .append(" WITH ").append(m_otherTargetTableName);
        if ( ! m_theIndexes.isEmpty()) {
            sb.append(" swapping indexes: \n");
            for (int ii = 0; ii < m_theIndexes.size(); ++ii) {
                sb.append(indent).append(m_theIndexes.get(ii))
                .append(" with ").append(m_otherIndexes.get(ii));
            }
        }
        return sb.toString();
    }

    /** SWAP TABLES has no effect on data ordering. */
    @Override
    public boolean isOrderDeterministic() {
        return true;
    }

    /**
     * Fill out all of the serializable attributes of the node, validating
     * its arguments' compatibility along the way to ensure successful
     * execution.
     * @param theTable the catalog definition of the 1st table swap argument
     * @param otherTable the catalog definition of the 2nd table swap argument
     */
    public void initializeSwapTablesPlanNode(Table theTable, Table otherTable) {
        String theName = theTable.getTypeName();
        setTargetTableName(theName);
        String otherName = otherTable.getTypeName();
        m_otherTargetTableName = otherName;

        FailureMessage failureMessage = new FailureMessage(theName, otherName);

        validateTableCompatibility(theName, otherName, theTable, otherTable, failureMessage);
        validateColumnCompatibility(theName, otherName, theTable, otherTable, failureMessage);

        // Maintain sets of indexes and index-supported (UNIQUE) constraints
        // and the primary key index found on otherTable.
        // Removing them as they are matched by indexes/constraints on theTable
        // and added to the list of swappable indexes should leave the sets empty.
        HashSet<Index> otherIndexSet = new HashSet<>();
        // The constraint set is actually a HashMap to retain the
        // defining constraint name for help with error messages.
        // Track the primary key separately since it should match one-to-one.
        HashMap<Index, String> otherConstraintIndexMap = new HashMap<>();
        Index otherPrimaryKeyIndex = null;

        // Collect the system-defined (internal) indexes supporting constraints
        // and the primary key index if any.
        CatalogMap<Constraint> candidateConstraints = otherTable.getConstraints();
        for (Constraint otherConstraint : candidateConstraints) {
            Index otherIndex = otherConstraint.getIndex();
            if (otherIndex == null) {
                // Some kinds of constraints that are not index-based have no
                // effect on the swap table plan.
                continue;
            }

            // Set aside the one primary key index for special handling.
            if (otherConstraint.getType() == ConstraintType.PRIMARY_KEY.getValue()) {
                otherPrimaryKeyIndex = otherIndex;
                continue;
            }

            otherConstraintIndexMap.put(otherIndex, otherConstraint.getTypeName());
        }

        // Collect the user-defined (external) indexes on otherTable.  The indexes
        // in this set are removed as corresponding matches are found.
        // System-generated indexes that support constraints are checked separately,
        // so don't add them to this set.
        CatalogMap<Index> candidateIndexes = otherTable.getIndexes();
        for (Index otherIndex : candidateIndexes) {
            if (otherIndex != otherPrimaryKeyIndex &&
                    !otherConstraintIndexMap.containsKey(otherIndex)) {
                otherIndexSet.add(otherIndex);
            }
        }

        // Collect the indexes that support constraints on theTable
        HashSet<Index> theConstraintIndexSet = new HashSet<>();
        Index thePrimaryKeyIndex = null;
        for (Constraint constraint : theTable.getConstraints()) {
            Index theIndex = constraint.getIndex();
            if (theIndex == null) {
                continue;
            }

            if (constraint.getType() == ConstraintType.PRIMARY_KEY.getValue()) {
                thePrimaryKeyIndex = theIndex;
                continue;
            }

            theConstraintIndexSet.add(constraint.getIndex());
        }

        // Make sure that both either both or neither tables have primary keys, and if both,
        // make sure the indexes are swappable.
        if (thePrimaryKeyIndex != null && otherPrimaryKeyIndex != null) {
            if (indexesCanBeSwapped(thePrimaryKeyIndex, otherPrimaryKeyIndex)) {
                m_theIndexes.add(thePrimaryKeyIndex.getTypeName());
                m_otherIndexes.add(otherPrimaryKeyIndex.getTypeName());
            } else {
                failureMessage.addReason("PRIMARY KEY constraints do not match on both tables");
            }
        } else if (thePrimaryKeyIndex != null || otherPrimaryKeyIndex != null) {
            failureMessage.addReason("one table has a PRIMARY KEY constraint and the other does not");
        }

        // Try to cross-reference each user-defined index on the two tables.
        for (Index theIndex : theTable.getIndexes()) {
            if (theConstraintIndexSet.contains(theIndex) || theIndex == thePrimaryKeyIndex) {
                // Constraints are checked below.
                continue;
            }

            boolean matched = false;
            for (Index otherIndex : otherIndexSet) {
                if (indexesCanBeSwapped(theIndex, otherIndex)) {
                    m_theIndexes.add(theIndex.getTypeName());
                    m_otherIndexes.add(otherIndex.getTypeName());
                    otherIndexSet.remove(otherIndex);
                    matched = true;
                    break;
                }
            }
            if (matched) {
                continue;
            }

            // No match: look for a likely near-match based on naming
            // convention for the most helpful error message.
            // Otherwise, give a more generic error message.
            String theIndexName = theIndex.getTypeName();
            String message = "the index " + theIndexName + " on table " + theName
                    + " has no corresponding index in the other table";
            String otherIndexName = theIndexName.replace(theName, otherName);
            Index otherIndex = candidateIndexes.getIgnoreCase(otherIndexName);
            if (otherIndex != null) {
                message += "; the closest candidate ("
                        + otherIndexName + ") has mismatches in the following attributes: "
                        + String.join(", ", diagnoseIndexMismatch(theIndex, otherIndex));
            }
            failureMessage.addReason(message);
        }

        // At this point, all of theTable's indexes are matched.
        // All of otherTable's indexes should also have been
        // matched along the way.
        if ( ! otherIndexSet.isEmpty()) {
            List<String> indexNames = otherIndexSet.stream().map(CatalogType::getTypeName)
                    .collect(Collectors.toList());
            failureMessage.addReason("the table " + otherName + " contains these index(es) "
                    + "which have no corresponding indexes on " + theName + ": "
                    + "(" + String.join(", ", indexNames) + ")");
        }

        // Try to cross-reference each system-defined index supporting
        // constraints on the two tables.
        for (Constraint theConstraint : theTable.getConstraints()) {
            Index theIndex = theConstraint.getIndex();
            if (theIndex == null) {
                // Some kinds of constraints that are not index-based have no
                // effect on the swap table plan.
                continue;
            }

            if (theConstraint.getType() == ConstraintType.PRIMARY_KEY.getValue()) {
                // Primary key compatibility checked above.
                continue;
            }

            boolean matched = false;
            for (Entry<Index, String> otherEntry : otherConstraintIndexMap.entrySet()) {
                Index otherIndex = otherEntry.getKey();
                if (indexesCanBeSwapped(theIndex, otherIndex)) {
                    m_theIndexes.add(theIndex.getTypeName());
                    m_otherIndexes.add(otherIndex.getTypeName());
                    otherConstraintIndexMap.remove(otherIndex);
                    matched = true;
                    break;
                }
            }
            if (matched) {
                continue;
            }

            String theConstraintName = theConstraint.getTypeName();
            failureMessage.addReason("the constraint " + theConstraintName + " on table " + theName + " "
                    + "has no corresponding constraint on the other table");
        }

        // At this point, all of theTable's index-based constraints are matched.
        // All of otherTable's index-based constraints should also have been
        // matched along the way.
        if ( ! otherConstraintIndexMap.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("these constraints (or system internal index names) on table ").append(otherName)
                    .append(" ").append("have no corresponding constraints on the other table: (");
            String separator = "";
            for (Entry<Index, String> remainder : otherConstraintIndexMap.entrySet()) {
                String constraintName = remainder.getValue();
                String description =
                        (constraintName != null && ! constraintName.equals("")) ?
                                constraintName :
                                    ("<anonymous with system internal index name: " +
                                            remainder.getKey().getTypeName() + ">");
                sb.append(separator).append(description);
                separator = ", ";
            }
            sb.append(")");
            failureMessage.addReason(sb.toString());
        }


        if (failureMessage.numFailures() > 0) {
            throw new PlanningErrorException(failureMessage.getMessage());
        }
    }

    /**
     * Give two strings, return a list of attributes that do not match
     * @param theIndex
     * @param otherIndex
     * @return list of attributes that do not match
     */
    private List<String> diagnoseIndexMismatch(Index theIndex, Index otherIndex) {
        List<String> mismatchedAttrs = new ArrayList<>();

        // Pairs of matching indexes must agree on type (int hash, etc.).
        if (theIndex.getType() != otherIndex.getType()) {
            mismatchedAttrs.add("index type (hash vs tree)");
        }

        // Pairs of matching indexes must agree whether they are (assume)unique.
        if (theIndex.getUnique() != otherIndex.getUnique() ||
                theIndex.getAssumeunique() != otherIndex.getAssumeunique()) {
            mismatchedAttrs.add("UNIQUE attribute");
        }

        // Pairs of matching indexes must agree whether they are partial
        // and if so, agree on the predicate.
        String thePredicateJSON = theIndex.getPredicatejson();
        String otherPredicateJSON = otherIndex.getPredicatejson();
        if (thePredicateJSON == null) {
            if (otherPredicateJSON != null) {
                mismatchedAttrs.add("WHERE predicate");
            }
        } else if ( ! thePredicateJSON.equals(otherPredicateJSON)) {
            mismatchedAttrs.add("WHERE predicate");
        }

        // Pairs of matching indexes must agree that they do or do not index
        // expressions and, if so, agree on the expressions.
        String theExprsJSON = theIndex.getExpressionsjson();
        String otherExprsJSON = otherIndex.getExpressionsjson();
        if (theExprsJSON == null) {
            if (otherExprsJSON != null) {
                mismatchedAttrs.add("indexed expression");
            }
        } else if ( ! theExprsJSON.equals(otherExprsJSON)) {
            mismatchedAttrs.add("indexed expression");
        }


        // Indexes must agree on the columns they are based on,
        // identifiable by the columns' order in the table.
        CatalogMap<ColumnRef> theColumns = theIndex.getColumns();
        int theColumnCount = theColumns.size();
        CatalogMap<ColumnRef> otherColumns = otherIndex.getColumns();
        if (theColumnCount != otherColumns.size() ) {
            mismatchedAttrs.add("indexed expression");
        }

        Iterator<ColumnRef> theColumnIterator = theColumns.iterator();
        Iterator<ColumnRef> otherColumnIterator = otherColumns.iterator();
        for (int ii = 0 ;ii < theColumnCount; ++ii) {
            int theColIndex = theColumnIterator.next().getColumn().getIndex();
            int otherColIndex = otherColumnIterator.next().getColumn().getIndex();
            if (theColIndex != otherColIndex) {
                mismatchedAttrs.add("indexed expression");
            }
        }

        return mismatchedAttrs;
    }

    /**
     * @param theIndex candidate match for otherIndex on the target table
     * @param otherIndex candidate match for theIndex on the other target table
     */
    private static boolean indexesCanBeSwapped(Index theIndex, Index otherIndex) {
        // Pairs of matching indexes must agree on type (int hash, etc.).
        if (theIndex.getType() != otherIndex.getType()) {
            return false;
        }

        // Pairs of matching indexes must agree whether they are (assume)unique.
        if (theIndex.getUnique() != otherIndex.getUnique() ||
                theIndex.getAssumeunique() != otherIndex.getAssumeunique()) {
            return false;
        }

        // Pairs of matching indexes must agree whether they are partial
        // and if so, agree on the predicate.
        String thePredicateJSON = theIndex.getPredicatejson();
        String otherPredicateJSON = otherIndex.getPredicatejson();
        if (thePredicateJSON == null) {
            if (otherPredicateJSON != null) {
                return false;
            }
        } else if (! thePredicateJSON.equals(otherPredicateJSON)) {
            return false;
        }


        // Pairs of matching indexes must agree that they do or do not index
        // expressions and, if so, agree on the expressions.
        String theExprsJSON = theIndex.getExpressionsjson();
        String otherExprsJSON = otherIndex.getExpressionsjson();
        if (theExprsJSON == null) {
            if (otherExprsJSON != null) {
                return false;
            }
        }
        else if ( ! theExprsJSON.equals(otherExprsJSON)) {
            return false;
        }

        // Indexes must agree on the columns they are based on,
        // identifiable by the columns' order in the table.
        CatalogMap<ColumnRef> theColumns = theIndex.getColumns();
        int theColumnCount = theColumns.size();
        CatalogMap<ColumnRef> otherColumns = otherIndex.getColumns();
        if (theColumnCount != otherColumns.size() ) {
            return false;
        }

        Iterator<ColumnRef> theColumnIterator = theColumns.iterator();
        Iterator<ColumnRef> otherColumnIterator = otherColumns.iterator();
        for (int ii = 0 ;ii < theColumnCount; ++ii) {
            int theColIndex = theColumnIterator.next().getColumn().getIndex();
            int otherColIndex = otherColumnIterator.next().getColumn().getIndex();
            if (theColIndex != otherColIndex) {
                return false;
            }
        }

        return true;
    }

    /**
     * Flag any issues of incompatibility between the two table operands
     * of a swap by appending error details to a feedback buffer. These
     * details and possibly others should get attached to a
     * PlannerErrorException's message by the caller.
     * @param theName the first argument to the table swap
     * @param otherName the second argument to the tble swap
     * @param theTable the catalog Table definition named by theName
     * @param otherTable the catalog Table definition named by otherName
     * @return the current feedback output separator,
     *         it will be == TRUE_FB_SEPARATOR
     *         if the feedback buffer is not empty.
     */
    private void validateTableCompatibility(String theName, String otherName,
            Table theTable, Table otherTable, FailureMessage failureMessage) {

        if (theTable.getIsdred() != otherTable.getIsdred()) {
            failureMessage.addReason("To swap table " + theName + " with table " + otherName +
            " both tables must be DR enabled or both tables must not be DR enabled.");
        }
        else if (theTable.getIsdred() && otherTable.getIsdred() &&
                 VoltDB.instance().getMode() != OperationMode.PAUSED ) {
            failureMessage.addReason("You cannot use @SwapTables on DRed tables while DR is active. " +
                                     "To swap tables, first pause all clusters, invoke @SwapTables, then resume.");
        }

        if (theTable.getIsreplicated() != otherTable.getIsreplicated()) {
            failureMessage.addReason("one table is partitioned and the other is not");
        }

        if ((theTable.getMaterializer() != null ||
                ! theTable.getMvhandlerinfo().isEmpty()) ||
                (otherTable.getMaterializer() != null ||
                ! otherTable.getMvhandlerinfo().isEmpty())) {
            failureMessage.addReason("one or both of the tables is actually a view");
        }

        StringBuilder viewNames = new StringBuilder();
        if (viewsDependOn(theTable, viewNames)) {
            failureMessage.addReason(theName + " is referenced in views " + viewNames.toString());
        }

        viewNames.setLength(0);
        if (viewsDependOn(otherTable, viewNames)) {
            failureMessage.addReason(otherName + " is referenced in views " + viewNames.toString());
        }
    }

    /**
     * @param
     * @return
     */
    private static boolean viewsDependOn(Table aTable, StringBuilder viewNames) {
        String separator = "(";
        for (MaterializedViewInfo anyView : aTable.getViews()) {
            viewNames.append(separator).append(anyView.getTypeName());
            separator = ", ";
        }
        for (Table anyTable : ((Database) aTable.getParent()).getTables()) {
            for (MaterializedViewHandlerInfo anyView : anyTable.getMvhandlerinfo()) {
                if (anyView.getSourcetables().getIgnoreCase(aTable.getTypeName()) != null) {
                    viewNames.append(separator).append(anyView.getDesttable().getTypeName());
                    separator = ", ";
                }
            }
        }
        if (", ".equals(separator)) {
            viewNames.append(")");
            return true;
        }
        return false;
    }

    /**
     * Flag any issues of incompatibility between the columns of the two table
     * operands of a swap by appending error details to a feedback buffer.
     * These details and possibly others should get attached to a
     * PlannerErrorException's message by the caller.
     * @param theName the first argument to the table swap
     * @param otherName the second argument to the tble swap
     * @param theTable the catalog Table definition named by theName
     * @param otherTable the catalog Table definition named by otherName
     * @return the current feedback output separator,
     *         it will be == TRUE_FB_SEPARATOR
     *         if the feedback buffer is not empty.
     */
    private void validateColumnCompatibility(String theName, String otherName,
                Table theTable, Table otherTable,
                FailureMessage failureMessage) {
        CatalogMap<Column> theColumns = theTable.getColumns();
        int theColCount = theColumns.size();
        CatalogMap<Column> otherColumns = otherTable.getColumns();

        if (theColCount != otherColumns.size()) {
            failureMessage.addReason("the tables have different numbers of columns");
            return;
        }

        Column[] theColArray = new Column[theColumns.size()];
        for (Column theColumn : theColumns) {
            theColArray[theColumn.getIndex()] = theColumn;
        }

        for (Column otherColumn : otherColumns) {
            int colIndex = otherColumn.getIndex();
            String colName = otherColumn.getTypeName();
            if (colIndex < theColCount) {
                Column theColumn = theColArray[colIndex];
                if (theColumn.getTypeName().equals(colName)) {
                    if (theColumn.getType() != otherColumn.getType() ||
                            theColumn.getSize() != otherColumn.getSize() ||
                            theColumn.getInbytes() != otherColumn.getInbytes()) {
                        failureMessage.addReason("columns named " + colName + " have different types or sizes");
                    }
                    continue;
                }
            }

            Column matchedByName = theColumns.get(colName);
            if (matchedByName != null) {
                failureMessage.addReason(colName + " is in a different ordinal position in the two tables");
            } else {
                failureMessage.addReason(colName + " appears in " + otherName + " but not in " + theName);
            }
        }

        if ( ! theTable.getIsreplicated() && ! otherTable.getIsreplicated() ) {
            if (! theTable.getPartitioncolumn().getTypeName().equals(
                    otherTable.getPartitioncolumn().getTypeName())) {
                failureMessage.addReason("the tables are not partitioned on the same column");
            }
        }
    }
}
