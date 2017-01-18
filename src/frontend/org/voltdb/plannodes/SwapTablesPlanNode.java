/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.MaterializedViewHandlerInfo;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Table;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.PlanNodeType;

public class SwapTablesPlanNode extends AbstractOperationPlanNode {
    private static class Members {
        static final String OTHER_TARGET_TABLE_NAME = "OTHER_TARGET_TABLE_NAME";
        static final String INDEXES = "INDEXES";
        static final String OTHER_INDEXES = "OTHER_INDEXES";
    }

    private static final String TRUE_FB_SEPARATOR = "\n";

    private String m_otherTargetTableName;
    private List<String> m_theIndexes = new ArrayList<>();
    private List<String> m_otherIndexes = new ArrayList<>();

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
    public void loadFromJSONObject(JSONObject jobj, Database db)
            throws JSONException {
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
    public boolean isOrderDeterministic() { return true; }

    /**
     * Fill out all of the serializable attributes of the node, validating
     * its arguments' compatibility along the way to ensure successful
     * execution.
     * @param theTable the catalog definition of the 1st table swap argument
     * @param otherTable the catalog definition of the 2nd table swap argument
     * @throws PlnnerErrorException if one or more compatibility validations fail
     */
    public void initializeSwapTablesPlanNode(Table theTable, Table otherTable) {
        String theName = theTable.getTypeName();
        setTargetTableName(theName);
        String otherName = otherTable.getTypeName();
        m_otherTargetTableName = otherName;

        StringBuilder feedback = new StringBuilder();
        // This only gets replaced with a true separator, TRUE_FB_SEPARATOR,
        // after the first line of feedback has been appended.
        String fbSeparator = "";

        fbSeparator = validateTableCompatibility(theName, otherName,
                theTable, otherTable, feedback, fbSeparator);
        fbSeparator = validateColumnCompatibility(theName, otherName,
                theTable, otherTable, feedback, fbSeparator);

        // Maintain sets of indexes and index-supported (UNIQUE) constraints
        // and the primary key index found on otherTable.
        // Removing them as they are matched by indexes/constraints on theTable
        // and added to the list of swappable indexes should leave the sets empty.
        HashSet<Index> candidateIndexSet = new HashSet<>();
        // The constraint set is actually a HashMap to retain the
        // defining constraint name for help with error messages.
        HashMap<Index, String> candidateConstraintIndexSet = new HashMap<>();
        // Track the primary key separately since it should match one-to-one.
        Index otherPrimaryKeyIndex = null;

        // Collect the user-defined (external) indexes on otherTable.
        CatalogMap<Index> candidateIndexes = otherTable.getIndexes();
        for (Index otherIndex : candidateIndexes) {
            candidateIndexSet.add(otherIndex);
        }

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

            candidateConstraintIndexSet.put(otherIndex, otherConstraint.getTypeName());
        }

        // Try to cross-reference each user-defined index on the two tables.
        for (Index theIndex : theTable.getIndexes()) {
            boolean matched = false;
            for (Index otherIndex : candidateIndexSet) {
                if (indexesCanBeSwapped(theIndex, otherIndex, null, null)) {
                    m_theIndexes.add(theIndex.getTypeName());
                    m_otherIndexes.add(otherIndex.getTypeName());
                    candidateIndexSet.remove(otherIndex);
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
            String otherIndexName = theIndexName.replace(theName, otherName);
            Index otherIndex = candidateIndexes.getIgnoreCase(otherIndexName);
            if (otherIndex == null) {
                feedback.append(fbSeparator)
                .append("Swapping requires the table ").append(otherName)
                .append(" to define an index ").append(otherIndexName)
                .append(" corresponding to the index ").append(theIndexName)
                .append(" on table ").append(theName);
                fbSeparator = TRUE_FB_SEPARATOR;
            }
            else {
                diagnoseIndexMismatch(theIndex, otherIndex, feedback);
                fbSeparator = TRUE_FB_SEPARATOR;
                candidateIndexSet.remove(otherIndex);
            }
        }

        // At this point, all of theTable's indexes are matched.
        // All of otherTable's indexes should also have been
        // matched along the way.
        if ( ! candidateIndexSet.isEmpty()) {
            feedback.append(fbSeparator)
            .append("Swapping with table ").append(otherName)
            .append(" requires that the table ").append(theName)
            .append("\" define indexes corresponding to ")
            .append(" all of those defined on table ").append(otherName)
            .append("including those with the following index names: (");
            String separator = "";
            for (Index remainder : candidateIndexSet) {
                feedback.append(separator).append(remainder.getTypeName());
                separator = ", ";
            }
            feedback.append(")");
            fbSeparator = TRUE_FB_SEPARATOR;
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
                if (otherPrimaryKeyIndex != null) {
                    if (indexesCanBeSwapped(theIndex, otherPrimaryKeyIndex,
                            feedback, fbSeparator)) {
                        m_theIndexes.add(theIndex.getTypeName());
                        m_otherIndexes.add(otherPrimaryKeyIndex.getTypeName());
                    }
                    else {
                        diagnoseIndexMismatch(theIndex, otherPrimaryKeyIndex,
                                feedback);
                        fbSeparator = TRUE_FB_SEPARATOR;
                    }
                    // signal that otherTable's primary key was matched
                    // (or mismatched).
                    otherPrimaryKeyIndex = null;
                }
                else {
                    feedback.append(fbSeparator)
                    .append("Swapping table ").append(theName)
                    .append(" with table ").append(otherName)
                    .append(" requires that they have the same primary key")
                    .append(" or that neither have one.");
                    fbSeparator = TRUE_FB_SEPARATOR;
                }
                continue;
            }

            boolean matched = false;
            for (Entry<Index, String> otherEntry : candidateConstraintIndexSet.entrySet()) {
                Index otherIndex = otherEntry.getKey();
                if (indexesCanBeSwapped(theIndex, otherIndex, null, null)) {
                    m_theIndexes.add(theIndex.getTypeName());
                    m_otherIndexes.add(otherIndex.getTypeName());
                    candidateConstraintIndexSet.remove(otherIndex);
                    matched = true;
                    break;
                }
            }
            if (matched) {
                continue;
            }

            String theConstraintName = theConstraint.getTypeName();
            String otherConstraintName = theConstraintName.replace(theName, otherName);
            Constraint otherConstraint =
                    candidateConstraints.getIgnoreCase(otherConstraintName);
            if (otherConstraint == null) {
                feedback.append(fbSeparator)
                .append("Swapping table ").append(theName)
                .append(" with table ").append(otherName)
                .append(" requires that constraint ")
                .append(theConstraintName)
                .append(" match an equivalent constraint on table ")
                .append(otherName);
                fbSeparator = TRUE_FB_SEPARATOR;
            }
            else {
                // A named constraint on theTable followed the same naming
                // convention as one on otherTable but had a dissimilar definition.
                feedback.append(fbSeparator)
                .append("Swapping table ").append(theName)
                .append(" with table ").append(otherName)
                .append(" requires that constraints named ")
                .append(theConstraintName).append(" and ")
                .append(otherConstraint.getTypeName())
                .append(" have the same definition:\n");
                fbSeparator = TRUE_FB_SEPARATOR;

                Index otherIndex = theConstraint.getIndex();
                if (otherIndex != null) {
                    diagnoseIndexMismatch(theIndex, otherIndex, feedback);
                    fbSeparator = TRUE_FB_SEPARATOR;
                    candidateConstraintIndexSet.remove(otherIndex);
                }
            }
        }

        if (otherPrimaryKeyIndex != null) {
            // A primary key was found but not matched.
            feedback.append(fbSeparator)
            .append("Swapping table ").append(theName)
            .append(" with table ").append(otherName)
            .append(" requires that they have the same primary key")
            .append(" or that neither have one.");
            fbSeparator = TRUE_FB_SEPARATOR;
        }

        // At this point, all of theTable's index-based constraints are matched.
        // All of otherTable's index-based constraints should also have been
        // matched along the way.
        if ( ! candidateConstraintIndexSet.isEmpty()) {
            feedback.append(fbSeparator)
            .append("Swapping with table ").append(otherName)
            .append(" requires that the table ").append(theName)
            .append("\" define constraints corresponding to ")
            .append(" all of those defined on table ").append(otherName)
            .append("including those with the following constraint or")
            .append(" system internal index names: (");
            String separator = "";
            for (Entry<Index, String> remainder : candidateConstraintIndexSet.entrySet()) {
                String constraintName = remainder.getValue();
                String description =
                        (constraintName != null && ! constraintName.equals("")) ?
                                constraintName :
                                    ("<anonymous with system internal index name: " +
                                            remainder.getKey().getTypeName() + ">");
                feedback.append(separator).append(description);
                separator = ", ";
            }
            feedback.append(")");
            fbSeparator = TRUE_FB_SEPARATOR;
        }


        if (feedback.length() > 0) {
            throw new PlanningErrorException(feedback.toString());
        }
    }

    private void diagnoseIndexMismatch(Index theIndex, Index otherIndex,
            StringBuilder feedback) {
        String diffSeparator = "";
        // Pairs of matching indexes must agree on type (int hash, etc.).
        if (theIndex.getType() != otherIndex.getType()) {
            feedback.append(diffSeparator).append("index type");
            diffSeparator = ", ";
        }

        // Pairs of matching indexes must agree whether they are (assume)unique.
        if (theIndex.getUnique() != otherIndex.getUnique() ||
                theIndex.getAssumeunique() != otherIndex.getAssumeunique()) {
            feedback.append(diffSeparator).append("UNIQUE attribute");
            diffSeparator = ", ";
        }

        // Pairs of matching indexes must agree whether they are partial
        // and if so, agree on the predicate.
        String thePredicateJSON = theIndex.getPredicatejson();
        String otherPredicateJSON = otherIndex.getPredicatejson();
        if (thePredicateJSON == null) {
            if (otherPredicateJSON != null) {
                feedback.append(diffSeparator).append("predicate");
                diffSeparator = ", ";
            }
        }
        else if ( ! thePredicateJSON.equals(otherPredicateJSON)) {
            feedback.append(diffSeparator).append("predicate");
            diffSeparator = ", ";
        }

        // Pairs of matching indexes must agree that they do or do not index
        // expressions and, if so, agree on the expressions.
        String theExprsJSON = theIndex.getExpressionsjson();
        String otherExprsJSON = otherIndex.getExpressionsjson();
        if (theExprsJSON == null) {
            if (otherExprsJSON != null) {
                feedback.append(diffSeparator).append("indexed expression");
                return;
            }
        }
        else if ( ! theExprsJSON.equals(otherExprsJSON)) {
            feedback.append(diffSeparator).append("indexed expression");
            return;
        }

        // Indexes must agree on the columns they are based on,
        // identifiable by the columns' order in the table.
        CatalogMap<ColumnRef> theColumns = theIndex.getColumns();
        int theColumnCount = theColumns.size();
        CatalogMap<ColumnRef> otherColumns = otherIndex.getColumns();
        if (theColumnCount != otherColumns.size() ) {
            feedback.append(diffSeparator).append("indexed expression");
            return;
        }

        Iterator<ColumnRef> theColumnIterator = theColumns.iterator();
        Iterator<ColumnRef> otherColumnIterator = otherColumns.iterator();
        for (int ii = 0 ;ii < theColumnCount; ++ii) {
            int theColIndex = theColumnIterator.next().getColumn().getIndex();
            int otherColIndex = otherColumnIterator.next().getColumn().getIndex();
            if (theColIndex != otherColIndex) {
                feedback.append(diffSeparator).append("indexed expression");
                return;
            }
        }
    }

    /**
     * @param theIndex candidate match for otherIndex on the target table
     * @param otherIndex candidate match for theIndex on the other target table
     * @param candidateIndexSet set of otherTable indexes as yet unmatched
     */
    private static boolean indexesCanBeSwapped(Index theIndex, Index otherIndex,
            StringBuilder feedback, String fbSeparator) {
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
        }
        else if ( ! thePredicateJSON.equals(otherPredicateJSON)) {
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
    private String validateTableCompatibility(String theName, String otherName,
            Table theTable, Table otherTable,
            StringBuilder feedback, String fbSeparator) {
        if (theTable.getIsdred()) {
            feedback.append(fbSeparator)
            .append("Swapping table ").append(theName)
            .append(" is not allowed because it is DR enabled.");
            fbSeparator = TRUE_FB_SEPARATOR;
        }

        if (otherTable.getIsdred()) {
            feedback.append(fbSeparator)
            .append("Swapping with table ").append(otherName)
            .append(" is not allowed because it is DR enabled.");
            fbSeparator = TRUE_FB_SEPARATOR;
        }

        if (theTable.getIsreplicated() != otherTable.getIsreplicated()) {
            feedback.append(fbSeparator)
            .append("Swapping table ").append(theName)
            .append(" with table ").append(otherName)
            .append(" requires that both be partitioned or that both be replicated.");
            fbSeparator = TRUE_FB_SEPARATOR;
        }

        if (theTable.getTuplelimit() != otherTable.getTuplelimit()) {
            feedback.append(fbSeparator)
            .append("Swapping table ").append(theName)
            .append(" with table ").append(otherName)
            .append(" requires that they define the same partition tuple limit.");
            fbSeparator = TRUE_FB_SEPARATOR;
        }

        if (theTable.getMaterializer() != null) {
            feedback.append(fbSeparator)
            .append("Swapping the view ").append(theName)
            .append(" is not allowed.");
            fbSeparator = TRUE_FB_SEPARATOR;
        }

        //
        // TODO: This view detection logic is not quite right.
        //

        if (otherTable.getMaterializer() != null) {
            feedback.append(fbSeparator)
            .append("Swapping with the view ").append(otherName)
            .append(" is not allowed.");
            fbSeparator = TRUE_FB_SEPARATOR;
        }

        if ( ! (theTable.getMvhandlerinfo().isEmpty() &&
                theTable.getViews().isEmpty())) {
            feedback.append(fbSeparator)
            .append("Swapping table ").append(theName)
            .append(" is not allowed because it is referenced in views ");
            fbSeparator = TRUE_FB_SEPARATOR;

            listViewNames(theTable, feedback);
        }

        if ( ! (otherTable.getMvhandlerinfo().isEmpty() &&
                otherTable.getViews().isEmpty())) {
            feedback.append(fbSeparator)
            .append("Swapping with table ").append(otherName)
            .append(" is not allowed because it is referenced in views ");
            fbSeparator = TRUE_FB_SEPARATOR;

            listViewNames(otherTable, feedback);
        }

        if (theTable.getIsdred()) {
            feedback.append(fbSeparator)
            .append("Swapping table ").append(theName)
            .append(" is not allowed because it is DR enabled");
            fbSeparator = TRUE_FB_SEPARATOR;
        }

        if (otherTable.getIsdred()) {
            feedback.append(fbSeparator)
            .append("Swapping with table ").append(otherName)
            .append(" is not allowed because it is DR enabled");
            fbSeparator = TRUE_FB_SEPARATOR;
        }

        return fbSeparator;
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
    private String validateColumnCompatibility(String theName, String otherName,
                Table theTable, Table otherTable,
                StringBuilder feedback, String fbSeparator) {
        CatalogMap<Column> theColumns = theTable.getColumns();
        int theColCount = theColumns.size();
        CatalogMap<Column> otherColumns = otherTable.getColumns();

        Column[] theColArray = new Column[theColumns.size()];
        for (Column theColumn : theColumns) {
            theColArray[theColumn.getIndex()] = theColumn;
        }

        // The "separator" appended before the description of each
        // column mismatch starts off as more of a general preamble...
        String colSeparator = fbSeparator +
                "Swapping table " + theName +
                " with table " + otherName +
                " requires that they define identical lists of columns: ";
        // ... but quickly settles down to something much simpler.
        final String TRUE_COL_ISSUE_SEPARATOR = "; ";

        // The high water index for matched columns on theTable.
        int matchedColHighWater = -1;
        for (Column otherColumn : otherColumns) {
            int colIndex = otherColumn.getIndex();
            String colName = otherColumn.getTypeName();
            if (colIndex < theColCount) {
                Column theColumn = theColArray[colIndex];
                if (theColumn.getTypeName().equals(colName)) {
                    if (matchedColHighWater < colIndex) {
                        matchedColHighWater = colIndex;
                    }
                    if (theColumn.getType() != otherColumn.getType() ||
                            theColumn.getSize() != otherColumn.getSize() ||
                            theColumn.getInbytes() != otherColumn.getInbytes()) {
                        feedback.append(colSeparator);
                        colSeparator = TRUE_COL_ISSUE_SEPARATOR;
                        fbSeparator = TRUE_FB_SEPARATOR;

                        feedback.append(" columns named ").append(colName)
                        .append(" have different types or sizes ");
                    }
                    continue;
                }
            }

            feedback.append(colSeparator);
            colSeparator = TRUE_COL_ISSUE_SEPARATOR;
            fbSeparator = TRUE_FB_SEPARATOR;

            if (colIndex >= theColCount) {
                // otherTable has too many columns to be compatible
                feedback.append(" not expecting column ");
            }
            else {
                feedback.append(" expecting column ")
                .append(otherName).append('.')
                .append(theColArray[colIndex].getTypeName())
                .append(" at position ").append(colIndex + 1).append(" not ");
            }
            feedback.append(otherName).append('.')
            .append(otherColumn.getTypeName());

            // Give an additional "heads up" if the column with the matching
            // name has the wrong type in addition to the wrong position.
            Column matchedByName = theColumns.get(otherName);
            if (matchedByName != null) {
                if (matchedByName.getType() != otherColumn.getType() ||
                        matchedByName.getSize() != otherColumn.getSize() ||
                                matchedByName.getInbytes() != otherColumn.getInbytes()) {
                    feedback.append(colSeparator);
                    colSeparator = TRUE_COL_ISSUE_SEPARATOR;
                    fbSeparator = TRUE_FB_SEPARATOR;

                    feedback.append(" columns named ").append(colName)
                    .append(" have different types or sizes ");
                }
            }
        }

        // If the highest matched colIndex falls short of theTable's last,
        // (colCount - 1). otherTable has too few columns to be compatible.
        if (matchedColHighWater < theColCount - 1) {
            feedback.append(colSeparator)
            .append(" expected more columns after ")
            .append(otherName).append('.')
            .append(theColArray[matchedColHighWater ].getTypeName());

            colSeparator = TRUE_COL_ISSUE_SEPARATOR;
            fbSeparator = TRUE_FB_SEPARATOR;

            // List the unmatched columns from theTable.
            String separator = " (";
            for (int ii = matchedColHighWater + 1; ii < theColCount; ++ii) {
                feedback.append(separator).append(theColArray[ii]);
                separator = ", ";
            }
            feedback.append(")");
        }

        if ( ! theTable.getIsreplicated() && ! otherTable.getIsreplicated() ) {
            if (! theTable.getPartitioncolumn().getTypeName().equals(
                    otherTable.getPartitioncolumn().getTypeName())) {
                feedback.append(fbSeparator)
                .append("Swapping the partitioned table ").append(theName)
                .append(" with the partitioned table ").append(otherName)
                .append(" requires that both be partitioned by the same column.");
                fbSeparator = TRUE_FB_SEPARATOR;
            }
        }

        return fbSeparator;
    }

    /**
     * @param otherTable
     * @param feedback
     */
    private static void listViewNames(Table otherTable, StringBuilder feedback) {
        // List view names.
        String separator = "(";
        for (MaterializedViewHandlerInfo viewInfo : otherTable.getMvhandlerinfo()) {
            feedback.append(separator).append(viewInfo.getDesttable().getTypeName());
            separator = ", ";
        }
        for (MaterializedViewInfo viewInfo : otherTable.getViews()) {
            feedback.append(separator).append(viewInfo.getTypeName());
            separator = ", ";
        }
        feedback.append(")");
    }

}
