/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.expressions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hsqldb_voltpatches.FunctionSQL;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.planner.ParsedColInfo;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.SortDirectionType;

/**
 * @param <aeClass>
 *
 */
public abstract class AbstractExpression implements JSONString, Cloneable {

    public enum Members {
        TYPE,
        LEFT,
        RIGHT,
        VALUE_TYPE,
        VALUE_SIZE,
        IN_BYTES,
        ARGS,
    }

    protected String m_id;
    protected ExpressionType m_type;
    protected AbstractExpression m_left = null;
    protected AbstractExpression m_right = null;
    protected List<AbstractExpression> m_args = null; // Never includes left and right "operator args".

    protected VoltType m_valueType = null;
    protected int m_valueSize = 0;
    private boolean m_inBytes = false;
    /*
     * We set this to non-null iff the expression has a non-deterministic
     * operation. The most common kind of non-deterministic operation is an
     * aggregate function applied to a floating point expression.
     */
    private String m_contentDeterminismMessage = null;

    /**
     * Note that this expression is inherently non-deterministic. This may be
     * called if the expression is already known to be non-deterministic, even
     * if the value is false, because we are careful to never go from true to
     * false here. Perhaps we should concatenate the messages. But since we only
     * have one now it would result in unnecessary duplication.
     *
     * @param value
     */
    public void updateContentDeterminismMessage(String value) {
        if (m_contentDeterminismMessage == null) {
            m_contentDeterminismMessage = value;
        }
    }

    /**
     * Get the inherent non-determinism state of this expression. This is not
     * valid before finalizeValueTypes is called.
     *
     * @return The state.
     */
    public String getContentDeterminismMessage() {
        return m_contentDeterminismMessage;
    }
    // Keep this flag turned off in production or when testing user-accessible EXPLAIN output or when
    // using EXPLAIN output to validate plans.
    protected static boolean m_verboseExplainForDebugging = false; // CODE REVIEWER! this SHOULD be false!
    public static void enableVerboseExplainForDebugging() { m_verboseExplainForDebugging = true; }
    public static boolean disableVerboseExplainForDebugging()
    {
        boolean was = m_verboseExplainForDebugging;
        m_verboseExplainForDebugging = false;
        return was;
    }
    public static void restoreVerboseExplainForDebugging(boolean was) { m_verboseExplainForDebugging = was; }

    public AbstractExpression(ExpressionType type) {
        m_type = type;
    }
    public AbstractExpression(ExpressionType type, AbstractExpression left, AbstractExpression right) {
        this(type);
        m_left = left;
        m_right = right;
    }
    public AbstractExpression() {
        //
        // This is needed for serialization
        //
    }

    public void validate() throws Exception {
        //
        // Validate our children first
        //
        if (m_left != null) {
            m_left.validate();
        }
        if (m_right != null) {
            m_right.validate();
        }

        if (m_args != null) {
            for (AbstractExpression argument : m_args) {
                argument.validate();
            }
        }

        //
        // Expression Type
        //
        if (m_type == null) {
            throw new Exception("ERROR: The ExpressionType for '" + this + "' is NULL");
        } else if (m_type == ExpressionType.INVALID) {
            throw new Exception("ERROR: The ExpressionType for '" + this + "' is " + m_type);
        //
        // Output Type
        //
        } else if (m_valueType == null) {
            throw new Exception("ERROR: The output VoltType for '" + this + "' is NULL");
        } else if (m_valueType == VoltType.INVALID) {
            throw new Exception("ERROR: The output VoltType for '" + this + "' is " + m_valueType);
        }
        //
        // Since it is possible for an AbstractExpression to be stored with
        // any ExpressionType, we do a simple check to make sure that it is the right class
        //
        Class<?> check_class = m_type.getExpressionClass();
        if (!check_class.isInstance(this)) {
            throw new Exception("ERROR: Expression '" + this + "' is class type '" + getClass().getSimpleName() + "' but needs to be '" + check_class.getSimpleName() + "'");
        }
    }

    @Override
    public Object clone() {
        AbstractExpression clone = null;
        try {
            clone = (AbstractExpression)super.clone();
        }
        catch (CloneNotSupportedException e) {
            // umpossible
            return null;
        }
        if (m_left != null) {
            clone.m_left = (AbstractExpression)m_left.clone();
        }
        if (m_right != null) {
            clone.m_right = (AbstractExpression)m_right.clone();
        }
        if (m_args != null) {
            clone.m_args = new ArrayList<>(m_args.size());
            for (AbstractExpression argument : m_args) {
                clone.m_args.add((AbstractExpression) argument.clone());
            }
        }

        return clone;
    }

    /**
     * @return the id
     */
    /*public String getId() {
        return m_id;
    }*/

    /**
     * @param id the id to set
     */
    /*public void setId(String id) {
        m_id = id;
    }*/

    /**
     * @return the type
     */
    public ExpressionType getExpressionType() {
        return m_type;
    }

    /**
     *
     * @param type
     */
    public void setExpressionType(ExpressionType type) {
        m_type = type;
    }

    /**
     * @return the left
     */
    public AbstractExpression getLeft() {
        return m_left;
    }

    /**
     * @param left the left to set
     */
    public void setLeft(AbstractExpression left) {
        m_left = left;
    }

    /**
     * @return the right
     */
    public AbstractExpression getRight() {
        return m_right;
    }

    /**
     * @param right the right to set
     */
    public void setRight(AbstractExpression right) {
        m_right = right;
    }

    /**
     *
     * @return the list of args
     */
    public List<AbstractExpression> getArgs () {
        return m_args;
    }

    /**
     * @param arguments to set
     */
    public void setArgs(List<AbstractExpression> arguments) {
        m_args = arguments;
    }

    /**
     * Update the argument at specified index
     * @param index   the index of the item to replace
     * @param arg     the new argument to insert into the list
     */
    public void setArgAtIndex(int index, AbstractExpression arg) {
        m_args.set(index, arg);
    }

    /**
     * @return The type of this expression's value.
     */
    public VoltType getValueType() {
        return m_valueType;
    }

    /**
     * @param type The type of this expression's value.
     */
    public void setValueType(VoltType type) {
        m_valueType = type;
    }

    /**
     * @return The size of this expression's value in bytes.
     */
    public int getValueSize() {
        return m_valueSize;
    }

    /**
     * @param size The size of this expression's value in bytes.
     */
    public void setValueSize(int size) {
        assert (size >= 0);
        assert (size <= 10000000);
        m_valueSize = size;
    }

    public boolean getInBytes() {
        return m_inBytes;
    }

    public void setInBytes(boolean inBytes) {
        m_inBytes = inBytes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toStringHelper("", sb);
        return sb.toString();
    }

    private static final String INDENT = "  | ";

    /**
     * Return a node name to help out toString.  Subclasses
     * can chime in if they have a notion to.  See TupleValueExpression,
     * for example.
     */
    protected String getExpressionNodeNameForToString() {
        return this.getClass().getSimpleName();
    }

    private void toStringHelper(String linePrefix, StringBuilder sb) {
        String header = getExpressionNodeNameForToString() + "[" + getExpressionType().toString() + "] : ";
        if (m_valueType != null) {
            header += m_valueType.toSQLString();
        }
        else {
            header += "[null type]";
        }
        sb.append(linePrefix + header + "\n");

        if (m_left != null) {
            sb.append(linePrefix + "Left:\n");
            m_left.toStringHelper(linePrefix + INDENT, sb);
        }

        if (m_right != null) {
            sb.append(linePrefix + "Right:\n");
            m_right.toStringHelper(linePrefix + INDENT, sb);
        }

        if (m_args != null) {
            sb.append(linePrefix + "Args:\n");
            for (AbstractExpression arg : m_args) {
                arg.toStringHelper(linePrefix + INDENT, sb);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AbstractExpression == false) return false;
        AbstractExpression expr = (AbstractExpression) obj;

        if (m_type != expr.m_type) {
            return false;
        }
        if ( ! hasEqualAttributes(expr)) {
            return false;
        }
        // The derived classes have verified that any added attributes are identical.

        // Check that the presence, or lack, of children is the same
        if ((m_left == null) != (expr.m_left == null)) {
            return false;
        }
        if ((m_right == null) != (expr.m_right == null)) {
            return false;
        }
        if ((m_args == null) != (expr.m_args == null)) {
            return false;
        }

        // Check that the children identify themselves as equal
        if (expr.m_left != null)
            if (expr.m_left.equals(m_left) == false)
                return false;
        if (expr.m_right != null)
            if (expr.m_right.equals(m_right) == false)
                return false;

        if (expr.m_args != null)
            if (expr.m_args.equals(m_args) == false) {
                return false;
        }

        return true;
    }

    /**
     * Deserialize 2 lists of AbstractExpressions from JSON format strings
     * and determine whether the expressions at each position are equal.
     * The issue that must be worked around is that json format string equality is sensitive
     * to the valuetype and valuesize of each expression and its subexpressions,
     * but AbstractExpression.equals is not -- overloaded abstract expressions are equal
     * -- like applications of two overloads of the same function or operator applied to columns
     * with the same name but different types.
     * These overloads are sometimes allowable in live schema updates where more general changes
     * might be forbidden.
     * @return true iff the two strings represent lists of the same expressions,
     * allowing for value type differences
     */
    public static boolean areOverloadedJSONExpressionLists(String jsontext1, String jsontext2)
    {
        try {
            List<AbstractExpression> list1 = fromJSONArrayString(jsontext1, null);
            List<AbstractExpression> list2 = fromJSONArrayString(jsontext2, null);
            return list1.equals(list2);
        } catch (JSONException je) {
            return false;
        }
    }

    // Derived classes that define attributes should compare them in their refinements of this method.
    // This implementation is provided as a convenience for Operators et. al. that have no attributes that could differ.
    protected boolean hasEqualAttributes(AbstractExpression expr) {
        return true;
    }

    // A check for "structural similarity" to an indexed expression that generally uses equality between
    // the expression trees but also matches a ParameterValueExpression having an "original value" constant
    // in the LHS to an equal ConstantValueExpression within the RHS -- that's actually taken care of
    // in the ParameterValueExpression override of this function.
    // @return - null if there is no match, otherwise a list of "bound parameters" used by the match,
    //           possibly an empty list if the found match was based on expression equality and
    //           didn't involve parameters.
    public List<AbstractExpression> bindingToIndexedExpression(AbstractExpression expr) {
        // Defer the result construction for as long as possible on the assumption that this
        // function mostly gets applied to eliminate negative cases.
        if (m_type != expr.m_type) {
            // The only allowed difference in expression types is between a parameter
            // and its original constant value.  That's handled in the independent override.
            return null;
        }

        // From here, this is much like the straight equality check, except that this function and "equals" must
        // each call themselves in the recursions.

        // Delegating to this factored-out component of the "equals" implementation eases simultaneous
        // refinement of both methods.
        if ( ! hasEqualAttributes(expr)) {
            return null;
        }
        // The derived classes have verified that any added attributes are identical.

        // Check that the presence, or lack, of children is the same
        if ((expr.m_left == null) != (m_left == null)) {
            return null;
        }
        if ((expr.m_right == null) != (m_right == null)) {
            return null;
        }
        if ((expr.m_args == null) != (m_args == null)) {
            return null;
        }

        // Check that the children identify themselves as matching
        List<AbstractExpression> leftBindings = null;
        if (m_left != null) {
            leftBindings = m_left.bindingToIndexedExpression(expr.m_left);
            if (leftBindings == null) {
                return null;
            }
        }
        List<AbstractExpression> rightBindings = null;
        if (m_right != null) {
            rightBindings = m_right.bindingToIndexedExpression(expr.m_right);
            if (rightBindings == null) {
                return null;
            }
        }
        List<AbstractExpression> argBindings = null;
        if (m_args != null) {
            if (m_args.size() != expr.m_args.size()) {
                return null;
            }
            argBindings = new ArrayList<>();
            int ii = 0;
            // iterate the args lists in parallel, binding pairwise
            for (AbstractExpression rhs : expr.m_args) {
                AbstractExpression lhs = m_args.get(ii++);
                List<AbstractExpression> moreBindings = lhs.bindingToIndexedExpression(rhs);
                if (moreBindings == null) { // fail on any non-match
                    return null;
                }
                argBindings.addAll(moreBindings);
            }
        }

        // It's a match, so gather up the details.
        // It's rare (if even possible) for the same bound parameter to get listed twice,
        // so don't worry about duplicate entries, here.
        // That should not cause any issue for the caller.
        List<AbstractExpression> result = new ArrayList<>();
        if (leftBindings != null) { // null here can only mean no left child
            result.addAll(leftBindings);
        }
        if (rightBindings != null) { // null here can only mean no right child
            result.addAll(rightBindings);
        }
        if (argBindings != null) { // null here can only mean no args
            result.addAll(argBindings);
        }
        return result;
    }

    @Override
    public int hashCode() {
        // based on implementation of equals
        int result = 0;
        // hash the children
        if (m_left != null) {
            result += m_left.hashCode();
        }
        if (m_right != null) {
            result += m_right.hashCode();
        }
        if (m_args != null) {
            result += m_args.hashCode();
        }
        if (m_type != null) {
            result += m_type.hashCode();
        }
        return result;
    }

    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try {
        stringer.object();
        toJSONString(stringer);
        stringer.endObject();
        } catch (JSONException e) {
            e.printStackTrace();
            return null;
        }
        return stringer.toString();
    }

    public void toJSONString(JSONStringer stringer) throws JSONException {
        stringer.key(Members.TYPE.name()).value(m_type.getValue());

        if (m_valueType == null) {
            stringer.key(Members.VALUE_TYPE.name()).value( VoltType.NULL.getValue());
            stringer.key(Members.VALUE_SIZE.name()).value(m_valueSize);
        } else {
            stringer.key(Members.VALUE_TYPE.name()).value(m_valueType.getValue());
            if (m_valueType.getLengthInBytesForFixedTypesWithoutCheck() == -1) {
                stringer.key(Members.VALUE_SIZE.name()).value(m_valueSize);
            }

            if (m_inBytes) {
                assert(m_valueType == VoltType.STRING);
                stringer.key(Members.IN_BYTES.name()).value(true);
            }
        }

        if (m_left != null) {
            assert (m_left instanceof JSONString);
            stringer.key(Members.LEFT.name()).value(m_left);
        }

        if (m_right != null) {
            assert (m_right instanceof JSONString);
            stringer.key(Members.RIGHT.name()).value(m_right);
        }

        if (m_args != null) {
            stringer.key(Members.ARGS.name()).array();
            for (AbstractExpression argument : m_args) {
                assert (argument instanceof JSONString);
                stringer.value(argument);
            }
            stringer.endArray();

        }
    }

    /**
     * We need some enumerals which are common to PartitionByPlanNode and OrderByPlanNode
     * and maybe others.  These are used as keys to create JSON.
     */
    public enum SortMembers {
        SORT_COLUMNS,
        SORT_DIRECTION,
        SORT_EXPRESSION
    }

    /**
     * Given a JSONStringer and a sequence of sort expressions and directions,
     * serialize the sort expressions.  These will be in an array which is
     * the value of SortMembers.SORT_COLUMNS in the current object of
     * the JSONString.  The JSONString should be in object state, not
     * array state.
     *
     * @param stringer         The stringer used to serialize the sort list.
     * @param sortExpressions  The sort expressions.
     * @param sortDirections   The sort directions.  These may be empty if the
     *                         directions are not valueable to us.
     * @throws JSONException
     */
    public static void toJSONArrayFromSortList(JSONStringer             stringer,
                                               List<AbstractExpression> sortExpressions,
                                               List<SortDirectionType>  sortDirections) throws JSONException {
        stringer.key(SortMembers.SORT_COLUMNS.name()).array();
        int listSize = sortExpressions.size();
        for (int ii = 0; ii < listSize; ii++) {
            stringer.object();
            stringer.key(SortMembers.SORT_EXPRESSION.name());
            stringer.object();
            sortExpressions.get(ii).toJSONString(stringer);
            stringer.endObject();
            if (sortDirections != null) {
                stringer.key(SortMembers.SORT_DIRECTION.name()).value(sortDirections.get(ii).toString());
            }
            stringer.endObject();
        }
        stringer.endArray();
    }

    protected void loadFromJSONObject(JSONObject obj) throws JSONException { }
    protected void loadFromJSONObject(JSONObject obj, StmtTableScan tableScan) throws JSONException
    {
        loadFromJSONObject(obj);
    }

    /**
     * For TVEs, it is only serialized column index and table index. In order to match expression,
     * there needs more information to revert back the table name, table alias and column name.
     * Without adding extra information, TVEs will only have column index and table index available.
     * This function is only used for various of plan nodes, except AbstractScanPlanNode.
     * @param jobj
     * @param label
     * @return
     * @throws JSONException
     */
    public static AbstractExpression fromJSONChild(JSONObject jobj, String label) throws JSONException
    {
        if(jobj.isNull(label)) {
            return null;
        }
        return fromJSONObject(jobj.getJSONObject(label), null);

    }

    public static AbstractExpression fromJSONChild(JSONObject jobj, String label,  StmtTableScan tableScan) throws JSONException
    {
        if(jobj.isNull(label)) {
            return null;
        }
        return fromJSONObject(jobj.getJSONObject(label), tableScan);
    }

    private static AbstractExpression fromJSONObject(JSONObject obj,  StmtTableScan tableScan) throws JSONException
    {
        ExpressionType type = ExpressionType.get(obj.getInt(Members.TYPE.name()));
        AbstractExpression expr;
        try {
            expr = type.getExpressionClass().newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
            return null;
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            return null;
        }

        expr.m_type = type;

        expr.m_valueType = VoltType.get((byte) obj.getInt(Members.VALUE_TYPE.name()));
        if (obj.has(Members.VALUE_SIZE.name())) {
            expr.m_valueSize = obj.getInt(Members.VALUE_SIZE.name());
        } else {
            expr.m_valueSize = expr.m_valueType.getLengthInBytesForFixedTypes();
        }

        expr.m_left = AbstractExpression.fromJSONChild(obj, Members.LEFT.name(), tableScan);
        expr.m_right = AbstractExpression.fromJSONChild(obj, Members.RIGHT.name(), tableScan);

        if (!obj.isNull(Members.ARGS.name())) {
            JSONArray jarray = obj.getJSONArray(Members.ARGS.name());
            ArrayList<AbstractExpression> arguments = new ArrayList<>();
            loadFromJSONArray(arguments, jarray, tableScan);
            expr.setArgs(arguments);
        }

        expr.loadFromJSONObject(obj, tableScan);
        return expr;
    }

    /**
     * Load two lists from a JSONObject.  One list is for sort expressions and the other is for sort directions.
     * The lists are cleared before they are filled in.  This is the inverse of toJSONArrayFromSortList.
     *
     * The JSONObject should be in object state, not array state.  It should have a member
     * named SORT_COLUMNS, which is an array with the <expression, direction> pairs.  Sometimes the
     * sort directions are not needed.  For example, when deserializing
     *
     * @param sortExpressions The container for the sort expressions.
     * @param sortDirections The container for the sort directions.  This may
     *                       be null if we don't care about directions.  If there
     *                       are no directions in the list this will be empty.
     * @param jarray
     * @throws JSONException
     */
    public static void loadSortListFromJSONArray(List<AbstractExpression> sortExpressions,
                                                 List<SortDirectionType>  sortDirections,
                                                 JSONObject               jobj) throws JSONException {
        if (jobj.has(AbstractExpression.SortMembers.SORT_COLUMNS.name())) {
            sortExpressions.clear();
            sortDirections.clear();
            JSONArray jarray = jobj.getJSONArray(SortMembers.SORT_COLUMNS.name());
            int size = jarray.length();
            for (int ii = 0; ii < size; ii += 1) {
                JSONObject tempObj = jarray.getJSONObject(ii);
                sortExpressions.add( AbstractExpression.fromJSONChild(tempObj, SortMembers.SORT_EXPRESSION.name()) );
                if (sortDirections != null && tempObj.has(SortMembers.SORT_DIRECTION.name())) {
                    sortDirections.add( SortDirectionType.get(tempObj.getString( SortMembers.SORT_DIRECTION.name())) );
                }
            }
        }
        assert(sortDirections == null || sortExpressions.size() == sortDirections.size());
    }

    public static List<AbstractExpression> fromJSONArrayString(String jsontext, StmtTableScan tableScan) throws JSONException
    {
        JSONArray jarray = new JSONArray(jsontext);
        List<AbstractExpression> result = new ArrayList<>();
        loadFromJSONArray(result, jarray, tableScan);
        return result;
    }

    public static void fromJSONArrayString(String jsontext, StmtTableScan tableScan, List<AbstractExpression> result) throws JSONException
    {
        result.addAll(fromJSONArrayString(jsontext, tableScan));
    }

    public static AbstractExpression fromJSONString(String jsontext, StmtTableScan tableScan) throws JSONException
    {
        JSONObject jobject = new JSONObject(jsontext);
        return fromJSONObject(jobject, tableScan);
    }

    /**
     * For TVEs, it is only serialized column index and table index. In order to match expression,
     * there needs more information to revert back the table name, table alisa and column name.
     * By adding @param tableScan, the TVE will load table name, table alias and column name for TVE.
     * @param starter
     * @param parent
     * @param label
     * @param tableScan
     * @throws JSONException
     */
    public static void loadFromJSONArrayChild(List<AbstractExpression> starter,
                                              JSONObject parent, String label, StmtTableScan tableScan)
    throws JSONException
    {
        if( parent.isNull(label) ) {
            return;
        }
        JSONArray jarray = parent.getJSONArray(label);
        loadFromJSONArray(starter, jarray, tableScan);
    }

    private static void loadFromJSONArray(List<AbstractExpression> starter,
                                          JSONArray jarray,  StmtTableScan tableScan) throws JSONException
    {
        int size = jarray.length();
        for( int i = 0 ; i < size; i++ ) {
            JSONObject tempjobj = jarray.getJSONObject( i );
            starter.add(fromJSONObject(tempjobj, tableScan));
        }
    }

    /**
     * This function recursively replace any Expression that in the aggTableIndexMap to a TVEs. Its column index and alias are also built up here.
     * @param aggTableIndexMap
     * @param indexToColumnMap
     * @return
     */
    public AbstractExpression replaceWithTVE(
            Map <AbstractExpression, Integer> aggTableIndexMap,
            Map <Integer, ParsedColInfo> indexToColumnMap)
    {
        Integer ii = aggTableIndexMap.get(this);
        if (ii != null) {
            ParsedColInfo col = indexToColumnMap.get(ii);
            TupleValueExpression tve = new TupleValueExpression(
                    col.tableName, col.tableAlias, col.columnName, col.alias, ii);

            tve.setTypeSizeAndInBytes(this);
            if (this instanceof TupleValueExpression) {
                tve.setOrigStmtId(((TupleValueExpression)this).getOrigStmtId());
            }
            // To prevent pushdown of LIMIT when ORDER BY references an agg. ENG-3487.
            if (hasAggregateSubexpression()) {
                tve.setHasAggregate(true);
            }

            return tve;
        }

        AbstractExpression lnode = null, rnode = null;
        ArrayList<AbstractExpression> newArgs = null;
        if (m_left != null) {
            lnode = m_left.replaceWithTVE(aggTableIndexMap, indexToColumnMap);
        }
        if (m_right != null) {
            rnode = m_right.replaceWithTVE(aggTableIndexMap, indexToColumnMap);
        }

        boolean changed = false;
        if (m_args != null) {
            newArgs = new ArrayList<>();
            for (AbstractExpression expr: m_args) {
                AbstractExpression ex = expr.replaceWithTVE(aggTableIndexMap, indexToColumnMap);
                newArgs.add(ex);
                if (ex != expr) {
                    changed = true;
                }
            }
        }

        if (m_left != lnode || m_right != rnode || changed) {
            AbstractExpression resExpr = (AbstractExpression) this.clone();
            resExpr.setLeft(lnode);
            resExpr.setRight(rnode);
            resExpr.setArgs(newArgs);
            return resExpr;
        }

        return this;
    }

    public boolean hasSubExpressionFrom(Set<AbstractExpression> expressionSet) {
        if (expressionSet.contains(this)) {
            return true;
        }

        if (m_left != null && expressionSet.contains(m_left)) {
            return true;
        }
        if (m_right != null && expressionSet.contains(m_right)) {
            return true;
        }
        if (m_args != null) {
            for (AbstractExpression expr: m_args) {
                if (expressionSet.contains(expr)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Replace avg expression with sum/count for optimization.
     * @return
     */
    public AbstractExpression replaceAVG () {
        if (m_left != null) {
            m_left = m_left.replaceAVG();
        }
        if (m_right != null) {
            m_right = m_right.replaceAVG();
        }
        if (m_args != null) {
            int index = 0;
            for (AbstractExpression expr: m_args) {
                AbstractExpression ex = expr.replaceAVG();
                if (ex != expr) {
                    m_args.set(index, ex);
                }
                ++index;
            }
        }
        return this;
    }

    /**
     * Recursively walk an expression and return a list of all its
     * contained tuple value expressions' table aliases.
     */
    public void findTVEAliases(Set<String> aliases) {
        // recursive calls
        if (m_left != null) {
            m_left.findTVEAliases(aliases);
        }
        if (m_right != null) {
            m_right.findTVEAliases(aliases);
        }
        if (m_args != null) {
            for (AbstractExpression argument : m_args) {
                argument.findTVEAliases(aliases);
            }
        }
    }

    /**
     * Recursively walk an expression and determine whether it contains
     * any tuple value expression referencing the given table aliases.
     */
    boolean containsMatchingTVE(String tableAlias) {
        Set<String> aliases = new HashSet<>();
        findTVEAliases(aliases);
        for (String alias : aliases) {
            if (alias.equals(tableAlias)) {
                return true;
            }
        }
        return false;
    }

    /**
     *  A condition is null-rejected for a given table in the following cases:
     *      If it is of the form A IS NOT NULL, where A is an attribute of any of the inner tables
     *      If it is a predicate containing a reference to an inner table that evaluates to UNKNOWN
     *          when one of its arguments is NULL
     *      If it is a conjunction containing a null-rejected condition as a conjunct
     *      If it is a disjunction of null-rejected conditions
     *
     * @param expr
     * @param tableAlias
     * @return
     */
    public boolean isNullRejectingExpression(final String tableAlias) {
        ExpressionType exprType = getExpressionType();
        if (exprType == ExpressionType.CONJUNCTION_AND) {
            assert(m_left != null && m_right != null);
            return m_left.isNullRejectingExpression(tableAlias) || m_right.isNullRejectingExpression(tableAlias);
        }
        if (exprType == ExpressionType.CONJUNCTION_OR) {
            assert(m_left != null && m_right != null);
            return m_left.isNullRejectingExpression(tableAlias) && m_right.isNullRejectingExpression(tableAlias);
        }
        if (exprType == ExpressionType.OPERATOR_NOT) {
            assert(m_left != null);
            // "NOT ( P and Q )" is as null-rejecting as "NOT P or NOT Q"
            // "NOT ( P or Q )" is as null-rejecting as "NOT P and NOT Q"
            // Handling AND and OR expressions requires a "negated" flag to the recursion that tweaks
            // (switches?) the handling of ANDs and ORs to enforce the above equivalences.
            if (m_left.getExpressionType() == ExpressionType.OPERATOR_IS_NULL) {
                return containsMatchingTVE(tableAlias);
            }
            if (m_left.getExpressionType() == ExpressionType.CONJUNCTION_AND ||
                    m_left.getExpressionType() == ExpressionType.CONJUNCTION_OR) {
                assert(m_left.m_left != null && m_left.m_right != null);
                // Need to test for an existing child NOT and skip it.
                // e.g. NOT (P AND NOT Q) --> (NOT P) OR NOT NOT Q --> (NOT P) OR Q
                AbstractExpression tempLeft = null;
                if (m_left.m_left.getExpressionType() != ExpressionType.OPERATOR_NOT) {
                    tempLeft = new OperatorExpression(ExpressionType.OPERATOR_NOT, m_left.m_left, null);
                }
                else {
                    assert(m_left.m_left.m_left != null);
                    tempLeft = m_left.m_left.m_left;
                }
                AbstractExpression tempRight = null;
                if (m_left.m_right.getExpressionType() != ExpressionType.OPERATOR_NOT) {
                    tempRight = new OperatorExpression(ExpressionType.OPERATOR_NOT, m_left.m_right, null);
                }
                else {
                    assert(m_left.m_right.m_left != null);
                    tempRight = m_left.m_right.m_left;
                }
                ExpressionType type = (m_left.getExpressionType() == ExpressionType.CONJUNCTION_AND) ?
                        ExpressionType.CONJUNCTION_OR : ExpressionType.CONJUNCTION_AND;
                AbstractExpression tempExpr = new ConjunctionExpression(type, tempLeft, tempRight);
                return tempExpr.isNullRejectingExpression(tableAlias);
            }
            if (m_left.getExpressionType() == ExpressionType.OPERATOR_NOT) {
                // It's probably safe to assume that HSQL will have stripped out other double negatives,
                // (like "NOT T.c IS NOT NULL"). Yet, we could also handle them here
                assert(m_left.m_left != null);
                return m_left.m_left.isNullRejectingExpression(tableAlias);
            }
            return m_left.isNullRejectingExpression(tableAlias);
        }
        if (exprType == ExpressionType.COMPARE_NOTDISTINCT) {
            return false;
        }
        if (exprType == ExpressionType.OPERATOR_IS_NULL) {
            // IS NOT NULL is NULL rejecting -- IS NULL is not
            return false;
        }

        // COALESCE expression is a sub-expression
        // For example, COALESCE (C1, C2) > 0

        SubexprFinderPredicate partOfACoalaesce = new SubexprFinderPredicate() {
            @Override
            public boolean matches(AbstractExpression expr) {
                OperatorExpression coalesceExpr = (OperatorExpression)expr;
                // This table is part of the COALESCE expression - it's not NULL-rejecting
                return (coalesceExpr.getExpressionType() == ExpressionType.OPERATOR_ALTERNATIVE) &&
                        coalesceExpr.containsMatchingTVE(tableAlias);
            }
        };

        if (hasAnyMatchingSubexpressionOfClass(OperatorExpression.class, partOfACoalaesce)) {
            return false;
        }

        // If we get here it means that the tableAlias is not part of any COALESCE expression.
        // Still need to check the catch all case.

        // @TODO ENG_3038 Is it safe to assume for the rest of the expressions that if
        // it contains a TVE with the matching table name then it is a NULL rejecting expression?
        // Presently, yes, logical expressions are not expected to appear inside other
        // generalized expressions, so since the handling of other kinds of expressions
        // is pretty much "containsMatchingTVE". This fallback should be safe.
        // The only planned developments that might contradict this restriction (AFAIK --paul)
        // would be support for standard pseudo-functions that take logical condition arguments.
        // These should probably be supported as special non-functions/operations for a number
        // of reasons and may need special casing here.
        return containsMatchingTVE(tableAlias);
    }

    /**
     * @param <aeClass>
     * @param aeClass AbstractExpression-based class of instances to search for.
     * @return a list of contained expressions that are instances of the desired class
     */
    public <T extends AbstractExpression> List<T> findAllSubexpressionsOfClass(
            Class< ? extends AbstractExpression> aeClass) {
        List<T> collected = new ArrayList<>();
        findAllSubexpressionsOfClass_recurse(aeClass, collected);
        return collected;
    }

    public <T extends AbstractExpression> void findAllSubexpressionsOfClass_recurse(
            Class< ? extends AbstractExpression> aeClass,
            List<T> collected) {
        if (aeClass.isInstance(this)) {
            // Suppress the expected warning for the "unchecked" cast.
            // The runtime isInstance check ensures that it is typesafe.
            @SuppressWarnings("unchecked")
            T e = (T) this;
            collected.add(e);
            // Don't return early, because in a few rare cases,
            // like when searching for function expressions,
            // an instance CAN be a parent expression of another instance.
            // It's probably not worth optimizing for the special cases.
        }

        if (m_left != null) {
            m_left.findAllSubexpressionsOfClass_recurse(aeClass, collected);
        }
        if (m_right != null) {
            m_right.findAllSubexpressionsOfClass_recurse(aeClass, collected);
        }
        if (m_args != null) {
            for (AbstractExpression argument : m_args) {
                argument.findAllSubexpressionsOfClass_recurse(aeClass, collected);
            }
        }
    }

    /**
     * @param aeClass expression class to search for
     * @return whether the expression or any contained expressions are of the desired type
     */
    protected boolean hasAnySubexpressionOfClass(Class< ? extends AbstractExpression> aeClass) {
        if (aeClass.isInstance(this)) {
            return true;
        }

        if (m_left != null && m_left.hasAnySubexpressionOfClass(aeClass)) {
            return true;
        }

        if (m_right != null && m_right.hasAnySubexpressionOfClass(aeClass)) {
            return true;
        }
        if (m_args != null) {
            for (AbstractExpression argument : m_args) {
                if (argument.hasAnySubexpressionOfClass(aeClass)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean hasTVE() {
        return hasAnySubexpressionOfClass(TupleValueExpression.class);
    }

    public boolean hasAggregateSubexpression() {
        return hasAnySubexpressionOfClass(AggregateExpression.class);
    }

    public boolean hasSubquerySubexpression() {
        return hasAnySubexpressionOfClass(SelectSubqueryExpression.class);
    }

    public boolean hasParameterSubexpression() {
        return hasAnySubexpressionOfClass(ParameterValueExpression.class);
    }

    /**
     * A predicate class for searching expression trees,
     * to be used with hasAnySubexpressionWithPredicate, below.
     */
    public static interface SubexprFinderPredicate {
        boolean matches(AbstractExpression expr);
    }

    /**
     * Searches the expression tree rooted at this for any node of the
     * specified class that also matches the specified condition.
     * @param aeClass the expected AbstractExpression-based class of the matching
     * instance
     * @param condition a Predicate object instantiated by the caller
     * @return true if the condition is true for any instance of the class in the
     * AbstractExpression's tree
     */
    public boolean hasAnyMatchingSubexpressionOfClass(
            Class< ? extends AbstractExpression> aeClass,
            SubexprFinderPredicate condition) {
        if (aeClass.isInstance(this)) {
            if (condition.matches(this)) {
                return true;
            }
        }

        if (m_left != null &&
                m_left.hasAnyMatchingSubexpressionOfClass(aeClass, condition)) {
            return true;
        }

        if (m_right != null &&
                m_right.hasAnyMatchingSubexpressionOfClass(aeClass, condition)) {
            return true;
        }

        if (m_args != null) {
            for (AbstractExpression argument : m_args) {
                if (argument.hasAnyMatchingSubexpressionOfClass(aeClass, condition)) {
                    return true;
                }
            }
        }
        return false;
    }

    /* see ComparisonExpression override */
    public boolean isColumnEquivalenceFilter() { return false; }

    public List<ParameterValueExpression> findAllParameterSubexpressions() {
        return findAllSubexpressionsOfClass(ParameterValueExpression.class);
    }

    public List<SelectSubqueryExpression> findAllSubquerySubexpressions() {
        return findAllSubexpressionsOfClass(SelectSubqueryExpression.class);
    }

    public List<TupleValueExpression> findAllTupleValueSubexpressions() {
        return findAllSubexpressionsOfClass(TupleValueExpression.class);
    }

    public <T extends AbstractExpression> List<T> findAllTupleValueSubexpressions(
            List<T> forReturnTypeOnly) {
        return findAllSubexpressionsOfClass(TupleValueExpression.class);
    }

    /**
     * This function will recursively find any function expression with ID functionId.
     * If found, return true. Otherwise, return false.
     *
     * @param expr
     * @param functionId
     * @return
     */
    private boolean containsFunctionById(final int functionId) {
        SubexprFinderPredicate matchingId = new SubexprFinderPredicate() {
            @Override
            public boolean matches(AbstractExpression expr) {
                FunctionExpression funcExpr = (FunctionExpression)expr;
                return funcExpr.hasFunctionId(functionId);
            }
        };
        return hasAnyMatchingSubexpressionOfClass(FunctionExpression.class, matchingId);
    }


    private static final SubexprFinderPredicate TVE_AGG_PROXY_MATCHER = new SubexprFinderPredicate() {
        @Override
        public boolean matches(AbstractExpression expr) {
            return ((TupleValueExpression)expr).hasAggregate();
        }
    };

    public boolean hasAggregateProxyTVE() {
        return hasAnyMatchingSubexpressionOfClass(
                TupleValueExpression.class,
                TVE_AGG_PROXY_MATCHER);
    }

    /**
     * Return true if the given expression usable as part of an index or MV's
     * group by and where clause expression.
     * If false, put the tail of an error message in the string buffer. The
     * string buffer will be initialized with the name of the index.
     *
     * @param expr The expression to check
     * @param msg  The StringBuffer to pack with the error message tail.
     * @return true iff the expression can be part of an index.
     */
    private boolean validateExprForIndexesAndMVs(StringBuffer msg) {
        if (containsFunctionById(FunctionSQL.voltGetCurrentTimestampId())) {
            msg.append("cannot include the function NOW or CURRENT_TIMESTAMP.");
            return false;
        }
        if (hasSubquerySubexpression()) {
            // There may not be any of these in HSQL1.9.3b.  However, in
            // HSQL2.3.2 subqueries are stored as expressions.  So, we may
            // find some here.  We will keep it here for the moment.
            msg.append(String.format("with subquery sources is not supported."));
            return false;
        }
        if (hasAggregateSubexpression()) {
            msg.append("with aggregate expression(s) is not supported.");
            return false;
        }
        return true;
    }

    /**
     * Return true if the all of the expressions in the list can be part of
     * an index expression or in group by and where clause of MV.  As with
     * validateExprForIndexesAndMVs for individual expression, the StringBuffer
     * parameter, msg, contains the name of the index.  Error messages should
     * be appended to it.
     *
     * @param checkList
     * @param msg
     * @return
     */
    public static boolean validateExprsForIndexesAndMVs(List<AbstractExpression> checkList, StringBuffer msg) {
        for (AbstractExpression expr : checkList) {
            if (!expr.validateExprForIndexesAndMVs(msg)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Convenience method for determining whether an Expression object should have a child
     * Expression on its RIGHT side. The follow types of Expressions do not need a right child:
     *      OPERATOR_NOT
     *      COMPARISON_IN
     *      OPERATOR_IS_NULL
     *      AggregageExpression
     *
     * @return Does this expression need a right expression to be valid?
     */
    public boolean needsRightExpression() {
        return false;
    }

    /**
     * Constant literals have a place-holder type of NUMERIC. These types
     * need to be converted to DECIMAL or FLOAT when used in a binary operator,
     * the choice based on the other operand's type
     * -- DECIMAL goes with DECIMAL, FLOAT goes with anything else.
     * This gets specialized as a NO-OP for leaf Expressions (AbstractValueExpression)
     */
    public void normalizeOperandTypes_recurse()
    {
        // Depth first search for NUMERIC children.
        if (m_left != null) {
            m_left.normalizeOperandTypes_recurse();
        }
        if (m_right != null) {
            m_right.normalizeOperandTypes_recurse();

            // XXX: There's no check here that the Numeric operands are actually constants.
            // Can a sub-expression of type Numeric arise in any other case?
            // Would that case always be amenable to having its valueType/valueSize redefined here?
            if (m_left != null) {
                if (m_left.m_valueType == VoltType.NUMERIC) {
                    m_left.refineOperandType(m_right.m_valueType);
                }
                if (m_right.m_valueType == VoltType.NUMERIC) {
                    m_right.refineOperandType(m_left.m_valueType);
                }
            }

        }
        if (m_args != null) {
            for (AbstractExpression argument : m_args) {
                argument.normalizeOperandTypes_recurse();
            }
        }
    }

    /**
     * Helper function to patch up NUMERIC typed constant operands and
     * the functions and operators that they parameterize.
     */
    void refineOperandType(VoltType valueType)
    {
        if (m_valueType != VoltType.NUMERIC) {
            return;
        }
        if (valueType == VoltType.DECIMAL) {
            m_valueType = VoltType.DECIMAL;
            m_valueSize = VoltType.DECIMAL.getLengthInBytesForFixedTypes();
        } else {
            m_valueType = VoltType.FLOAT;
            m_valueSize = VoltType.FLOAT.getLengthInBytesForFixedTypes();
        }
    }

    /**
     * Specify or specialize the type and optionally the maximum size of an expression value.
     * This is used when the expression's type or size is implied or constrained by its usage,
     * especially to match the type/size of a target column in an INSERT or UPDATE statement.
     * This default implementation is currently a no-op, which covers expressions whose value
     * type/size is pre-determined, like logical expressions, comparisons, tuple address
     * expressions, etc.
     * Derived classes define custom implementations typically to allow refinement of types or
     * sizes for expressions that the HSQL parser was unable to determine the type of, leaving
     * the type as null, or was only able to narrow down to NUMERIC (of some undetermined type).
     * Type refinement can have an effect on the interpretation of constant values.
     * E.g. is this constant string being used as a timestamp? If so, a compile-time check for
     * a valid date/time format could provide a valuable heads-up to the user.
     * It also can bolster parameter type checking.
     * E.g. should values for this parameter only be allowed in the tiny int range?
     * @param neededType - the target type of the value as suggested by its usage.
     * @param neededSize - the maximum allowed size for the value as suggested by its usage.
     * TODO: we might want to enable asserts when we appear to be trying to refine a type or size
     * on an expression that expects to have a fixed pre-determined type -- at least when
     * non-trivially trying to refine the type/size to something other than its current type/size.
     * The ONLY reason this default no-op implementation contains any code at all is to make it
     * easier to catch such cases in a debugger, possibly in preparation for a more agressive assert.
     */
    public void refineValueType(VoltType neededType, int neededSize) {
        if (neededType.equals(m_valueType)) {
            return; // HSQL already initialized the expression to have the refined type.
        }
        //TODO: For added safety, we MAY want to (re)enable this general assert
        // OR only assert after we give a pass for refining from "generic types". See the comment below.
        // assert(false);
        if ((m_valueType != null) && (m_valueType != VoltType.NUMERIC)) {
            // This code path leaves a generic type (null or NUMERIC) in the expression tree rather
            // than assume that it's safe to change the value type of an arbitrary AbstractExpression.
            // The EE MAY complain about it later.
            // There may be special cases where we want to assert(false) rather than waiting for the EE
            // to complain or even special cases where we want to go ahead and switch the type (scary!).
            return;
        }
        // A request to switch an arbitrary AbstractExpression from the specific value type that HSQL
        // assigned to it to a different specific value type that SEEMS to be called for by the usage
        // (target column) is a hard thing to know how to handle.
        // It seems equally dangerous to ignore the HSQL type OR the target type.
        // Maybe this will never occur because HSQL is so smart (and we keep it that way),
        // or maybe the type difference won't really matter to the EE because it's so flexible.
        // Or maybe some of each -- this no-op behavior assumes something like that.
        // BUT maybe it's just always wrong to be trying any such thing, so we should assert(false).
        // Or maybe there need to be special cases.
        // The sad news is that when this assert first went live, it inexplicably caused quiet
        // thread death and hanging rather than an identifiable assert, even when run in the debugger.
        // assert(false);
    }

    /** Instead of recursing by default, allow derived classes to recurse as needed
     * using finalizeChildValueTypes.
     */
    public abstract void finalizeValueTypes();

    /**
     * Do the recursive part of finalizeValueTypes as requested. Note that this
     * updates the content non-determinism state.
     */
    protected final void finalizeChildValueTypes() {
        if (m_left != null) {
            m_left.finalizeValueTypes();
            updateContentDeterminismMessage(m_left.getContentDeterminismMessage());
        }
        if (m_right != null) {
            m_right.finalizeValueTypes();
            updateContentDeterminismMessage(m_right.getContentDeterminismMessage());
        }
        if (m_args != null) {
            for (AbstractExpression argument : m_args) {
                argument.finalizeValueTypes();
                updateContentDeterminismMessage(argument.getContentDeterminismMessage());
            }
        }
    }

    /** Associate underlying TupleValueExpressions with columns in the table
     * and propagate the type implications to parent expressions.
     */
    public void resolveForTable(Table table) {
        resolveChildrenForTable(table);
    }

    /** Do the recursive part of resolveForTable as required for tree-structured expression types. */
    protected final void resolveChildrenForTable(Table table) {
        if (m_left != null)
            m_left.resolveForTable(table);
        if (m_right != null)
            m_right.resolveForTable(table);
        if (m_args != null) {
            for (AbstractExpression argument : m_args) {
                argument.resolveForTable(table);
            }
        }
    }

    public abstract String explain(String impliedTableName);

    public static boolean hasInlineVarType(AbstractExpression expr) {
        VoltType type = expr.getValueType();
        int size = expr.getValueSize();
        boolean inBytes = expr.getInBytes();

        switch(type) {
        case STRING:
            if (inBytes && size < 64) {
                return true;
            }
            if (!inBytes && size < 16) {
                return true;
            }
            break;
        case VARBINARY:
            if (size < 64) {
                return true;
            }
            break;
        default:
            break;
        }

        return false;
    }

    /**
     * Returns true iff the expression is indexable.
     * If the expression is not indexable, expression information
     * gets populated in the msg string buffer passed in.
     * @param msg
     * @return
     */
    public boolean isValueTypeIndexable(StringBuffer msg) {
        if (!m_valueType.isIndexable()) {
            msg.append("expression of type " + m_valueType.getName());
            return false;
        }
        return true;
    }

    /**
     * Returns true iff the expression is indexable in a unique index.
     * If the expression is not indexable, expression information
     * gets populated in the msg string buffer passed in.
     * @param msg
     * @return
     */
    public boolean isValueTypeUniqueIndexable(StringBuffer msg) {
        // This call to isValueTypeIndexable is needed because
        // all comparison, all conjunction, and some operator expressions
        // need to refine it to compensate for their false claims that
        // their value types (actually non-indexable boolean) is BIGINT.
        // that their value type is actually boolean.
        // If they were fixed, isValueTypeIndexable and
        // isValueTypeUniqueIndexable could be replaced by VoltType functions.
        if (!isValueTypeIndexable(msg)) {
            return false;
        }
        if (!m_valueType.isUniqueIndexable()) {
            msg.append("expression of type " + m_valueType.getName());
            return false;
        }
        return true;
    }

    /**
     * Traverse this expression tree.  Where we find a SelectSubqueryExpression, wrap it
     * in a ScalarValueExpression if its parent is not one of:
     * - comparison (=, !=, <, etc)
     * - operator exists
     * @param expr   - the expression that may contain subqueries that need to be wrapped
     * @return the expression with subqueries wrapped where needed
     */
    public AbstractExpression wrapScalarSubqueries(AbstractExpression parentExpr) {

        // Bottom-up recursion.  Proceed to the children first.
        AbstractExpression leftChild = getLeft();
        if (leftChild != null) {
            AbstractExpression newLeft = leftChild.wrapScalarSubqueries(this);
            if (newLeft != leftChild) {
                setLeft(newLeft);
            }
        }

        AbstractExpression rightChild = getRight();
        if (rightChild != null) {
            AbstractExpression newRight = rightChild.wrapScalarSubqueries(this);
            if (newRight != rightChild) {
                setRight(newRight);
            }
        }

        // args may also contain subqueries.
        List<AbstractExpression> args = getArgs();
        if (args != null) {
            for (int i = 0; i < args.size(); ++i) {
                AbstractExpression arg = args.get(i);
                AbstractExpression newArg = arg.wrapScalarSubqueries(this);
                if (newArg != arg) {
                    setArgAtIndex(i, newArg);
                }
            }
        }
        return this;
    }

}
