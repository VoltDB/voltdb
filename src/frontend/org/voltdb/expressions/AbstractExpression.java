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

package org.voltdb.expressions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

/**
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
    protected boolean m_inBytes = false;

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
        } catch (CloneNotSupportedException e) {
            // umpossible
            return null;
        }
        clone.m_id = m_id;
        clone.m_type = m_type;
        clone.m_valueType = m_valueType;
        clone.m_valueSize = m_valueSize;
        clone.m_inBytes = m_inBytes;
        if (m_left != null)
        {
            AbstractExpression left_clone = (AbstractExpression)m_left.clone();
            clone.m_left = left_clone;
        }
        if (m_right != null)
        {
            AbstractExpression right_clone = (AbstractExpression)m_right.clone();
            clone.m_right = right_clone;
        }
        if (m_args != null) {
            clone.m_args = new ArrayList<AbstractExpression>();
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
        return "Expression: " + toJSONString();
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
            argBindings = new ArrayList<AbstractExpression>();
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
        List<AbstractExpression> result = new ArrayList<AbstractExpression>();
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

        if (m_args != null && m_args.size() > 0) {
            stringer.key(Members.ARGS.name()).array();
            for (AbstractExpression argument : m_args) {
                assert (argument instanceof JSONString);
                stringer.value(argument);
            }
            stringer.endArray();

        }
    }

    protected void loadFromJSONObject(JSONObject obj) throws JSONException { }
    protected void loadFromJSONObject(JSONObject obj, StmtTableScan tableScan) throws JSONException
    {
        loadFromJSONObject(obj);
    }

    /**
     * For TVEs, it is only serialized column index and table index. In order to match expression,
     * there needs more information to revert back the table name, table alisa and column name.
     * Without adding extra information, TVEs will only have column index and table index available.
     *
     *
     */

    /**
     * For TVEs, it is only serialized column index and table index. In order to match expression,
     * there needs more information to revert back the table name, table alisa and column name.
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
            ArrayList<AbstractExpression> arguments = new ArrayList<AbstractExpression>();
            loadFromJSONArray(arguments, jarray, tableScan);
            expr.setArgs(arguments);
        }

        expr.loadFromJSONObject(obj, tableScan);
        return expr;
    }

    public static List<AbstractExpression> fromJSONArrayString(String jsontext, StmtTableScan tableScan) throws JSONException
    {
        JSONArray jarray = new JSONArray(jsontext);
        List<AbstractExpression> result = new ArrayList<AbstractExpression>();
        loadFromJSONArray(result, jarray, tableScan);
        return result;
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
            tve.setTypeSizeBytes(getValueType(), getValueSize(), getInBytes());

            // To prevent pushdown of LIMIT when ORDER BY references an agg. ENG-3487.
            if (hasAnySubexpressionOfClass(AggregateExpression.class))
                tve.setHasAggregate(true);

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
            newArgs = new ArrayList<AbstractExpression>();
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
        if (getExpressionType() == ExpressionType.AGGREGATE_AVG) {
            AbstractExpression child = getLeft();
            AbstractExpression left = new AggregateExpression(ExpressionType.AGGREGATE_SUM);
            left.setLeft((AbstractExpression) child.clone());
            AbstractExpression right = new AggregateExpression(ExpressionType.AGGREGATE_COUNT);
            right.setLeft((AbstractExpression) child.clone());

            return new OperatorExpression(ExpressionType.OPERATOR_DIVIDE, left, right);
        }

        AbstractExpression lnode = null, rnode = null;
        ArrayList<AbstractExpression> newArgs = null;
        if (m_left != null) {
            lnode = m_left.replaceAVG();
        }
        if (m_right != null) {
            rnode = m_right.replaceAVG();
        }
        boolean changed = false;
        if (m_args != null) {
            newArgs = new ArrayList<AbstractExpression>();
            for (AbstractExpression expr: m_args) {
                AbstractExpression ex = expr.replaceAVG();
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

    public ArrayList<AbstractExpression> findBaseTVEs() {
        return findAllSubexpressionsOfType(ExpressionType.VALUE_TUPLE);
    }

    /**
     * @param type expression type to search for
     * @return a list of contained expressions that are of the desired type
     */
    public ArrayList<AbstractExpression> findAllSubexpressionsOfType(ExpressionType type) {
        ArrayList<AbstractExpression> collected = new ArrayList<AbstractExpression>();
        findAllSubexpressionsOfType_recurse(type, collected);
        return collected;
    }

    private void findAllSubexpressionsOfType_recurse(ExpressionType type,ArrayList<AbstractExpression> collected) {
        if (getExpressionType() == type)
            collected.add(this);

        if (m_left != null) {
            m_left.findAllSubexpressionsOfType_recurse(type, collected);
        }
        if (m_right != null) {
            m_right.findAllSubexpressionsOfType_recurse(type, collected);
        }
        if (m_args != null) {
        for (AbstractExpression argument : m_args) {
            argument.findAllSubexpressionsOfType_recurse(type, collected);
        }
    }
    }

    /**
     * @param type expression type to search for
     * @return whether the expression or any contained expressions are of the desired type
     */
    public boolean hasAnySubexpressionOfType(ExpressionType type) {
        if (getExpressionType() == type) {
            return true;
        }

        if (m_left != null && m_left.hasAnySubexpressionOfType(type)) {
            return true;
        }

        if (m_right != null && m_right.hasAnySubexpressionOfType(type)) {
            return true;
        }
        if (m_args != null) {
            for (AbstractExpression argument : m_args) {
                if (argument.hasAnySubexpressionOfType(type)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param type expression type to search for
     * @return a list of contained expressions that are of the desired type
     */
    public ArrayList<AbstractExpression> findAllSubexpressionsOfClass(Class< ? extends AbstractExpression> aeClass) {
        ArrayList<AbstractExpression> collected = new ArrayList<AbstractExpression>();
        findAllSubexpressionsOfClass_recurse(aeClass, collected);
        return collected;
    }

    public void findAllSubexpressionsOfClass_recurse(Class< ? extends AbstractExpression> aeClass,
            ArrayList<AbstractExpression> collected) {
        if (aeClass.isInstance(this))
            collected.add(this);

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
    public boolean hasAnySubexpressionOfClass(Class< ? extends AbstractExpression> aeClass) {
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

    /** Do the recursive part of finalizeValueTypes as requested. */
    protected final void finalizeChildValueTypes() {
        if (m_left != null)
            m_left.finalizeValueTypes();
        if (m_right != null)
            m_right.finalizeValueTypes();
        if (m_args != null) {
            for (AbstractExpression argument : m_args) {
                argument.finalizeValueTypes();
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

}
