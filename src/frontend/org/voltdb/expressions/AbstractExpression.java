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

package org.voltdb.expressions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
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
        ARGS,
    }

    protected String m_id;
    protected ExpressionType m_type;
    protected AbstractExpression m_left = null;
    protected AbstractExpression m_right = null;
    protected List<AbstractExpression> m_args = null; // Never includes left and right "operator args".

    public void setArgs(List<AbstractExpression> arguments) {
        m_args = arguments;
    }

    protected VoltType m_valueType = null;
    protected int m_valueSize = 0;

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
        stringer.key(Members.TYPE.name()).value(m_type.toString());
        stringer.key(Members.VALUE_TYPE.name()).value(m_valueType == null ? null : m_valueType.name());
        stringer.key(Members.VALUE_SIZE.name()).value(m_valueSize);

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

    abstract protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException;

    public static AbstractExpression fromJSONObject(JSONObject obj, Database db) throws JSONException {
        ExpressionType type = ExpressionType.valueOf(obj.getString(Members.TYPE.name()));
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

        expr.m_valueType = VoltType.typeFromString(obj.getString(Members.VALUE_TYPE.name()));
        expr.m_valueSize = obj.getInt(Members.VALUE_SIZE.name());

        JSONObject leftObject = null;
        if (!obj.isNull(Members.LEFT.name())) {
            try {
                leftObject = obj.getJSONObject(Members.LEFT.name());
            } catch (JSONException e) {
                //ok for it not to be there.
            }
        }

        if (leftObject != null) {
            expr.m_left = AbstractExpression.fromJSONObject(obj.getJSONObject(Members.LEFT.name()), db);
        }

        JSONObject rightObject = null;
        if (!obj.isNull(Members.RIGHT.name())) {
            try {
                rightObject = obj.getJSONObject(Members.RIGHT.name());
            } catch (JSONException e) {
                //ok for it not to be there.
            }
        }

        if (rightObject != null) {
            expr.m_right = AbstractExpression.fromJSONObject(obj.getJSONObject(Members.RIGHT.name()), db);
        }

        if (!obj.isNull(Members.ARGS.name())) {
            ArrayList<AbstractExpression> arguments = new ArrayList<AbstractExpression>();
            try {
                JSONArray argsObject = obj.getJSONArray(Members.ARGS.name());
                if (argsObject != null) {
                    for (int i = 0; i < argsObject.length(); i++) {
                        JSONObject argObject = argsObject.getJSONObject(i);
                        if (argObject != null) {
                            arguments.add(AbstractExpression.fromJSONObject(argObject, db));
                        }
                    }
                }
            } catch (JSONException e) {
                // ok for it not to be there?
            }
            expr.setArgs(arguments);
        }

        expr.loadFromJSONObject(obj, db);

        return expr;
    }

    public static List<AbstractExpression> fromJSONArrayString(String jsontext, Database db) throws JSONException {
        JSONArray jarray = new JSONArray(jsontext);
        return loadFromJSONArray(null, jarray, db);
    }

    public static List<AbstractExpression> loadFromJSONArray(List<AbstractExpression> starter, JSONObject parent, String key, Database db) throws JSONException {
        if( parent.isNull( key ) ) {
            return starter;
        }
        JSONArray jarray = parent.getJSONArray( key );
        return loadFromJSONArray(starter, jarray, db);
    }

    public static List<AbstractExpression> loadFromJSONArray(List<AbstractExpression> starter, JSONArray jarray, Database db) throws JSONException {
        List<AbstractExpression> result = starter;
        if (result == null) {
            result = new ArrayList<AbstractExpression>();
        }
        int size = jarray.length();
        for( int i = 0 ; i < size; i++ ) {
            JSONObject tempjobj = jarray.getJSONObject( i );
            result.add(fromJSONObject(tempjobj, db));
        }
        return result;
    }

    /**
     *
     * @param aggTableIndexMap
     * @return
     */
    public AbstractExpression replaceWithTVE(
            HashMap <AbstractExpression, Integer> aggTableIndexMap,
            HashMap <AbstractExpression, String> exprToAliasMap)
    {
        Integer ii = aggTableIndexMap.get(this);
        if (ii != null) {
            TupleValueExpression tve = new TupleValueExpression();
            tve.setValueType(getValueType());
            tve.setValueSize(getValueSize());
            tve.setColumnIndex(ii);
            tve.setColumnName("");
            tve.setColumnAlias(exprToAliasMap.get(this));
            tve.setTableName("VOLT_TEMP_TABLE");
            return tve;
        }

        AbstractExpression lnode = null, rnode = null;
        ArrayList<AbstractExpression> newArgs = new ArrayList<AbstractExpression>();
        if (m_left != null) {
            lnode = m_left.replaceWithTVE(aggTableIndexMap, exprToAliasMap);
        }
        if (m_right != null) {
            rnode = m_right.replaceWithTVE(aggTableIndexMap, exprToAliasMap);
        }

        boolean changed = false;
        if (m_args != null) {
            for (int jj = 0; jj < m_args.size(); jj++) {
                AbstractExpression exp = m_args.get(jj).replaceWithTVE(aggTableIndexMap, exprToAliasMap);
                newArgs.set(jj, exp);
                if (exp != m_args.get(jj)) {
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
     * Helper function to patch up NUMERIC or untyped constants and parameters and
     * the functions and operators that they parameterize,
     * especially when they need to match an expected INSERT/UPDATE column
     * or to match a parameterized function that HSQL or other planner
     * processing has determined the return type for.
     * This default implementation is currently a no-op which is fine for most purposes --
     * 99% of the time, we are trying to refine a type to itself.
     * TODO: we might want to enable asserts when we fall out of well-known non-no-op cases.
     * We don't just go ahead and arbitrarily change any AbstractExpression's value type on the assumption
     * that AbstractExpression classes usually either have immutable value types
     * (hard-coded or easily determined up front by HSQL),
     * OR they have their own specific restrictions or side effects -- and so their own refineValueType method.
     */
    public void refineValueType(VoltType columnType) {
        if (columnType.equals(m_valueType)) {
            return; // HSQL already initialized the expression to have the refined type.
        }
        //TODO: For added safety, we MAY want to (re)enable this general assert
        // OR the one after we give a pass for the "generic types". See the comment below.
        // assert(false);
        if ((m_valueType != null) && (m_valueType != VoltType.NUMERIC)) {
            // This code path leaves a generic type (null or NUMERIC) in the expression tree rather than assume that
            // it's safe to change the value type of an arbitrary AbstractExpression.
            // The EE MAY complain about it later.
            // There may be special cases where we want to assert(false) rather than waiting for the EE to complain.
            // or even special cases where we want to go ahead and switch the type (scary!).
            return;
        }
        // A request to switch an arbitrary AbstractExpression from the specific value type that HSQL assigned to it
        // to a different specific value type that SEEMS to be called for by the usage (target column) is a hard thing.
        // It seems equally dangerous to ignore the HSQL type OR the target type.
        // Maybe this will never occur because HSQL is so smart (and we keep it that way),
        // or maybe the type difference won't really matter to the EE because it's so flexible.
        // Or maybe some of each -- this no-op behavior assumes something like that.
        // BUT maybe it's just always wrong to be trying any such thing, so we should assert(false).
        // Or maybe there need to be special cases.
        // The sad news is that when this assert first went live, it just caused quiet thread death and hanging rather
        // than an identifiable assert, even when run in the debugger.
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

    /** Associate underlying TupleValueExpressions with columns in the schema
     * and propagate the type implications to parent expressions.
     */
    public void resolveForDB(Database db) {
        resolveChildrenForDB(db);
    }

    /** Do the recursive part of resolveForDB as required for tree-structured expession types. */
    protected final void resolveChildrenForDB(Database db) {
        if (m_left != null)
            m_left.resolveForDB(db);
        if (m_right != null)
            m_right.resolveForDB(db);
        if (m_args != null) {
            for (AbstractExpression argument : m_args) {
                argument.resolveForDB(db);
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
