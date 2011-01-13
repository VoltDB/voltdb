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

package org.voltdb.expressions;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.types.*;

/**
 *
 */
public abstract class AbstractExpression implements JSONString, Cloneable {

    public enum Members {
        TYPE,
        LEFT,
        RIGHT,
        VALUE_TYPE,
        VALUE_SIZE;
    }

    protected String m_id;
    protected ExpressionType m_type;
    protected AbstractExpression m_left = null;
    protected AbstractExpression m_right = null;
    protected VoltType m_valueType = null;
    protected int m_valueSize = 0;

    // used by the planner internally (not needed in the EE)
    public boolean m_isJoiningClause = false;

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
    public Object clone() throws CloneNotSupportedException {
        AbstractExpression clone = (AbstractExpression)super.clone();
        clone.m_id = m_id;
        clone.m_isJoiningClause = m_isJoiningClause;
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

        // check that the presence, or lack thereof, of children is the same
        if ((expr.m_left == null) != (m_left == null))
            return false;
        if ((expr.m_right == null) != (m_right == null))
            return false;

        // check that the children identify themselves as the same
        if (expr.m_left != null)
            if (expr.m_left.equals(m_left) == false)
                return false;
        if (expr.m_right != null)
            if (expr.m_right.equals(m_right) == false)
                return false;

        if (m_type != expr.m_type)
            return false;

        // this abstract base class gets here if the children verify local members
        return true;
    }

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

        expr.loadFromJSONObject(obj, db);

        return expr;
    }
}
