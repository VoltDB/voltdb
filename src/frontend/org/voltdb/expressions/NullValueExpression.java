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

package org.voltdb.expressions;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.types.ExpressionType;

/**
 *
 */
public class NullValueExpression extends AbstractValueExpression {
    public NullValueExpression() {
        super(ExpressionType.VALUE_NULL);
        setValueType(VoltType.NULL);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof NullValueExpression);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        NullValueExpression clone = (NullValueExpression)super.clone();
        return clone;
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        //
        // The output type must always be null!
        //
        if (m_valueType != VoltType.NULL) {
            throw new Exception("ERROR: The output ValueType for '" + this + "' is '" + m_valueType + "'");
        }
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {}
}
