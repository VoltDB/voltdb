/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.types.ExpressionType;


/**
 *
 */
public class TupleAddressExpression extends AbstractValueExpression {
    public TupleAddressExpression() {
        super(ExpressionType.VALUE_TUPLE_ADDRESS);
        m_valueType = VoltType.BIGINT;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    public TupleAddressExpression(AbstractExpression left, AbstractExpression right) {
        super(ExpressionType.VALUE_TUPLE_ADDRESS, right, left);
        m_type = ExpressionType.VALUE_TUPLE_ADDRESS;
        m_valueType = VoltType.BIGINT;
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {}
}
