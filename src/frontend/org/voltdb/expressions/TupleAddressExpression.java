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
public class TupleAddressExpression extends AbstractValueExpression {
    public TupleAddressExpression() {
        super(ExpressionType.VALUE_TUPLE_ADDRESS);
        m_valueType = VoltType.BIGINT;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    @Override
    public boolean equals(Object obj) {
        // This is slightly over-permissive
        // -- it assumes that the (implied) target tables are the same whenever equality is
        // being checked within the context of identical expressions.
        // If that ever matters, add some kind of table identifier attribute to this class.
        return (obj instanceof TupleAddressExpression);
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        // No attributes -- nothing to load.
    }
}
