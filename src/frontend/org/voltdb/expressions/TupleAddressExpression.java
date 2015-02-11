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

import org.voltdb.VoltType;
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
    public String explain(String impliedTableName) {
        return "tuple address";
    }

}
