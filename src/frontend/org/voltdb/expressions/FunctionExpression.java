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
import org.voltdb.catalog.Database;

public class FunctionExpression extends AbstractExpression {
    public FunctionExpression() {
        //
        // This is needed for serialization
        //
        super();
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        //
        // Validate that there are no children other than the argument list (mandatory even if empty)
        //
        if (m_left != null) {
            throw new Exception("ERROR: The left child expression '" + m_left + "' for '" + this + "' is not NULL");
        }

        if (m_right != null) {
            throw new Exception("ERROR: The right child expression '" + m_right + "' for '" + this + "' is not NULL");
        }

        if (m_args == null) {
            throw new Exception("ERROR: The function argument list for '" + this + "' is NULL");
        }

    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        FunctionExpression clone = (FunctionExpression)super.clone();
        return clone;
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {}
}
