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

package org.voltdb.importer;

import java.io.IOException;

/**
 *
 * @author akhanzode
 */
public class AdHocInvocation implements Invocation {

    private final Object[] m_sql;
    private final String m_proc;

    public AdHocInvocation(String proc, Object... sql) {
        m_sql = sql;
        m_proc = proc;
    }

    @Override
    public String getProcedure() {
        return m_proc;
    }

    @Override
    public Object[] getParams() throws IOException {
        return m_sql;
    }

}
