/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

/**
 *
 * @author akhanzode
 */
public class Invocation {
    private final String m_proc;
    private final Object[] m_params;

    public Invocation(String proc, Object[] params) {
        m_proc = proc;
        m_params = params;
    }

    public String getProcedure() {
        return m_proc;
    }
    public Object [] getParams() {
        return m_params;
    }
}
