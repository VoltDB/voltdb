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

package org.voltdb;

import org.voltdb.catalog.Procedure;

public class ProcedureRunnerFactory {

    protected SiteProcedureConnection m_site;
    protected SystemProcedureExecutionContext m_context;
    protected CatalogSpecificPlanner m_csp;

    public void configure(SiteProcedureConnection site,
            SystemProcedureExecutionContext context,
            CatalogSpecificPlanner csp) {
        m_site = site;
        m_context = context;
        m_csp = csp;
    }

    public ProcedureRunner create(
            VoltProcedure procedure,
            Procedure catProc) {
        return new ProcedureRunner(procedure, m_site, m_context, catProc, m_csp);
    }

}
