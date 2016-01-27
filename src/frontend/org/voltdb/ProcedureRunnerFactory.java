/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb;

import org.voltdb.catalog.Procedure;

public class ProcedureRunnerFactory {

    protected SiteProcedureConnection m_site;
    protected SystemProcedureExecutionContext m_context;

    public void configure(SiteProcedureConnection site,
            SystemProcedureExecutionContext context) {
        m_site = site;
        m_context = context;
    }

    public ProcedureRunner create(
            VoltProcedure procedure,
            Procedure catProc,
            CatalogSpecificPlanner csp) {
        return new ProcedureRunner(procedure, m_site, m_context, catProc, csp);
    }

}
