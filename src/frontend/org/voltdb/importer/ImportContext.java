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

import java.util.Properties;

/**
 *
 * @author akhanzode
 */

public interface ImportContext {

    /**
     * This is called to configure properties. Just save or configure anything you want here.
     * The readyForData() will be called to actually start processing the data.
     * @param p properties specified in the deployment.
     */
    public void configure(Properties p);
    /**
     * Called when cluster is ready to ingest data.
     */
    public void readyForData();
    /**
     * Called when stopping the node so the importer will cleanup
     */
    public void stop();

    /**
     * Call this to get the ingested data passed to procedure.
     * @param ic
     * @param procName
     * @param fieldList
     * @return true if successfully accepted the work.
     */
    public boolean callProcedure(ImportContext ic, String procName, Object... fieldList);

    /**
     * This is the real handler dont need to call or extend anything
     * @param handler
     */
    public void setHandler(Object handler);

    /**
     * Give a friendly name for the importer.
     * @return
     */
    public String getName();

}
