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

package org.voltdb.iv2;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;

/**
 * Abstracts the top-level interface to create and configure an Iv2
 * MP or SP initiator.
 */
public interface Initiator
{
    /** Configure an Initiator and prepare it for work */
    public void configure(BackendTarget backend, String serializedCatalog,
                          CatalogContext catalogContext,
                          int kfactor, CatalogSpecificPlanner csp,
                          int numberOfPartitions,
                          boolean createForRejoin);

    /** Shutdown an Initiator and its sub-components. */
    public void shutdown();

    /** Ask for the HSId used to address this Initiator. */
    public long getInitiatorHSId();

    /** This initiator participates in rejoin processing */
    public boolean isRejoinable();
}
