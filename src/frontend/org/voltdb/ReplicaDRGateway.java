/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltdb.iv2.Cartographer;

// Interface through which the outside world can interact with the replica-side
// of DR. Currently, there's not much to do here, since the subsystem is
// largely self-contained
public abstract class ReplicaDRGateway extends Thread implements Promotable {

    public abstract void initializeReplicaCluster(Cartographer cartographer);

    public abstract void updateCatalog(CatalogContext catalog);

    public abstract void shutdown();

}
