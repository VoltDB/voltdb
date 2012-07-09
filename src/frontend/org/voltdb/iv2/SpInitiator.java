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

import org.voltcore.messaging.HostMessenger;

import org.voltdb.VoltZK;

/**
 * Subclass of Initiator to manage single-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public class SpInitiator extends BaseInitiator
{
    public SpInitiator(HostMessenger messenger, Integer partition)
    {
        super(VoltZK.iv2masters, messenger, partition,
                new SpScheduler(new SiteTaskerQueue()),
                "SP");
    }

    /**
     * SpInitiator has userdata that must be rejoined.
     */
    @Override
    public boolean isRejoinable()
    {
        return true;
    }
}
