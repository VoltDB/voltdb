/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.fault;

import java.util.Set;

/**
 * The FaultHandler interface should be implemented by any object wishing to
 * receive callback notification when a fault occurs.
 */
public interface FaultHandler
{

    /**
     * This method will be called by the FaultDistributor when this object
     * is a registered handler for a fault.
     *
     * @param faults The faults which have occured but not handled by this handler.  Objects implementing this interface
     *              and registering interest in particular FaultTypes will
     *              need to downcast this VoltFault to the appropriate subclass. The set of faults
     *              includes all faults that have not been reported as handled by this fault handler since it was
     *              registered. Unhandled faults are reported once when they are first detected and repeatedly along
     *              with any new faults that are detected until the unhandled fault is reported as handled.
     */
    public void faultOccured(Set<VoltFault> faults);

    /**
     * This method will be called by the FaultDistributor when this object
     * is a registered handler for a fault.
     *
     * @param faults The faults which have occured and been cleared.  Objects implementing this interface
     *              and registering interest in particular FaultTypes will
     *              need to downcast this VoltFault to the appropriate subclass.
     */
    public void faultCleared(Set<VoltFault> faults);
}
