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
package org.voltdb.fault;

/**
 * VoltFault contains information about a reported fault in a node.
 *
 * To add fault types other than UNKNOWN to the system, add a new value
 * to the FaultType enum below, and then, if necessary,
 * create a new subclass of VoltFault to carry any additional data related
 * to the fault which handlers might require.
 */
public class VoltFault
{
    public enum FaultType
    {
        UNKNOWN, SITE_FAILURE;
    }

    public VoltFault(FaultType faultType)
    {
        m_faultType = faultType;
    }

    public FaultType getFaultType()
    {
        return m_faultType;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("VoltFault: \n");
        sb.append("  FaultType: " + m_faultType.name());

        return sb.toString();
    }

    private FaultType m_faultType;
}
