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
package org.voltdb.snmp;

import org.voltcore.messaging.HostMessenger;
import org.voltdb.compiler.deploymentfile.SnmpType;

/**
 *
 * @author akhanzode
 */
public interface SnmpTrapSender {

    // From deployment build the SNMP sender.
    public void initialize(SnmpType snmpType, HostMessenger hm, int clusterId);
    public void shutdown();

    // Update SNMP properties.
    public void notifyOfCatalogUpdate(SnmpType snmpType);

    // Methods to send specific SNMP traps, where enabled.
    // At the discretion of the implementation, a log message may
    // also be written.
    public void crash(String msg);
    public void hostDown(FaultLevel level, int hostId, String msg);
    public void hostUp(String msg);
    public void statistics(FaultFacility facility, String msg);
    public void resource(ThresholdType criteria, FaultFacility facility, long threshold, long actual, String msg);
    public void resourceClear(ThresholdType criteria, FaultFacility facility, long threshold, long actual, String msg);
    public void pause(String msg);
    public void resume(String msg);
    public void streamBlocked(String msg);
}
