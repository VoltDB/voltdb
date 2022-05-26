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
 * Dummy SnmpTrapSender
 * @author akhanzode
 */
public class DummySnmpTrapSender implements SnmpTrapSender {

    @Override
    public void initialize(SnmpType snmpType, HostMessenger hm, int clusterId) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void notifyOfCatalogUpdate(SnmpType snmpType) {
    }

    @Override
    public void crash(String msg) {
    }

    @Override
    public void hostDown(FaultLevel level, int hostId, String msg) {
    }

    @Override
    public void hostUp(String msg) {
    }

    @Override
    public void statistics(FaultFacility facility, String msg) {
    }

    @Override
    public void resource(ThresholdType criteria, FaultFacility facility,
            long threshold, long actual, String msg) {
    }

    @Override
    public void resourceClear(ThresholdType criteria, FaultFacility facility,
            long threshold, long actual, String msg) {
    }

    @Override
    public void pause(String msg) {
    }

    @Override
    public void resume(String msg) {
    }

    public void streamBlocked(String msg) {
    }
}
