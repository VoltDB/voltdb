/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.util.Arrays;
import java.util.List;

public class SysprocFaultInjection {

    public enum FaultType {
        /* SwapTablesCore */
        SwapTablesCoreRun,
        SwapTablesCoreFragment,
        /* UpdateSettings */
        UpdateSettingsRun,
        UpdateSettingsFirstFragment,
        UpdateSettingsFirstFragmentAggregate,
        UpdateSettingsSecondFragment,
        UpdateSettingsSecondFragmentAggregate,
        /* LoadMultipartitionTable */
        LoadMultipartitionTableRun,
        LoadMultipartitionTableFragment,
        LoadMultipartitionTableAggregate,
        /* BalancePartitions */
        BalancePartitionsRun,
        BalancePartitionsFirstFragment,
        BalancePartitionsFirstFragmentAggregate,
        BalancePartitionsSendDataFragment,
        BalancePartitionsSendDataFragmentAggregate,
        BalancePartitionsReceiveDataFragment,
        BalancePartitionsClearIndexFragment,
        BalancePartitionsClearIndexFragmentAggregate,
    }

    private FaultType m_type;
    private boolean m_once;
    private byte[] m_bitmap;
    // The host that hits the fault
    private int m_hostId;

    public SysprocFaultInjection(FaultType faultType, int hostId, boolean once) {
        m_type = faultType;
        m_once = once;
        m_hostId = hostId;
        m_bitmap = new byte[FaultType.values().length];
        Arrays.fill(m_bitmap, (byte)0);
    }

    /**
     * For test only. Check if there is any injected fault, trigger the fault if it is set.
     * @param type The injected fault type.
     * @throws Exception
     */
    public static void check(FaultType type) {
        List<SysprocFaultInjection> faults = VoltDB.instance().getInjectedFault();
        if (faults == null) {
            return;
        }
        int hostId = VoltDB.instance().getHostMessenger().getHostId();
        for (SysprocFaultInjection fault : faults) {
            if (type == fault.m_type && hostId == fault.m_hostId) {
                fault.checkInjectedFault();
            }
        }
    }

    public void checkInjectedFault() {
        if (m_once) {
            injectFaultOnce();
        } else {
            injectFault();
        }
    }

    private void injectFault() {
        VoltDB.crashLocalVoltDB("Kill the server due to the injected fault: " + m_type);
    }

    private void injectFaultOnce() {
        if (m_bitmap[m_type.ordinal()] == 0) {
            m_bitmap[m_type.ordinal()] = 1;
            VoltDB.crashLocalVoltDB("Kill the server due to the injected fault: " + m_type);
        }
    }

    // Valid argument: [FaultType]:[hostId]:[once]
    // Example: SwapTablesCoreRun:1:false
    //          BalancePartitionsFirstFragment:0:true
    public static SysprocFaultInjection parse(String arg) {
        String[] args = arg.split(":");
        if (args.length != 3) {
            return null;
        }
        String typeStr = args[0];
        int hostId = Integer.valueOf(args[1]);
        boolean once = Boolean.valueOf(args[2]);
        FaultType faultType;
        try {
            faultType = FaultType.valueOf(FaultType.class, typeStr);
        } catch (IllegalArgumentException e) {
            return null;
        }

        return new SysprocFaultInjection(faultType, hostId, once);
    }

    public String toString() {
        return m_type.toString() + ":" + m_hostId + ":" + m_once;
    }
}
