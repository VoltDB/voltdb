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

public class SysprocFaultInjection {

    private enum FaultType {
        None,
        Exception,
        Crash,
    }

    private enum FaultInjectionType {
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
    private FaultInjectionType m_injectionType;
    private Exception m_exception;
    private boolean m_once;
    private byte[] m_bitmap;

    public SysprocFaultInjection(FaultType faultType, FaultInjectionType injection, boolean once, Exception exception) {
        m_type = faultType;
        m_injectionType = injection;
        m_once = once;
        m_exception = exception;
        m_bitmap = new byte[FaultInjectionType.values().length];
        Arrays.fill(m_bitmap, (byte)0);
    }

    /**
     * Check if there is any injected fault, trigger the fault if it is set.
     * @param injection The injected fault, can be null.
     * @throws Exception
     */
    public static void check(SysprocFaultInjection injection) throws Exception {
        if (injection == null) return;
        injection.checkInjectedFault();
    }

    public void checkInjectedFault() throws Exception {
        if (m_once) {
            injectOnce();
        } else {
            inject();
        }
    }

    private void inject() throws Exception {
        if (m_type == FaultType.Crash) {
            VoltDB.crashLocalVoltDB("Kill the server due to injected fault: " + m_injectionType);
        } else if (m_type == FaultType.Exception) {
            assert (m_exception != null);
            throw m_exception;
        }
    }

    private void injectOnce() throws Exception {
        if (m_type == FaultType.Crash) {
            if (m_bitmap[m_injectionType.ordinal()] == (byte)0) {
                m_bitmap[m_injectionType.ordinal()] = (byte)1;
                VoltDB.crashLocalVoltDB("Kill the server due to injected fault: " + m_injectionType);
            }
        } else if (m_type == FaultType.Exception) {
            assert (m_exception != null);
            if (m_bitmap[m_injectionType.ordinal()] == (byte)0) {
                m_bitmap[m_injectionType.ordinal()] = (byte)1;
                throw m_exception;
            }
        }
    }
}
