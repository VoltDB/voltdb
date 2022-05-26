/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.iv2;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Random;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.ParameterSet;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import com.google_voltpatches.common.collect.Sets;

// Generate a random stream of messages seen by an SPI.
public class RandomMsgGenerator
{
    TxnEgo m_mpiTxnEgo;
    UniqueIdGenerator m_mpiGenerator = new UniqueIdGenerator(MpInitiator.MP_INIT_PID, 0);
    Random m_rand;
    boolean m_mpInProgress = false;
    boolean m_currentMpReadOnly = false;
    static final double MPCHANCE = .25;
    static final double MPDONECHANCE = .75;
    static final double READCHANCE = .50;
    static final double MPRESTARTCHANCE = 0.20;
    static final double BINARYLOGCHANCE = 0.75;

    RandomMsgGenerator()
    {
        long seed = System.currentTimeMillis();
        System.out.println("Running with seed: " + seed);
        m_rand = new Random(seed);
        m_mpiTxnEgo = TxnEgo.makeZero(MpInitiator.MP_INIT_PID);
    }

    private Iv2InitiateTaskMessage makeIv2InitiateTaskMsg(boolean readOnly, boolean binaryLog, boolean isMp)
    {
        StoredProcedureInvocation spi = mock(StoredProcedureInvocation.class);
        ParameterSet ps = mock(ParameterSet.class);
        when(spi.getParams()).thenReturn(ps);
        if (binaryLog) {
            when(ps.toArray()).thenReturn(new Object[] {null, 0l, 0l, Long.MIN_VALUE, Long.MIN_VALUE, null});
            if (!isMp) {
                when(spi.getProcName()).thenReturn("@ApplyBinaryLogSP");
            } else {
                when(spi.getProcName()).thenReturn("@ApplyBinaryLogMP");
            }
        } else {
            when(ps.toArray()).thenReturn(new Object[] {null, 0l, 0l, Long.MIN_VALUE, null});
            when(spi.getProcName()).thenReturn("dummy");
        }
        Iv2InitiateTaskMessage msg =
                new Iv2InitiateTaskMessage(0l, 0l, 0l, Long.MIN_VALUE, 0l, readOnly, !isMp, false, spi,
                        0l, 0l, false);
        return msg;
    }

    private FragmentTaskMessage makeFragmentTaskMsg(boolean readOnly, boolean isFinal)
    {
        FragmentTaskMessage msg =
            new FragmentTaskMessage(0l, 0l, m_mpiTxnEgo.getTxnId(),
                    UniqueIdGenerator.makeIdFromComponents(System.currentTimeMillis(), 0, MpInitiator.MP_INIT_PID),
                    readOnly, isFinal, false, false, TransactionInfoBaseMessage.INITIAL_TIMESTAMP);
        return msg;
    }

    private CompleteTransactionMessage makeCompleteTxnMsg(boolean readOnly, boolean isRollback,
            boolean isRestart)
    {
        CompleteTransactionMessage msg =
            new CompleteTransactionMessage(0l, 0l, m_mpiTxnEgo.getTxnId(), readOnly, 0, isRollback,
                    false, isRestart, false, false, false, false);
        return msg;
    }

    public TransactionInfoBaseMessage generateRandomMessageInStream()
    {
        if (m_rand.nextDouble() > MPCHANCE) {
            double type = m_rand.nextDouble();
            boolean readOnly = (type < READCHANCE);
            boolean binaryLog = (!readOnly && type < BINARYLOGCHANCE);
            Iv2InitiateTaskMessage msg = makeIv2InitiateTaskMsg(readOnly, binaryLog, false);
            return msg;
        }
        else if (!m_mpInProgress) {
            double type = m_rand.nextDouble();
            m_currentMpReadOnly = (type < READCHANCE);
            boolean binaryLog = (!m_currentMpReadOnly && type < BINARYLOGCHANCE);
            FragmentTaskMessage msg = makeFragmentTaskMsg(m_currentMpReadOnly, false);
            msg.setStateForDurability(makeIv2InitiateTaskMsg(false, binaryLog, true), Sets.newHashSet(0, 1, 2));
            m_mpInProgress = true;
            return msg;
        }
        else if (m_rand.nextDouble() > MPDONECHANCE) {
            // generate another MP fragment
            FragmentTaskMessage msg = makeFragmentTaskMsg(m_currentMpReadOnly, false);
            return msg;
        }
        else {
            // generate MP complete
            // fake restarts
            boolean restart = (!m_currentMpReadOnly && m_rand.nextDouble() < MPRESTARTCHANCE);
            CompleteTransactionMessage msg = makeCompleteTxnMsg(m_currentMpReadOnly, restart, restart);
            if (!restart) {
                m_mpInProgress = false;
                m_mpiTxnEgo = m_mpiTxnEgo.makeNext();
            }
            return msg;
        }
    }
}
