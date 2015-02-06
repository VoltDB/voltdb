/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientInterfaceHandleManager;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;

public class Iv2Trace
{
    private static VoltLogger iv2log = new VoltLogger("IV2TRACE");
    private static VoltLogger iv2queuelog = new VoltLogger("IV2QUEUETRACE");
    public static final boolean IV2_TRACE_ENABLED;
    public static final boolean IV2_QUEUE_TRACE_ENABLED;
    static {
        IV2_TRACE_ENABLED = iv2log.isTraceEnabled();
        IV2_QUEUE_TRACE_ENABLED = iv2queuelog.isTraceEnabled();
    }

    public static void logTopology(long leaderHSId, List<Long> replicas, int partitionId)
    {
        if (IV2_TRACE_ENABLED) {
            String logmsg = "topology partition %d leader %s replicas (%s)";
            iv2log.trace(String.format(logmsg, partitionId, CoreUtils.hsIdToString(leaderHSId),
                    CoreUtils.hsIdCollectionToString(replicas)));
        }
    }

    public static void logCreateTransaction(Iv2InitiateTaskMessage msg)
    {
        if (IV2_TRACE_ENABLED) {
            String logmsg = "createTxn %s ciHandle %s initHSId %s proc %s";
            iv2log.trace(String.format(logmsg, CoreUtils.hsIdToString(msg.getInitiatorHSId()),
                        ClientInterfaceHandleManager.handleToString(msg.getClientInterfaceHandle()),
                        CoreUtils.hsIdToString(msg.getCoordinatorHSId()),
                        msg.getStoredProcedureInvocation().getProcName()));
        }
    }

    public static void logFinishTransaction(InitiateResponseMessage msg, long localHSId)
    {
        if (IV2_TRACE_ENABLED) {
            String logmsg = "finishTxn %s ciHandle %s initHSId %s status %s";
            iv2log.trace(String.format(logmsg, CoreUtils.hsIdToString(localHSId),
                        ClientInterfaceHandleManager.handleToString(msg.getClientInterfaceHandle()),
                        CoreUtils.hsIdToString(msg.getCoordinatorHSId()),
                        respStatusToString(msg.getClientResponseData().getStatus())));

        }
    }

    private static String txnIdToString(long txnId)
    {
        if (txnId == Long.MIN_VALUE) {
            return "UNUSED";
        }
        else {
            return TxnEgo.txnIdToString(txnId);
        }
    }

    private static String respStatusToString(byte status)
    {
        switch(status) {
            case ClientResponse.SUCCESS:
                return "SUCCESS";
            case ClientResponse.USER_ABORT:
                return "USER_ABORT";
            case ClientResponse.GRACEFUL_FAILURE:
                return "GRACEFUL_FAILURE";
            case ClientResponse.UNEXPECTED_FAILURE:
                return "UNEXPECTED_FAILURE";
            case ClientResponse.CONNECTION_LOST:
                return "CONNECTION_LOST";
            case ClientResponse.SERVER_UNAVAILABLE:
                return "SERVER_UNAVAILABLE";
            case ClientResponse.CONNECTION_TIMEOUT:
                return "CONNECTION_TIMEOUT";
        }
        return "UNKNOWN_CLIENT_STATUS";
    }

    private static String fragStatusToString(byte status)
    {
        if (status == FragmentResponseMessage.SUCCESS) {
            return "SUCCESS";
        }
        else if (status == FragmentResponseMessage.USER_ERROR) {
            return "USER_ERROR";
        }
        else if (status == FragmentResponseMessage.UNEXPECTED_ERROR) {
            return "UNEXPECTED_ERROR";
        }
        return "UNKNOWN_STATUS_CODE!";
    }

    public static void logInitiatorRxMsg(VoltMessage msg, long localHSId)
    {
        if (IV2_TRACE_ENABLED) {
            if (msg instanceof InitiateResponseMessage) {
                InitiateResponseMessage iresp = (InitiateResponseMessage)msg;
                String logmsg = "rxInitRsp %s from %s ciHandle %s txnId %s spHandle %s status %s";
                iv2log.trace(String.format(logmsg, CoreUtils.hsIdToString(localHSId),
                            CoreUtils.hsIdToString(iresp.m_sourceHSId),
                            ClientInterfaceHandleManager.handleToString(iresp.getClientInterfaceHandle()),
                            txnIdToString(iresp.getTxnId()),
                            txnIdToString(iresp.getSpHandle()),
                            respStatusToString(iresp.getClientResponseData().getStatus())));
            }
            else if (msg instanceof FragmentResponseMessage) {
                FragmentResponseMessage fresp = (FragmentResponseMessage)msg;
                String logmsg = "rxFragRsp %s from %s txnId %s spHandle %s status %s";
                iv2log.trace(String.format(logmsg, CoreUtils.hsIdToString(localHSId),
                            CoreUtils.hsIdToString(fresp.m_sourceHSId),
                            txnIdToString(fresp.getTxnId()),
                            txnIdToString(fresp.getSpHandle()),
                            fragStatusToString(fresp.getStatusCode())));
            }
        }
    }

    public static void logIv2InitiateTaskMessage(Iv2InitiateTaskMessage itask, long localHSId, long txnid,
            long spHandle)
    {
        if (IV2_TRACE_ENABLED) {
            String logmsg = "rxInitMsg %s from %s ciHandle %s txnId %s spHandle %s trunc %s";
            if (itask.getTxnId() != Long.MIN_VALUE && itask.getTxnId() != txnid) {
                iv2log.error("Iv2InitiateTaskMessage TXN ID conflict.  Message: " + itask.getTxnId() +
                        ", locally held: " + txnid);
            }
            if (itask.getSpHandle() != Long.MIN_VALUE && itask.getSpHandle() != spHandle) {
                iv2log.error("Iv2InitiateTaskMessage SP HANDLE conflict.  Message: " + itask.getSpHandle() +
                        ", locally held: " + spHandle);
            }
            iv2log.trace(String.format(logmsg, CoreUtils.hsIdToString(localHSId),
                        CoreUtils.hsIdToString(itask.m_sourceHSId),
                        ClientInterfaceHandleManager.handleToString(itask.getClientInterfaceHandle()),
                        txnIdToString(txnid),
                        txnIdToString(spHandle),
                        txnIdToString(itask.getTruncationHandle())));
        }
    }

    public static void logIv2MultipartSentinel(MultiPartitionParticipantMessage message, long localHSId,
            long txnId)
    {
        if (IV2_TRACE_ENABLED) {
            String logmsg = "rxSntlMsg %s from %s txnId %s";
            iv2log.trace(String.format(logmsg, CoreUtils.hsIdToString(localHSId),
                        CoreUtils.hsIdToString(message.m_sourceHSId),
                        txnIdToString(txnId)));
        }
    }

    public static void logFragmentTaskMessage(FragmentTaskMessage ftask, long localHSId, long spHandle,
            boolean borrow)
    {
        if (IV2_TRACE_ENABLED) {
            String label = "rxFragMsg";
            if (borrow) {
                label = "rxBrrwMsg";
            }
            if (ftask.getSpHandle() != Long.MIN_VALUE && ftask.getSpHandle() != spHandle) {
                iv2log.error("FragmentTaskMessage SP HANDLE conflict.  Message: " + ftask.getSpHandle() +
                        ", locally held: " + spHandle);
            }
            String logmsg = "%s %s from %s txnId %s spHandle %s trunc %s readonly %s";
            iv2log.trace(String.format(logmsg, label, CoreUtils.hsIdToString(localHSId),
                        CoreUtils.hsIdToString(ftask.m_sourceHSId),
                        txnIdToString(ftask.getTxnId()),
                        txnIdToString(spHandle),
                        txnIdToString(ftask.getTruncationHandle()),
                        ftask.isReadOnly()));
        }
    }

    public static void logCompleteTransactionMessage(CompleteTransactionMessage ctask, long localHSId)
    {
        if (IV2_TRACE_ENABLED) {
            String logmsg = "rxCompMsg %s from %s txnId %s %s %s";
            iv2log.trace(String.format(logmsg, CoreUtils.hsIdToString(localHSId),
                        CoreUtils.hsIdToString(ctask.m_sourceHSId),
                        txnIdToString(ctask.getTxnId()),
                        ctask.isRollback() ? "ROLLBACK" : "COMMIT",
                        ctask.isRestart() ? "RESTART" : ""));
        }
    }

    public static void logTransactionTaskQueueOffer(TransactionTask task)
    {
        if (IV2_QUEUE_TRACE_ENABLED) {
            String logmsg = "txnQOffer txnId %s spHandle %s type %s";
            iv2queuelog.trace(String.format(logmsg, txnIdToString(task.getTxnId()),
                        txnIdToString(task.getSpHandle()),
                    task.m_txnState.isSinglePartition() ? "SP" : "MP"));
        }
    }

    public static void logSiteTaskerQueueOffer(TransactionTask task)
    {
        if (IV2_QUEUE_TRACE_ENABLED) {
            String logmsg = "tskQOffer txnId %s spHandle %s type %s";
            iv2queuelog.trace(String.format(logmsg, txnIdToString(task.getTxnId()),
                            txnIdToString(task.getSpHandle()),
                    task.m_txnState.isSinglePartition() ? "SP" : "MP"));
        }
    }
}
