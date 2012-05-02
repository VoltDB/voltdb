package org.voltdb.iv2;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ParameterSet;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.utils.LogKeys;

public class FragmentTask extends SiteTasker
{
    private static final VoltLogger execLog = new VoltLogger("EXEC");
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    final Iv2MPTransactionState m_txn;
    final InitiatorMailbox m_initiator;
    final FragmentTaskMessage m_task;

    FragmentTask(InitiatorMailbox initiator, long localTxnId,
                 FragmentTaskMessage message)
    {
        m_initiator = initiator;
        m_txn = new Iv2MPTransactionState(localTxnId, message);
        m_task = message;
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        if (!m_txn.isReadOnly()) {
            m_txn.setBeginUndoToken(siteConnection.getLatestUndoToken());
        }
        final FragmentResponseMessage response = processFragmentTask(siteConnection);
        // completion?
        m_initiator.deliver(response);
    }

    @Override
    public int priority()
    {
        return 0;
    }

    // Cut and pasted from ExecutionSite processFragmentTask(), then
    // modifed to work in the new world
    public FragmentResponseMessage processFragmentTask(SiteProcedureConnection siteConnection)
    {
        // IZZY: actually need the "executor" HSId these days?
        final FragmentResponseMessage currentFragResponse =
            new FragmentResponseMessage(m_task, m_initiator.getHSId());
        currentFragResponse.setStatus(FragmentResponseMessage.SUCCESS, null);

        if (m_task.isSysProcTask())
        {
            throw new RuntimeException("IV2 unable to handle system procedures yet");
        }

        for (int frag = 0; frag < m_task.getFragmentCount(); frag++)
        {
            final long fragmentId = m_task.getFragmentId(frag);
            final int outputDepId = m_task.getOutputDepId(frag);

            // this is a horrible performance hack, and can be removed with small changes
            // to the ee interface layer.. (rtb: not sure what 'this' encompasses...)
            // (izzy: still not sure what 'this' encompasses...)
            ParameterSet params = null;
            final ByteBuffer paramData = m_task.getParameterDataForFragment(frag);
            if (paramData != null) {
                final FastDeserializer fds = new FastDeserializer(paramData);
                try {
                    params = fds.readObject(ParameterSet.class);
                }
                catch (final IOException e) {
                    hostLog.l7dlog(Level.FATAL,
                                   LogKeys.host_ExecutionSite_FailedDeserializingParamsForFragmentTask.name(), e);
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                }
            }
            else {
                params = new ParameterSet();
            }

            final int inputDepId = m_task.getOnlyInputDepId(frag);

            /*
             * Currently the error path when executing plan fragments
             * does not adequately distinguish between fatal errors and
             * abort type errors that should result in a roll back.
             * Assume that it is ninja: succeeds or doesn't return.
             * No roll back support.
             */
            try {
                final VoltTable dependency =
                    siteConnection.executePlanFragment(fragmentId,
                                                       inputDepId,
                                                       params,
                                                       m_txn.txnId,
                                                       m_txn.isReadOnly());
                if (hostLog.isTraceEnabled()) {
                    hostLog.l7dlog(Level.TRACE,
                       LogKeys.org_voltdb_ExecutionSite_SendingDependency.name(),
                       new Object[] { outputDepId }, null);
                }
                currentFragResponse.addDependency(outputDepId, dependency);
            } catch (final EEException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                break;
            } catch (final SQLException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                break;
            }
        }
        return currentFragResponse;
    }
}
