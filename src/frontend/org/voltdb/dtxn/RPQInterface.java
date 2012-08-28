package org.voltdb.dtxn;

public interface RPQInterface {
    /**
     * Only return transaction state objects that are ready to run.
     */
    public OrderableTransaction poll();

    public boolean add(OrderableTransaction txnState);

    public long noteTransactionRecievedAndReturnLastSeen(long initiatorHSId, long txnId,
            boolean isHeartbeat, long lastSafeTxnIdFromInitiator);

    public Long getNewestSafeTransactionForInitiator(long initiator);

    public void gotFaultForInitiator(long initiatorId);

    public int ensureInitiatorIsKnown(long initiatorId);

    public boolean isEmpty();
}
