/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.elt.processors;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.elt.ELTDataBlock;
import org.voltdb.elt.ELTDataProcessor;

/**
 * A loader that does no work but does periodically write how much data has been ELed in the past ten seconds
 *
 */
public class NullProcessor implements ELTDataProcessor, Runnable, BlockingQueue<ELTDataBlock> {

    private final Thread m_thread = new Thread(this);

    private final AtomicLong m_bytesQueued = new AtomicLong(0);

    public volatile boolean m_shouldContinue = true;

    private Logger m_logger;

    @Override
    public void addHost(final String host, final String port, final String database,
            final String username, final String password) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addTable(final String database, final String tableName, final int tableId) {
        // TODO Auto-generated method stub

    }

    @Override
    public void readyForData() {
        m_logger.info("Processor ready for data.");
        m_thread.start();
    }

    @Override
    public boolean process(final ELTDataBlock block) {
        offer(block);
        return true;
    }

    @Override
    public void run() {
        System.err.println("Null loader started");
        while (m_shouldContinue) {
            final long started = System.currentTimeMillis();
            try {
                Thread.sleep(10000);
            } catch (final InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            final long bytesQueued = m_bytesQueued.getAndSet(0);
            final long kilobytesQueued = bytesQueued / 1024;
            final double megabytesQueued = kilobytesQueued / 1024.0;
            final long finished = System.currentTimeMillis();
            final double seconds = (finished - started) / 1000.0;
            System.err.println("Queued " + megabytesQueued + " mb" + " in " + seconds + " seconds aka " + (megabytesQueued / seconds) + " mb/sec");
        }
    }

    @Override
    public boolean add(final ELTDataBlock e) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean contains(final Object o) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int drainTo(final Collection<? super ELTDataBlock> c) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int drainTo(final Collection<? super ELTDataBlock> c, final int maxElements) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean offer(final ELTDataBlock e) {
        if (e.isStopMessage()) {
            m_shouldContinue = false;
            return true;
        }
        m_bytesQueued.addAndGet(e.m_data.b.remaining());
        e.m_data.discard();
       return true;
    }

    @Override
    public boolean offer(final ELTDataBlock e, final long timeout, final TimeUnit unit)
            throws InterruptedException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ELTDataBlock poll(final long timeout, final TimeUnit unit)
            throws InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void put(final ELTDataBlock e) throws InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public int remainingCapacity() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean remove(final Object o) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ELTDataBlock take() throws InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ELTDataBlock element() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ELTDataBlock peek() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ELTDataBlock poll() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ELTDataBlock remove() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean addAll(final Collection<? extends ELTDataBlock> c) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void clear() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isEmpty() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Iterator<ELTDataBlock> iterator() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int size() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Object[] toArray() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T[] toArray(final T[] a) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isIdle() {
        return true;
    }

    @Override
    public void addLogger(Logger logger) {
        m_logger = logger;
    }

}
