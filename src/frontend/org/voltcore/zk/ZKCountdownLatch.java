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

package org.voltcore.zk;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A minimal countdown latch implementation that is not particularly scalable
 * nor good at cleaning up after itself. However it is fine for once per cluster type barriers
 * at startup although preferably it shouldn't be used in the common case where it would slow down unit
 * test startup.
 *
 * Initially created for a barrier after command logs are initialized and CL isn't on in most unit tests
 */
public class ZKCountdownLatch
{
    private final ZooKeeper m_zk;
    private final String m_path;
    private boolean countedDown = false;

    public ZKCountdownLatch(ZooKeeper zk, String path, int count) throws InterruptedException, KeeperException {
        m_zk = zk;
        m_path = path;

        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(count);
        ZKUtil.asyncMkdirs(zk, path, buf.array());
    }

    public void await() throws InterruptedException, KeeperException {
        if (countedDown) return;
        while (true) {
            ZKUtil.FutureWatcher fw = new ZKUtil.FutureWatcher();
            int count = ByteBuffer.wrap(m_zk.getData(m_path, fw, null)).getInt();
            if (count > 0) {
                fw.get();
                continue;
            }
            countedDown = true;
            return;
        }
    }

    public void countDown() throws InterruptedException, KeeperException {
        countDown(false);
    }

    public void countDown(boolean expectNonZeroCount) throws InterruptedException, KeeperException {
        while (true) {
            Stat stat = new Stat();
            ByteBuffer buf = ByteBuffer.wrap(m_zk.getData(m_path, false, stat));
            int count = buf.getInt();
            if (count == 0) {
                countedDown = true;
                if (expectNonZeroCount) {
                    throw new RuntimeException("Count should be > 0");
                }
                return;
            }

            count--;

            //Save a few milliseconds
            if (count == 0) countedDown = true;

            buf.clear();
            buf.putInt(count);
            try {
                m_zk.setData(m_path, buf.array(), stat.getVersion());
            } catch (KeeperException.BadVersionException e) {
                continue;
            }
            return;
        }
    }
}
